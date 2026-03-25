// Copyright 2019 free5GC.org
//
// SPDX-License-Identifier: Apache-2.0
//

package producer

import (
	"context"
	"net/http"
	"strconv"

	"github.com/5GC-DEV/openapi-cdac"
	"github.com/5GC-DEV/openapi-cdac/Nudm_SubscriberDataManagement"
	Nudr "github.com/5GC-DEV/openapi-cdac/Nudr_DataRepository"
	"github.com/5GC-DEV/openapi-cdac/models"
	"github.com/antihax/optional"
	udm_context "github.com/omec-project/udm/context"
	"github.com/omec-project/udm/logger"
	stats "github.com/omec-project/udm/metrics"
	"github.com/omec-project/udm/util"
	"github.com/omec-project/util/httpwrapper"
)

const (
	queryPlmnID            = "plmn-id"
	querySupportedFeatures = "supported-features"
	metricAmData           = "am-data"
	metricIdTranslation    = "id-translation-result"
	metricSharedData       = "shared-data"
	metricSmData           = "sm-data"
	metricSmfSelectData    = "smf-select-data"
	metricSharedDataSubs   = "shared-data-subscriptions"
	metricSdmSubs          = "sdm-subscriptions"
	metricTraceData        = "trace-data"
	metricUeCtxInSmf       = "ue-context-in-smf-data"
	errQueryAmDataClose    = "QueryAmData response body cannot close: %+v"
)

func HandleGetAmDataRequest(request *httpwrapper.Request) *httpwrapper.Response {
	logger.SdmLog.Infoln("handle GetAmData")
	supi := request.Params["supi"]
	plmnID := request.Query.Get(queryPlmnID)
	supportedFeatures := request.Query.Get(querySupportedFeatures)
	response, problemDetails := getAmDataProcedure(supi, plmnID, supportedFeatures)
	if response != nil {
		stats.IncrementUdmSubscriberDataManagementStats("get", metricAmData, "SUCCESS")
		// status code is based on SPEC, and option headers
		return httpwrapper.NewResponse(http.StatusOK, nil, response)
	} else if problemDetails != nil {
		stats.IncrementUdmSubscriberDataManagementStats("get", metricAmData, "FAILURE")
		return httpwrapper.NewResponse(int(problemDetails.Status), nil, problemDetails)
	}
	problemDetails = &models.ProblemDetails{
		Status: http.StatusForbidden,
		Cause:  "UNSPECIFIED",
	}
	stats.IncrementUdmSubscriberDataManagementStats("get", metricAmData, "FAILURE")
	return httpwrapper.NewResponse(http.StatusForbidden, nil, problemDetails)
}

// GetAmDataProcedure
func getAmDataProcedure(supi string, plmnID string, supportedFeatures string) (
	response *models.AccessAndMobilitySubscriptionData, problemDetails *models.ProblemDetails,
) {
	var queryAmDataParamOpts Nudr.QueryAmDataParamOpts
	queryAmDataParamOpts.SupportedFeatures = optional.NewString(supportedFeatures)

	clientAPI, err := createUDMClientToUDR(supi)
	if err != nil {
		return nil, util.ProblemDetailsSystemFailure(err.Error())
	}

	accessAndMobilitySubscriptionDataResp, res, err := clientAPI.AccessAndMobilitySubscriptionDataDocumentApi.
		QueryAmData(context.Background(), supi, plmnID, &queryAmDataParamOpts)
	if err != nil {
		if res == nil {
			logger.SdmLog.Errorln(err.Error())
		} else if err.Error() != res.Status {
			logger.SdmLog.Errorln(err.Error())
		} else {
			problemDetails = &models.ProblemDetails{
				Status: int32(res.StatusCode),
				Cause:  err.(openapi.GenericOpenAPIError).Model().(models.ProblemDetails).Cause,
				Detail: err.Error(),
			}
			return nil, problemDetails
		}
	}
	defer func() {
		if rspCloseErr := res.Body.Close(); rspCloseErr != nil {
			logger.SdmLog.Errorf(errQueryAmDataClose, rspCloseErr)
		}
	}()

	if res.StatusCode == http.StatusOK {
		udmUe := udm_context.UDM_Self().NewUdmUe(supi)
		udmUe.SetAMSubsriptionData(&accessAndMobilitySubscriptionDataResp)
		return &accessAndMobilitySubscriptionDataResp, nil
	} else {
		problemDetails = &models.ProblemDetails{
			Status: http.StatusNotFound,
			Cause:  "DATA_NOT_FOUND",
		}
		return nil, problemDetails
	}
}

func HandleGetIdTranslationResultRequest(request *httpwrapper.Request) *httpwrapper.Response {
	logger.SdmLog.Infoln("handle GetIdTranslationResultRequest")
	gpsi := request.Params["gpsi"]
	response, problemDetails := getIdTranslationResultProcedure(gpsi)
	if response != nil {
		stats.IncrementUdmSubscriberDataManagementStats("get", metricIdTranslation, "SUCCESS")
		// status code is based on SPEC, and option headers
		return httpwrapper.NewResponse(http.StatusOK, nil, response)
	} else if problemDetails != nil {
		stats.IncrementUdmSubscriberDataManagementStats("get", metricIdTranslation, "FAILURE")
		return httpwrapper.NewResponse(int(problemDetails.Status), nil, problemDetails)
	}
	problemDetails = &models.ProblemDetails{
		Status: http.StatusForbidden,
		Cause:  "UNSPECIFIED",
	}
	stats.IncrementUdmSubscriberDataManagementStats("get", metricIdTranslation, "FAILURE")
	return httpwrapper.NewResponse(http.StatusForbidden, nil, problemDetails)
}

func getIdTranslationResultProcedure(gpsi string) (response *models.IdTranslationResult,
	problemDetails *models.ProblemDetails,
) {
	var idTranslationResult models.IdTranslationResult
	var getIdentityDataParamOpts Nudr.GetIdentityDataParamOpts

	clientAPI, err := createUDMClientToUDR(gpsi)
	if err != nil {
		return nil, util.ProblemDetailsSystemFailure(err.Error())
	}

	idTranslationResultResp, res, err := clientAPI.QueryIdentityDataBySUPIOrGPSIDocumentApi.GetIdentityData(
		context.Background(), gpsi, &getIdentityDataParamOpts)
	if err != nil {
		if res == nil {
			logger.SdmLog.Errorln(err.Error())
		} else if err.Error() != res.Status {
			logger.SdmLog.Errorln(err.Error())
		} else {
			problemDetails = &models.ProblemDetails{
				Status: int32(res.StatusCode),
				Cause:  err.(openapi.GenericOpenAPIError).Model().(models.ProblemDetails).Cause,
				Detail: err.Error(),
			}

			return nil, problemDetails
		}
	}
	defer func() {
		if rspCloseErr := res.Body.Close(); rspCloseErr != nil {
			logger.SdmLog.Errorf("GetIdentityData response body cannot close: %+v", rspCloseErr)
		}
	}()

	if res.StatusCode == http.StatusOK {
		if idTranslationResultResp.SupiList != nil {
			// GetCorrespondingSupi get corresponding Supi(here IMSI) matching the given Gpsi from the queried SUPI list from UDR
			idTranslationResult.Supi = udm_context.GetCorrespondingSupi(idTranslationResultResp)
			idTranslationResult.Gpsi = gpsi

			return &idTranslationResult, nil
		} else {
			problemDetails = &models.ProblemDetails{
				Status: http.StatusNotFound,
				Cause:  "USER_NOT_FOUND",
			}

			return nil, problemDetails
		}
	} else {
		problemDetails = &models.ProblemDetails{
			Status: http.StatusNotFound,
			Cause:  "DATA_NOT_FOUND",
		}

		return nil, problemDetails
	}
}

func HandleGetSupiRequest(request *httpwrapper.Request) *httpwrapper.Response {
	logger.SdmLog.Infoln("handle GetSupiRequest")
	supi := request.Params["supi"]
	plmnID := request.Query.Get(queryPlmnID)
	dataSetNames := request.Query["dataset-names"]
	supportedFeatures := request.Query.Get(querySupportedFeatures)
	response, problemDetails := getSupiProcedure(supi, plmnID, dataSetNames, supportedFeatures)
	if response != nil {
		stats.IncrementUdmSubscriberDataManagementStats("get", "supi", "SUCCESS")
		// status code is based on SPEC, and option headers
		return httpwrapper.NewResponse(http.StatusOK, nil, response)
	} else if problemDetails != nil {
		stats.IncrementUdmSubscriberDataManagementStats("get", "supi", "FAILURE")
		return httpwrapper.NewResponse(int(problemDetails.Status), nil, problemDetails)
	}
	problemDetails = &models.ProblemDetails{
		Status: http.StatusForbidden,
		Cause:  "UNSPECIFIED",
	}
	stats.IncrementUdmSubscriberDataManagementStats("get", "supi", "FAILURE")
	return httpwrapper.NewResponse(http.StatusForbidden, nil, problemDetails)
}

func getSupiProcedure(supi string, plmnID string, dataSetNames []string, supportedFeatures string) (
	response *models.SubscriptionDataSets, problemDetails *models.ProblemDetails,
) {
	clientAPI, err := createUDMClientToUDR(supi)
	if err != nil {
		return nil, util.ProblemDetailsSystemFailure(err.Error())
	}

	response = &models.SubscriptionDataSets{}

	// 1. Access and Mobility Data
	if prob := fetchAmData(clientAPI, supi, plmnID, supportedFeatures, response); prob != nil {
		return nil, prob
	}

	// 2. SMF Selection Data
	if prob := fetchSmfSelectData(clientAPI, supi, plmnID, supportedFeatures, response); prob != nil {
		return nil, prob
	}

	// 3. Trace Data
	if prob := fetchTraceData(clientAPI, supi, plmnID, response); prob != nil {
		return nil, prob
	}

	// 4. Session Management Data
	if prob := fetchSmData(clientAPI, supi, plmnID, response); prob != nil {
		return nil, prob
	}

	// 5. UE Context in SMF Data
	if prob := fetchUeContextInSmfData(clientAPI, supi, supportedFeatures, response); prob != nil {
		return nil, prob
	}

	return response, nil
}

// Update handleUdrResponse: Remove the internal defer Close() so it doesn't conflict
func handleUdrResponse(res *http.Response, err error, contextStr string) *models.ProblemDetails {
	if err != nil {
		if res == nil || err.Error() != res.Status {
			logger.SdmLog.Errorln(err.Error())
			return util.ProblemDetailsSystemFailure(err.Error())
		}
		return &models.ProblemDetails{
			Status: int32(res.StatusCode),
			Cause:  err.(openapi.GenericOpenAPIError).Model().(models.ProblemDetails).Cause,
			Detail: err.Error(),
		}
	}
	if res == nil || res.StatusCode != http.StatusOK {
		return &models.ProblemDetails{
			Status: http.StatusNotFound,
			Cause:  "DATA_NOT_FOUND",
		}
	}
	return nil
}

func fetchAmData(client *Nudr.APIClient, supi, plmn, feat string, ds *models.SubscriptionDataSets) *models.ProblemDetails {
	opts := &Nudr.QueryAmDataParamOpts{SupportedFeatures: optional.NewString(feat)}
	data, res, err := client.AccessAndMobilitySubscriptionDataDocumentApi.QueryAmData(context.Background(), supi, plmn, opts)
	if res != nil {
		defer func() {
			if cerr := res.Body.Close(); cerr != nil {
				logger.SdmLog.Errorf("QueryAmData response body cannot close: %+v", cerr)
			}
		}()
	}
	if prob := handleUdrResponse(res, err, "QueryAmData"); prob != nil {
		return prob
	}

	udmUe := udm_context.UDM_Self().NewUdmUe(supi)
	udmUe.SetAMSubsriptionData(&data)
	ds.AmData = &data
	return nil
}

func fetchSmfSelectData(client *Nudr.APIClient, supi, plmn, feat string, ds *models.SubscriptionDataSets) *models.ProblemDetails {
	opts := &Nudr.QuerySmfSelectDataParamOpts{SupportedFeatures: optional.NewString(feat)}
	data, res, err := client.SMFSelectionSubscriptionDataDocumentApi.QuerySmfSelectData(context.Background(), supi, plmn, opts)
	if res != nil {
		defer func() {
			if cerr := res.Body.Close(); cerr != nil {
				logger.SdmLog.Errorf("QuerySmfSelectData response body cannot close: %+v", cerr)
			}
		}()
	}
	if prob := handleUdrResponse(res, err, "QuerySmfSelectData"); prob != nil {
		return prob
	}

	udmUe := udm_context.UDM_Self().NewUdmUe(supi)
	udmUe.SetSmfSelectionSubsData(&data)
	ds.SmfSelData = &data
	return nil
}

func fetchTraceData(client *Nudr.APIClient, supi, plmn string, ds *models.SubscriptionDataSets) *models.ProblemDetails {
	data, res, err := client.TraceDataDocumentApi.QueryTraceData(context.Background(), supi, plmn, nil)
	if res != nil {
		defer func() {
			if cerr := res.Body.Close(); cerr != nil {
				logger.SdmLog.Errorf("QueryTraceData response body cannot close: %+v", cerr)
			}
		}()
	}
	if prob := handleUdrResponse(res, err, "QueryTraceData"); prob != nil {
		return prob
	}

	udmUe := udm_context.UDM_Self().NewUdmUe(supi)
	udmUe.TraceData = &data
	udmUe.TraceDataResponse.TraceData = &data
	ds.TraceData = &data
	return nil
}

func fetchSmData(client *Nudr.APIClient, supi, plmn string, ds *models.SubscriptionDataSets) *models.ProblemDetails {
	data, res, err := client.SessionManagementSubscriptionDataApi.QuerySmData(context.Background(), supi, plmn, nil)
	if res != nil {
		defer func() {
			if cerr := res.Body.Close(); cerr != nil {
				logger.SdmLog.Errorf("QuerySmData response body cannot close: %+v", cerr)
			}
		}()
	}
	if prob := handleUdrResponse(res, err, "QuerySmData"); prob != nil {
		return prob
	}

	udmUe := udm_context.UDM_Self().NewUdmUe(supi)
	// Fix dogsled: Check the return values instead of using 3 blank identifiers
	smData, snssai, dnnByDnn, allDnns := udm_context.UDM_Self().ManageSmData(data, "", "")
	_ = snssai   // bypass unused if necessary
	_ = dnnByDnn // bypass unused if necessary
	_ = allDnns  // bypass unused if necessary

	udmUe.SetSMSubsData(smData)
	ds.SmData = data
	return nil
}

func fetchUeContextInSmfData(client *Nudr.APIClient, supi, feat string, ds *models.SubscriptionDataSets) *models.ProblemDetails {
	opts := &Nudr.QuerySmfRegListParamOpts{SupportedFeatures: optional.NewString(feat)}
	pdusess, res, err := client.SMFRegistrationsCollectionApi.QuerySmfRegList(context.Background(), supi, opts)

	if prob := handleUdrResponse(res, err, "QuerySmfRegList"); prob != nil {
		return prob
	}

	ueCtx := &models.UeContextInSmfData{
		PduSessions: make(map[string]models.PduSession),
		PgwInfo:     []models.PgwInfo{},
	}

	for _, element := range pdusess {
		pduSession := models.PduSession{
			Dnn:           element.Dnn,
			SmfInstanceId: element.SmfInstanceId,
			PlmnId:        element.PlmnId,
		}
		ueCtx.PduSessions[strconv.Itoa(int(element.PduSessionId))] = pduSession

		pgwInfo := models.PgwInfo{
			Dnn:     element.Dnn,
			PgwFqdn: element.PgwFqdn,
			PlmnId:  element.PlmnId,
		}
		ueCtx.PgwInfo = append(ueCtx.PgwInfo, pgwInfo)
	}

	udmUe := udm_context.UDM_Self().NewUdmUe(supi)
	udmUe.UeCtxtInSmfData = ueCtx
	ds.UecSmfData = ueCtx
	return nil
}

func HandleGetSharedDataRequest(request *httpwrapper.Request) *httpwrapper.Response {
	logger.SdmLog.Infoln("handle GetSharedData")
	sharedDataIds := request.Query["sharedDataIds"]
	supportedFeatures := request.Query.Get(querySupportedFeatures)
	response, problemDetails := getSharedDataProcedure(sharedDataIds, supportedFeatures)
	if response != nil {
		stats.IncrementUdmSubscriberDataManagementStats("get", metricSharedData, "SUCCESS")
		// status code is based on SPEC, and option headers
		return httpwrapper.NewResponse(http.StatusOK, nil, response)
	} else if problemDetails != nil {
		stats.IncrementUdmSubscriberDataManagementStats("get", metricSharedData, "FAILURE")
		return httpwrapper.NewResponse(int(problemDetails.Status), nil, problemDetails)
	}
	problemDetails = &models.ProblemDetails{
		Status: http.StatusForbidden,
		Cause:  "UNSPECIFIED",
	}
	stats.IncrementUdmSubscriberDataManagementStats("get", metricSharedData, "FAILURE")
	return httpwrapper.NewResponse(http.StatusForbidden, nil, problemDetails)
}

func getSharedDataProcedure(sharedDataIds []string, supportedFeatures string) (
	response []models.SharedData, problemDetails *models.ProblemDetails,
) {
	clientAPI, err := createUDMClientToUDR("")
	if err != nil {
		return nil, util.ProblemDetailsSystemFailure(err.Error())
	}

	var getSharedDataParamOpts Nudr.GetSharedDataParamOpts
	getSharedDataParamOpts.SupportedFeatures = optional.NewString(supportedFeatures)

	sharedDataResp, res, err := clientAPI.RetrievalOfSharedDataApi.GetSharedData(context.Background(), sharedDataIds,
		&getSharedDataParamOpts)
	if err != nil {
		if res == nil {
			logger.SdmLog.Warnln(err)
		} else if err.Error() != res.Status {
			logger.SdmLog.Warnln(err)
		} else {
			logger.SdmLog.Warnln(err)
			problemDetails = &models.ProblemDetails{
				Status: int32(res.StatusCode),
				Cause:  err.(openapi.GenericOpenAPIError).Model().(models.ProblemDetails).Cause,
				Detail: err.Error(),
			}

			return nil, problemDetails
		}
	}
	defer func() {
		if rspCloseErr := res.Body.Close(); rspCloseErr != nil {
			logger.SdmLog.Errorf("GetShareData response body cannot close: %+v", rspCloseErr)
		}
	}()

	if res.StatusCode == http.StatusOK {
		udm_context.UDM_Self().SharedSubsDataMap = udm_context.MappingSharedData(sharedDataResp)
		sharedData := udm_context.ObtainRequiredSharedData(sharedDataIds, sharedDataResp)
		return sharedData, nil
	} else {
		problemDetails = &models.ProblemDetails{
			Status: http.StatusNotFound,
			Cause:  "DATA_NOT_FOUND",
		}
		return nil, problemDetails
	}
}

func HandleGetSmDataRequest(request *httpwrapper.Request) *httpwrapper.Response {
	logger.SdmLog.Infoln("handle GetSmData")
	supi := request.Params["supi"]
	plmnID := request.Query.Get(queryPlmnID)
	Dnn := request.Query.Get("dnn")
	Snssai := request.Query.Get("single-nssai")
	supportedFeatures := request.Query.Get(querySupportedFeatures)
	response, problemDetails := getSmDataProcedure(supi, plmnID, Dnn, Snssai, supportedFeatures)
	if response != nil {
		stats.IncrementUdmSubscriberDataManagementStats("get", metricSmData, "SUCCESS")
		// status code is based on SPEC, and option headers
		return httpwrapper.NewResponse(http.StatusOK, nil, response)
	} else if problemDetails != nil {
		stats.IncrementUdmSubscriberDataManagementStats("get", metricSmData, "FAILURE")
		return httpwrapper.NewResponse(int(problemDetails.Status), nil, problemDetails)
	}
	problemDetails = &models.ProblemDetails{
		Status: http.StatusForbidden,
		Cause:  "UNSPECIFIED",
	}
	stats.IncrementUdmSubscriberDataManagementStats("get", metricSmData, "FAILURE")
	return httpwrapper.NewResponse(http.StatusForbidden, nil, problemDetails)
}

// selectSmDataResponse determines which subset of Session Management data to return
// based on the presence of Snssai and Dnn query parameters.
func selectSmDataResponse(ue *udm_context.UdmUeContext, snssai, dnn, snssaiKey string, dnnConfigs []models.DnnConfiguration, allDnns []map[string]models.DnnConfiguration) interface{} {
	// Acquire a read lock to safely access the UE context data
	ue.SmSubsDataLock.RLock()
	defer ue.SmSubsDataLock.RUnlock()

	switch {
	// Case 1: Neither Snssai nor Dnn provided - return all DNN configurations across all slices
	case snssai == "" && dnn == "":
		return allDnns

	// Case 2: Only Snssai provided - return all DNN configurations for that specific slice
	case snssai != "" && dnn == "":
		return ue.SessionManagementSubsData[snssaiKey].DnnConfigurations

	// Case 3: Only Dnn provided - return configurations for that DNN across all slices where it exists
	case snssai == "" && dnn != "":
		return dnnConfigs

	// Case 4: Both Snssai and Dnn provided - return a flat list of matching subscription data
	case snssai != "" && dnn != "":
		rspSMSubDataList := make([]models.SessionManagementSubscriptionData, 0, len(ue.SessionManagementSubsData))
		for _, eachSMSubData := range ue.SessionManagementSubsData {
			rspSMSubDataList = append(rspSMSubDataList, eachSMSubData)
		}
		return rspSMSubDataList

	// Default: Return the full map of session management subscription data
	default:
		return ue.SessionManagementSubsData
	}
}

// getSmDataProcedure retrieves Session Management subscription data from UDR and filters it.
func getSmDataProcedure(supi string, plmnID string, Dnn string, Snssai string, supportedFeatures string) (
	response interface{}, problemDetails *models.ProblemDetails,
) {
	logger.SdmLog.Infof("getSmDataProcedure: SUPI[%s] PLMNID[%s] DNN[%s] SNssai[%s]", supi, plmnID, Dnn, Snssai)

	// Step 1: Initialize the UDR Client
	clientAPI, err := createUDMClientToUDR(supi)
	if err != nil {
		return nil, util.ProblemDetailsSystemFailure(err.Error())
	}

	// Step 2: Prepare query options
	querySmDataParamOpts := Nudr.QuerySmDataParamOpts{
		SingleNssai: optional.NewInterface(Snssai),
	}

	// Step 3: Communicate with UDR
	sessionResp, res, err := clientAPI.SessionManagementSubscriptionDataApi.
		QuerySmData(context.Background(), supi, plmnID, &querySmDataParamOpts)
	// Step 4: Handle Communication/Protocol Errors
	if err != nil {
		logger.SdmLog.Warnln(err)
		// If the error matches the response status, it's a protocol-defined error (e.g., 4xx/5xx)
		if res != nil && err.Error() == res.Status {
			return nil, &models.ProblemDetails{
				Status: int32(res.StatusCode),
				Cause:  err.(openapi.GenericOpenAPIError).Model().(models.ProblemDetails).Cause,
				Detail: err.Error(),
			}
		}
	}

	// Step 5: Validate Response Success
	if res == nil || res.StatusCode != http.StatusOK {
		return nil, &models.ProblemDetails{
			Status: http.StatusNotFound,
			Cause:  "DATA_NOT_FOUND",
		}
	}

	// Ensure the response body is closed after processing
	defer func() {
		if rspCloseErr := res.Body.Close(); rspCloseErr != nil {
			logger.SdmLog.Errorf("QuerySmData response body cannot close: %+v", rspCloseErr)
		}
	}()

	// Step 6: Update Local UDM Context
	// NewUdmUe initializes or retrieves the UE context in memory
	udmUe := udm_context.UDM_Self().NewUdmUe(supi)

	// ManageSmData parses the UDR response into internal structures
	smData, snssaiKey, allDnnByDnn, allDnns := udm_context.UDM_Self().ManageSmData(sessionResp, Snssai, Dnn)
	udmUe.SetSMSubsData(smData)

	// Step 7: Select and return the appropriate data subset via the helper
	return selectSmDataResponse(udmUe, Snssai, Dnn, snssaiKey, allDnnByDnn, allDnns), nil
}

func HandleGetNssaiRequest(request *httpwrapper.Request) *httpwrapper.Response {
	logger.SdmLog.Infoln("handle GetNssai")
	supi := request.Params["supi"]
	plmnID := request.Query.Get(queryPlmnID)
	supportedFeatures := request.Query.Get(querySupportedFeatures)
	response, problemDetails := getNssaiProcedure(supi, plmnID, supportedFeatures)
	if response != nil {
		stats.IncrementUdmSubscriberDataManagementStats("get", "nssai", "SUCCESS")
		// status code is based on SPEC, and option headers
		return httpwrapper.NewResponse(http.StatusOK, nil, response)
	} else if problemDetails != nil {
		stats.IncrementUdmSubscriberDataManagementStats("get", "nssai", "FAILURE")
		return httpwrapper.NewResponse(int(problemDetails.Status), nil, problemDetails)
	}
	problemDetails = &models.ProblemDetails{
		Status: http.StatusForbidden,
		Cause:  "UNSPECIFIED",
	}
	stats.IncrementUdmSubscriberDataManagementStats("get", "nssai", "FAILURE")
	return httpwrapper.NewResponse(http.StatusForbidden, nil, problemDetails)
}

func getNssaiProcedure(supi string, plmnID string, supportedFeatures string) (
	*models.Nssai, *models.ProblemDetails,
) {
	var queryAmDataParamOpts Nudr.QueryAmDataParamOpts
	queryAmDataParamOpts.SupportedFeatures = optional.NewString(supportedFeatures)
	var nssaiResp models.Nssai
	clientAPI, err := createUDMClientToUDR(supi)
	if err != nil {
		return nil, util.ProblemDetailsSystemFailure(err.Error())
	}

	accessAndMobilitySubscriptionDataResp, res, err := clientAPI.AccessAndMobilitySubscriptionDataDocumentApi.
		QueryAmData(context.Background(), supi, plmnID, &queryAmDataParamOpts)
	if err != nil {
		if res == nil {
			logger.SdmLog.Warnln(err)
		} else if err.Error() != res.Status {
			logger.SdmLog.Warnln(err)
		} else {
			logger.SdmLog.Warnln(err)
			problemDetails := &models.ProblemDetails{
				Status: int32(res.StatusCode),
				Cause:  err.(openapi.GenericOpenAPIError).Model().(models.ProblemDetails).Cause,
				Detail: err.Error(),
			}

			return nil, problemDetails
		}
	}
	defer func() {
		if rspCloseErr := res.Body.Close(); rspCloseErr != nil {
			logger.SdmLog.Errorf(errQueryAmDataClose, rspCloseErr)
		}
	}()

	nssaiResp = *accessAndMobilitySubscriptionDataResp.Nssai

	if res.StatusCode == http.StatusOK {
		udmUe := udm_context.UDM_Self().NewUdmUe(supi)
		udmUe.Nssai = &nssaiResp
		return udmUe.Nssai, nil
	} else {
		problemDetails := &models.ProblemDetails{
			Status: http.StatusNotFound,
			Cause:  "DATA_NOT_FOUND",
		}
		return nil, problemDetails
	}
}

func HandleGetSmfSelectDataRequest(request *httpwrapper.Request) *httpwrapper.Response {
	logger.SdmLog.Infoln("handle GetSmfSelectData")
	supi := request.Params["supi"]
	plmnID := request.Query.Get(queryPlmnID)
	supportedFeatures := request.Query.Get(querySupportedFeatures)
	response, problemDetails := getSmfSelectDataProcedure(supi, plmnID, supportedFeatures)
	if response != nil {
		stats.IncrementUdmSubscriberDataManagementStats("get", metricSmfSelectData, "SUCCESS")
		// status code is based on SPEC, and option headers
		return httpwrapper.NewResponse(http.StatusOK, nil, response)
	} else if problemDetails != nil {
		stats.IncrementUdmSubscriberDataManagementStats("get", metricSmfSelectData, "FAILURE")
		return httpwrapper.NewResponse(int(problemDetails.Status), nil, problemDetails)
	}
	problemDetails = &models.ProblemDetails{
		Status: http.StatusForbidden,
		Cause:  "UNSPECIFIED",
	}
	stats.IncrementUdmSubscriberDataManagementStats("get", metricSmfSelectData, "FAILURE")
	return httpwrapper.NewResponse(http.StatusForbidden, nil, problemDetails)
}

func getSmfSelectDataProcedure(supi string, plmnID string, supportedFeatures string) (
	response *models.SmfSelectionSubscriptionData, problemDetails *models.ProblemDetails,
) {
	var querySmfSelectDataParamOpts Nudr.QuerySmfSelectDataParamOpts
	querySmfSelectDataParamOpts.SupportedFeatures = optional.NewString(supportedFeatures)
	var body models.SmfSelectionSubscriptionData

	clientAPI, err := createUDMClientToUDR(supi)
	if err != nil {
		return nil, util.ProblemDetailsSystemFailure(err.Error())
	}

	udm_context.UDM_Self().CreateSmfSelectionSubsDataforUe(supi, body)

	smfSelectionSubscriptionDataResp, res, err := clientAPI.SMFSelectionSubscriptionDataDocumentApi.
		QuerySmfSelectData(context.Background(), supi, plmnID, &querySmfSelectDataParamOpts)
	if err != nil {
		if res == nil {
			logger.SdmLog.Warnln(err)
		} else if err.Error() != res.Status {
			logger.SdmLog.Warnln(err)
		} else {
			logger.SdmLog.Warnln(err)
			problemDetails = &models.ProblemDetails{
				Status: int32(res.StatusCode),
				Cause:  err.(openapi.GenericOpenAPIError).Model().(models.ProblemDetails).Cause,
				Detail: err.Error(),
			}
			return nil, problemDetails
		}
		return
	}
	defer func() {
		if rspCloseErr := res.Body.Close(); rspCloseErr != nil {
			logger.SdmLog.Errorf("QuerySmfSelectData response body cannot close: %+v", rspCloseErr)
		}
	}()

	if res.StatusCode == http.StatusOK {
		udmUe := udm_context.UDM_Self().NewUdmUe(supi)
		udmUe.SetSmfSelectionSubsData(&smfSelectionSubscriptionDataResp)
		return udmUe.SmfSelSubsData, nil
	} else {
		problemDetails = &models.ProblemDetails{
			Status: http.StatusNotFound,
			Cause:  "DATA_NOT_FOUND",
		}
		return nil, problemDetails
	}
}

func HandleSubscribeToSharedDataRequest(request *httpwrapper.Request) *httpwrapper.Response {
	logger.SdmLog.Infoln("handle SubscribeToSharedData")
	sdmSubscription := request.Body.(models.SdmSubscription)
	header, response, problemDetails := subscribeToSharedDataProcedure(&sdmSubscription)
	if response != nil {
		stats.IncrementUdmSubscriberDataManagementStats("create", metricSharedDataSubs, "SUCCESS")
		// status code is based on SPEC, and option headers
		return httpwrapper.NewResponse(http.StatusCreated, header, response)
	} else if problemDetails != nil {
		stats.IncrementUdmSubscriberDataManagementStats("create", metricSharedDataSubs, "FAILURE")
		return httpwrapper.NewResponse(int(problemDetails.Status), nil, problemDetails)
	} else {
		stats.IncrementUdmSubscriberDataManagementStats("create", metricSharedDataSubs, "FAILURE")
		return httpwrapper.NewResponse(http.StatusNotFound, nil, nil)
	}
}

func subscribeToSharedDataProcedure(sdmSubscription *models.SdmSubscription) (
	header http.Header, response *models.SdmSubscription, problemDetails *models.ProblemDetails,
) {
	cfg := Nudm_SubscriberDataManagement.NewConfiguration()
	udmClientAPI := Nudm_SubscriberDataManagement.NewAPIClient(cfg)

	sdmSubscriptionResp, res, err := udmClientAPI.SubscriptionCreationForSharedDataApi.SubscribeToSharedData(
		context.Background(), *sdmSubscription)
	if err != nil {
		if res == nil {
			logger.SdmLog.Warnln(err)
		} else if err.Error() != res.Status {
			logger.SdmLog.Warnln(err)
		} else {
			problemDetails = &models.ProblemDetails{
				Status: int32(res.StatusCode),
				Cause:  err.(openapi.GenericOpenAPIError).Model().(models.ProblemDetails).Cause,
				Detail: err.Error(),
			}
			return nil, nil, problemDetails
		}
	}
	defer func() {
		if rspCloseErr := res.Body.Close(); rspCloseErr != nil {
			logger.SdmLog.Errorf("SubscribeToSharedData response body cannot close: %+v", rspCloseErr)
		}
	}()

	switch res.StatusCode {
	case http.StatusCreated:
		header = make(http.Header)
		udm_context.UDM_Self().CreateSubstoNotifSharedData(sdmSubscriptionResp.SubscriptionId, &sdmSubscriptionResp)
		reourceUri := udm_context.UDM_Self().GetSDMUri() + "//shared-data-subscriptions/" + sdmSubscriptionResp.SubscriptionId
		header.Set("Location", reourceUri)
		return header, &sdmSubscriptionResp, nil
	case http.StatusNotFound:
		problemDetails = &models.ProblemDetails{
			Status: http.StatusNotFound,
			Cause:  "DATA_NOT_FOUND",
		}

		return nil, nil, problemDetails
	default:
		problemDetails = &models.ProblemDetails{
			Status: http.StatusNotImplemented,
			Cause:  "UNSUPPORTED_RESOURCE_URI",
		}

		return nil, nil, problemDetails
	}
}

func HandleSubscribeRequest(request *httpwrapper.Request) *httpwrapper.Response {
	logger.SdmLog.Infoln("handle Subscribe")
	sdmSubscription := request.Body.(models.SdmSubscription)
	supi := request.Params["supi"]
	header, response, problemDetails := subscribeProcedure(&sdmSubscription, supi)
	if response != nil {
		stats.IncrementUdmSubscriberDataManagementStats("create", metricSdmSubs, "SUCCESS")
		// status code is based on SPEC, and option headers
		return httpwrapper.NewResponse(http.StatusCreated, header, response)
	} else if problemDetails != nil {
		stats.IncrementUdmSubscriberDataManagementStats("create", metricSdmSubs, "FAILURE")
		return httpwrapper.NewResponse(int(problemDetails.Status), nil, problemDetails)
	} else {
		stats.IncrementUdmSubscriberDataManagementStats("create", metricSdmSubs, "FAILURE")
		return httpwrapper.NewResponse(http.StatusNotFound, nil, nil)
	}
}

func subscribeProcedure(sdmSubscription *models.SdmSubscription, supi string) (
	header http.Header, response *models.SdmSubscription, problemDetails *models.ProblemDetails,
) {
	clientAPI, err := createUDMClientToUDR(supi)
	if err != nil {
		return nil, nil, util.ProblemDetailsSystemFailure(err.Error())
	}

	sdmSubscriptionResp, res, err := clientAPI.SDMSubscriptionsCollectionApi.CreateSdmSubscriptions(
		context.Background(), supi, *sdmSubscription)
	if err != nil {
		if res == nil {
			logger.SdmLog.Warnln(err)
		} else if err.Error() != res.Status {
			logger.SdmLog.Warnln(err)
		} else {
			logger.SdmLog.Warnln(err)
			problemDetails = &models.ProblemDetails{
				Status: int32(res.StatusCode),
				Cause:  err.(openapi.GenericOpenAPIError).Model().(models.ProblemDetails).Cause,
				Detail: err.Error(),
			}
			return nil, nil, problemDetails
		}
	}
	defer func() {
		if rspCloseErr := res.Body.Close(); rspCloseErr != nil {
			logger.SdmLog.Errorf("CreateSdmSubscriptions response body cannot close: %+v", rspCloseErr)
		}
	}()

	switch res.StatusCode {
	case http.StatusCreated:
		header = make(http.Header)
		udmUe, _ := udm_context.UDM_Self().UdmUeFindBySupi(supi)
		if udmUe == nil {
			udmUe = udm_context.UDM_Self().NewUdmUe(supi)
		}
		udmUe.CreateSubscriptiontoNotifChange(sdmSubscriptionResp.SubscriptionId, &sdmSubscriptionResp)
		header.Set("Location", udmUe.GetLocationURI2(udm_context.LocationUriSdmSubscription, supi))
		return header, &sdmSubscriptionResp, nil
	case http.StatusNotFound:
		problemDetails = &models.ProblemDetails{
			Status: http.StatusNotFound,
			Cause:  "DATA_NOT_FOUND",
		}
		return nil, nil, problemDetails
	default:
		problemDetails = &models.ProblemDetails{
			Status: http.StatusNotImplemented,
			Cause:  "UNSUPPORTED_RESOURCE_URI",
		}
		return nil, nil, problemDetails
	}
}

func HandleUnsubscribeForSharedDataRequest(request *httpwrapper.Request) *httpwrapper.Response {
	logger.SdmLog.Infoln("handle UnsubscribeForSharedData")
	subscriptionID := request.Params["subscriptionId"]
	problemDetails := unsubscribeForSharedDataProcedure(subscriptionID)
	if problemDetails != nil {
		stats.IncrementUdmSubscriberDataManagementStats("delete", metricSharedDataSubs, "FAILURE")
		return httpwrapper.NewResponse(int(problemDetails.Status), nil, problemDetails)
	}
	stats.IncrementUdmSubscriberDataManagementStats("delete", metricSharedDataSubs, "SUCCESS")
	return httpwrapper.NewResponse(http.StatusNoContent, nil, nil)
}

func unsubscribeForSharedDataProcedure(subscriptionID string) *models.ProblemDetails {
	cfg := Nudm_SubscriberDataManagement.NewConfiguration()
	udmClientAPI := Nudm_SubscriberDataManagement.NewAPIClient(cfg)

	res, err := udmClientAPI.SubscriptionDeletionForSharedDataApi.UnsubscribeForSharedData(
		context.Background(), subscriptionID)
	if err != nil {
		if res == nil {
			logger.SdmLog.Warnln(err)
		} else if err.Error() != res.Status {
			logger.SdmLog.Warnln(err)
		} else {
			logger.SdmLog.Warnln(err)
			problemDetails := &models.ProblemDetails{
				Status: int32(res.StatusCode),
				Cause:  err.(openapi.GenericOpenAPIError).Model().(models.ProblemDetails).Cause,
				Detail: err.Error(),
			}
			return problemDetails
		}
	}
	defer func() {
		if rspCloseErr := res.Body.Close(); rspCloseErr != nil {
			logger.SdmLog.Errorf("UnsubscribeForSharedData response body cannot close: %+v", rspCloseErr)
		}
	}()

	if res.StatusCode == http.StatusNoContent {
		return nil
	} else {
		problemDetails := &models.ProblemDetails{
			Status: http.StatusNotFound,
			Cause:  "DATA_NOT_FOUND",
		}
		return problemDetails
	}
}

func HandleUnsubscribeRequest(request *httpwrapper.Request) *httpwrapper.Response {
	logger.SdmLog.Infoln("handle Unsubscribe")
	supi := request.Params["supi"]
	subscriptionID := request.Params["subscriptionId"]
	problemDetails := unsubscribeProcedure(supi, subscriptionID)
	if problemDetails != nil {
		stats.IncrementUdmSubscriberDataManagementStats("delete", metricSdmSubs, "FAILURE")
		return httpwrapper.NewResponse(int(problemDetails.Status), nil, problemDetails)
	}
	stats.IncrementUdmSubscriberDataManagementStats("delete", metricSdmSubs, "SUCCESS")
	return httpwrapper.NewResponse(http.StatusNoContent, nil, nil)
}

func unsubscribeProcedure(supi string, subscriptionID string) *models.ProblemDetails {
	clientAPI, err := createUDMClientToUDR(supi)
	if err != nil {
		return util.ProblemDetailsSystemFailure(err.Error())
	}

	res, err := clientAPI.SDMSubscriptionDocumentApi.RemovesdmSubscriptions(context.Background(), "====", subscriptionID)
	if err != nil {
		if res == nil {
			logger.SdmLog.Warnln(err)
		} else if err.Error() != res.Status {
			logger.SdmLog.Warnln(err)
		} else {
			logger.SdmLog.Warnln(err)
			problemDetails := &models.ProblemDetails{
				Status: int32(res.StatusCode),
				Cause:  err.(openapi.GenericOpenAPIError).Model().(models.ProblemDetails).Cause,
				Detail: err.Error(),
			}
			return problemDetails
		}
	}
	defer func() {
		if rspCloseErr := res.Body.Close(); rspCloseErr != nil {
			logger.SdmLog.Errorf("RemovesdmSubscriptions response body cannot close: %+v", rspCloseErr)
		}
	}()

	if res.StatusCode == http.StatusNoContent {
		return nil
	} else {
		problemDetails := &models.ProblemDetails{
			Status: http.StatusNotFound,
			Cause:  "USER_NOT_FOUND",
		}
		return problemDetails
	}
}

func HandleModifyRequest(request *httpwrapper.Request) *httpwrapper.Response {
	logger.SdmLog.Infoln("handle Modify")
	sdmSubsModification := request.Body.(models.SdmSubsModification)
	supi := request.Params["supi"]
	subscriptionID := request.Params["subscriptionId"]
	response, problemDetails := modifyProcedure(&sdmSubsModification, supi, subscriptionID)
	if response != nil {
		stats.IncrementUdmSubscriberDataManagementStats("update", metricSdmSubs, "SUCCESS")
		// status code is based on SPEC, and option headers
		return httpwrapper.NewResponse(http.StatusOK, nil, response)
	} else if problemDetails != nil {
		stats.IncrementUdmSubscriberDataManagementStats("update", metricSdmSubs, "FAILURE")
		return httpwrapper.NewResponse(int(problemDetails.Status), nil, problemDetails)
	}
	problemDetails = &models.ProblemDetails{
		Status: http.StatusForbidden,
		Cause:  "UNSPECIFIED",
	}
	stats.IncrementUdmSubscriberDataManagementStats("update", metricSdmSubs, "FAILURE")
	return httpwrapper.NewResponse(http.StatusForbidden, nil, problemDetails)
}

func modifyProcedure(sdmSubsModification *models.SdmSubsModification, supi string, subscriptionID string) (
	response *models.SdmSubscription, problemDetails *models.ProblemDetails,
) {
	clientAPI, err := createUDMClientToUDR(supi)
	if err != nil {
		return nil, util.ProblemDetailsSystemFailure(err.Error())
	}

	sdmSubscription := models.SdmSubscription{}
	body := Nudr.UpdatesdmsubscriptionsParamOpts{
		SdmSubscription: optional.NewInterface(sdmSubscription),
	}
	res, err := clientAPI.SDMSubscriptionDocumentApi.Updatesdmsubscriptions(
		context.Background(), supi, subscriptionID, &body)
	if err != nil {
		if res == nil {
			logger.SdmLog.Warnln(err)
		} else if err.Error() != res.Status {
			logger.SdmLog.Warnln(err)
		} else {
			problemDetails = &models.ProblemDetails{
				Status: int32(res.StatusCode),
				Cause:  err.(openapi.GenericOpenAPIError).Model().(models.ProblemDetails).Cause,
				Detail: err.Error(),
			}
			return nil, problemDetails
		}
	}
	defer func() {
		if rspCloseErr := res.Body.Close(); rspCloseErr != nil {
			logger.SdmLog.Errorf("Updatesdmsubscriptions response body cannot close: %+v", rspCloseErr)
		}
	}()

	if res.StatusCode == http.StatusOK {
		return &sdmSubscription, nil
	} else {
		problemDetails = &models.ProblemDetails{
			Status: http.StatusNotFound,
			Cause:  "USER_NOT_FOUND",
		}

		return nil, problemDetails
	}
}

func HandleModifyForSharedDataRequest(request *httpwrapper.Request) *httpwrapper.Response {
	logger.SdmLog.Infoln("handle ModifyForSharedData")
	sdmSubsModification := request.Body.(models.SdmSubsModification)
	supi := request.Params["supi"]
	subscriptionID := request.Params["subscriptionId"]
	response, problemDetails := modifyForSharedDataProcedure(&sdmSubsModification, supi, subscriptionID)
	if response != nil {
		stats.IncrementUdmSubscriberDataManagementStats("update", metricSharedDataSubs, "SUCCESS")
		// status code is based on SPEC, and option headers
		return httpwrapper.NewResponse(http.StatusOK, nil, response)
	} else if problemDetails != nil {
		stats.IncrementUdmSubscriberDataManagementStats("update", metricSharedDataSubs, "FAILURE")
		return httpwrapper.NewResponse(int(problemDetails.Status), nil, problemDetails)
	}
	problemDetails = &models.ProblemDetails{
		Status: http.StatusForbidden,
		Cause:  "UNSPECIFIED",
	}
	stats.IncrementUdmSubscriberDataManagementStats("update", metricSharedDataSubs, "FAILURE")
	return httpwrapper.NewResponse(http.StatusForbidden, nil, problemDetails)
}

func modifyForSharedDataProcedure(sdmSubsModification *models.SdmSubsModification, supi string,
	subscriptionID string,
) (response *models.SdmSubscription, problemDetails *models.ProblemDetails) {
	clientAPI, err := createUDMClientToUDR(supi)
	if err != nil {
		return nil, util.ProblemDetailsSystemFailure(err.Error())
	}

	var sdmSubscription models.SdmSubscription
	sdmSubs := models.SdmSubscription{}
	body := Nudr.UpdatesdmsubscriptionsParamOpts{
		SdmSubscription: optional.NewInterface(sdmSubs),
	}

	res, err := clientAPI.SDMSubscriptionDocumentApi.Updatesdmsubscriptions(
		context.Background(), supi, subscriptionID, &body)
	if err != nil {
		if res == nil {
			logger.SdmLog.Warnln(err)
		} else if err.Error() != res.Status {
			logger.SdmLog.Warnln(err)
		} else {
			problemDetails = &models.ProblemDetails{
				Status: int32(res.StatusCode),
				Cause:  err.(openapi.GenericOpenAPIError).Model().(models.ProblemDetails).Cause,
				Detail: err.Error(),
			}
			return nil, problemDetails
		}
	}
	defer func() {
		if rspCloseErr := res.Body.Close(); rspCloseErr != nil {
			logger.SdmLog.Errorf("Updatesdmsubscriptions response body cannot close: %+v", rspCloseErr)
		}
	}()

	if res.StatusCode == http.StatusOK {
		return &sdmSubscription, nil
	} else {
		problemDetails = &models.ProblemDetails{
			Status: http.StatusNotFound,
			Cause:  "USER_NOT_FOUND",
		}

		return nil, problemDetails
	}
}

func HandleGetTraceDataRequest(request *httpwrapper.Request) *httpwrapper.Response {
	logger.SdmLog.Infoln("handle GetTraceData")
	supi := request.Params["supi"]
	plmnID := request.Query.Get(queryPlmnID)
	response, problemDetails := getTraceDataProcedure(supi, plmnID)
	if response != nil {
		stats.IncrementUdmSubscriberDataManagementStats("get", metricTraceData, "SUCCESS")
		// status code is based on SPEC, and option headers
		return httpwrapper.NewResponse(http.StatusOK, nil, response)
	} else if problemDetails != nil {
		stats.IncrementUdmSubscriberDataManagementStats("get", metricTraceData, "FAILURE")
		return httpwrapper.NewResponse(int(problemDetails.Status), nil, problemDetails)
	}
	problemDetails = &models.ProblemDetails{
		Status: http.StatusForbidden,
		Cause:  "UNSPECIFIED",
	}
	stats.IncrementUdmSubscriberDataManagementStats("get", metricTraceData, "FAILURE")
	return httpwrapper.NewResponse(http.StatusForbidden, nil, problemDetails)
}

func getTraceDataProcedure(supi string, plmnID string) (
	response *models.TraceData, problemDetails *models.ProblemDetails,
) {
	var body models.TraceData
	var queryTraceDataParamOpts Nudr.QueryTraceDataParamOpts

	clientAPI, err := createUDMClientToUDR(supi)
	if err != nil {
		return nil, util.ProblemDetailsSystemFailure(err.Error())
	}

	udm_context.UDM_Self().CreateTraceDataforUe(supi, body)

	traceDataRes, res, err := clientAPI.TraceDataDocumentApi.QueryTraceData(
		context.Background(), supi, plmnID, &queryTraceDataParamOpts)
	if err != nil {
		if res == nil {
			logger.SdmLog.Warnln(err)
		} else if err.Error() != res.Status {
			logger.SdmLog.Warnln(err)
		} else {
			problemDetails = &models.ProblemDetails{
				Status: int32(res.StatusCode),
				Cause:  err.(openapi.GenericOpenAPIError).Model().(models.ProblemDetails).Cause,
				Detail: err.Error(),
			}

			return nil, problemDetails
		}
	}
	defer func() {
		if rspCloseErr := res.Body.Close(); rspCloseErr != nil {
			logger.SdmLog.Errorf("QueryTraceData response body cannot close: %+v", rspCloseErr)
		}
	}()

	if res.StatusCode == http.StatusOK {
		udmUe := udm_context.UDM_Self().NewUdmUe(supi)
		udmUe.TraceData = &traceDataRes
		udmUe.TraceDataResponse.TraceData = &traceDataRes

		return udmUe.TraceDataResponse.TraceData, nil
	} else {
		problemDetails = &models.ProblemDetails{
			Status: http.StatusNotFound,
			Cause:  "USER_NOT_FOUND",
		}

		return nil, problemDetails
	}
}

func HandleGetUeContextInSmfDataRequest(request *httpwrapper.Request) *httpwrapper.Response {
	logger.SdmLog.Infoln("handle GetUeContextInSmfData")
	supi := request.Params["supi"]
	supportedFeatures := request.Query.Get(querySupportedFeatures)
	response, problemDetails := getUeContextInSmfDataProcedure(supi, supportedFeatures)
	if response != nil {
		stats.IncrementUdmSubscriberDataManagementStats("get", metricUeCtxInSmf, "SUCCESS")
		// status code is based on SPEC, and option headers
		return httpwrapper.NewResponse(http.StatusOK, nil, response)
	} else if problemDetails != nil {
		stats.IncrementUdmSubscriberDataManagementStats("get", metricUeCtxInSmf, "FAILURE")
		return httpwrapper.NewResponse(int(problemDetails.Status), nil, problemDetails)
	}
	problemDetails = &models.ProblemDetails{
		Status: http.StatusForbidden,
		Cause:  "UNSPECIFIED",
	}
	stats.IncrementUdmSubscriberDataManagementStats("get", metricUeCtxInSmf, "FAILURE")
	return httpwrapper.NewResponse(http.StatusForbidden, nil, problemDetails)
}

func getUeContextInSmfDataProcedure(supi string, supportedFeatures string) (
	response *models.UeContextInSmfData, problemDetails *models.ProblemDetails,
) {
	var body models.UeContextInSmfData
	var ueContextInSmfData models.UeContextInSmfData
	var pgwInfoArray []models.PgwInfo
	var querySmfRegListParamOpts Nudr.QuerySmfRegListParamOpts
	querySmfRegListParamOpts.SupportedFeatures = optional.NewString(supportedFeatures)

	clientAPI, err := createUDMClientToUDR(supi)
	if err != nil {
		return nil, util.ProblemDetailsSystemFailure(err.Error())
	}

	pduSessionMap := make(map[string]models.PduSession)
	udm_context.UDM_Self().CreateUeContextInSmfDataforUe(supi, body)

	pdusess, res, err := clientAPI.SMFRegistrationsCollectionApi.QuerySmfRegList(
		context.Background(), supi, &querySmfRegListParamOpts)
	if err != nil {
		if res == nil {
			logger.SdmLog.Infoln(err)
		} else if err.Error() != res.Status {
			logger.SdmLog.Infoln(err)
		} else {
			logger.SdmLog.Infoln(err)
			problemDetails = &models.ProblemDetails{
				Status: int32(res.StatusCode),
				Cause:  err.(openapi.GenericOpenAPIError).Model().(models.ProblemDetails).Cause,
				Detail: err.Error(),
			}

			return nil, problemDetails
		}
	}
	defer func() {
		if rspCloseErr := res.Body.Close(); rspCloseErr != nil {
			logger.SdmLog.Errorf("QuerySmfRegList response body cannot close: %+v", rspCloseErr)
		}
	}()

	for _, element := range pdusess {
		var pduSession models.PduSession
		pduSession.Dnn = element.Dnn
		pduSession.SmfInstanceId = element.SmfInstanceId
		pduSession.PlmnId = element.PlmnId
		pduSessionMap[strconv.Itoa(int(element.PduSessionId))] = pduSession
	}
	ueContextInSmfData.PduSessions = pduSessionMap

	for _, element := range pdusess {
		var pgwInfo models.PgwInfo
		pgwInfo.Dnn = element.Dnn
		pgwInfo.PgwFqdn = element.PgwFqdn
		pgwInfo.PlmnId = element.PlmnId
		pgwInfoArray = append(pgwInfoArray, pgwInfo)
	}
	ueContextInSmfData.PgwInfo = pgwInfoArray

	if res.StatusCode == http.StatusOK {
		udmUe := udm_context.UDM_Self().NewUdmUe(supi)
		udmUe.UeCtxtInSmfData = &ueContextInSmfData
		return udmUe.UeCtxtInSmfData, nil
	} else {
		problemDetails = &models.ProblemDetails{
			Status: http.StatusNotFound,
			Cause:  "DATA_NOT_FOUND",
		}
		return nil, problemDetails
	}
}
