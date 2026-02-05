// Copyright 2019 free5GC.org
//
// SPDX-License-Identifier: Apache-2.0
//

package producer

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/5GC-DEV/openapi-cdac"
	"github.com/5GC-DEV/openapi-cdac/Nudr_DataRepository"
	"github.com/5GC-DEV/openapi-cdac/models"
	"github.com/antihax/optional"
	"github.com/omec-project/udm/consumer"
	udmContext "github.com/omec-project/udm/context"
	"github.com/omec-project/udm/logger"
	stats "github.com/omec-project/udm/metrics"
	"github.com/omec-project/udm/producer/callback"
	"github.com/omec-project/udm/util"
	"github.com/omec-project/util/httpwrapper"
)

func createUDMClientToUDR(id string) (*Nudr_DataRepository.APIClient, error) {
	uri := getUdrURI(id)
	if uri == "" {
		logger.Handlelog.Errorf("ID[%s] does not match any UDR", id)
		return nil, fmt.Errorf("no UDR URI found")
	}
	cfg := Nudr_DataRepository.NewConfiguration()
	cfg.SetBasePath(uri)
	clientAPI := Nudr_DataRepository.NewAPIClient(cfg)
	return clientAPI, nil
}

func getUdrURI(id string) string {
	if strings.Contains(id, "imsi") || strings.Contains(id, "nai") { // supi
		ue, ok := udmContext.UDM_Self().UdmUeFindBySupi(id)
		if ok {
			ue.UdrUri = consumer.SendNFInstancesUDR(id, consumer.NFDiscoveryToUDRParamSupi)
			return ue.UdrUri
		} else {
			ue = udmContext.UDM_Self().NewUdmUe(id)
			ue.UdrUri = consumer.SendNFInstancesUDR(id, consumer.NFDiscoveryToUDRParamSupi)
			return ue.UdrUri
		}
	} else if strings.Contains(id, "pei") {
		var udrURI string
		udmContext.UDM_Self().UdmUePool.Range(func(key, value interface{}) bool {
			ue := value.(*udmContext.UdmUeContext)
			if ue.Amf3GppAccessRegistration != nil && ue.Amf3GppAccessRegistration.Pei == id {
				ue.UdrUri = consumer.SendNFInstancesUDR(ue.Supi, consumer.NFDiscoveryToUDRParamSupi)
				udrURI = ue.UdrUri
				return false
			} else if ue.AmfNon3GppAccessRegistration != nil && ue.AmfNon3GppAccessRegistration.Pei == id {
				ue.UdrUri = consumer.SendNFInstancesUDR(ue.Supi, consumer.NFDiscoveryToUDRParamSupi)
				udrURI = ue.UdrUri
				return false
			}
			return true
		})
		return udrURI
	} else if strings.Contains(id, "extgroupid") {
		// extra group id
		return consumer.SendNFInstancesUDR(id, consumer.NFDiscoveryToUDRParamExtGroupId)
	} else if strings.Contains(id, "msisdn") || strings.Contains(id, "extid") {
		// gpsi
		return consumer.SendNFInstancesUDR(id, consumer.NFDiscoveryToUDRParamGpsi)
	}
	return consumer.SendNFInstancesUDR("", consumer.NFDiscoveryToUDRParamNone)
}

func HandleGetAmf3gppAccessRequest(request *httpwrapper.Request) *httpwrapper.Response {
	logger.UecmLog.Infof("Handle HandleGetAmf3gppAccessRequest")
	ueID := request.Params["ueId"]
	supportedFeatures := request.Query.Get("supported-features")
	response, problemDetails := GetAmf3gppAccessProcedure(ueID, supportedFeatures)
	if response != nil {
		stats.IncrementUdmUeContextManagementStats("get", "amf-3gpp-access", "SUCCESS")
		// status code is based on SPEC, and option headers
		return httpwrapper.NewResponse(http.StatusOK, nil, response)
	} else if problemDetails != nil {
		stats.IncrementUdmUeContextManagementStats("get", "amf-3gpp-access", "FAILURE")
		return httpwrapper.NewResponse(int(problemDetails.Status), nil, problemDetails)
	}
	problemDetails = &models.ProblemDetails{
		Status: http.StatusForbidden,
		Cause:  "UNSPECIFIED",
	}
	stats.IncrementUdmUeContextManagementStats("get", "amf-3gpp-access", "FAILURE")
	return httpwrapper.NewResponse(http.StatusForbidden, nil, problemDetails)
}

func GetAmf3gppAccessProcedure(ueID string, supportedFeatures string) (
	response *models.Amf3GppAccessRegistration, problemDetails *models.ProblemDetails,
) {
	var queryAmfContext3gppParamOpts Nudr_DataRepository.QueryAmfContext3gppParamOpts
	queryAmfContext3gppParamOpts.SupportedFeatures = optional.NewString(supportedFeatures)

	clientAPI, err := createUDMClientToUDR(ueID)
	if err != nil {
		return nil, util.ProblemDetailsSystemFailure(err.Error())
	}

	amf3GppAccessRegistration, resp, err := clientAPI.AMF3GPPAccessRegistrationDocumentApi.
		QueryAmfContext3gpp(context.Background(), ueID, &queryAmfContext3gppParamOpts)
	if err != nil {
		problemDetails = &models.ProblemDetails{
			Status: int32(resp.StatusCode),
			Cause:  err.(openapi.GenericOpenAPIError).Model().(models.ProblemDetails).Cause,
			Detail: err.Error(),
		}
		return nil, problemDetails
	}
	defer func() {
		if rspCloseErr := resp.Body.Close(); rspCloseErr != nil {
			logger.SdmLog.Errorf("QueryAmfContext3gpp response body cannot close: %+v", rspCloseErr)
		}
	}()

	return &amf3GppAccessRegistration, nil
}

func HandleGetAmfNon3gppAccessRequest(request *httpwrapper.Request) *httpwrapper.Response {
	logger.UecmLog.Infoln("handle GetAmfNon3gppAccessRequest")
	ueId := request.Params["ueId"]
	supportedFeatures := request.Query.Get("supported-features")
	var queryAmfContextNon3gppParamOpts Nudr_DataRepository.QueryAmfContextNon3gppParamOpts
	queryAmfContextNon3gppParamOpts.SupportedFeatures = optional.NewString(supportedFeatures)
	response, problemDetails := GetAmfNon3gppAccessProcedure(queryAmfContextNon3gppParamOpts, ueId)
	if response != nil {
		stats.IncrementUdmUeContextManagementStats("get", "amf-non-3gpp-access", "SUCCESS")
		// status code is based on SPEC, and option headers
		return httpwrapper.NewResponse(http.StatusOK, nil, response)
	} else if problemDetails != nil {
		stats.IncrementUdmUeContextManagementStats("get", "amf-non-3gpp-access", "FAILURE")
		return httpwrapper.NewResponse(int(problemDetails.Status), nil, problemDetails)
	}
	problemDetails = &models.ProblemDetails{
		Status: http.StatusForbidden,
		Cause:  "UNSPECIFIED",
	}
	stats.IncrementUdmUeContextManagementStats("get", "amf-non-3gpp-access", "FAILURE")
	return httpwrapper.NewResponse(http.StatusForbidden, nil, problemDetails)
}

func GetAmfNon3gppAccessProcedure(queryAmfContextNon3gppParamOpts Nudr_DataRepository.
	QueryAmfContextNon3gppParamOpts, ueID string) (response *models.AmfNon3GppAccessRegistration,
	problemDetails *models.ProblemDetails,
) {
	clientAPI, err := createUDMClientToUDR(ueID)
	if err != nil {
		return nil, util.ProblemDetailsSystemFailure(err.Error())
	}

	amfNon3GppAccessRegistration, resp, err := clientAPI.AMFNon3GPPAccessRegistrationDocumentApi.
		QueryAmfContextNon3gpp(context.Background(), ueID, &queryAmfContextNon3gppParamOpts)
	if err != nil {
		problemDetails = &models.ProblemDetails{
			Status: int32(resp.StatusCode),
			Cause:  err.(openapi.GenericOpenAPIError).Model().(models.ProblemDetails).Cause,
			Detail: err.Error(),
		}
		return nil, problemDetails
	}
	defer func() {
		if rspCloseErr := resp.Body.Close(); rspCloseErr != nil {
			logger.SdmLog.Errorf("QueryAmfContext3gpp response body cannot close: %+v", rspCloseErr)
		}
	}()

	return &amfNon3GppAccessRegistration, nil
}

func HandleRegistrationAmf3gppAccessRequest(request *httpwrapper.Request) *httpwrapper.Response {
	logger.UecmLog.Infoln("handle RegistrationAmf3gppAccess")
	registerRequest := request.Body.(models.Amf3GppAccessRegistration)
	ueID := request.Params["ueId"]
	logger.UecmLog.Info("UEID: ", ueID)
	header, response, problemDetails := RegistrationAmf3gppAccessProcedure(registerRequest, ueID)
	if response != nil {
		stats.IncrementUdmUeContextManagementStats("create", "amf-3gpp-access", "SUCCESS")
		// status code is based on SPEC, and option headers
		return httpwrapper.NewResponse(http.StatusCreated, header, response)
	} else if problemDetails != nil {
		stats.IncrementUdmUeContextManagementStats("create", "amf-3gpp-access", "FAILURE")
		return httpwrapper.NewResponse(int(problemDetails.Status), nil, problemDetails)
	} else {
		stats.IncrementUdmUeContextManagementStats("create", "amf-3gpp-access", "SUCCESS")
		return httpwrapper.NewResponse(http.StatusNoContent, nil, nil)
	}
}

// RegistrationAmf3gppAccessProcedure TS 29.503 5.3.2.2.2
func RegistrationAmf3gppAccessProcedure(registerRequest models.Amf3GppAccessRegistration, ueID string) (
	header http.Header, response *models.Amf3GppAccessRegistration, problemDetails *models.ProblemDetails,
) {
	// TODO: EPS interworking with N26 is not supported yet in this stage
	var oldAmf3GppAccessRegContext *models.Amf3GppAccessRegistration
	if udmContext.UDM_Self().UdmAmf3gppRegContextExists(ueID) {
		ue, _ := udmContext.UDM_Self().UdmUeFindBySupi(ueID)
		oldAmf3GppAccessRegContext = ue.Amf3GppAccessRegistration
	}

	udmContext.UDM_Self().CreateAmf3gppRegContext(ueID, registerRequest)

	clientAPI, err := createUDMClientToUDR(ueID)
	if err != nil {
		return nil, nil, util.ProblemDetailsSystemFailure(err.Error())
	}

	var createAmfContext3gppParamOpts Nudr_DataRepository.CreateAmfContext3gppParamOpts
	optInterface := optional.NewInterface(registerRequest)
	createAmfContext3gppParamOpts.Amf3GppAccessRegistration = optInterface
	resp, err := clientAPI.AMF3GPPAccessRegistrationDocumentApi.CreateAmfContext3gpp(context.Background(),
		ueID, &createAmfContext3gppParamOpts)
	if err != nil {
		// Explicitly check if the error is actually a 201 Created success
		if resp != nil && resp.StatusCode == 201 {
			logger.UecmLog.Infoln("UDR returned 201 Created")
		} else {
			logger.UecmLog.Errorln("CreateAmfContext3gpp error : ", err)
			problemDetails = &models.ProblemDetails{
				Status: int32(resp.StatusCode),
				Cause:  err.(openapi.GenericOpenAPIError).Model().(models.ProblemDetails).Cause,
				Detail: err.Error(),
			}
			return nil, nil, problemDetails
		}
	}
	defer func() {
		if rspCloseErr := resp.Body.Close(); rspCloseErr != nil {
			logger.UecmLog.Errorf("CreateAmfContext3gpp response body cannot close: %+v", rspCloseErr)
		}
	}()

	// TS 23.502 4.2.2.2.2 14d: UDM initiate a Nudm_UECM_DeregistrationNotification to the old AMF
	// corresponding to the same (e.g. 3GPP) access, if one exists
	if oldAmf3GppAccessRegContext != nil {
		deregistData := models.DeregistrationData{
			DeregReason: models.DeregistrationReason_SUBSCRIPTION_WITHDRAWN,
			AccessType:  models.AccessType__3_GPP_ACCESS,
		}
		callback.SendOnDeregistrationNotification(ueID, oldAmf3GppAccessRegContext.DeregCallbackUri,
			deregistData) // Deregistration Notify Triggered

		return nil, nil, nil
	} else {
		header = make(http.Header)
		udmUe, _ := udmContext.UDM_Self().UdmUeFindBySupi(ueID)
		header.Set("Location", udmUe.GetLocationURI(udmContext.LocationUriAmf3GppAccessRegistration))
		return header, &registerRequest, nil
	}
}

// HandleRegisterAmfNon3gppAccessRequest TS 29.503 5.3.2.2.3
func HandleRegisterAmfNon3gppAccessRequest(request *httpwrapper.Request) *httpwrapper.Response {
	logger.UecmLog.Infoln("handle RegisterAmfNon3gppAccessRequest")
	registerRequest := request.Body.(models.AmfNon3GppAccessRegistration)
	ueID := request.Params["ueId"]
	header, response, problemDetails := RegisterAmfNon3gppAccessProcedure(registerRequest, ueID)
	if response != nil {
		stats.IncrementUdmUeContextManagementStats("create", "amf-non-3gpp-access", "SUCCESS")
		// status code is based on SPEC, and option headers
		return httpwrapper.NewResponse(http.StatusCreated, header, response)
	} else if problemDetails != nil {
		stats.IncrementUdmUeContextManagementStats("create", "amf-non-3gpp-access", "FAILURE")
		return httpwrapper.NewResponse(int(problemDetails.Status), nil, problemDetails)
	} else {
		stats.IncrementUdmUeContextManagementStats("create", "amf-non-3gpp-access", "SUCCESS")
		return httpwrapper.NewResponse(http.StatusNoContent, nil, nil)
	}
}

func RegisterAmfNon3gppAccessProcedure(registerRequest models.AmfNon3GppAccessRegistration, ueID string) (
	header http.Header, response *models.AmfNon3GppAccessRegistration, problemDetails *models.ProblemDetails,
) {
	var oldAmfNon3GppAccessRegContext *models.AmfNon3GppAccessRegistration
	if udmContext.UDM_Self().UdmAmfNon3gppRegContextExists(ueID) {
		ue, _ := udmContext.UDM_Self().UdmUeFindBySupi(ueID)
		oldAmfNon3GppAccessRegContext = ue.AmfNon3GppAccessRegistration
	}

	udmContext.UDM_Self().CreateAmfNon3gppRegContext(ueID, registerRequest)

	clientAPI, err := createUDMClientToUDR(ueID)
	if err != nil {
		return nil, nil, util.ProblemDetailsSystemFailure(err.Error())
	}

	var createAmfContextNon3gppParamOpts Nudr_DataRepository.CreateAmfContextNon3gppParamOpts
	optInterface := optional.NewInterface(registerRequest)
	createAmfContextNon3gppParamOpts.AmfNon3GppAccessRegistration = optInterface
	resp, err := clientAPI.AMFNon3GPPAccessRegistrationDocumentApi.CreateAmfContextNon3gpp(
		context.Background(), ueID, &createAmfContextNon3gppParamOpts)
	if err != nil {
		problemDetails = &models.ProblemDetails{
			Status: int32(resp.StatusCode),
			Cause:  err.(openapi.GenericOpenAPIError).Model().(models.ProblemDetails).Cause,
			Detail: err.Error(),
		}
		return nil, nil, problemDetails
	}
	defer func() {
		if rspCloseErr := resp.Body.Close(); rspCloseErr != nil {
			logger.UecmLog.Errorf("CreateAmfContext3gpp response body cannot close: %+v", rspCloseErr)
		}
	}()

	// TS 23.502 4.2.2.2.2 14d: UDM initiate a Nudm_UECM_DeregistrationNotification to the old AMF
	// corresponding to the same (e.g. 3GPP) access, if one exists
	if oldAmfNon3GppAccessRegContext != nil {
		deregistData := models.DeregistrationData{
			DeregReason: models.DeregistrationReason_SUBSCRIPTION_WITHDRAWN,
			AccessType:  models.AccessType_NON_3_GPP_ACCESS,
		}
		callback.SendOnDeregistrationNotification(ueID, oldAmfNon3GppAccessRegContext.DeregCallbackUri,
			deregistData) // Deregistration Notify Triggered

		return nil, nil, nil
	} else {
		header = make(http.Header)
		udmUe, _ := udmContext.UDM_Self().UdmUeFindBySupi(ueID)
		header.Set("Location", udmUe.GetLocationURI(udmContext.LocationUriAmfNon3GppAccessRegistration))
		return header, &registerRequest, nil
	}
}

// HandleUpdateAmf3gppAccessRequest TODO: ueID may be SUPI or GPSI, but this function did not handle this condition
func HandleUpdateAmf3gppAccessRequest(request *httpwrapper.Request) *httpwrapper.Response {
	logger.UecmLog.Infoln("handle UpdateAmf3gppAccessRequest")
	amf3GppAccessRegistrationModification := request.Body.(models.Amf3GppAccessRegistrationModification)
	ueID := request.Params["ueId"]
	problemDetails := UpdateAmf3gppAccessProcedure(amf3GppAccessRegistrationModification, ueID)
	if problemDetails != nil {
		stats.IncrementUdmUeContextManagementStats("update", "amf-3gpp-access", "FAILURE")
		return httpwrapper.NewResponse(int(problemDetails.Status), nil, problemDetails)
	} else {
		stats.IncrementUdmUeContextManagementStats("update", "amf-3gpp-access", "SUCCESS")
		return httpwrapper.NewResponse(http.StatusNoContent, nil, nil)
	}
}

func UpdateAmf3gppAccessProcedure(request models.Amf3GppAccessRegistrationModification, ueID string) (
	problemDetails *models.ProblemDetails,
) {
	var patchItemReqArray []models.PatchItem
	currentContext := udmContext.UDM_Self().GetAmf3gppRegContext(ueID)
	if currentContext == nil {
		logger.UecmLog.Errorln("[UpdateAmf3gppAccess] Empty Amf3gppRegContext")
		problemDetails = &models.ProblemDetails{
			Status: http.StatusNotFound,
			Cause:  "CONTEXT_NOT_FOUND",
		}
		return problemDetails
	}

	if request.Guami != nil {
		udmUe, _ := udmContext.UDM_Self().UdmUeFindBySupi(ueID)
		if udmUe.SameAsStoredGUAMI3gpp(*request.Guami) { // deregistration
			logger.UecmLog.Infoln("UpdateAmf3gppAccess - deregistration")
			request.PurgeFlag = true
		} else {
			logger.UecmLog.Errorln("INVALID_GUAMI")
			problemDetails = &models.ProblemDetails{
				Status: http.StatusForbidden,
				Cause:  "INVALID_GUAMI",
			}
			return problemDetails
		}

		var patchItemTmp models.PatchItem
		patchItemTmp.Path = "/" + "Guami"
		patchItemTmp.Op = models.PatchOperation_REPLACE
		patchItemTmp.Value = *request.Guami
		patchItemReqArray = append(patchItemReqArray, patchItemTmp)
	}

	if request.PurgeFlag {
		var patchItemTmp models.PatchItem
		patchItemTmp.Path = "/" + "PurgeFlag"
		patchItemTmp.Op = models.PatchOperation_REPLACE
		patchItemTmp.Value = request.PurgeFlag
		patchItemReqArray = append(patchItemReqArray, patchItemTmp)
	}

	if request.Pei != "" {
		var patchItemTmp models.PatchItem
		patchItemTmp.Path = "/" + "Pei"
		patchItemTmp.Op = models.PatchOperation_REPLACE
		patchItemTmp.Value = request.Pei
		patchItemReqArray = append(patchItemReqArray, patchItemTmp)
	}

	if request.ImsVoPs != "" {
		var patchItemTmp models.PatchItem
		patchItemTmp.Path = "/" + "ImsVoPs"
		patchItemTmp.Op = models.PatchOperation_REPLACE
		patchItemTmp.Value = request.ImsVoPs
		patchItemReqArray = append(patchItemReqArray, patchItemTmp)
	}

	if request.BackupAmfInfo != nil {
		var patchItemTmp models.PatchItem
		patchItemTmp.Path = "/" + "BackupAmfInfo"
		patchItemTmp.Op = models.PatchOperation_REPLACE
		patchItemTmp.Value = request.BackupAmfInfo
		patchItemReqArray = append(patchItemReqArray, patchItemTmp)
	}

	clientAPI, err := createUDMClientToUDR(ueID)
	if err != nil {
		return util.ProblemDetailsSystemFailure(err.Error())
	}

	resp, err := clientAPI.AMF3GPPAccessRegistrationDocumentApi.AmfContext3gpp(context.Background(), ueID,
		patchItemReqArray)
	if err != nil {
		problemDetails = &models.ProblemDetails{
			Status: int32(resp.StatusCode),
			Cause:  err.(openapi.GenericOpenAPIError).Model().(models.ProblemDetails).Cause,
			Detail: err.Error(),
		}

		return problemDetails
	}
	defer func() {
		if rspCloseErr := resp.Body.Close(); rspCloseErr != nil {
			logger.UecmLog.Errorf("AmfContext3gpp response body cannot close: %+v", rspCloseErr)
		}
	}()

	return nil
}

// HandleUpdateAmfNon3gppAccessRequest TODO: ueID may be SUPI or GPSI, but this function did not handle this condition
func HandleUpdateAmfNon3gppAccessRequest(request *httpwrapper.Request) *httpwrapper.Response {
	logger.UecmLog.Infoln("handle UpdateAmfNon3gppAccessRequest")
	requestMSG := request.Body.(models.AmfNon3GppAccessRegistrationModification)
	ueID := request.Params["ueId"]
	problemDetails := UpdateAmfNon3gppAccessProcedure(requestMSG, ueID)
	if problemDetails != nil {
		stats.IncrementUdmUeContextManagementStats("update", "amf-non-3gpp-access", "FAILURE")
		return httpwrapper.NewResponse(int(problemDetails.Status), nil, problemDetails)
	} else {
		stats.IncrementUdmUeContextManagementStats("update", "amf-non-3gpp-access", "SUCCESS")
		return httpwrapper.NewResponse(http.StatusNoContent, nil, nil)
	}
}

func UpdateAmfNon3gppAccessProcedure(request models.AmfNon3GppAccessRegistrationModification, ueID string) (
	problemDetails *models.ProblemDetails,
) {
	var patchItemReqArray []models.PatchItem
	currentContext := udmContext.UDM_Self().GetAmfNon3gppRegContext(ueID)
	if currentContext == nil {
		logger.UecmLog.Errorln("[UpdateAmfNon3gppAccess] Empty AmfNon3gppRegContext")
		problemDetails = &models.ProblemDetails{
			Status: http.StatusNotFound,
			Cause:  "CONTEXT_NOT_FOUND",
		}
		return problemDetails
	}

	if request.Guami != nil {
		udmUe, _ := udmContext.UDM_Self().UdmUeFindBySupi(ueID)
		if udmUe.SameAsStoredGUAMINon3gpp(*request.Guami) { // deregistration
			logger.UecmLog.Infoln("UpdateAmfNon3gppAccess - deregistration")
			request.PurgeFlag = true
		} else {
			logger.UecmLog.Errorln("INVALID_GUAMI")
			problemDetails = &models.ProblemDetails{
				Status: http.StatusForbidden,
				Cause:  "INVALID_GUAMI",
			}
			return problemDetails
		}

		var patchItemTmp models.PatchItem
		patchItemTmp.Path = "/" + "Guami"
		patchItemTmp.Op = models.PatchOperation_REPLACE
		patchItemTmp.Value = *request.Guami
		patchItemReqArray = append(patchItemReqArray, patchItemTmp)
	}

	if request.PurgeFlag {
		var patchItemTmp models.PatchItem
		patchItemTmp.Path = "/" + "PurgeFlag"
		patchItemTmp.Op = models.PatchOperation_REPLACE
		patchItemTmp.Value = request.PurgeFlag
		patchItemReqArray = append(patchItemReqArray, patchItemTmp)
	}

	if request.Pei != "" {
		var patchItemTmp models.PatchItem
		patchItemTmp.Path = "/" + "Pei"
		patchItemTmp.Op = models.PatchOperation_REPLACE
		patchItemTmp.Value = request.Pei
		patchItemReqArray = append(patchItemReqArray, patchItemTmp)
	}

	if request.ImsVoPs != "" {
		var patchItemTmp models.PatchItem
		patchItemTmp.Path = "/" + "ImsVoPs"
		patchItemTmp.Op = models.PatchOperation_REPLACE
		patchItemTmp.Value = request.ImsVoPs
		patchItemReqArray = append(patchItemReqArray, patchItemTmp)
	}

	if request.BackupAmfInfo != nil {
		var patchItemTmp models.PatchItem
		patchItemTmp.Path = "/" + "BackupAmfInfo"
		patchItemTmp.Op = models.PatchOperation_REPLACE
		patchItemTmp.Value = request.BackupAmfInfo
		patchItemReqArray = append(patchItemReqArray, patchItemTmp)
	}

	clientAPI, err := createUDMClientToUDR(ueID)
	if err != nil {
		return util.ProblemDetailsSystemFailure(err.Error())
	}

	resp, err := clientAPI.AMFNon3GPPAccessRegistrationDocumentApi.AmfContextNon3gpp(context.Background(),
		ueID, patchItemReqArray)
	if err != nil {
		problemDetails = &models.ProblemDetails{
			Status: int32(resp.StatusCode),
			Cause:  err.(openapi.GenericOpenAPIError).Model().(models.ProblemDetails).Cause,
			Detail: err.Error(),
		}
		return problemDetails
	}
	defer func() {
		if rspCloseErr := resp.Body.Close(); rspCloseErr != nil {
			logger.UecmLog.Errorf("AmfContextNon3gpp response body cannot close: %+v", rspCloseErr)
		}
	}()

	return nil
}

func HandleDeregistrationSmfRegistrations(request *httpwrapper.Request) *httpwrapper.Response {
	logger.UecmLog.Infoln("handle DeregistrationSmfRegistrations")
	ueID := request.Params["ueId"]
	pduSessionID := request.Params["pduSessionId"]
	problemDetails := DeregistrationSmfRegistrationsProcedure(ueID, pduSessionID)
	if problemDetails != nil {
		stats.IncrementUdmUeContextManagementStats("delete", "smf-registrations", "FAILURE")
		return httpwrapper.NewResponse(int(problemDetails.Status), nil, problemDetails)
	} else {
		stats.IncrementUdmUeContextManagementStats("delete", "smf-registrations", "SUCCESS")
		return httpwrapper.NewResponse(http.StatusNoContent, nil, nil)
	}
}

func DeregistrationSmfRegistrationsProcedure(ueID string, pduSessionID string) (problemDetails *models.ProblemDetails) {
	clientAPI, err := createUDMClientToUDR(ueID)
	if err != nil {
		return util.ProblemDetailsSystemFailure(err.Error())
	}

	resp, err := clientAPI.SMFRegistrationDocumentApi.DeleteSmfContext(context.Background(), ueID, pduSessionID)
	if err != nil {
		problemDetails = &models.ProblemDetails{
			Status: int32(resp.StatusCode),
			Cause:  err.(openapi.GenericOpenAPIError).Model().(models.ProblemDetails).Cause,
			Detail: err.Error(),
		}
		return problemDetails
	}
	defer func() {
		if rspCloseErr := resp.Body.Close(); rspCloseErr != nil {
			logger.UecmLog.Errorf("DeleteSmfContext response body cannot close: %+v", rspCloseErr)
		}
	}()

	return nil
}

func HandleRegistrationSmfRegistrationsRequest(request *httpwrapper.Request) *httpwrapper.Response {
	logger.UecmLog.Infoln("handle RegistrationSmfRegistrations")
	registerRequest := request.Body.(models.SmfRegistration)
	ueID := request.Params["ueId"]
	pduSessionID := request.Params["pduSessionId"]

	header, response, problemDetails := RegistrationSmfRegistrationsProcedure(&registerRequest, ueID, pduSessionID)

	if problemDetails != nil {
		// Proper 3GPP Content-Type for Errors (TS 29.571 Section 5.2.2)
		errHeader := make(http.Header)
		errHeader.Set("Content-Type", "application/problem+json")
		return httpwrapper.NewResponse(int(problemDetails.Status), errHeader, problemDetails)
	}

	if response != nil {
		stats.IncrementUdmUeContextManagementStats("create", "smf-registrations", "SUCCESS")
		return httpwrapper.NewResponse(http.StatusCreated, header, response)
	}

	stats.IncrementUdmUeContextManagementStats("create", "smf-registrations", "SUCCESS")
	return httpwrapper.NewResponse(http.StatusNoContent, nil, nil)
}

// RegistrationSmfRegistrationsProcedure
func RegistrationSmfRegistrationsProcedure(request *models.SmfRegistration, ueID string, pduSessionID string) (
	header http.Header, response *models.SmfRegistration, problemDetails *models.ProblemDetails,
) {
	// Maintain local UDM state
	contextExisted := false
	udmContext.UDM_Self().CreateSmfRegContext(ueID, pduSessionID)
	if !udmContext.UDM_Self().UdmSmfRegContextNotExists(ueID) {
		contextExisted = true
	}

	// Setup UDR Client
	clientAPI, err := createUDMClientToUDR(ueID)
	if err != nil {
		return nil, nil, util.ProblemDetailsSystemFailure(err.Error())
	}

	// The UDM must authorize the request based on subscription data presence and access barring.
	var servingPlmnStr string
	if request.PlmnId != nil {
		servingPlmnStr = request.PlmnId.Mcc + request.PlmnId.Mnc
	}

	// Fetch Subscription Data to verify authorization
	amData, resp, errQuery := clientAPI.AccessAndMobilitySubscriptionDataDocumentApi.
		QueryAmData(context.Background(), ueID, servingPlmnStr, nil)

	if errQuery != nil {
		// If the UE does not have required subscription data, return 403 Forbidden.
		if resp != nil && resp.StatusCode == http.StatusNotFound {
			logger.UecmLog.Warnf("Authorization Failed: No subscription data for UE %s", ueID)
			return nil, nil, &models.ProblemDetails{
				Status: http.StatusNotFound,
				Cause:  "USER_NOT_FOUND",
				Detail: "The UE does not have the required subscription data to authorize this operation",
			}
		}
		logger.UecmLog.Warnf("UDR query error during authorization: %v", errQuery)
	} else if resp.StatusCode == http.StatusOK {
		// Check for Access Barring (Operator Determined Barring)
		if string(amData.OdbPacketServices) != "" {
			logger.UecmLog.Warnf("Authorization Failed: Access Barring (ODB) active for UE %s", ueID)
			return nil, nil, &models.ProblemDetails{
				Status: http.StatusForbidden,
				Cause:  "ROAMING_NOT_ALLOWED",
				Detail: "Access is restricted based on ODB",
			}
		}

		// TODO: Implement Roaming Restriction Procedure
		// Currently bypassed as this is a Private 5G implementation.
		//
		// Proper 3GPP Procedure for Roaming Authorization:
		// 1. Compare the Serving PLMN (request.PlmnId) with the UDM's Home PLMN Support List (factory.UdmConfig).
		// 2. If the PLMNs do not match, the UE is officially roaming.
		// 3. Evaluate 'RoamingRestrictions' or 'ForbiddenAreas' within the retrieved Subscription Data (amData).
		// 4. If roaming is restricted for the Serving PLMN, return 403 Forbidden with cause "ROAMING_NOT_ALLOWED".
		//
		// References:
		// - 3GPP TS 23.502 4.3.2.2.1 (PDU Session Establishment procedure)
		// - 3GPP TS 29.503 5.3.2.2.4 (SMF registration authorization)
	}

	if resp != nil && resp.Body != nil {
		resp.Body.Close()
	}

	// Proceed with SMF Registration in UDR if authorized
	pduID64, errParse := strconv.ParseInt(pduSessionID, 10, 32)
	if errParse != nil {
		logger.UecmLog.Errorf("Invalid PDU Session ID [%s]: %v", pduSessionID, errParse)
		return nil, nil, &models.ProblemDetails{
			Status: http.StatusBadRequest,
			Cause:  "INVALID_PDU_SESSION_ID",
			Detail: "The PDU Session ID provided is not a valid integer",
		}
	}
	pduID32 := int32(pduID64)
	var opts Nudr_DataRepository.CreateSmfContextNon3gppParamOpts
	opts.SmfRegistration = optional.NewInterface(*request)

	resp, err = clientAPI.SMFRegistrationDocumentApi.CreateSmfContextNon3gpp(context.Background(), ueID, pduID32, &opts)
	if err != nil {
		if resp != nil && (resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusCreated) {
			logger.UecmLog.Debugln("UDR SMF Registration success")
		} else {
			problemDetails = &models.ProblemDetails{Status: http.StatusInternalServerError}
			if apiErr, ok := err.(openapi.GenericOpenAPIError); ok {
				if model := apiErr.Model(); model != nil {
					if pd, ok := model.(models.ProblemDetails); ok {
						problemDetails = &pd
					}
				}
			}
			return nil, nil, problemDetails
		}
	}

	if resp != nil && resp.Body != nil {
		resp.Body.Close()
	}

	// 5. Build Success Response
	if contextExisted {
		return nil, nil, nil
	}
	header = make(http.Header)
	udmUe, _ := udmContext.UDM_Self().UdmUeFindBySupi(ueID)
	header.Set("Location", udmUe.GetLocationURI(udmContext.LocationUriSmfRegistration))
	return header, request, nil
}
