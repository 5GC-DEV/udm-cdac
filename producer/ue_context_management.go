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

// HandleRegistrationSmfRegistrationsRequest SmfRegistrations
func HandleRegistrationSmfRegistrationsRequest(request *httpwrapper.Request) *httpwrapper.Response {
	registerRequest := request.Body.(models.SmfRegistration)
	ueID := request.Params["ueId"]
	pduSessionID := request.Params["pduSessionId"]

	header, response, problemDetails := RegistrationSmfRegistrationsProcedure(&registerRequest, ueID, pduSessionID)

	if problemDetails != nil {
		// Proper 3GPP Error Content-Type (TS 29.571)
		errHeader := make(http.Header)
		errHeader.Set("Content-Type", "application/problem+json")
		return httpwrapper.NewResponse(int(problemDetails.Status), errHeader, problemDetails)
	}

	if response != nil {
		return httpwrapper.NewResponse(http.StatusCreated, header, response)
	}

	return httpwrapper.NewResponse(http.StatusNoContent, nil, nil)
}

// RegistrationSmfRegistrationsProcedure follows 3GPP TS 29.503 Section 5.3.2.10
func RegistrationSmfRegistrationsProcedure(request *models.SmfRegistration, ueID string, pduSessionID string) (
	header http.Header, response *models.SmfRegistration, problemDetails *models.ProblemDetails,
) {
	// 1. Setup UDR Client
	clientAPI, err := createUDMClientToUDR(ueID)
	if err != nil {
		return nil, nil, util.ProblemDetailsSystemFailure(err.Error())
	}

	// 2. Official Roaming Authorization (TS 23.501 Section 5.3.4.1)
	if request.PlmnId != nil {
		servingPlmn := request.PlmnId.Mcc + request.PlmnId.Mnc

		// Query AM Data for this specific Serving PLMN
		amData, resp, errQuery := clientAPI.AccessAndMobilitySubscriptionDataDocumentApi.
			QueryAmData(context.Background(), ueID, servingPlmn, nil)

		if errQuery != nil {
			// SPEC: If no subscription data exists for this PLMN, roaming is not authorized
			if resp != nil && resp.StatusCode == http.StatusNotFound {
				logger.UecmLog.Warnf("Roaming Unauthorized: No profile for PLMN %s", servingPlmn)
				return nil, nil, &models.ProblemDetails{
					Status: http.StatusForbidden,
					Cause:  "ROAMING_NOT_ALLOWED",
					Detail: "Subscriber has no roaming agreement with the serving network",
				}
			}
		} else if resp.StatusCode == http.StatusOK {
			// 3. Check Operator Determined Barring (ODB) for Roaming
			// 3GPP TS 29.503 Section 6.1.7.3: ROAMING_NOT_ALLOWED is sent if ODB blocks the service
			if string(amData.OdbPacketServices) != "" {
				logger.UecmLog.Warnf("Roaming Rejected: ODB active (%v)", amData.OdbPacketServices)
				return nil, nil, &models.ProblemDetails{
					Status: http.StatusForbidden,
					Cause:  "ROAMING_NOT_ALLOWED",
					Detail: "Roaming is barred by operator determined barring (ODB)",
				}
			}

			// 4. Check Roaming Restrictions Object (if populated)
			var isRoaming bool
			var servingPlmnId string

			if request.PlmnId != nil {
				servingPlmnId = request.PlmnId.Mcc + request.PlmnId.Mnc
				// Roaming Check: If IMSI prefix doesn't match the Serving PLMN
				isRoaming = !strings.HasPrefix(ueID, "imsi-"+servingPlmnId)
			}

			if isRoaming {
				logger.UecmLog.Infof("UE %s is detected as Roaming in PLMN %s", ueID, servingPlmnId)

				// 1. Fetch Subscription Data (Access And Mobility)
				amData, resp, errQuery := clientAPI.AccessAndMobilitySubscriptionDataDocumentApi.
					QueryAmData(context.Background(), ueID, servingPlmnId, nil)

				if errQuery != nil {
					// Proper Check 1: If UDR returns 404, there is no roaming agreement for this PLMN
					if resp != nil && resp.StatusCode == http.StatusNotFound {
						logger.UecmLog.Warnf("Registration Rejected: No profile for PLMN %s", servingPlmnId)
						return nil, nil, &models.ProblemDetails{
							Status: http.StatusForbidden,
							Cause:  "ROAMING_NOT_ALLOWED",
							Detail: "Subscriber has no roaming agreement with the serving network",
						}
					}
					logger.UecmLog.Warnf("Failed to query AM Data: %v", errQuery)
				} else if resp.StatusCode == http.StatusOK {
					// Proper Check 2: Check OdbPacketServices (Operator Determined Barring)
					// Cast to string to check if it's populated
					if string(amData.OdbPacketServices) != "" {
						logger.UecmLog.Warnf("Registration Rejected: ODB active (%v)", amData.OdbPacketServices)
						return nil, nil, &models.ProblemDetails{
							Status: http.StatusForbidden,
							Cause:  "ROAMING_NOT_ALLOWED",
							Detail: "Roaming is barred by operator determined barring (ODB)",
						}
					}
				}

				if resp != nil && resp.Body != nil {
					resp.Body.Close()
				}
			}
		}
	}
	// 5. Proceed with Registration if authorized
	pduID64, _ := strconv.ParseInt(pduSessionID, 10, 32)
	pduID32 := int32(pduID64)

	var opts Nudr_DataRepository.CreateSmfContextNon3gppParamOpts
	opts.SmfRegistration = optional.NewInterface(*request)

	resp, err := clientAPI.SMFRegistrationDocumentApi.CreateSmfContextNon3gpp(context.Background(), ueID, pduID32, &opts)

	if err != nil {
		// Handle non-standard 200/201 "errors" from library
		if resp != nil && (resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusCreated) {
			logger.UecmLog.Debugln("UDR saved successfully")
		} else {
			// Proper ProblemDetails Extraction
			problemDetails = &models.ProblemDetails{Status: http.StatusInternalServerError}
			if apiErr, ok := err.(openapi.GenericOpenAPIError); ok {
				if pd, ok := apiErr.Model().(models.ProblemDetails); ok {
					problemDetails = &pd
				}
			}
			return nil, nil, problemDetails
		}
	}

	// Cleanup and Success Response
	if resp != nil && resp.Body != nil {
		resp.Body.Close()
	}

	header = make(http.Header)
	udmUe, _ := udmContext.UDM_Self().UdmUeFindBySupi(ueID)
	header.Set("Location", udmUe.GetLocationURI(udmContext.LocationUriSmfRegistration))
	return header, request, nil
}
