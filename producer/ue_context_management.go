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

// handleOpenApiError safely extracts ProblemDetails from OpenAPI errors to prevent panics
func handleOpenApiError(err error, resp *http.Response) *models.ProblemDetails {
	prob := &models.ProblemDetails{
		Status: http.StatusInternalServerError,
		Detail: err.Error(),
	}

	if resp != nil {
		prob.Status = int32(resp.StatusCode)
	}

	// Safely check if error is the expected OpenAPI type using comma-ok syntax
	if openApiErr, ok := err.(openapi.GenericOpenAPIError); ok {
		if model, ok := openApiErr.Model().(models.ProblemDetails); ok {
			prob.Cause = model.Cause
			if model.Status != 0 {
				prob.Status = model.Status
			}
			if model.Detail != "" {
				prob.Detail = model.Detail
			}
		}
	}
	return prob
}

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
		return consumer.SendNFInstancesUDR(id, consumer.NFDiscoveryToUDRParamExtGroupId)
	} else if strings.Contains(id, "msisdn") || strings.Contains(id, "extid") {
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
		return nil, handleOpenApiError(err, resp)
	}
	if resp != nil && resp.Body != nil {
		defer resp.Body.Close()
	}

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
		return nil, handleOpenApiError(err, resp)
	}
	if resp != nil && resp.Body != nil {
		defer resp.Body.Close()
	}

	return &amfNon3GppAccessRegistration, nil
}

func HandleRegistrationAmf3gppAccessRequest(request *httpwrapper.Request) *httpwrapper.Response {
	logger.UecmLog.Infoln("handle RegistrationAmf3gppAccess")
	registerRequest := request.Body.(models.Amf3GppAccessRegistration)
	ueID := request.Params["ueId"]
	header, response, problemDetails := RegistrationAmf3gppAccessProcedure(registerRequest, ueID)
	if response != nil {
		stats.IncrementUdmUeContextManagementStats("create", "amf-3gpp-access", "SUCCESS")
		return httpwrapper.NewResponse(http.StatusCreated, header, response)
	} else if problemDetails != nil {
		stats.IncrementUdmUeContextManagementStats("create", "amf-3gpp-access", "FAILURE")
		return httpwrapper.NewResponse(int(problemDetails.Status), nil, problemDetails)
	} else {
		stats.IncrementUdmUeContextManagementStats("create", "amf-3gpp-access", "SUCCESS")
		return httpwrapper.NewResponse(http.StatusNoContent, nil, nil)
	}
}

func RegistrationAmf3gppAccessProcedure(registerRequest models.Amf3GppAccessRegistration, ueID string) (
	header http.Header, response *models.Amf3GppAccessRegistration, problemDetails *models.ProblemDetails,
) {
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
	createAmfContext3gppParamOpts.Amf3GppAccessRegistration = optional.NewInterface(registerRequest)
	resp, err := clientAPI.AMF3GPPAccessRegistrationDocumentApi.CreateAmfContext3gpp(context.Background(),
		ueID, &createAmfContext3gppParamOpts)
	if err != nil {
		if resp != nil && resp.StatusCode == 201 {
			logger.UecmLog.Infoln("UDR returned 201 Created")
		} else {
			return nil, nil, handleOpenApiError(err, resp)
		}
	}
	if resp != nil && resp.Body != nil {
		defer resp.Body.Close()
	}

	if oldAmf3GppAccessRegContext != nil {
		deregistData := models.DeregistrationData{
			DeregReason: models.DeregistrationReason_SUBSCRIPTION_WITHDRAWN,
			AccessType:  models.AccessType__3_GPP_ACCESS,
		}
		callback.SendOnDeregistrationNotification(ueID, oldAmf3GppAccessRegContext.DeregCallbackUri, deregistData)
		return nil, nil, nil
	} else {
		header = make(http.Header)
		udmUe, _ := udmContext.UDM_Self().UdmUeFindBySupi(ueID)
		header.Set("Location", udmUe.GetLocationURI(udmContext.LocationUriAmf3GppAccessRegistration))
		return header, &registerRequest, nil
	}
}

func HandleRegisterAmfNon3gppAccessRequest(request *httpwrapper.Request) *httpwrapper.Response {
	logger.UecmLog.Infoln("handle RegisterAmfNon3gppAccessRequest")
	registerRequest := request.Body.(models.AmfNon3GppAccessRegistration)
	ueID := request.Params["ueId"]
	header, response, problemDetails := RegisterAmfNon3gppAccessProcedure(registerRequest, ueID)
	if response != nil {
		stats.IncrementUdmUeContextManagementStats("create", "amf-non-3gpp-access", "SUCCESS")
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
	createAmfContextNon3gppParamOpts.AmfNon3GppAccessRegistration = optional.NewInterface(registerRequest)
	resp, err := clientAPI.AMFNon3GPPAccessRegistrationDocumentApi.CreateAmfContextNon3gpp(
		context.Background(), ueID, &createAmfContextNon3gppParamOpts)
	if err != nil {
		return nil, nil, handleOpenApiError(err, resp)
	}
	if resp != nil && resp.Body != nil {
		defer resp.Body.Close()
	}

	if oldAmfNon3GppAccessRegContext != nil {
		deregistData := models.DeregistrationData{
			DeregReason: models.DeregistrationReason_SUBSCRIPTION_WITHDRAWN,
			AccessType:  models.AccessType_NON_3_GPP_ACCESS,
		}
		callback.SendOnDeregistrationNotification(ueID, oldAmfNon3GppAccessRegContext.DeregCallbackUri, deregistData)
		return nil, nil, nil
	} else {
		header = make(http.Header)
		udmUe, _ := udmContext.UDM_Self().UdmUeFindBySupi(ueID)
		header.Set("Location", udmUe.GetLocationURI(udmContext.LocationUriAmfNon3GppAccessRegistration))
		return header, &registerRequest, nil
	}
}

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
		return &models.ProblemDetails{Status: http.StatusNotFound, Cause: "CONTEXT_NOT_FOUND"}
	}

	if request.Guami != nil {
		udmUe, _ := udmContext.UDM_Self().UdmUeFindBySupi(ueID)
		if udmUe.SameAsStoredGUAMI3gpp(*request.Guami) {
			request.PurgeFlag = true
		} else {
			return &models.ProblemDetails{Status: http.StatusForbidden, Cause: "INVALID_GUAMI"}
		}
		patchItemReqArray = append(patchItemReqArray, models.PatchItem{Path: "/Guami", Op: models.PatchOperation_REPLACE, Value: *request.Guami})
	}

	if request.PurgeFlag {
		patchItemReqArray = append(patchItemReqArray, models.PatchItem{Path: "/PurgeFlag", Op: models.PatchOperation_REPLACE, Value: request.PurgeFlag})
	}

	if request.Pei != "" {
		patchItemReqArray = append(patchItemReqArray, models.PatchItem{Path: "/Pei", Op: models.PatchOperation_REPLACE, Value: request.Pei})
	}

	clientAPI, err := createUDMClientToUDR(ueID)
	if err != nil {
		return util.ProblemDetailsSystemFailure(err.Error())
	}

	resp, err := clientAPI.AMF3GPPAccessRegistrationDocumentApi.AmfContext3gpp(context.Background(), ueID, patchItemReqArray)
	if err != nil {
		return handleOpenApiError(err, resp)
	}
	if resp != nil && resp.Body != nil {
		defer resp.Body.Close()
	}

	return nil
}

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
		return &models.ProblemDetails{Status: http.StatusNotFound, Cause: "CONTEXT_NOT_FOUND"}
	}

	if request.Guami != nil {
		udmUe, _ := udmContext.UDM_Self().UdmUeFindBySupi(ueID)
		if udmUe.SameAsStoredGUAMINon3gpp(*request.Guami) {
			request.PurgeFlag = true
		} else {
			return &models.ProblemDetails{Status: http.StatusForbidden, Cause: "INVALID_GUAMI"}
		}
		patchItemReqArray = append(patchItemReqArray, models.PatchItem{Path: "/Guami", Op: models.PatchOperation_REPLACE, Value: *request.Guami})
	}

	if request.PurgeFlag {
		patchItemReqArray = append(patchItemReqArray, models.PatchItem{Path: "/PurgeFlag", Op: models.PatchOperation_REPLACE, Value: request.PurgeFlag})
	}

	clientAPI, err := createUDMClientToUDR(ueID)
	if err != nil {
		return util.ProblemDetailsSystemFailure(err.Error())
	}

	resp, err := clientAPI.AMFNon3GPPAccessRegistrationDocumentApi.AmfContextNon3gpp(context.Background(), ueID, patchItemReqArray)
	if err != nil {
		return handleOpenApiError(err, resp)
	}
	if resp != nil && resp.Body != nil {
		defer resp.Body.Close()
	}

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
		return handleOpenApiError(err, resp)
	}
	if resp != nil && resp.Body != nil {
		defer resp.Body.Close()
	}

	return nil
}

func HandleRegistrationSmfRegistrationsRequest(request *httpwrapper.Request) *httpwrapper.Response {
	logger.UecmLog.Infoln("handle RegistrationSmfRegistrations")
	registerRequest := request.Body.(models.SmfRegistration)
	ueID := request.Params["ueId"]
	pduSessionID := request.Params["pduSessionId"]
	header, response, problemDetails := RegistrationSmfRegistrationsProcedure(&registerRequest, ueID, pduSessionID)
	if response != nil {
		stats.IncrementUdmUeContextManagementStats("create", "smf-registrations", "SUCCESS")
		return httpwrapper.NewResponse(http.StatusCreated, header, response)
	} else if problemDetails != nil {
		stats.IncrementUdmUeContextManagementStats("create", "smf-registrations", "FAILURE")
		return httpwrapper.NewResponse(int(problemDetails.Status), nil, problemDetails)
	} else {
		stats.IncrementUdmUeContextManagementStats("create", "smf-registrations", "SUCCESS")
		return httpwrapper.NewResponse(http.StatusNoContent, nil, nil)
	}
}

func RegistrationSmfRegistrationsProcedure(request *models.SmfRegistration, ueID string, pduSessionID string) (
	header http.Header, response *models.SmfRegistration, problemDetails *models.ProblemDetails,
) {
	contextExisted := false
	udmContext.UDM_Self().CreateSmfRegContext(ueID, pduSessionID)
	if !udmContext.UDM_Self().UdmSmfRegContextNotExists(ueID) {
		contextExisted = true
	}

	pduID64, _ := strconv.ParseInt(pduSessionID, 10, 32)
	pduID32 := int32(pduID64)

	var createSmfContextNon3gppParamOpts Nudr_DataRepository.CreateSmfContextNon3gppParamOpts
	// FIX: Dereference pointer to avoid type mismatch "should be SmfRegistration"
	createSmfContextNon3gppParamOpts.SmfRegistration = optional.NewInterface(*request)

	clientAPI, err := createUDMClientToUDR(ueID)
	if err != nil {
		return nil, nil, util.ProblemDetailsSystemFailure(err.Error())
	}

	resp, err := clientAPI.SMFRegistrationDocumentApi.CreateSmfContextNon3gpp(context.Background(), ueID,
		pduID32, &createSmfContextNon3gppParamOpts)
	if err != nil {
		logger.UecmLog.Errorf("UDR CreateSmfContext error: %v", err)
		return nil, nil, handleOpenApiError(err, resp)
	}
	if resp != nil && resp.Body != nil {
		defer resp.Body.Close()
	}

	if contextExisted {
		return nil, nil, nil
	} else {
		header = make(http.Header)
		udmUe, _ := udmContext.UDM_Self().UdmUeFindBySupi(ueID)
		header.Set("Location", udmUe.GetLocationURI(udmContext.LocationUriSmfRegistration))
		return header, request, nil
	}
}
