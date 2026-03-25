// Copyright 2019 free5GC.org
//
// SPDX-License-Identifier: Apache-2.0
//

package producer

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json" // For decoding the UDR response body.
	"fmt"
	"io" // For reading the response body as a fallback.
	"math/big"
	"net/http"
	"reflect"
	"strings"

	"github.com/5GC-DEV/openapi-cdac"
	"github.com/5GC-DEV/openapi-cdac/Nudr_DataRepository"
	"github.com/5GC-DEV/openapi-cdac/models"
	"github.com/antihax/optional"
	udm_context "github.com/omec-project/udm/context"
	"github.com/omec-project/udm/logger"
	stats "github.com/omec-project/udm/metrics"
	"github.com/omec-project/udm/util"
	"github.com/omec-project/util/httpwrapper"
	"github.com/omec-project/util/milenage"
	"github.com/omec-project/util/ueauth"
	"github.com/omec-project/util/util_3gpp/suci"
)

const (
	SqnMAx    int64 = 0xFFFFFFFFFFFF
	ind       int64 = 32
	keyStrLen int   = 32
	opStrLen  int   = 32
	opcStrLen int   = 32
)

type milenageResult struct {
	macA, res, ck, ik, ak []byte
}

const (
	authenticationRejected = "AUTHENTICATION_REJECTED"
	userNotFoundError      = "USER_NOT_FOUND"
)

func aucSQN(opc, k, auts, rand []byte) ([]byte, []byte) {
	AK, SQNms := make([]byte, 6), make([]byte, 6)
	macS := make([]byte, 8)
	ConcSQNms := auts[:6]
	AMF, err := hex.DecodeString("0000")
	if err != nil {
		return nil, nil
	}

	logger.UeauLog.Debugln("ConcSQNms", ConcSQNms)

	err = milenage.F2345(opc, k, rand, nil, nil, nil, nil, AK)
	if err != nil {
		logger.UeauLog.Errorln("milenage F2345 err ", err)
	}

	for i := 0; i < 6; i++ {
		SQNms[i] = AK[i] ^ ConcSQNms[i]
	}

	err = milenage.F1(opc, k, rand, SQNms, AMF, nil, macS)
	if err != nil {
		logger.UeauLog.Errorln("milenage F1 err", err)
	}

	logger.UeauLog.Debugln("SQNms", SQNms)
	logger.UeauLog.Debugln("macS", macS)
	return SQNms, macS
}

// Replaced Sprintln with Sprint for format the hexadecimal string properly
func strictHex(s string, n int) string {
	l := len(s)
	if l < n {
		return fmt.Sprint(strings.Repeat("0", n-l) + s)
	} else {
		return s[l-n : l]
	}
}

func HandleGenerateAuthDataRequest(request *httpwrapper.Request) *httpwrapper.Response {
	logger.UeauLog.Infoln("handle GenerateAuthDataRequest")
	authInfoRequest := request.Body.(models.AuthenticationInfoRequest)
	supiOrSuci := request.Params["supiOrSuci"]
	response, problemDetails := GenerateAuthDataProcedure(authInfoRequest, supiOrSuci)
	if response != nil {
		stats.IncrementUdmUeAuthenticationStats("create", "SUCCESS")
		// status code is based on SPEC, and option headers
		return httpwrapper.NewResponse(http.StatusOK, nil, response)
	} else if problemDetails != nil {
		stats.IncrementUdmUeAuthenticationStats("create", "FAILURE")
		return httpwrapper.NewResponse(int(problemDetails.Status), nil, problemDetails)
	}
	problemDetails = &models.ProblemDetails{
		Status: http.StatusForbidden,
		Cause:  "UNSPECIFIED",
	}
	stats.IncrementUdmUeAuthenticationStats("create", "FAILURE")
	return httpwrapper.NewResponse(http.StatusForbidden, nil, problemDetails)
}

func HandleConfirmAuthDataRequest(request *httpwrapper.Request) *httpwrapper.Response {
	logger.UeauLog.Infoln("Handle ConfirmAuthDataRequest")

	authEvent := request.Body.(models.AuthEvent)
	supi := request.Params["supi"]

	// The procedure now returns all necessary components for the HTTP response.
	header, response, problemDetails := ConfirmAuthDataProcedure(authEvent, supi)
	// Handles the new return values from the procedure.
	if response != nil {
		stats.IncrementUdmUeAuthenticationStats("create", "SUCCESS")
		// Return a 201 Created with the header and body from the procedure.
		return httpwrapper.NewResponse(http.StatusCreated, header, response)
	} else if problemDetails != nil {
		stats.IncrementUdmUeAuthenticationStats("create", "FAILURE")
		return httpwrapper.NewResponse(int(problemDetails.Status), nil, problemDetails)
	}

	// Fallback error in case the procedure returns unexpectedly.
	problemDetails = &models.ProblemDetails{
		Status: http.StatusInternalServerError,
		Cause:  "UNSPECIFIED",
	}
	stats.IncrementUdmUeAuthenticationStats("create", "FAILURE")
	return httpwrapper.NewResponse(http.StatusInternalServerError, nil, problemDetails)
}

// extractAuthEventBody handles the complexity of retrieving the body from either the error object or the response stream.
func extractAuthEventBody(resp *http.Response, err error) ([]byte, error) {
	if err != nil {
		if openApiErr, ok := err.(openapi.GenericOpenAPIError); ok && len(openApiErr.Body()) > 0 {
			return openApiErr.Body(), nil
		}
	}
	if resp != nil && resp.Body != nil {
		return io.ReadAll(resp.Body)
	}
	return nil, fmt.Errorf("no response body available")
}

// buildAuthDataProblemDetails centralizes the error mapping logic.
func buildAuthDataProblemDetails(resp *http.Response, err error) *models.ProblemDetails {
	pd := &models.ProblemDetails{
		Status: http.StatusInternalServerError,
		Cause:  "UDR_ERROR",
		Detail: "Received an unexpected status code or error from UDR.",
	}
	if resp != nil {
		pd.Status = int32(resp.StatusCode)
	}
	if err != nil {
		if openApiErr, ok := err.(openapi.GenericOpenAPIError); ok {
			if prob, ok := openApiErr.Model().(models.ProblemDetails); ok {
				pd.Cause = prob.Cause
			}
		}
		pd.Detail = err.Error()
	}
	return pd
}

func ConfirmAuthDataProcedure(authEvent models.AuthEvent, supi string) (header http.Header, response *models.AuthEvent, problemDetails *models.ProblemDetails) {
	createAuthParam := Nudr_DataRepository.CreateAuthenticationStatusParamOpts{
		AuthEvent: optional.NewInterface(authEvent),
	}

	client, err := createUDMClientToUDR(supi)
	if err != nil {
		return nil, nil, util.ProblemDetailsSystemFailure(err.Error())
	}

	resp, err := client.AuthenticationStatusDocumentApi.CreateAuthenticationStatus(context.Background(), supi, &createAuthParam)
	if resp != nil {
		defer func() {
			if closeErr := resp.Body.Close(); closeErr != nil {
				logger.UeauLog.Errorf("CreateAuthenticationStatus response body cannot close: %+v", closeErr)
			}
		}()
	}

	// Exit early if the response is not 201 Created
	if resp == nil || resp.StatusCode != http.StatusCreated {
		return nil, nil, buildAuthDataProblemDetails(resp, err)
	}

	// Extract and Decode Body
	responseBody, readErr := extractAuthEventBody(resp, err)
	if readErr != nil {
		return nil, nil, util.ProblemDetailsSystemFailure("UDR Response Body Read Failure")
	}

	var createdEvent models.AuthEvent
	if decodeErr := json.Unmarshal(responseBody, &createdEvent); decodeErr != nil {
		return nil, nil, util.ProblemDetailsSystemFailure("UDR Response Decode Failure")
	}

	// Update Context
	ue, ok := udm_context.UDM_Self().UdmUeFindBySupi(supi)
	if !ok {
		ue = udm_context.UDM_Self().NewUdmUe(supi)
	}
	ue.LastAuthenticationEvent = &createdEvent

	// Success Response
	locationURI := udm_context.UDM_Self().GetLocationURI3(udm_context.LocationUriAuthEvents, supi, createdEvent.AuthEventId)
	header = make(http.Header)
	header.Set("Location", locationURI)
	return header, &createdEvent, nil
}

func GenerateAuthDataProcedure(authInfoRequest models.AuthenticationInfoRequest, supiOrSuci string) (*models.AuthenticationInfoResult, *models.ProblemDetails) {
	logger.UeauLog.Debugln("in GenerateAuthDataProcedure")

	// 1. Resolve Identity and Fetch Subscription Data
	supi, authSubs, client, prob := fetchAuthSubscription(supiOrSuci)
	if prob != nil {
		return nil, prob
	}

	// 2. Extract and Derive Credentials (K, OP, OPC)
	k, opc, prob := deriveAuthenticationKeys(authSubs)
	if prob != nil {
		return nil, prob
	}

	// 3. Manage SQN and RAND (Handle Resync and Increment)
	sqnBytes, randBytes, prob := handleSqnAndResync(client, supi, authSubs, authInfoRequest, k, opc)
	if prob != nil {
		return nil, prob
	}

	// 4. Run Milenage Algorithm
	mOut, err := runMilenage(k, opc, randBytes, sqnBytes)
	if err != nil {
		logger.UeauLog.Errorln("Milenage error:", err)
		return nil, util.ProblemDetailsSystemFailure("Milenage algorithm execution failed")
	}

	// 5. Derive Authentication Vector (5G AKA or EAP-AKA')
	response, prob := buildAuthResponse(authInfoRequest, authSubs, mOut, supi, randBytes)
	if prob != nil {
		return nil, prob
	}

	return response, nil
}

// fetchAuthSubscription handles SUCI-to-SUPI conversion and initial UDR data retrieval.
func fetchAuthSubscription(supiOrSuci string) (string, *models.AuthenticationSubscription, *Nudr_DataRepository.APIClient, *models.ProblemDetails) {
	supi, err := suci.ToSupi(supiOrSuci, udm_context.UDM_Self().SuciProfiles)
	if err != nil {
		logger.UeauLog.Errorln("suciToSupi error:", err.Error())
		return "", nil, nil, &models.ProblemDetails{Status: http.StatusForbidden, Cause: authenticationRejected, Detail: err.Error()}
	}

	client, err := createUDMClientToUDR(supi)
	if err != nil {
		return "", nil, nil, util.ProblemDetailsSystemFailure(err.Error())
	}

	authSubs, res, err := client.AuthenticationDataDocumentApi.QueryAuthSubsData(context.Background(), supi, nil)
	if err != nil {
		return "", nil, nil, mapUdrErrorToProblemDetails(res, err)
	}
	defer res.Body.Close()

	return supi, &authSubs, client, nil
}

// deriveAuthenticationKeys extracts K and identifies/generates OPC.
func deriveAuthenticationKeys(authSubs *models.AuthenticationSubscription) ([]byte, []byte, *models.ProblemDetails) {
	if authSubs.PermanentKey == nil || len(authSubs.PermanentKey.PermanentKeyValue) != keyStrLen {
		return nil, nil, &models.ProblemDetails{Status: http.StatusForbidden, Cause: authenticationRejected}
	}

	k, err := hex.DecodeString(authSubs.PermanentKey.PermanentKeyValue)
	if err != nil {
		return nil, nil, &models.ProblemDetails{Status: http.StatusForbidden, Cause: authenticationRejected, Detail: "K decode fail"}
	}

	var op, opc []byte
	if authSubs.Opc != nil && len(authSubs.Opc.OpcValue) == opcStrLen {
		opc, err = hex.DecodeString(authSubs.Opc.OpcValue)
		if err == nil {
			return k, opc, nil
		}
	}

	if authSubs.Milenage != nil && authSubs.Milenage.Op != nil && len(authSubs.Milenage.Op.OpValue) == opStrLen {
		op, err = hex.DecodeString(authSubs.Milenage.Op.OpValue)
		if err != nil {
			return nil, nil, &models.ProblemDetails{Status: http.StatusForbidden, Cause: authenticationRejected, Detail: "OP decode fail"}
		}
		opc, err = milenage.GenerateOPC(k, op)
		if err != nil {
			return nil, nil, &models.ProblemDetails{Status: http.StatusForbidden, Cause: authenticationRejected, Detail: "OPC derive fail"}
		}
		return k, opc, nil
	}

	return nil, nil, &models.ProblemDetails{Status: http.StatusForbidden, Cause: authenticationRejected}
}

func handleSqnAndResync(client *Nudr_DataRepository.APIClient, supi string, subs *models.AuthenticationSubscription, req models.AuthenticationInfoRequest, k, opc []byte) ([]byte, []byte, *models.ProblemDetails) {
	sqnStr := strictHex(subs.SequenceNumber, 12)
	randBytes := make([]byte, 16)
	if _, err := rand.Read(randBytes); err != nil {
		return nil, nil, util.ProblemDetailsSystemFailure("Random generator failed")
	}

	if req.ResynchronizationInfo != nil {
		var prob *models.ProblemDetails
		sqnStr, prob = performResync(supi, req.ResynchronizationInfo, k, opc, randBytes)
		if prob != nil {
			return nil, nil, prob
		}
	}

	if prob := updateSqnInUdr(client, supi, sqnStr); prob != nil {
		return nil, nil, prob
	}

	sqnBytes, err := hex.DecodeString(sqnStr)
	if err != nil {
		return nil, nil, util.ProblemDetailsSystemFailure("SQN string is not valid hex")
	}
	return sqnBytes, randBytes, nil
}

func performResync(supi string, resync *models.ResynchronizationInfo, k, opc, newRand []byte) (string, *models.ProblemDetails) {
	auts, err1 := hex.DecodeString(resync.Auts)
	oldRand, err2 := hex.DecodeString(resync.Rand)
	if err1 != nil || err2 != nil {
		return "", &models.ProblemDetails{
			Status: http.StatusForbidden,
			Cause:  authenticationRejected,
			Detail: "Resync parameters are not valid hex",
		}
	}

	sqnMs, macS := aucSQN(opc, k, auts, oldRand)
	if !reflect.DeepEqual(macS, auts[6:]) {
		logger.UeauLog.Errorln("Re-Sync MAC failed", supi)
		return "", &models.ProblemDetails{Status: http.StatusForbidden, Cause: "modification is rejected"}
	}

	bigSQN := big.NewInt(0).SetBytes(sqnMs)
	bigInc := big.NewInt(ind + 1)
	bigSQN.Add(bigSQN, bigInc).Mod(bigSQN, big.NewInt(SqnMAx))

	return strictHex(fmt.Sprintf("%x", bigSQN), 12), nil
}

func updateSqnInUdr(client *Nudr_DataRepository.APIClient, supi, currentSqnStr string) *models.ProblemDetails {
	bigSQN, _ := big.NewInt(0).SetString(currentSqnStr, 16)
	nextSqnStr := strictHex(fmt.Sprintf("%x", bigSQN.Add(bigSQN, big.NewInt(1))), 12)

	patch := []models.PatchItem{{Op: models.PatchOperation_REPLACE, Path: "/sequenceNumber", Value: nextSqnStr}}
	rsp, err := client.AuthenticationDataDocumentApi.ModifyAuthentication(context.Background(), supi, patch)
	if err != nil {
		return &models.ProblemDetails{Status: http.StatusForbidden, Cause: "modification is rejected", Detail: err.Error()}
	}
	rsp.Body.Close()
	return nil
}

func runMilenage(k, opc, rand, sqn []byte) (milenageResult, error) {
	amf, _ := hex.DecodeString("8000")
	res := milenageResult{
		macA: make([]byte, 8), ck: make([]byte, 16), ik: make([]byte, 16),
		res: make([]byte, 8), ak: make([]byte, 6),
	}

	// We check for errors from milenage functions as required by errcheck
	if err := milenage.F1(opc, k, rand, sqn, amf, res.macA, make([]byte, 8)); err != nil {
		return res, err
	}
	if err := milenage.F2345(opc, k, rand, res.res, res.ck, res.ik, res.ak, make([]byte, 6)); err != nil {
		return res, err
	}

	return res, nil
}

func buildAuthResponse(req models.AuthenticationInfoRequest, subs *models.AuthenticationSubscription, m milenageResult, supi string, randBytes []byte) (*models.AuthenticationInfoResult, *models.ProblemDetails) {
	amf, _ := hex.DecodeString("8000")
	sqnBytes, _ := hex.DecodeString(strictHex(subs.SequenceNumber, 12))

	sqnXorAk := make([]byte, 6)
	for i := 0; i < 6; i++ {
		sqnXorAk[i] = sqnBytes[i] ^ m.ak[i]
	}
	autn := append(append(sqnXorAk, amf...), m.macA...)

	av := &models.AuthenticationVector{Rand: hex.EncodeToString(randBytes), Autn: hex.EncodeToString(autn)}
	result := &models.AuthenticationInfoResult{Supi: supi, AuthenticationVector: av}

	key := append(m.ck, m.ik...)
	snName := []byte(req.ServingNetworkName)

	if subs.AuthenticationMethod == models.AuthMethod__5_G_AKA {
		result.AuthType = models.AuthType__5_G_AKA
		xresStar, err := ueauth.GetKDFValue(key, ueauth.FC_FOR_RES_STAR_XRES_STAR_DERIVATION, snName, ueauth.KDFLen(snName), randBytes, ueauth.KDFLen(randBytes), m.res, ueauth.KDFLen(m.res))
		if err != nil {
			return nil, util.ProblemDetailsSystemFailure(err.Error())
		}
		kausf, err := ueauth.GetKDFValue(key, ueauth.FC_FOR_KAUSF_DERIVATION, snName, ueauth.KDFLen(snName), sqnXorAk, ueauth.KDFLen(sqnXorAk))
		if err != nil {
			return nil, util.ProblemDetailsSystemFailure(err.Error())
		}
		av.XresStar = hex.EncodeToString(xresStar[len(xresStar)/2:])
		av.Kausf = hex.EncodeToString(kausf)
	} else {
		result.AuthType = models.AuthType_EAP_AKA_PRIME
		kdf, err := ueauth.GetKDFValue(key, ueauth.FC_FOR_CK_PRIME_IK_PRIME_DERIVATION, snName, ueauth.KDFLen(snName), sqnXorAk, ueauth.KDFLen(sqnXorAk))
		if err != nil {
			return nil, util.ProblemDetailsSystemFailure(err.Error())
		}
		av.Xres = hex.EncodeToString(m.res)
		av.CkPrime = hex.EncodeToString(kdf[:16])
		av.IkPrime = hex.EncodeToString(kdf[16:])
	}
	return result, nil
}

func mapUdrErrorToProblemDetails(res *http.Response, err error) *models.ProblemDetails {
	pd := &models.ProblemDetails{Detail: err.Error(), Status: http.StatusForbidden, Cause: authenticationRejected}
	if res != nil {
		switch res.StatusCode {
		case http.StatusNotFound:
			pd.Status, pd.Cause = http.StatusNotFound, userNotFoundError
		case http.StatusForbidden:
			pd.Status, pd.Cause = http.StatusForbidden, authenticationRejected
		default:
			pd.Status = http.StatusInternalServerError
		}
	}
	return pd
}

// New handler for the PUT request to delete an authentication event.
func HandleDeleteAuthRequest(request *httpwrapper.Request) *httpwrapper.Response {
	supi := request.Params["supi"]
	authEventId := request.Params["authEventId"]
	problemDetails := DeleteAuthProcedure(supi, authEventId)

	if problemDetails != nil {
		stats.IncrementUdmUeAuthenticationStats("delete", "FAILURE")
		return httpwrapper.NewResponse(int(problemDetails.Status), nil, problemDetails)
	}

	stats.IncrementUdmUeAuthenticationStats("delete", "SUCCESS")
	return httpwrapper.NewResponse(http.StatusNoContent, nil, nil)
}

// New procedure to handle the logic for deleting an authentication event.
func DeleteAuthProcedure(supi string, authEventId string) (problemDetails *models.ProblemDetails) {
	// Find the UE's context and check if an event has been stored.
	ue, ok := udm_context.UDM_Self().UdmUeFindBySupi(supi)
	if !ok || ue.LastAuthenticationEvent == nil {
		return &models.ProblemDetails{
			Status: http.StatusNotFound,
			Cause:  "CONTEXT_NOT_FOUND",
		}
	}
	// Validate that the ID from the request matches the one stored in memory.
	if ue.LastAuthenticationEvent.AuthEventId != authEventId {
		return &models.ProblemDetails{
			Status: http.StatusNotFound,
			Cause:  "NOT_FOUND",
			Detail: "The requested authEventId does not match the last known event.",
		}
	}
	// Remove the event from the context to prevent reuse.
	ue.LastAuthenticationEvent = nil
	logger.UeauLog.Infof("Authentication event removed from UDM context for SUPI [%s].", supi)
	return nil // Success
}
