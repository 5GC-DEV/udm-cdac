// SPDX-FileCopyrightText: 2022 Infosys Limited
// SPDX-FileCopyrightText: 2024 Canonical Ltd.
// Copyright 2019 free5GC.org
//
// SPDX-License-Identifier: Apache-2.0
//

package subscribecallback

import (
	"net/http"

	"github.com/5GC-DEV/openapi-cdac"
	"github.com/5GC-DEV/openapi-cdac/models"
	"github.com/gin-gonic/gin"
	"github.com/omec-project/udm/logger"
	"github.com/omec-project/udm/producer"
	"github.com/omec-project/util/httpwrapper"
)

const contentTypeJson = "application/json"

func HTTPNfSubscriptionStatusNotify(c *gin.Context) {
	var nfSubscriptionStatusNotification models.NotificationData

	requestBody, err := c.GetRawData()
	if err != nil {
		logger.CallbackLog.Errorf("get Request Body error: %+v", err)
		problemDetail := models.ProblemDetails{
			Title:  "System failure",
			Status: http.StatusInternalServerError,
			Detail: err.Error(),
			Cause:  "SYSTEM_FAILURE",
		}
		c.JSON(http.StatusInternalServerError, problemDetail)
		return
	}

	err = openapi.Deserialize(&nfSubscriptionStatusNotification, requestBody, contentTypeJson)
	if err != nil {
		problemDetail := "[Request Body] " + err.Error()
		rsp := models.ProblemDetails{
			Title:  "Malformed request syntax",
			Status: http.StatusBadRequest,
			Detail: problemDetail,
		}
		logger.CallbackLog.Errorln(problemDetail)
		c.JSON(http.StatusBadRequest, rsp)
		return
	}

	req := httpwrapper.NewRequest(c.Request, nfSubscriptionStatusNotification)

	rsp := producer.HandleNfSubscriptionStatusNotify(req)

	responseBody, err := openapi.Serialize(rsp.Body, contentTypeJson)
	if err != nil {
		logger.CallbackLog.Errorln(err)
		problemDetails := models.ProblemDetails{
			Status: http.StatusInternalServerError,
			Cause:  "SYSTEM_FAILURE",
			Detail: err.Error(),
		}
		c.JSON(http.StatusInternalServerError, problemDetails)
	} else if rsp.Body != nil {
		c.Data(rsp.Status, contentTypeJson, responseBody)
	}
}
