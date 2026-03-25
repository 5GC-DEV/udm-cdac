// SPDX-License-Identifier: Apache-2.0
// Copyright 2019 free5GC.org
// Copyright 2021 Open Networking Foundation <info@opennetworking.org>
// SPDX-FileCopyrightText: 2024 Canonical Ltd.
// Copyright 2022 Intel Corporation
//

package service

import (
	"bufio"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/5GC-DEV/openapi-cdac/models"
	nrfCache "github.com/5GC-DEV/openapi-cdac/nrfcache"
	grpcClient "github.com/omec-project/config5g/proto/client"
	protos "github.com/omec-project/config5g/proto/sdcoreConfig"
	"github.com/omec-project/udm/consumer"
	"github.com/omec-project/udm/context"
	"github.com/omec-project/udm/eventexposure"
	"github.com/omec-project/udm/factory"
	"github.com/omec-project/udm/httpcallback"
	"github.com/omec-project/udm/logger"
	"github.com/omec-project/udm/metrics"
	"github.com/omec-project/udm/parameterprovision"
	"github.com/omec-project/udm/subscribecallback"
	"github.com/omec-project/udm/subscriberdatamanagement"
	"github.com/omec-project/udm/ueauthentication"
	"github.com/omec-project/udm/uecontextmanagement"
	"github.com/omec-project/udm/util"
	"github.com/omec-project/util/http2_util"
	utilLogger "github.com/omec-project/util/logger"
	"github.com/urfave/cli/v3"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type UDM struct{}

var ConfigPodTrigger chan bool

func init() {
	ConfigPodTrigger = make(chan bool)
}

const (
	msgSendConfigTrigger = "send config trigger to main routine"
	errUpdateNrf         = "UDM update to NRF Error[%s]"
)

type (
	// Config information.
	Config struct {
		cfg string
	}
)

var config Config

var udmCLi = []cli.Flag{
	&cli.StringFlag{
		Name:     "cfg",
		Usage:    "udm config file",
		Required: true,
	},
}

var (
	KeepAliveTimer      *time.Timer
	KeepAliveTimerMutex sync.Mutex
)

func (*UDM) GetCliCmd() (flags []cli.Flag) {
	return udmCLi
}

func (udm *UDM) Initialize(c *cli.Command) error {
	config = Config{
		cfg: c.String("cfg"),
	}

	absPath, err := filepath.Abs(config.cfg)
	if err != nil {
		logger.CfgLog.Errorln(err)
		return err
	}

	if err := factory.InitConfigFactory(absPath); err != nil {
		return err
	}

	factory.UdmConfig.CfgLocation = absPath

	udm.setLogLevel()

	if err := factory.CheckConfigVersion(); err != nil {
		return err
	}

	if os.Getenv("MANAGED_BY_CONFIG_POD") == "true" {
		logger.InitLog.Infoln("MANAGED_BY_CONFIG_POD is true")
		go manageGrpcClient(factory.UdmConfig.Configuration.WebuiUri, udm)
	} else {
		go func() {
			logger.InitLog.Infoln("use helm chart config ")
			ConfigPodTrigger <- true
		}()
	}

	return nil
}

// manageGrpcClient connects the config pod GRPC server and subscribes the config changes.
// Then it updates UDM configuration.
// checkClientHealth monitors connectivity and handles cleanup if the client is unreachable for too long.
func checkClientHealth(client grpcClient.ConfClient, count *int) bool {
	if client.CheckGrpcConnectivity() == "READY" {
		*count = 0 // Reset counter on successful connection
		return true
	}

	// Not READY: Wait and increment retry counter
	logger.InitLog.Infoln("checking the connectivity readiness")
	time.Sleep(time.Second * 30)
	*count++

	if *count > 5 {
		if err := client.GetConfigClientConn().Close(); err != nil {
			logger.InitLog.Infof("failing ConfigClient is not closed properly: %+v", err)
		}
		*count = 0
		return false // Signal that client should be reset
	}
	return true // Stay in current state, try again next iteration
}

// ensureConfigSubscription ensures the GRPC stream and config channel are initialized.
func ensureConfigSubscription(
	client grpcClient.ConfClient,
	stream *protos.ConfigService_NetworkSliceSubscribeClient,
	configChannel *chan *protos.NetworkSliceResponse,
	udm *UDM,
) {
	var err error
	// 1. Ensure Stream
	if *stream == nil {
		*stream, err = client.SubscribeToConfigServer()
		if err != nil {
			logger.InitLog.Infof("failing SubscribeToConfigServer: %+v", err)
			return
		}
	}

	// 2. Ensure Channel and Start Observer
	if *configChannel == nil {
		*configChannel = client.PublishOnConfigChange(true, *stream)
		logger.InitLog.Infoln("PublishOnConfigChange is triggered")
		go udm.updateConfig(*configChannel)
		logger.InitLog.Infoln("UDM updateConfig is triggered")
	}
}

// manageGrpcClient connects the config pod GRPC server and subscribes the config changes.
func manageGrpcClient(webuiUri string, udm *UDM) {
	var configChannel chan *protos.NetworkSliceResponse
	var client grpcClient.ConfClient
	var stream protos.ConfigService_NetworkSliceSubscribeClient
	count := 0

	for {
		// State: Disconnected - Attempt to connect
		if client == nil {
			logger.InitLog.Infoln("connecting to config server")
			var err error
			client, err = grpcClient.ConnectToConfigServer(webuiUri)
			stream = nil
			configChannel = nil
			if err != nil {
				logger.InitLog.Errorf("connection failed: %+v", err)
				time.Sleep(time.Second * 5) // Backoff before retrying
			}
			continue
		}

		// State: Connected - Check Health
		if !checkClientHealth(client, &count) {
			client = nil // Trigger reconnection in next loop iteration
			continue
		}

		// State: Healthy - Ensure Subscriptions are active
		ensureConfigSubscription(client, &stream, &configChannel, udm)

		// Heartbeat sleep to prevent 100% CPU usage
		time.Sleep(time.Second * 5)
	}
}

func (udm *UDM) setLogLevel() {
	if factory.UdmConfig.Logger == nil {
		logger.InitLog.Warnln("UDM config without log level setting")
		return
	}

	if factory.UdmConfig.Logger.UDM != nil {
		if factory.UdmConfig.Logger.UDM.DebugLevel != "" {
			if level, err := zapcore.ParseLevel(factory.UdmConfig.Logger.UDM.DebugLevel); err != nil {
				logger.InitLog.Warnf("UDM Log level [%s] is invalid, set to [info] level",
					factory.UdmConfig.Logger.UDM.DebugLevel)
				logger.SetLogLevel(zap.InfoLevel)
			} else {
				logger.InitLog.Infof("UDM Log level is set to [%s] level", level)
				logger.SetLogLevel(level)
			}
		} else {
			logger.InitLog.Infoln("UDM Log level is default set to [info] level")
			logger.SetLogLevel(zap.InfoLevel)
		}
	}
}

func (udm *UDM) FilterCli(c *cli.Command) (args []string) {
	for _, flag := range udm.GetCliCmd() {
		name := flag.Names()[0]
		value := fmt.Sprint(c.Generic(name))
		if value == "" {
			continue
		}

		args = append(args, "--"+name, value)
	}
	return args
}

func (udm *UDM) Start() {
	config := factory.UdmConfig
	configuration := config.Configuration
	sbi := configuration.Sbi
	serviceName := configuration.ServiceList

	logger.InitLog.Infof("UDM Config Info: Version[%s] Description[%s]", config.Info.Version, config.Info.Description)

	logger.InitLog.Infoln("server started")

	router := utilLogger.NewGinWithZap(logger.GinLog)

	eventexposure.AddService(router)
	httpcallback.AddService(router)
	parameterprovision.AddService(router)
	subscriberdatamanagement.AddService(router)
	ueauthentication.AddService(router)
	uecontextmanagement.AddService(router)
	subscribecallback.AddService(router)

	go metrics.InitMetrics()

	self := context.UDM_Self()
	util.InitUDMContext(self)
	context.UDM_Self().InitNFService(serviceName, config.Info.Version)

	addr := fmt.Sprintf("%s:%d", self.BindingIPv4, self.SBIPort)
	if self.EnableNrfCaching {
		logger.InitLog.Infoln("enable NRF caching feature")
		nrfCache.InitNrfCaching(self.NrfCacheEvictionInterval*time.Second, consumer.SendNfDiscoveryToNrf)
	}
	go udm.RegisterNF()

	signalChannel := make(chan os.Signal, 1)
	signal.Notify(signalChannel, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-signalChannel
		udm.Terminate()
		os.Exit(0)
	}()

	sslLog := filepath.Dir(factory.UdmConfig.CfgLocation) + "/sslkey.log"
	server, err := http2_util.NewServer(addr, sslLog, router)
	if server == nil {
		logger.InitLog.Errorf("initialize HTTP server failed: %+v", err)
		return
	}

	if err != nil {
		logger.InitLog.Warnf("initialize HTTP server: +%v", err)
	}

	serverScheme := factory.UdmConfig.Configuration.Sbi.Scheme
	switch serverScheme {
	case "http":
		err = server.ListenAndServe()
	case "https":
		err = server.ListenAndServeTLS(sbi.Tls.Pem, sbi.Tls.Key)
	default:
		logger.InitLog.Fatalf("HTTP server setup failed: invalid server scheme %+v", serverScheme)
		return
	}

	if err != nil {
		logger.InitLog.Fatalf("HTTP server setup failed: %+v", err)
	}
}

func (udm *UDM) Exec(c *cli.Command) error {
	// UDM.Initialize(cfgPath, c)

	logger.InitLog.Debugln("args:", c.String("udmcfg"))
	args := udm.FilterCli(c)
	logger.InitLog.Debugln("filter:", args)
	command := exec.Command("./udm", args...)

	stdout, err := command.StdoutPipe()
	if err != nil {
		logger.InitLog.Fatalln(err)
	}
	wg := sync.WaitGroup{}
	wg.Add(3)
	go func() {
		in := bufio.NewScanner(stdout)
		for in.Scan() {
			logger.InitLog.Infoln(in.Text())
		}
		wg.Done()
	}()

	stderr, err := command.StderrPipe()
	if err != nil {
		logger.InitLog.Fatalln(err)
	}
	go func() {
		in := bufio.NewScanner(stderr)
		for in.Scan() {
			logger.InitLog.Infoln(in.Text())
		}
		wg.Done()
	}()

	go func() {
		if err = command.Start(); err != nil {
			logger.InitLog.Errorf("UDM start error: %v", err)
		}
		wg.Done()
	}()

	wg.Wait()

	return err
}

func (udm *UDM) Terminate() {
	logger.InitLog.Infoln("terminating UDM")
	// deregister with NRF
	problemDetails, err := consumer.SendDeregisterNFInstance()
	if problemDetails != nil {
		logger.InitLog.Errorf("deregister NF instance Failed Problem[%+v]", problemDetails)
	} else if err != nil {
		logger.InitLog.Errorf("deregister NF instance Error[%+v]", err)
	} else {
		logger.InitLog.Infoln("deregister from NRF successfully")
	}
	logger.InitLog.Infoln("UDM terminated")
}

// addPlmnFromSlice extracts PLMN information from a network slice and adds it to the context if it is unique.
func addPlmnFromSlice(self *context.UDMContext, ns *protos.NetworkSlice) {
	if ns.Site == nil || ns.Site.Plmn == nil {
		return
	}

	site := ns.Site
	newPlmn := site.Plmn

	// Check for duplicates in the current PlmnList
	for _, item := range self.PlmnList {
		if item.PlmnId.Mcc == newPlmn.Mcc && item.PlmnId.Mnc == newPlmn.Mnc {
			return
		}
	}

	// Add unique PLMN to context
	self.PlmnList = append(self.PlmnList, factory.PlmnSupportItem{
		PlmnId: models.PlmnId{
			Mcc: newPlmn.Mcc,
			Mnc: newPlmn.Mnc,
		},
	})
	logger.GrpcLog.Infof("plmn [%s:%s] added in the context", newPlmn.Mcc, newPlmn.Mnc)
}

// updateConfigTriggerState manages the minConfig state and notifies the main routine of changes.
func updateConfigTriggerState(minConfig *bool, plmnCount int) {
	hasPlmns := plmnCount > 0

	// State Machine: Trigger only if first config is received or if we are already in a configured state
	if !*minConfig && hasPlmns {
		*minConfig = true
		ConfigPodTrigger <- true
		logger.GrpcLog.Infoln(msgSendConfigTrigger)
	} else if *minConfig {
		*minConfig = hasPlmns
		ConfigPodTrigger <- hasPlmns
		logger.GrpcLog.Infoln(msgSendConfigTrigger)
	}
}

func (udm *UDM) updateConfig(commChannel chan *protos.NetworkSliceResponse) bool {
	var minConfig bool
	self := context.UDM_Self()

	for rsp := range commChannel {
		logger.GrpcLog.Infoln("received updateConfig in the udm app:", rsp)

		// Process each slice to update the PLMN list
		for _, ns := range rsp.NetworkSlice {
			logger.GrpcLog.Infoln("network Slice Name", ns.Name)
			addPlmnFromSlice(self, ns)
		}

		// Update the trigger status based on the current PlmnList size
		updateConfigTriggerState(&minConfig, len(self.PlmnList))
	}
	return true
}

func (udm *UDM) StartKeepAliveTimer(nfProfile models.NfProfile) {
	KeepAliveTimerMutex.Lock()
	defer KeepAliveTimerMutex.Unlock()
	udm.StopKeepAliveTimer()
	if nfProfile.HeartBeatTimer == 0 {
		nfProfile.HeartBeatTimer = 60
	}
	logger.InitLog.Infof("started KeepAlive Timer: %v sec", nfProfile.HeartBeatTimer)
	// AfterFunc starts timer and waits for KeepAliveTimer to elapse and then calls udm.UpdateNF function
	KeepAliveTimer = time.AfterFunc(time.Duration(nfProfile.HeartBeatTimer)*time.Second, udm.UpdateNF)
}

func (udm *UDM) StopKeepAliveTimer() {
	if KeepAliveTimer != nil {
		logger.InitLog.Infoln("stopped KeepAlive Timer")
		KeepAliveTimer.Stop()
		KeepAliveTimer = nil
	}
}

func (udm *UDM) BuildAndSendRegisterNFInstance() (models.NfProfile, error) {
	self := context.UDM_Self()
	profile, err := consumer.BuildNFInstance(self)
	if err != nil {
		logger.InitLog.Errorf("build UDM Profile Error: %v", err)
		return profile, err
	}
	logger.InitLog.Infof("UDM Profile Registering to NRF: %v", profile)
	// Indefinite attempt to register until success
	profile, _, self.NfId, err = consumer.SendRegisterNFInstance(self.NrfUri, self.NfId, profile)
	return profile, err
}

// UpdateNF is the callback function, this is called when keepalivetimer elapsed
func (udm *UDM) UpdateNF() {
	KeepAliveTimerMutex.Lock()
	defer KeepAliveTimerMutex.Unlock()
	if KeepAliveTimer == nil {
		logger.InitLog.Warnln("keepAlive timer has been stopped")
		return
	}
	// setting default value 30 sec
	var heartBeatTimer int32 = 30
	pitem := models.PatchItem{
		Op:    "replace",
		Path:  "/nfStatus",
		Value: "REGISTERED",
	}
	var patchItem []models.PatchItem
	patchItem = append(patchItem, pitem)
	nfProfile, problemDetails, err := consumer.SendUpdateNFInstance(patchItem)
	if problemDetails != nil {
		logger.InitLog.Errorf("UDM update to NRF ProblemDetails[%v]", problemDetails)
		// 5xx response from NRF, 404 Not Found, 400 Bad Request
		if (problemDetails.Status/100) == 5 ||
			problemDetails.Status == 404 || problemDetails.Status == 400 {
			// register with NRF full profile
			nfProfile, err = udm.BuildAndSendRegisterNFInstance()
			if err != nil {
				logger.InitLog.Errorf(errUpdateNrf, err.Error())
			}
		}
	} else if err != nil {
		logger.InitLog.Errorf(errUpdateNrf, err.Error())
		nfProfile, err = udm.BuildAndSendRegisterNFInstance()
		if err != nil {
			logger.InitLog.Errorf(errUpdateNrf, err.Error())
		}
	}

	if nfProfile.HeartBeatTimer != 0 {
		// use hearbeattimer value with received timer value from NRF
		heartBeatTimer = nfProfile.HeartBeatTimer
	}
	logger.InitLog.Debugf("restarted KeepAlive Timer: %v sec", heartBeatTimer)
	// restart timer with received HeartBeatTimer value
	KeepAliveTimer = time.AfterFunc(time.Duration(heartBeatTimer)*time.Second, udm.UpdateNF)
}

func (udm *UDM) RegisterNF() {
	self := context.UDM_Self()
	for msg := range ConfigPodTrigger {
		logger.InitLog.Infof("minimum configuration from config pod available %v", msg)
		profile, err := consumer.BuildNFInstance(self)
		if err != nil {
			logger.InitLog.Errorln(err.Error())
		} else {
			var prof models.NfProfile
			prof, _, self.NfId, err = consumer.SendRegisterNFInstance(self.NrfUri, self.NfId, profile)
			if err != nil {
				logger.InitLog.Errorln(err.Error())
			} else {
				udm.StartKeepAliveTimer(prof)
				logger.CfgLog.Infoln("sent Register NF Instance with updated profile")
			}
		}
	}
}
