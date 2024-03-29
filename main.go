package main

import (
	"expvar"
	"flag"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	httppprof "net/http/pprof"
	"os"
	"runtime/pprof"
	"strconv"
	"time"

	"log"

	"github.com/kardianos/service"
	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/api"
	"github.com/yottachain/YTCoreService/env"
	"github.com/yottachain/YTS3/backend/s3mem"
	"github.com/yottachain/YTS3/routers"
	"github.com/yottachain/YTS3/yts3"
)

var logger service.Logger
var serviceConfig = &service.Config{
	Name:        "yts3",
	DisplayName: "go yts3 service",
	Description: "go yts3 daemons service",
}

type S3Program struct{}

func (p *S3Program) Start(s service.Service) error {
	go p.run()
	return nil
}

func (p *S3Program) Stop(s service.Service) error {
	s3StopServer()
	return nil
}

func (p *S3Program) run() {
	s3StartServer()
}

func main() {

	prog := &S3Program{}
	s, err := service.New(prog, serviceConfig)
	if err != nil {
		panic(err)
	}
	logger, err = s.Logger(nil)
	if err != nil {
		panic(err)
	}
	if len(os.Args) > 1 {
		cmd := os.Args[1]
		if cmd == "version" {
			fmt.Println(env.Version)
			return
		}
		if cmd == "console" {
			env.Console = true
			err = s.Run()
			if err != nil {
				logger.Info("Run console err:", err.Error())
			}
			return
		}
		if cmd == "start" {
			err = s.Start()
			if err != nil {
				logger.Info("Maybe the daemons are not installed.Start err:", err.Error())
			} else {
				logger.Info("Start OK.")
			}
			return
		}
		if cmd == "restart" {
			err = s.Restart()
			if err != nil {
				logger.Info("Maybe the daemons are not installed.Restart err:", err.Error())
			} else {
				logger.Info("Restart OK.")
			}
			return
		}
		if cmd == "stop" {
			err = s.Stop()
			if err != nil {
				logger.Info("Stop err:", err.Error())
			} else {
				logger.Info("Stop OK.")
			}
			return
		}
		if cmd == "install" {
			err = s.Install()
			if err != nil {
				logger.Info("Install err:", err.Error())
			} else {
				logger.Info("Install OK.")
			}
			return
		}
		if cmd == "uninstall" {
			err = s.Uninstall()
			if err != nil {
				logger.Info("Uninstall err:", err.Error())
			} else {
				logger.Info("Uninstall OK.")
			}
			return
		}
		logger.Info("Commands:")
		logger.Info("version      Show versionid.")
		logger.Info("console      Launch in the current console.")
		logger.Info("start        Start in the background as a daemon process.")
		logger.Info("stop         Stop if running as a daemon or in another console.")
		logger.Info("restart      Restart if running as a daemon or in another console.")
		logger.Info("install      Install to start automatically when system boots.")
		logger.Info("uninstall    Uninstall.")
		return
	}
	err = s.Run()
	if err != nil {
		logger.Info("Run err:", err.Error())
	}
}

func s3StopServer() {

}

var crt, key string

func s3StartServer() {
	/*
		var (
			fileName2 = "E:\\text2.txt"
			content2  = "enter to s3StarServer"
			err2      error
		)
		if err2 = ioutil.WriteFile(fileName2, []byte(content2), 0666); err2 != nil {
			fmt.Println("Writefile2 Error =", err2)
			//return
		}
		//读取文件
		fileContent2, err2 := ioutil.ReadFile(fileName2)
		if err2 != nil {
			fmt.Println("Read file2 err =", err2)
			//return
		}
		fmt.Println("Read file2 success =", string(fileContent2))
	*/
	flag.Parse()

	api.StartApi()
	s3mem.InitObjectUpPool()
	crt = env.YTFS_HOME + "crt/server.crt"
	key = env.YTFS_HOME + "crt/server.key"
	_, err := ioutil.ReadFile(crt)
	if err != nil {
		crt = ""
	}
	_, err = ioutil.ReadFile(key)
	if err != nil {
		crt = ""
	}
	go func() {
		router := routers.InitRouter()
		port := env.GetConfig().GetInt("s3port", 8080)
		var e error
		if crt == "" {
			e = router.Run(":" + strconv.Itoa(port))
		} else {
			e = router.RunTLS(":"+strconv.Itoa(port), crt, key)
		}
		if e != nil {
			logrus.Errorf("[Main]Port %d,err:s%\n", port, e)
		}
	}()
	if err := run(); err != nil {
		logrus.Fatalf("[Main]s3server run err:%s\n", err)
	}
	select {}
}

type yts3Flags struct {
	host          string
	backendKind   string
	initialBucket string
	fixedTimeStr  string
	noIntegrity   bool
	hostBucket    bool

	boltDb         string
	directFsPath   string
	directFsMeta   string
	directFsBucket string
	fsPath         string
	fsMeta         string

	debugCPU  string
	debugHost string
}

func (f *yts3Flags) attach(flagSet *flag.FlagSet) {
	flagSet.StringVar(&f.host, "host", ":8083", "Host to run the service")
	flagSet.StringVar(&f.fixedTimeStr, "time", "", "RFC3339 format. If passed, the server's clock will always see this time; does not affect existing stored dates.")
	flagSet.StringVar(&f.initialBucket, "initialbucket", "", "If passed, this bucket will be created on startup if it does not already exist.")
	flagSet.BoolVar(&f.noIntegrity, "no-integrity", false, "Pass this flag to disable Content-MD5 validation when uploading.")
	flagSet.BoolVar(&f.hostBucket, "hostbucket", false, "If passed, the bucket name will be extracted from the first segment of the hostname, rather than the first part of the URL path.")
	flagSet.StringVar(&f.initialBucket, "bucket", "", `Deprecated; use -initialbucket`)

}

func (f *yts3Flags) timeOptions() (source yts3.TimeSource, skewLimit time.Duration, err error) {
	skewLimit = yts3.DefaultSkewLimit

	if f.fixedTimeStr != "" {
		fixedTime, err := time.Parse(time.RFC3339Nano, f.fixedTimeStr)
		if err != nil {
			return nil, 0, err
		}
		source = yts3.FixedTimeSource(fixedTime)
		skewLimit = 0
	}

	return source, skewLimit, nil
}

func debugServer(host string) {
	mux := http.NewServeMux()
	mux.Handle("/debug/vars", expvar.Handler())
	mux.HandleFunc("/debug/pprof/", httppprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", httppprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", httppprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", httppprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", httppprof.Trace)

	srv := &http.Server{Addr: host}
	srv.Handler = mux
	if err := srv.ListenAndServe(); err != nil {
		panic(err)
	}
}

func run() error {
	var values yts3Flags
	flagSet := flag.NewFlagSet("", 0)
	values.attach(flagSet)
	values.backendKind = "mem"
	values.initialBucket = "bucket"
	values.fsPath = "test"

	if err := flagSet.Parse(os.Args[1:]); err != nil {
		return err
	}

	stopper, err := profile(values)
	if err != nil {
		return err
	}
	defer stopper()
	if values.debugHost != "" {
		go debugServer(values.debugHost)
	}
	var backend yts3.Backend
	timeSource, timeSkewLimit, err := values.timeOptions()
	if err != nil {
		return err
	}
	switch values.backendKind {
	case "":
		flag.PrintDefaults()
		fmt.Println()
		return fmt.Errorf("-backend is required")
	case "mem", "memory":
		if values.initialBucket == "" {
			log.Println("no buckets available; consider passing -initialbucket")
		}
		backend = s3mem.New(s3mem.WithTimeSource(timeSource))
		log.Println("using memory backend")
	default:
		return fmt.Errorf("unknown backend %q", values.backendKind)
	}
	if values.initialBucket != "" {
	}
	faker := yts3.New(backend,
		yts3.WithIntegrityCheck(!values.noIntegrity),
		yts3.WithTimeSkewLimit(timeSkewLimit),
		yts3.WithTimeSource(timeSource),
		yts3.WithLogger(yts3.GlobalLog()),
		yts3.WithHostBucket(values.hostBucket),
	)
	return listenAndServe(values.host, faker.Server())
}

func listenAndServe(addr string, handler http.Handler) error {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	defer listener.Close()
	server := &http.Server{Addr: addr, Handler: handler}
	env.SetVersionID("2.0.1.6")
	if crt != "" {
		logrus.Infof("[Main]Start S3 server https port :%d\n", listener.Addr().(*net.TCPAddr).Port)
		return server.ServeTLS(listener, crt, key)
	} else {
		logrus.Infof("[Main]Start S3 server http port :%d\n", listener.Addr().(*net.TCPAddr).Port)
		return server.Serve(listener)
	}
}

func profile(values yts3Flags) (func(), error) {
	fn := func() {}

	if values.debugCPU != "" {
		f, err := os.Create(values.debugCPU)
		if err != nil {
			return fn, err
		}
		if err := pprof.StartCPUProfile(f); err != nil {
			return fn, err
		}
		return pprof.StopCPUProfile, nil
	}

	return fn, nil
}
