package main

import (
	"expvar"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	httppprof "net/http/pprof"
	"os"
	"runtime/pprof"
	"strconv"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/api"
	"github.com/yottachain/YTCoreService/env"
	"github.com/yottachain/YTS3/backend/s3mem"
	"github.com/yottachain/YTS3/routers"
	"github.com/yottachain/YTS3/yts3"
)

func main() {
	fmt.Println("Yts3 starting......")
	s3StartServer()
}

var crt, key string

func s3StartServer() {
	api.StartApi()
	flag.Parse()
	env.SetLimit()
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
