package main

import (
	"expvar"
	"flag"
	"fmt"
	"net"
	"net/http"
	httppprof "net/http/pprof"
	"os"
	"runtime/pprof"
	"time"

	"log"

	"github.com/yottachain/YTCoreService/api"
	"github.com/yottachain/YTCoreService/env"
	"github.com/yottachain/YTS3/backend/s3mem"
	"github.com/yottachain/YTS3/yts3"
)

func main() {
	// flag.Parse()

	// var path string
	// if len(os.Args) > 1 {
	// 	if os.Args[1] != "" {
	// 		path = os.Args[1]
	// 	} else {
	// 		path = "conf/yotta_config.ini"
	// 	}

	// } else {
	// 	path = "conf/yotta_config.ini"
	// }

	// cfg, err := conf.CreateConfig(path)
	// if err != nil {
	// 	panic(err)
	// }

	// 初始化SDK服务
	// env.Console = true
	// api.StartApi()

	// router := routers.InitRouter()
	// port := cfg.GetHTTPInfo("port")
	// err1 := router.Run(port)
	// if err1 != nil {
	// 	panic(err1)
	// }
	env.Console = true
	api.StartApi()
	go func() {
		for {

			_, err := api.NewClient("ianmooneyy11", "5JnLRW1bTRD2bxo93wZ1qnpXfMDHzA97qcQjabnoqgmJTt7kBoH")
			if err == nil {
				break
			} else {
				time.Sleep(time.Second * 5)
				api.NewClient("ianmooneyy11", "5JnLRW1bTRD2bxo93wZ1qnpXfMDHzA97qcQjabnoqgmJTt7kBoH")
			}
		}
		// logrus.Info("User Register Success,UserName:" + c.Username)
		// fmt.Println("UserID:", c.UserId)
	}()
	if err := run(); err != nil {
		log.Fatal(err)
	}

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
	flagSet.StringVar(&f.host, "host", ":9000", "Host to run the service")
	flagSet.StringVar(&f.fixedTimeStr, "time", "", "RFC3339 format. If passed, the server's clock will always see this time; does not affect existing stored dates.")
	flagSet.StringVar(&f.initialBucket, "initialbucket", "", "If passed, this bucket will be created on startup if it does not already exist.")
	flagSet.BoolVar(&f.noIntegrity, "no-integrity", false, "Pass this flag to disable Content-MD5 validation when uploading.")
	flagSet.BoolVar(&f.hostBucket, "hostbucket", false, "If passed, the bucket name will be extracted from the first segment of the hostname, rather than the first part of the URL path.")

	// Backend specific:
	flagSet.StringVar(&f.backendKind, "backend", "", "Backend to use to store data (memory, bolt, directfs, fs)")
	flagSet.StringVar(&f.boltDb, "bolt.db", "locals3.db", "Database path / name when using bolt backend")
	flagSet.StringVar(&f.directFsPath, "directfs.path", "", "File path to serve using S3. You should not modify the contents of this path outside gofakes3 while it is running as it can cause inconsistencies.")
	flagSet.StringVar(&f.directFsMeta, "directfs.meta", "", "Optional path for storing S3 metadata for your bucket. If not passed, metadata will not persist between restarts of gofakes3.")
	flagSet.StringVar(&f.directFsBucket, "directfs.bucket", "mybucket", "Name of the bucket for your file path; this will be the only supported bucket by the 'directfs' backend for the duration of your run.")
	flagSet.StringVar(&f.fsPath, "fs.path", "", "Path to your S3 buckets. Buckets are stored under the '/buckets' subpath.")
	flagSet.StringVar(&f.fsMeta, "fs.meta", "", "Optional path for storing S3 metadata for your buckets. Defaults to the '/metadata' subfolder of -fs.path if not passed.")

	// Debugging:
	flagSet.StringVar(&f.debugHost, "debug.host", "", "Run the debug server on this host")
	flagSet.StringVar(&f.debugCPU, "debug.cpu", "", "Create CPU profile in this file")

	// Deprecated:
	flagSet.StringVar(&f.boltDb, "db", "locals3.db", "Deprecated; use -bolt.db")
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

	// c, err := api.NewClient("qiyufengxing", "5J8FvWrq26M86nqF48MamCjQWV8N6S3FrPFnH4KjjnD2CCEKvF3")
	// if err != nil {

	// }
	// logrus.Info("User Register Success,UserName:" + c.Username)
	// fmt.Println("UserID:", c.UserId)
	var values yts3Flags

	flagSet := flag.NewFlagSet("", 0)
	values.attach(flagSet)
	values.backendKind = "mem"
	values.initialBucket = "bucket"
	values.fsPath = "ttttt"

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

	log.Println("using port:", listener.Addr().(*net.TCPAddr).Port)
	server := &http.Server{Addr: addr, Handler: handler}

	return server.Serve(listener)
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
