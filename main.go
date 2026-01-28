package main

import (
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"

	corelog "log"

	kitprometheus "github.com/go-kit/kit/metrics/prometheus"
	"github.com/go-kit/log"
	"github.com/mikesay/user/api"
	"github.com/mikesay/user/db"
	"github.com/mikesay/user/db/mongodb"

	stdopentracing "github.com/opentracing/opentracing-go"
	zipkinot "github.com/openzipkin-contrib/zipkin-go-opentracing"
	"github.com/openzipkin/zipkin-go"
	httpreporter "github.com/openzipkin/zipkin-go/reporter/http"

	"github.com/prometheus/client_golang/prometheus"
	stdprometheus "github.com/prometheus/client_golang/prometheus"
	commonMiddleware "github.com/weaveworks/common/middleware"
)

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

var (
	port string
	zip  string
)

var (
	HTTPLatency = stdprometheus.NewHistogramVec(stdprometheus.HistogramOpts{
		Name:    "http_request_duration_seconds",
		Help:    "Time (in seconds) spent serving HTTP requests.",
		Buckets: stdprometheus.DefBuckets,
	}, []string{"method", "path", "status_code", "isWS"})

	HTTPRequestActive = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "http_request_active",
		Help: "The number of HTTP requests currently being handled.",
	}, []string{"method", "path"})

	HTTPRequestSizeBytes = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "http_request_size_bytes",
		Help: "Size of HTTP request bodies in bytes.",
		// Exponential buckets are better for sizes (e.g., 100B to 10MB).
		Buckets: prometheus.ExponentialBuckets(100, 10, 6),
	}, []string{"method", "handler"})

	HTTPResponseSizeBytes = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "http_response_size_bytes",
		Help:    "Size of HTTP response bodies in bytes.",
		Buckets: prometheus.ExponentialBuckets(100, 10, 6),
	}, []string{"method", "handler"})
)

const (
	ServiceName = "user"
)

func init() {
	stdprometheus.MustRegister(HTTPLatency)
	stdprometheus.MustRegister(HTTPRequestActive)
	stdprometheus.MustRegister(HTTPRequestSizeBytes)
	stdprometheus.MustRegister(HTTPResponseSizeBytes)
	flag.StringVar(&zip, "zipkin", os.Getenv("ZIPKIN"), "Zipkin address")
	flag.StringVar(&port, "port", env("PORT", "8084"), "Port on which to run")
	db.Register("mongodb", &mongodb.Mongo{})
}

func main() {

	flag.Parse()
	// Mechanical stuff.
	errc := make(chan error)

	// Log domain.
	var logger log.Logger
	{
		logger = log.NewLogfmtLogger(os.Stderr)
		logger = log.With(logger, "ts", log.DefaultTimestampUTC)
		logger = log.With(logger, "caller", log.DefaultCaller)
	}

	// Find service local IP.
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		logger.Log("err", err)
		os.Exit(1)
	}
	localAddr := conn.LocalAddr().(*net.UDPAddr)
	host := strings.Split(localAddr.String(), ":")[0]
	defer conn.Close()

	var tracer stdopentracing.Tracer
	{
		if zip == "" {
			tracer = stdopentracing.NoopTracer{}
		} else {
			// 2. Setup Zipkin Reporter (replaces NewHTTPCollector)
			// Note: Use /api/v2/spans for modern Zipkin
			reporter := httpreporter.NewReporter(zip)
			defer reporter.Close()

			// 3. Create Local Endpoint
			endpoint, err := zipkin.NewEndpoint(ServiceName, fmt.Sprintf("%v:%v", host, port))
			if err != nil {
				logger.Log("err", err)
				os.Exit(1)
			}

			// 4. Initialize Native Zipkin Tracer
			nativeTracer, err := zipkin.NewTracer(
				reporter,
				zipkin.WithLocalEndpoint(endpoint),
			)
			if err != nil {
				logger.Log("err", err)
				os.Exit(1)
			}

			// 5. Wrap for OpenTracing (replaces zipkin.NewRecorder)
			tracer = zipkinot.Wrap(nativeTracer)
		}
	}
	dbconn := false
	for !dbconn {
		err := db.Init()
		if err != nil {
			if err == db.ErrNoDatabaseSelected {
				corelog.Fatal(err)
			}
			corelog.Print(err)
		} else {
			dbconn = true
		}
	}

	fieldKeys := []string{"method"}
	// Service domain.
	var service api.Service
	{
		service = api.NewFixedService()
		service = api.LoggingMiddleware(logger)(service)
		service = api.NewInstrumentingService(
			kitprometheus.NewCounterFrom(
				stdprometheus.CounterOpts{
					Namespace: "microservices_demo",
					Subsystem: "user",
					Name:      "request_count",
					Help:      "Number of requests received.",
				},
				fieldKeys),
			kitprometheus.NewSummaryFrom(stdprometheus.SummaryOpts{
				Namespace: "microservices_demo",
				Subsystem: "user",
				Name:      "request_latency_microseconds",
				Help:      "Total duration of requests in microseconds.",
			}, fieldKeys),
			service,
		)
	}

	// Endpoint domain.
	endpoints := api.MakeEndpoints(service, tracer)

	// HTTP router
	router := api.MakeHTTPHandler(endpoints, logger, tracer)

	httpMiddleware := []commonMiddleware.Interface{
		commonMiddleware.Instrument{
			Duration:         HTTPLatency,
			RouteMatcher:     router,
			InflightRequests: HTTPRequestActive,
			RequestBodySize:  HTTPRequestSizeBytes,
			ResponseBodySize: HTTPResponseSizeBytes,
		},
	}

	// Handler
	handler := commonMiddleware.Merge(httpMiddleware...).Wrap(router)

	// Create and launch the HTTP server.
	go func() {
		logger.Log("transport", "HTTP", "port", port)
		errc <- http.ListenAndServe(fmt.Sprintf(":%v", port), handler)
	}()

	// Capture interrupts.
	go func() {
		c := make(chan os.Signal)
		signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
		errc <- fmt.Errorf("%s", <-c)
	}()

	logger.Log("exit", <-errc)
}
