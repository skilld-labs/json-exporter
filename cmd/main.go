// Copyright 2020 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/prometheus-community/json_exporter/config"
	"github.com/prometheus-community/json_exporter/exporter"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/common/promlog"
	"github.com/prometheus/common/promlog/flag"
	"github.com/prometheus/common/version"
	"github.com/vigneshuvi/GoDateFormat"
	"gopkg.in/alecthomas/kingpin.v2"
	"io/ioutil"
	"net/http"
	"os"
	"os/signal"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

var (
	configFile            = kingpin.Flag("config.file", "JSON exporter configuration file.").Default("examples/configAki.yml").ExistingFile()
	listenAddress         = kingpin.Flag("web.listen-address", "The address to listen on for HTTP requests.").Default(":7979").String()
	configCheck           = kingpin.Flag("config.check", "If true validate the config file and then exit.").Default("false").Bool()
	scrapeTimestamps      = make(map[string]time.Time)
	re                    = regexp.MustCompile(`\$\{__(from|to)(?::(date):?(.*?))?\}`)
	scrapeDurationSeconds = time.Duration(0) // configuration
	defaultFormat         = time.RFC3339
	reloadCh              chan chan error
	sc                    = &SafeConfig{
		C: &config.Config{},
	}
)

func Run() {

	promlogConfig := &promlog.Config{}

	flag.AddFlags(kingpin.CommandLine, promlogConfig)
	kingpin.Version(version.Print("json_exporter"))
	kingpin.HelpFlag.Short('h')
	kingpin.Parse()
	logger := promlog.New(promlogConfig)

	level.Info(logger).Log("msg", "Starting json_exporter", "version", version.Info()) //nolint:errcheck
	level.Info(logger).Log("msg", "Build context", "build", version.BuildContext())    //nolint:errcheck

	reloadCh = make(chan chan error)

	level.Info(logger).Log("msg", "Loading config file", "file", *configFile) //nolint:errcheck
	config, err := config.LoadConfig(*configFile)
	if err != nil {
		level.Error(logger).Log("msg", "Error loading config", "err", err) //nolint:errcheck
		os.Exit(1)
	}
	configJson, err := json.Marshal(config)
	if err != nil {
		level.Error(logger).Log("msg", "Failed to marshal config to JSON", "err", err) //nolint:errcheck
	}
	level.Info(logger).Log("msg", "Loaded config file", "config", string(configJson)) //nolint:errcheck

	if *configCheck {
		os.Exit(0)
	}

	sc.SetConfig(&config)
	reloadConfigOnChannel(logger, *configFile)
	reloadConfigOnSignal(logger)

	http.Handle("/metrics", promhttp.Handler())
	http.HandleFunc("/probe", func(w http.ResponseWriter, req *http.Request) {
		probeHandler(w, req, logger)
	})

	http.HandleFunc("/config/reload", reloadConfigHandler(logger, *configFile, false))
	http.HandleFunc("/config/update", reloadConfigHandler(logger, *configFile, true))

	if err := http.ListenAndServe(*listenAddress, nil); err != nil {
		level.Error(logger).Log("msg", "Failed to start the server", "err", err) //nolint:errcheck
	}
}

func probeHandler(w http.ResponseWriter, r *http.Request, logger log.Logger) {

	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()
	r = r.WithContext(ctx)

	registry := prometheus.NewPedanticRegistry()
	config := *sc.GetConfig()
	metrics, err := exporter.CreateMetricsList(config)
	if err != nil {
		level.Error(logger).Log("msg", "Failed to create metrics list from config", "err", err) //nolint:errcheck
	}

	jsonMetricCollector := exporter.JsonMetricCollector{JsonMetrics: metrics}
	jsonMetricCollector.Logger = logger

	target := computeTarget(r.URL.Query().Get("target"))

	if target == "" {
		http.Error(w, "Target parameter is missing", http.StatusBadRequest)
		return
	}

	data, err := exporter.FetchJson(ctx, logger, target, config)
	if err != nil {
		http.Error(w, "Failed to fetch JSON response. TARGET: "+target+", ERROR: "+err.Error(), http.StatusServiceUnavailable)
		return
	}

	jsonMetricCollector.Data = data

	registry.MustRegister(jsonMetricCollector)
	h := promhttp.HandlerFor(registry, promhttp.HandlerOpts{})
	h.ServeHTTP(w, r)

}

func computeTarget(target string) string {
	var newTarget = target
	now := time.Now()

	matches := re.FindAllStringSubmatch(target, -1)
	for _, match := range matches {
		var replaceTime = now
		var replace string
		if match[1] == "from" {
			replaceTime = getLastScrapeTime(target, replaceTime)
		}
		if match[2] != "" {
			switch format := match[3]; format {
			case "":
				replace = replaceTime.Format(defaultFormat) // No args, set to default
			case "iso":
				replace = replaceTime.Format(time.RFC3339) // ISO 8601/RFC 3339
			case "seconds":
				replace = strconv.FormatInt(replaceTime.Unix(), 10) // Unix seconds epoch
			default:
				replace = replaceTime.Format(GoDateFormat.ConvertFormat(format)) // Any custom date format
			}
		} else {
			replace = strconv.FormatInt(replaceTime.Unix()*1e3, 10) //Unix millisecond epoch
		}
		newTarget = strings.Replace(newTarget, match[0], replace, -1)
	}
	scrapeTimestamps[target] = now
	return newTarget
}

func getLastScrapeTime(target string, now time.Time) time.Time {
	lastScrapeTime, exists := scrapeTimestamps[target]
	if !exists {
		// handle first scrape
		if scrapeDurationSeconds == 0 {
			return time.Unix(0, 0)
		}
		return now.Add(-scrapeDurationSeconds * time.Second)
	}
	return lastScrapeTime
}

func reloadConfigHandler(logger log.Logger, configFile string, updateFromBody bool) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case "POST":
			if updateFromBody {
				body, err := ioutil.ReadAll(r.Body)
				if err != nil {
					level.Error(logger).Log("Error reading body:", err)
					http.Error(w, "can't read body", http.StatusBadRequest)
					return
				}
				if len(body) != 0 {
					config.WriteFile(configFile, body)
				}
				r.Body.Close()
				level.Info(logger).Log(configFile, "is rewriten")
			}
			if err := sendReloadChannel(); err != nil {
				http.Error(w, fmt.Sprintf("failed to reload config: %s", err), http.StatusInternalServerError)
			}

		default:
			http.Error(w, "POST method expected", 400)
		}
	}
}

func sendReloadChannel() error {
	rc := make(chan error)
	reloadCh <- rc
	return <-rc
}

func reloadConfigOnSignal(logger log.Logger) {
	hup := make(chan os.Signal, 1)
	signal.Notify(hup, syscall.SIGHUP)
	go func() {
		for {
			select {
			case <-hup:
				if err := sendReloadChannel(); err != nil {
					level.Error(logger).Log(err.Error())
				}
			}
		}
	}()
}

func reloadConfigOnChannel(logger log.Logger, configFile string) {
	go func() {
		for {
			select {
			case rc := <-reloadCh:
				if err := sc.reloadConfig(configFile); err != nil {
					level.Error(logger).Log("error reloading config:", err)
					rc <- err
				} else {
					level.Info(logger).Log("config file was reloaded")
					rc <- nil
				}
			}
		}
	}()
}

type SafeConfig struct {
	sync.RWMutex
	C *config.Config
}

func (sc *SafeConfig) GetConfig() *config.Config {
	sc.RLock()
	c := sc.C
	sc.RUnlock()
	return c
}

func (sc *SafeConfig) SetConfig(c *config.Config) {
	sc.Lock()
	sc.C = c
	sc.Unlock()
}

func (sc *SafeConfig) reloadConfig(configFile string) error {
	config, err := config.LoadConfig(configFile)
	if err != nil {
		return err
	}
	sc.SetConfig(&config)
	return nil
}
