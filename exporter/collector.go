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

package exporter

import (
	"fmt"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/prometheus-community/json_exporter/extractor"
	"github.com/prometheus/client_golang/prometheus"
)

type JsonMetricCollector struct {
	JsonMetrics []JsonMetric
	Data        []byte
	Logger      log.Logger
}

type JsonMetric struct {
	Desc                 *prometheus.Desc
	KeyExtractorPath     string
	ValueExtractorPath   string
	LabelsExtractorPaths []string
	Extractor            extractor.Extractor
}

func (mc JsonMetricCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, m := range mc.JsonMetrics {
		ch <- m.Desc
	}
}

func (mc JsonMetricCollector) Collect(ch chan<- prometheus.Metric) {
	for _, m := range mc.JsonMetrics {
		fmt.Println(mc.Data)
		if m.ValueExtractorPath == "" { // ScrapeType is 'value'
			floatValue, err := m.Extractor.ExtractValue(mc.Logger, mc.Data, m.KeyExtractorPath)
			if err != nil {
				// Avoid noise and continue silently if it was a missing path error
				if err.Error() == "Path not found" {
					level.Debug(mc.Logger).Log("msg", "Failed to extract float value for metric", "path", m.KeyExtractorPath, "err", err, "metric", m.Desc) //nolint:errcheck
					continue
				}
				level.Error(mc.Logger).Log("msg", "Failed to extract float value for metric", "path", m.KeyExtractorPath, "err", err, "metric", m.Desc) //nolint:errcheck
				continue
			}

			labels, err := m.Extractor.ExtractLabels(mc.Logger, mc.Data, m.LabelsExtractorPaths)
			if err != nil {
				level.Error(mc.Logger).Log("msg", "Failed to extract Labels", err)
			}
			ch <- prometheus.MustNewConstMetric(
				m.Desc,
				prometheus.UntypedValue,
				floatValue,
				labels...,
			)
		} else { // ScrapeType is 'object'
			iterator, err := m.Extractor.ExtractObject(mc.Logger, mc.Data, m.KeyExtractorPath)
			if err != nil {
				level.Error(mc.Logger).Log("msg", "Failed to extract object", "path", m.KeyExtractorPath, "err", err) //nolint:errcheck
			}
			for {
				result, ok, err := iterator()
				if err != nil {
					level.Error(mc.Logger).Log("msg", "Failed to extract value", "path", m.ValueExtractorPath, "err", err) //nolint:errcheck
					continue
				}
				if ok {
					floatValue, err := m.Extractor.ExtractValue(mc.Logger, result, m.ValueExtractorPath)
					if err != nil {
						level.Error(mc.Logger).Log("msg", "Failed to extract value", "path", m.ValueExtractorPath, "err", err) //nolint:errcheck
						continue
					}

					labels, err := m.Extractor.ExtractLabels(mc.Logger, result, m.LabelsExtractorPaths)
					if err != nil {
						level.Error(mc.Logger).Log("msg", "Failed to extract Labels", err)
					}

					ch <- prometheus.MustNewConstMetric(
						m.Desc,
						prometheus.UntypedValue,
						floatValue,
						labels...,
					)
				} else {
					break
				}
			}
		}
	}
}
