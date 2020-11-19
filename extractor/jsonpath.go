package extractor

import (
	"errors"
	"fmt"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/kawamuray/jsonpath" // Originally: "github.com/NickSardo/jsonpath"
	"math"
	"strconv"
)

type JsonPathExtractor struct {
}

// Returns the first matching float value at the given json path
func (e *JsonPathExtractor) ExtractValue(logger log.Logger, json []byte, path string) (float64, error) {
	var floatValue = -1.0
	var result *jsonpath.Result
	var err error

	if len(path) < 1 || path[0] != '$' {
		// Static value
		return e.parseValue([]byte(path))
	}

	// Dynamic value
	p, err := e.compilePath(path)
	if err != nil {
		return floatValue, err
	}

	eval, err := jsonpath.EvalPathsInBytes(json, []*jsonpath.Path{p})
	if err != nil {
		return floatValue, err
	}

	result, ok := eval.Next()
	if result == nil || !ok {
		if eval.Error != nil {
			return floatValue, eval.Error
		} else {
			level.Debug(logger).Log("msg", "Path not found", "path", path, "json", string(json)) //nolint:errcheck
			return floatValue, errors.New("Path not found")
		}
	}

	return e.sanitizeValue(result)
}

// Returns the list of labels created from the list of provided json paths
func (e *JsonPathExtractor) ExtractLabels(logger log.Logger, json []byte, paths []string) ([]string, error) {
	labels := make([]string, len(paths))
	for i, path := range paths {

		// Dynamic value
		p, err := e.compilePath(path)
		if err != nil {
			level.Error(logger).Log("msg", "Failed to compile path for label", "path", path, "err", err) //nolint:errcheck
			continue
		}

		eval, err := jsonpath.EvalPathsInBytes(json, []*jsonpath.Path{p})
		if err != nil {
			level.Error(logger).Log("msg", "Failed to create evaluator for json", "path", path, "err", err) //nolint:errcheck
			continue
		}

		result, ok := eval.Next()
		if result == nil || !ok {
			if eval.Error != nil {
				level.Error(logger).Log("msg", "Failed to evaluate", "json", string(json), "err", eval.Error) //nolint:errcheck
			} else {
				level.Warn(logger).Log("msg", "Label path not found in json", "path", path)                        //nolint:errcheck
				level.Debug(logger).Log("msg", "Label path not found in json", "path", path, "json", string(json)) //nolint:errcheck
			}
			continue
		}

		l, err := strconv.Unquote(string(result.Value))
		if err == nil {
			labels[i] = l
		} else {
			labels[i] = string(result.Value)
		}
	}
	return labels, nil
}

func (e *JsonPathExtractor) ExtractObject(logger log.Logger, json []byte, path string) (ObjectIterator, error) {
	p, err := e.compilePath(path)
	if err != nil {
		return nil, err
	}

	eval, err := jsonpath.EvalPathsInBytes(json, []*jsonpath.Path{p})
	if err != nil {
		return nil, err
	}
	return func() ([]byte, bool, error) {
		results, ok := eval.Next()
		return results.Value, ok, nil
	}, nil
}

func (e *JsonPathExtractor) compilePath(path string) (*jsonpath.Path, error) {
	// All paths in this package is for extracting a value.
	// Complete trailing '+' sign if necessary.
	if path[len(path)-1] != '+' {
		path += "+"
	}

	paths, err := jsonpath.ParsePaths(path)
	if err != nil {
		return nil, err
	}
	return paths[0], nil
}

func (e *JsonPathExtractor) sanitizeValue(v *jsonpath.Result) (float64, error) {
	var value float64
	var boolValue bool
	var err error
	switch v.Type {
	case jsonpath.JsonNumber:
		value, err = e.parseValue(v.Value)
	case jsonpath.JsonString:
		// If it is a string, lets pull off the quotes and attempt to parse it as a number
		value, err = e.parseValue(v.Value[1 : len(v.Value)-1])
	case jsonpath.JsonNull:
		value = math.NaN()
	case jsonpath.JsonBool:
		if boolValue, err = strconv.ParseBool(string(v.Value)); boolValue {
			value = 1.0
		} else {
			value = 0.0
		}
	default:
		value, err = e.parseValue(v.Value)
	}
	if err != nil {
		// Should never happen.
		return -1.0, err
	}
	return value, err
}

func (e *JsonPathExtractor) parseValue(bytes []byte) (float64, error) {
	value, err := strconv.ParseFloat(string(bytes), 64)
	if err != nil {
		return -1.0, fmt.Errorf("failed to parse value as float; value: %q; err: %w", bytes, err)
	}
	return value, nil
}
