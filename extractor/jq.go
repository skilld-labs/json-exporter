package extractor

import (
	gojson "encoding/json"
	"fmt"
	"github.com/go-kit/kit/log"
	//	"github.com/go-kit/kit/log/level"
	"github.com/itchyny/gojq"
	"math"
	"strconv"
)

type JqExtractor struct {
}

// Returns the first matching float value at the given json path
func (e *JqExtractor) ExtractValue(logger log.Logger, json []byte, path string) (float64, error) {
	var floatValue = -1.0
	var err error

	if len(path) < 1 || path[0] != '$' {
		// Static value
		return e.parseValue([]byte(path))
	}

	// Dynamic value
	var input interface{}
	if err := gojson.Unmarshal(json, &input); err != nil {
		return floatValue, err
	}
	v, err := e.extract(input, path[1:])
	if err != nil {
		return -1, err
	}

	return e.sanitizeValue(v)
}

// Returns the list of labels created from the list of provided json paths
func (e *JqExtractor) ExtractLabels(logger log.Logger, json []byte, paths []string) ([]string, error) {
	var labels []string
	var input interface{}
	if err := gojson.Unmarshal(json, &input); err != nil {
		return []string{}, err
	}
	for _, path := range paths {

		// Dynamic value
		vv, err := e.extract(input, path[1:])
		if err != nil {
			return []string{}, err
		}
		switch vi := vv.(type) {
		case []interface{}:
			for _, v := range vi {
				vs, isString := v.(string)
				if !isString {
					return []string{}, fmt.Errorf("error while extracting labels: path '%s' value is an array but contains a non string type", path)
				}
				if isString {
					labels = append(labels, vs)
				}
			}
		case string:
			labels = append(labels, vi)
		default:
			return []string{}, fmt.Errorf("error while extracting labels: path '%s' value type is invalid - should be string or []string", path)
		}
	}
	return labels, nil
}

func (e *JqExtractor) ExtractObject(logger log.Logger, json []byte, path string) (ObjectIterator, error) {
	var input interface{}
	if err := gojson.Unmarshal(json, &input); err != nil {
		return nil, err
	}
	v, err := e.extract(input, path[1:])
	if err != nil {
		return nil, err
	}
	switch vv := v.(type) {
	case []interface{}:
		var idx = 0
		return func() ([]byte, bool, error) {
			if idx == len(vv) {
				return nil, false, nil
			}
			vm, err := gojson.Marshal(vv[idx])
			if err != nil {
				return nil, false, err
			}
			idx = idx + 1
			return vm, true, nil
		}, nil
	case map[string]interface{}:
		vm, err := gojson.Marshal(vv)
		return func() ([]byte, bool, error) {
			return vm, false, nil
		}, err
	default:
		return nil, fmt.Errorf("error while extracting object: path '%s' value is not an object", path)
	}
}

func (e *JqExtractor) extract(input interface{}, path string) (interface{}, error) {
	query, err := gojq.Parse(path)
	if err != nil {
		return nil, err
	}
	code, err := gojq.Compile(query)
	if err != nil {
		return nil, err
	}
	v, _ := code.Run(input).Next()
	return v, nil
}

func (e *JqExtractor) sanitizeValue(v interface{}) (float64, error) {
	var value float64
	var err error
	switch v.(type) {
	case float64:
		value, _ = v.(float64)
	case string:
		// If it is a string, lets pull off the quotes and attempt to parse it as a number
		str, _ := v.(string)
		l, errQuote := strconv.Unquote(str)
		if errQuote == nil {
			str = l
		}
		fmt.Println(str)
		value, err = strconv.ParseFloat(str, 64)

	case nil:
		value = math.NaN()
	case bool:
		if boolValue, _ := v.(bool); boolValue {
			value = 1.0
		} else {
			value = 0.0
		}
	default:
		return -1, fmt.Errorf("error while sanitizing value unknown type")
	}
	if err != nil {
		return -1.0, err
	}
	return value, err
}

func (e *JqExtractor) parseValue(bytes []byte) (float64, error) {
	fmt.Println(string(bytes))
	value, err := strconv.ParseFloat(string(bytes), 64)
	if err != nil {
		return -1.0, fmt.Errorf("failed to parse value as float; value: %q; err: %w", bytes, err)
	}
	return value, nil
}
