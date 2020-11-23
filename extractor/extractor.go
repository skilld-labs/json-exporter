package extractor

import (
	"github.com/go-kit/kit/log"
)

type Extractor interface {
	ExtractLabels(logger log.Logger, json []byte, paths []string) ([]string, error)
	ExtractValue(logger log.Logger, json []byte, path string) (float64, error)
	ExtractObject(logger log.Logger, json []byte, path string) (ObjectIterator, error)
}

type ObjectIterator func() (json []byte, exists bool, err error)
