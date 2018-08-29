package gofiledb

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/teejays/clog"
	"io"
	"os"
	"reflect"
)

type (
	Index struct {
		IndexInfo
		KeyValue map[string][]string // Field value -> all the doc keys
		ValueKey map[string][]string // DocKey -> all the field values for it (useful when re-indexing...)
	}

	IndexInfo struct {
		FieldLocator   string
		FieldType      string
		NumValues      int
		FilePath       string
		CollectionName string
	}
)

var ErrIndexIsExist error = fmt.Errorf("Index already exists for that field")
var ErrIndexIsNotExist error = fmt.Errorf("Index does not exist")

func (idx Index) addDocByPath(docKey, docPath string) (Index, error) {

	docMap, err := getFileJson(docPath)
	if err != nil {
		return idx, nil
	}

	idx, err = idx.addData(docKey, docMap)
	if err != nil {
		return idx, err
	}

	return idx, nil
}

func (idx Index) addData(docKey string, docMap map[string]interface{}) (Index, error) {

	// Get the field values
	values, err := GetNestedFieldValuesOfStruct(docMap, idx.FieldLocator)
	if err != nil {
		return idx, err
	}

	// Each of the 'values' correspond to the value for this doc for the given field
	// we shoud store them in the index
	for _, v := range values {
		// Todo: make sure that the values are hashable (i.e. string, int, float etc. and not map, channels etc.)?
		if v.CanInterface() {
			v_i := v.Interface()
			v_str := fmt.Sprintf("%v", v_i)

			// theoretically, values that correspond to the provided field locator could be of different types
			// so, if we encounter different types, we should error out
			if idx.FieldType == "" { // if hasn't been set yet, it's probably the first iteration so set it
				idx.FieldType = reflect.TypeOf(v_i).Kind().String()
			}

			// make sure that the field of this value is the same as what we expect
			if idx.FieldType != reflect.TypeOf(v_i).Kind().String() {
				return idx, fmt.Errorf("Field locator %s corresponds to more than one data type. Cannot create an index.", idx.FieldLocator)
			}
			// add values to maps
			idx.KeyValue[v_str] = append(idx.KeyValue[v_str], docKey)
			idx.ValueKey[docKey] = append(idx.ValueKey[docKey], v_str)

		}
	}

	idx.NumValues = len(idx.KeyValue)

	return idx, nil
}

// Todo: removeIndex()
