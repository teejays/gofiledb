package gofiledb

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

func ValueToString(v reflect.Value) (string, error) {
	v = reflect.Indirect(v) // in case it's a pointer

	if !isAmongKind(v, []reflect.Kind{reflect.String, reflect.Int64, reflect.Float64}) {
		return "", fmt.Errorf("The value v is of kind %s that cannot be converted to a string.", v.Kind().String())
	}

	var str string

	switch v.Kind() {
	case reflect.String:
		str = v.String()
	case reflect.Int64:
		str = strconv.FormatInt(v.Int(), 10)
	case reflect.Float64:
		str = strconv.FormatFloat(v.Float(), 'f', 1, 64)
	default:
		return str, fmt.Errorf("The value v is of kind %s that cannot be converted to a string.", v.Kind().String())
	}
	// implement int, int32, float32?

	return str, nil

}

func getValidFieldByNameInt(v reflect.Value, fieldName string) (int64, error) {
	d_v, err := GetValidFieldValue(v, fieldName)
	if err != nil {
		return 0, err
	}
	if d_v.Kind() != reflect.Int64 {
		return 0, fmt.Errorf("The field '%s' is not an int64, it's %s.", fieldName, d_v.Kind().String())
	}
	d := d_v.Int()
	return d, nil
}
func getValidFieldByNameInterface(v reflect.Value, fieldName string) (interface{}, error) {
	d_v, err := GetValidFieldValue(v, fieldName)
	if err != nil {
		return nil, err
	}
	if !d_v.CanInterface() {
		return nil, fmt.Errorf("The value of field name '%s' (of kind %s) can not be converted to an reflect.Interface.", fieldName, d_v.Kind().String())
	}
	d := d_v.Interface()
	return d, nil
}

func GetValidFieldValue(v reflect.Value, fieldName string) (reflect.Value, error) {
	v = reflect.Indirect(v) // in case it's a pointer
	var d_v reflect.Value
	//log.Debugf(" fieldName: %s | v: %v | v.Kind(): %s | v.Type(): %v\n", fieldName, v, v.Kind().String(), v.Type())
	switch v.Kind() {
	case reflect.Struct:
		d_v = v.FieldByName(fieldName)
	case reflect.Map:
		key_v := reflect.ValueOf(fieldName)
		d_v = v.MapIndex(key_v)
	case reflect.Interface:
		return GetValidFieldValue(v.Elem(), fieldName)

	}
	if !d_v.IsValid() {
		return d_v, fmt.Errorf("The value of field '%s' is not valid in the object provided.", fieldName)
	}
	return d_v, nil
}

// Recursive function, that takes a data object and a string of form "A.B.[]C.D.Id", and gets the value of the field represented by the string
// works when the fieldName is nested within slices as well
func GetNestedFieldValues(v reflect.Value, fieldName string) ([]reflect.Value, error) {
	var response []reflect.Value
	var expectIterable bool

	if len(fieldName) > 2 {
		if fieldName[:2] == "[]" { // if the first two chars are '[]', we should iterate
			expectIterable = true
			fieldName = fieldName[2:]
		}
	}

	// each key could correspond to a nested field, e.g. BulkUserId vs. Data.Product.Id, etc. // split by "."
	parts := strings.Split(fieldName, ".")
	currentFieldName := parts[0]

	// base conditions -> cannot further split, and is not a iterable
	if fieldName == "" {
		response = append(response, v)
		return response, nil
	}

	if len(parts) == 1 && !expectIterable { // this is the last field name, it's value is the final value
		data, err := GetValidFieldValue(v, currentFieldName)
		if err != nil {
			return nil, err
		}
		response = append(response, data)
		return response, nil
	}

	// recursive condition
	// a. iterable
	if expectIterable { // this means v is iterable
		if !isAmongKind(v, []reflect.Kind{reflect.Slice, reflect.Array}) {
			return nil, fmt.Errorf("expecting v of type []iterable for fieldName %s, but v's kind is %s instead.", fieldName, v.Kind().String())
		}
		for i := 0; i < v.Len(); i++ {
			elem := v.Index(i)
			elemFieldvalue, err := GetNestedFieldValues(elem, fieldName)
			if err != nil {
				return nil, err
			}
			response = append(response, elemFieldvalue...)
		}
		return response, nil

	}

	// b. non-iterable (struct, map)
	currentFieldValue, err := GetValidFieldValue(v, currentFieldName)
	if err != nil {
		return nil, err
	}
	remainingFieldName := fieldName[len(currentFieldName)+1:] // construct next fieldName. i.e. if fieldName is A.B.[]C.Id, them remainingFieldName should be B.[]C.Id
	data, err := GetNestedFieldValues(currentFieldValue, remainingFieldName)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func isAmongKind(v reflect.Value, kinds []reflect.Kind) bool {
	for _, k := range kinds {
		if v.Kind() == k {
			return true
		}
	}
	return false
}

/* getIntOrStringFieldFromStruct() - Description
This function takes a struct in the form reflect.Value, and returns the int/float/string value of the fieldName as string.
*/
func getFieldFromStructAsString(d_v reflect.Value, fieldName string) (string, error) {

	if !d_v.IsValid() {
		return "", fmt.Errorf("d_v is either null or invalid.")
	}

	if d_v.Kind() != reflect.Struct {
		return "", fmt.Errorf("reflect.Kind() of data is not a struct, it's %s. Cannot create bulk data response.", d_v.Kind().String())
	}

	var key string

	key_v := d_v.FieldByName(fieldName)
	key_v = reflect.Indirect(key_v) //if the key is a pointer

	if !key_v.IsValid() { // this means the key (e.g. OrgId) is probably null
		return "", fmt.Errorf("Field '%s' in the data is either null or not valid.", fieldName)
	}

	if key_v.Kind() != reflect.String && key_v.Kind() != reflect.Int64 && key_v.Kind() != reflect.Float64 {
		return "", fmt.Errorf("The value for fieldName '%s' is neither int64, float64 or string, it's '%s'.", fieldName, key_v.Kind())
	}

	if key_v.Kind() == reflect.String {
		key = key_v.String()
	}
	if key_v.Kind() == reflect.Int64 {
		key = strconv.FormatInt(key_v.Int(), 10)
	}

	if key_v.Kind() == reflect.Float64 {
		key = strconv.FormatFloat(key_v.Float(), 'f', 1, 64)
	}

	return key, nil

}
