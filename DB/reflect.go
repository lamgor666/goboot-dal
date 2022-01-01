package DB

import (
	"reflect"
	"time"
	"unsafe"
)

func indirect(arg0 interface{}) interface{} {
	if arg0 == nil {
		return nil
	}

	if rt := reflect.TypeOf(arg0); rt.Kind() != reflect.Ptr {
		return arg0
	}

	rv := reflect.ValueOf(arg0)

	for rv.Kind() == reflect.Ptr && !rv.IsNil() {
		rv = rv.Elem()
	}

	return rv.Interface()
}

func getStructPtr(arg0 interface{}) unsafe.Pointer {
	return unsafe.Pointer(reflect.ValueOf(arg0).Pointer())
}

func getStructFieldPtr(structPtr unsafe.Pointer, field reflect.StructField) uintptr {
	return uintptr(structPtr) + field.Offset
}

func setStringToStructField(structPtr unsafe.Pointer, field reflect.StructField, value string) {
	fieldPtr := getStructFieldPtr(structPtr, field)
	*((*string)(unsafe.Pointer(fieldPtr))) = value
}

func setBoolToStructField(structPtr unsafe.Pointer, field reflect.StructField, value bool) {
	fieldPtr := getStructFieldPtr(structPtr, field)
	*((*bool)(unsafe.Pointer(fieldPtr))) = value
}

func setIntToStructField(structPtr unsafe.Pointer, field reflect.StructField, value int) {
	fieldPtr := getStructFieldPtr(structPtr, field)
	*((*int)(unsafe.Pointer(fieldPtr))) = value
}

func setInt32ToStructField(structPtr unsafe.Pointer, field reflect.StructField, value int32) {
	fieldPtr := getStructFieldPtr(structPtr, field)
	*((*int32)(unsafe.Pointer(fieldPtr))) = value
}

func setInt64ToStructField(structPtr unsafe.Pointer, field reflect.StructField, value int64) {
	fieldPtr := getStructFieldPtr(structPtr, field)
	*((*int64)(unsafe.Pointer(fieldPtr))) = value
}

func setUintToStructField(structPtr unsafe.Pointer, field reflect.StructField, value uint) {
	fieldPtr := getStructFieldPtr(structPtr, field)
	*((*uint)(unsafe.Pointer(fieldPtr))) = value
}

func setUint32ToStructField(structPtr unsafe.Pointer, field reflect.StructField, value uint32) {
	fieldPtr := getStructFieldPtr(structPtr, field)
	*((*uint32)(unsafe.Pointer(fieldPtr))) = value
}

func setUint64ToStructField(structPtr unsafe.Pointer, field reflect.StructField, value uint64) {
	fieldPtr := getStructFieldPtr(structPtr, field)
	*((*uint64)(unsafe.Pointer(fieldPtr))) = value
}

func setFloat32ToStructField(structPtr unsafe.Pointer, field reflect.StructField, value float32) {
	fieldPtr := getStructFieldPtr(structPtr, field)
	*((*float32)(unsafe.Pointer(fieldPtr))) = value
}

func setFloat64ToStructField(structPtr unsafe.Pointer, field reflect.StructField, value float64) {
	fieldPtr := getStructFieldPtr(structPtr, field)
	*((*float64)(unsafe.Pointer(fieldPtr))) = value
}

func setTimeToStructField(structPtr unsafe.Pointer, field reflect.StructField, value time.Time) {
	fieldPtr := getStructFieldPtr(structPtr, field)
	*((*time.Time)(unsafe.Pointer(fieldPtr))) = value
}

func setTimePtrToStructField(structPtr unsafe.Pointer, field reflect.StructField, value *time.Time) {
	fieldPtr := getStructFieldPtr(structPtr, field)
	*((**time.Time)(unsafe.Pointer(fieldPtr))) = value
}
