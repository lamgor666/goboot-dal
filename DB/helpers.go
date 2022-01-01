package DB

import (
	"database/sql"
	"fmt"
	"github.com/lamgor666/goboot-common/util/slicex"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"
)

const (
	dateFormatFull     = "2006-01-02 15:04:05"
	dateFormatDateOnly = "2006-01-02"
)

var regexpAS = regexp.MustCompile(`(?i)[\x20\t]+as[\x20\t]+`)
var regexpSpace = regexp.MustCompile(`[\x20\t]+`)
var regexpGormColumn = regexp.MustCompile(`column[\x20\t]*:[\x20\t]*(^[\x20\t;]+)`)
var regexpCommaSep = regexp.MustCompile(`[\x20\t]*,[\x20\t]*`)

func parseToNameAndAlias(str string) (string, string) {
	var parts []string

	if regexpAS.MatchString(str) {
		parts = regexpAS.Split(str, -1)
	} else {
		parts = strings.Split(regexpSpace.ReplaceAllString(str, " "), " ")
	}

	if len(parts) < 1 {
		return "", ""
	}

	if len(parts) > 1 {
		return parts[0], parts[1]
	}

	return parts[0], ""
}

func quote(str string) string {
	str = strings.ReplaceAll(str, "`", "")

	if !strings.Contains(str, ".") {
		if str == "*" {
			return str
		}

		return "`" + str + "`"
	}

	p1 := substringBefore(str, ".")
	p2 := substringAfter(str, ".")

	if p2 != "*" {
		p2 = ensureLeft(p2, "`")
		p2 = ensureRight(p2, "`")
	}

	return p1 + "." + p2
}

func parseOrderBy(orderBy string) string {
	parts := regexpSpace.Split(orderBy, -1)

	if len(parts) < 2 {
		return quote(parts[0]) + " ASC"
	}

	return quote(parts[0]) + " " + strings.ToUpper(parts[1])
}

func buildScanFields(rs *sql.Rows) (scanFields []*scanField, scanArgs []interface{}) {
	defer func() {
		if r := recover(); r != nil {
			scanFields = make([]*scanField, 0)
			scanArgs = make([]interface{}, 0)
		}
	}()

	columnTypes, _ := rs.ColumnTypes()
	n1 := len(columnTypes)
	scanFields = make([]*scanField, n1, n1)
	scanArgs = make([]interface{}, n1, n1)

	for idx, columnType := range columnTypes {
		scanType := columnType.ScanType().Name()

		if strings.Contains(scanType, "NullString") {
			field := &scanField{TypeName: "NullString"}
			scanFields[idx] = field
			scanArgs[idx] = &field.NullStringVal
			continue
		}

		if strings.Contains(scanType, "NullBool") {
			field := &scanField{TypeName: "NullBool"}
			scanFields[idx] = field
			scanArgs[idx] = &field.NullBoolVal
			continue
		}

		if strings.Contains(scanType, "NullInt32") {
			field := &scanField{TypeName: "NullInt32"}
			scanFields[idx] = field
			scanArgs[idx] = &field.NullInt32Val
			continue
		}

		if strings.Contains(scanType, "NullInt64") {
			field := &scanField{TypeName: "NullInt64"}
			scanFields[idx] = field
			scanArgs[idx] = &field.NullInt64Val
			continue
		}

		if strings.Contains(scanType, "NullFloat64") {
			field := &scanField{TypeName: "NullFloat64"}
			scanFields[idx] = field
			scanArgs[idx] = &field.NullFloat64Val
			continue
		}

		if strings.Contains(scanType, "NullTime") {
			field := &scanField{TypeName: "NullTime"}
			scanFields[idx] = field
			scanArgs[idx] = &field.NullTimeVal
			continue
		}

		scanType = strings.ToLower(scanType)

		if strings.Contains(scanType, "rawbytes") {
			field := &scanField{TypeName: "NullString"}
			scanFields[idx] = field
			scanArgs[idx] = &field.NullStringVal
			continue
		}

		if strings.Contains(scanType, "string") {
			field := &scanField{TypeName: "string"}
			scanFields[idx] = field
			scanArgs[idx] = &field.StringVal
			continue
		}

		if strings.Contains(scanType, "bool") {
			field := &scanField{TypeName: "bool"}
			scanFields[idx] = field
			scanArgs[idx] = &field.BoolVal
			continue
		}

		if strings.Contains(scanType, "int64") {
			field := &scanField{TypeName: "int64"}
			scanFields[idx] = field
			scanArgs[idx] = &field.Int64Val
			continue
		}

		if strings.Contains(scanType, "int") {
			field := &scanField{TypeName: "int"}
			scanFields[idx] = field
			scanArgs[idx] = &field.IntVal
			continue
		}

		if strings.Contains(scanType, "float") {
			field := &scanField{TypeName: "float64"}
			scanFields[idx] = field
			scanArgs[idx] = &field.Float64Val
			continue
		}

		if strings.Contains(scanType, "time") {
			field := &scanField{TypeName: "time"}
			scanFields[idx] = field
			scanArgs[idx] = &field.TimeVal
			continue
		}

		field := &scanField{TypeName: "interface"}
		scanFields[idx] = field
		scanArgs[idx] = &field.InterfaceVal
	}

	return
}

func scanIntoMapList(rs *sql.Rows) ([]map[string]interface{}, error) {
	if rs == nil {
		return []map[string]interface{}{}, nil
	}

	defer rs.Close()
	list := make([]map[string]interface{}, 0)

	for rs.Next() {
		item, err := scanIntoMap(rs)

		if err != nil {
			return []map[string]interface{}{}, err
		}

		list = append(list, item)
	}

	return list, nil
}

func scanIntoMap(rs *sql.Rows) (map[string]interface{}, error) {
	columnNames, err := rs.Columns()

	if err != nil {
		return map[string]interface{}{}, err
	}

	columnTypes, err := rs.ColumnTypes()

	if err != nil || len(columnTypes) != len(columnNames) {
		return map[string]interface{}{}, err
	}

	scanFields, scanArgs := buildScanFields(rs)

	if len(scanFields) < 1 || len(scanArgs) < 1 || len(scanFields) != len(scanArgs) || len(scanFields) != len(columnNames) {
		return map[string]interface{}{}, NewDbException("scan error")
	}

	for _, scanField := range scanFields {
		if scanField == nil {
			return map[string]interface{}{}, NewDbException("scan error")
		}
	}

	for _, scanArg := range scanArgs {
		if scanArg == nil {
			return map[string]interface{}{}, NewDbException("scan error")
		}
	}

	if err := rs.Scan(scanArgs...); err != nil {
		return map[string]interface{}{}, err
	}

	data := map[string]interface{}{}

	for idx, columnName := range columnNames {
		scanField := scanFields[idx]

		switch scanField.TypeName {
		case "NullString":
			if scanField.NullStringVal.Valid {
				data[columnName] = scanField.NullStringVal.String
			} else {
				data[columnName] = ""
			}

			break
		case "NullBool":
			if scanField.NullBoolVal.Valid {
				data[columnName] = scanField.NullBoolVal.Bool
			} else {
				data[columnName] = false
			}

			break
		case "NullInt32":
			if scanField.NullInt32Val.Valid {
				data[columnName] = int(scanField.NullInt32Val.Int32)
			} else {
				data[columnName] = 0
			}

			break
		case "NullInt64":
			if scanField.NullInt64Val.Valid {
				data[columnName] = scanField.NullInt64Val.Int64
			} else {
				data[columnName] = 0
			}

			break
		case "NullFloat64":
			if scanField.NullFloat64Val.Valid {
				data[columnName] = toDecimalString(scanField.NullFloat64Val)
			} else {
				data[columnName] = "0.00"
			}

			break
		case "NullTime":
			if scanField.NullTimeVal.Valid {
				t1 := scanField.NullTimeVal.Time

				if t1.Unix() > 0 {
					dbType := strings.ToLower(columnTypes[idx].DatabaseTypeName())

					if strings.Contains(dbType, "datetime") {
						data[columnName] = t1.Format(dateFormatFull)
					} else if strings.Contains(dbType, "date") {
						data[columnName] = t1.Format(dateFormatDateOnly)
					}
				}
			} else {
				data[columnName] = nil
			}

			break
		case "string":
			data[columnName] = scanField.StringVal
			break
		case "bool":
			data[columnName] = scanField.BoolVal
			break
		case "int":
			data[columnName] = scanField.IntVal
			break
		case "int64":
			data[columnName] = scanField.Int64Val
			break
		case "float64":
			data[columnName] = toDecimalString(scanField.Float64Val)
			break
		case "time":
			t1 := scanField.TimeVal

			if t1.Unix() > 0 {
				dbType := strings.ToLower(columnTypes[idx].DatabaseTypeName())

				if strings.Contains(dbType, "datetime") {
					data[columnName] = t1.Format(dateFormatFull)
				} else if strings.Contains(dbType, "date") {
					data[columnName] = t1.Format(dateFormatDateOnly)
				}
			}

			break
		case "interface":
			data[columnName] = scanField.InterfaceVal
			break
		}
	}

	return data, nil
}

func scanIntoModel(rs *sql.Rows, model interface{}) error {
	err1 := NewDbException("model is not struct pointer")

	if model == nil {
		return err1
	}

	rt := reflect.TypeOf(model)

	if rt.Kind() != reflect.Ptr || rt.Elem().Kind() != reflect.Struct {
		return err1
	}

	columnNames, err := rs.Columns()

	if err != nil {
		return err
	}

	scanFields, scanArgs := buildScanFields(rs)

	if len(scanFields) < 1 || len(scanArgs) < 1 || len(scanFields) != len(scanArgs) || len(scanFields) != len(columnNames) {
		return NewDbException("scan error")
	}

	for _, scanField := range scanFields {
		if scanField == nil {
			return NewDbException("scan error")
		}
	}

	for _, scanArg := range scanArgs {
		if scanArg == nil {
			return NewDbException("scan error")
		}
	}

	if err := rs.Scan(scanArgs...); err != nil {
		return err
	}

	rt = rt.Elem()
	ptr := getStructPtr(model)

	for i := 0; i < rt.NumField(); i++ {
		field := rt.Field(i)
		idx := getColumnIdxByGormTag(field.Tag, columnNames)

		if idx < 0 {
			idx = getColumnIdxByStructFieldName(field.Name, columnNames)
		}

		if idx < 0 {
			continue
		}

		scanField := scanFields[idx]

		switch field.Type.Kind() {
		case reflect.String:
			switch scanField.TypeName {
			case "NullString":
				if scanField.NullStringVal.Valid {
					setStringToStructField(ptr, field, scanField.NullStringVal.String)
				}

				break
			case "string":
				setStringToStructField(ptr, field, scanField.StringVal)
				break
			}

			continue
		case reflect.Bool:
			switch scanField.TypeName {
			case "NullBool":
				if scanField.NullBoolVal.Valid {
					setBoolToStructField(ptr, field, scanField.NullBoolVal.Bool)
				}

				break
			case "bool":
				setBoolToStructField(ptr, field, scanField.BoolVal)
				break
			case "int":
				setBoolToStructField(ptr, field, scanField.IntVal == 1)
				break
			}

			continue
		case reflect.Int:
			switch scanField.TypeName {
			case "NullInt64":
				if scanField.NullInt64Val.Valid {
					setIntToStructField(ptr, field, int(scanField.NullInt64Val.Int64))
				}

				break
			case "NullInt32":
				if scanField.NullInt32Val.Valid {
					setIntToStructField(ptr, field, int(scanField.NullInt32Val.Int32))
				}

				break
			case "int64":
				setIntToStructField(ptr, field, int(scanField.Int64Val))
				break
			case "int":
				setIntToStructField(ptr, field, scanField.IntVal)
				break
			case "string":
				if n1, err := strconv.Atoi(scanField.StringVal); err == nil {
					setIntToStructField(ptr, field, n1)
				}

				break
			}

			continue
		case reflect.Int32:
			switch scanField.TypeName {
			case "NullInt64":
				if scanField.NullInt64Val.Valid {
					setInt32ToStructField(ptr, field, int32(scanField.NullInt64Val.Int64))
				}

				break
			case "NullInt32":
				if scanField.NullInt32Val.Valid {
					setInt32ToStructField(ptr, field, scanField.NullInt32Val.Int32)
				}

				break
			case "int64":
				setInt32ToStructField(ptr, field, int32(scanField.Int64Val))
				break
			case "int":
				setInt32ToStructField(ptr, field, int32(scanField.IntVal))
				break
			case "string":
				if n1, err := strconv.ParseInt(scanField.StringVal, 10, 32); err == nil {
					setInt32ToStructField(ptr, field, int32(n1))
				}

				break
			}

			continue
		case reflect.Int64:
			switch scanField.TypeName {
			case "NullInt64":
				if scanField.NullInt64Val.Valid {
					setInt64ToStructField(ptr, field, scanField.NullInt64Val.Int64)
				}

				break
			case "NullInt32":
				if scanField.NullInt32Val.Valid {
					setInt64ToStructField(ptr, field, int64(scanField.NullInt32Val.Int32))
				}

				break
			case "int64":
				setInt64ToStructField(ptr, field, scanField.Int64Val)
			case "int":
				setInt64ToStructField(ptr, field, int64(scanField.IntVal))
				break
			case "string":
				if n1, err := strconv.ParseInt(scanField.StringVal, 10, 64); err == nil {
					setInt64ToStructField(ptr, field, n1)
				}

				break
			}

			continue
		case reflect.Uint:
			switch scanField.TypeName {
			case "NullInt64":
				if scanField.NullInt64Val.Valid {
					setUintToStructField(ptr, field, uint(scanField.NullInt64Val.Int64))
				}

				break
			case "NullInt32":
				if scanField.NullInt32Val.Valid {
					setUintToStructField(ptr, field, uint(scanField.NullInt32Val.Int32))
				}

				break
			case "int64":
				setUintToStructField(ptr, field, uint(scanField.Int64Val))
				break
			case "int":
				setUintToStructField(ptr, field, uint(scanField.IntVal))
				break
			case "string":
				if n1, err := strconv.Atoi(scanField.StringVal); err == nil {
					setUintToStructField(ptr, field, uint(n1))
				}

				break
			}

			continue
		case reflect.Uint32:
			switch scanField.TypeName {
			case "NullInt64":
				if scanField.NullInt64Val.Valid {
					setUint32ToStructField(ptr, field, uint32(scanField.NullInt64Val.Int64))
				}

				break
			case "NullInt32":
				if scanField.NullInt32Val.Valid {
					setUint32ToStructField(ptr, field, uint32(scanField.NullInt32Val.Int32))
				}

				break
			case "int64":
				setUint32ToStructField(ptr, field, uint32(scanField.Int64Val))
				break
			case "int":
				setUint32ToStructField(ptr, field, uint32(scanField.IntVal))
				break
			case "string":
				if n1, err := strconv.ParseInt(scanField.StringVal, 10, 32); err == nil {
					setUint32ToStructField(ptr, field, uint32(n1))
				}

				break
			}

			continue
		case reflect.Uint64:
			switch scanField.TypeName {
			case "NullInt64":
				if scanField.NullInt64Val.Valid {
					setUint64ToStructField(ptr, field, uint64(scanField.NullInt64Val.Int64))
				}

				break
			case "NullInt32":
				if scanField.NullInt32Val.Valid {
					setUint64ToStructField(ptr, field, uint64(scanField.NullInt32Val.Int32))
				}

				break
			case "int64":
				setUint64ToStructField(ptr, field, uint64(scanField.Int64Val))
				break
			case "int":
				setUint64ToStructField(ptr, field, uint64(scanField.IntVal))
				break
			case "string":
				if n1, err := strconv.ParseInt(scanField.StringVal, 10, 64); err == nil {
					setUint64ToStructField(ptr, field, uint64(n1))
				}

				break
			}

			continue
		case reflect.Float32:
			switch scanField.TypeName {
			case "NullFloat64":
				if scanField.NullFloat64Val.Valid {
					setFloat32ToStructField(ptr, field, float32(scanField.NullFloat64Val.Float64))
				}

				break
			case "float64":
				setFloat32ToStructField(ptr, field, float32(scanField.Float64Val))
				break
			case "string":
				if n1, err := strconv.ParseFloat(scanField.StringVal, 32); err == nil {
					setFloat32ToStructField(ptr, field, float32(n1))
				}

				break
			}

			continue
		case reflect.Float64:
			switch scanField.TypeName {
			case "NullFloat64":
				if scanField.NullFloat64Val.Valid {
					setFloat64ToStructField(ptr, field, scanField.NullFloat64Val.Float64)
				}

				break
			case "float64":
				setFloat64ToStructField(ptr, field, scanField.Float64Val)
				break
			case "string":
				if n1, err := strconv.ParseFloat(scanField.StringVal, 64); err == nil {
					setFloat64ToStructField(ptr, field, n1)
				}

				break
			}

			continue
		}

		fieldType := fmt.Sprintf("%v", field.Type)

		switch fieldType {
		case "time.Time":
			switch scanField.TypeName {
			case "NullTime":
				if scanField.NullTimeVal.Valid {
					setTimeToStructField(ptr, field, scanField.NullTimeVal.Time)
				}

				break
			case "time":
				setTimeToStructField(ptr, field, scanField.TimeVal)
				break
			case "string":
				s1 := scanField.StringVal

				if d1, err := time.ParseInLocation(dateFormatFull, s1, time.Local); err == nil {
					setTimeToStructField(ptr, field, d1)
				} else if d1, err := time.ParseInLocation(dateFormatDateOnly, s1, time.Local); err == nil {
					setTimeToStructField(ptr, field, d1)
				}

				break
			}

			break
		case "*time.Time":
			switch scanField.TypeName {
			case "NullTime":
				if scanField.NullTimeVal.Valid {
					setTimePtrToStructField(ptr, field, &scanField.NullTimeVal.Time)
				}

				break
			case "time":
				t1 := scanField.TimeVal
				setTimePtrToStructField(ptr, field, &t1)
				break
			case "string":
				s1 := scanField.StringVal

				if d1, err := time.ParseInLocation(dateFormatFull, s1, time.Local); err == nil {
					setTimePtrToStructField(ptr, field, &d1)
				} else if d1, err := time.ParseInLocation(dateFormatDateOnly, s1, time.Local); err == nil {
					setTimePtrToStructField(ptr, field, &d1)
				}

				break
			}

			break
		}
	}

	return nil
}

func getColumnIdxByGormTag(tag reflect.StructTag, columnNames []string) int {
	s1 := tag.Get("gorm")

	if s1 == "" {
		return -1
	}

	matches := regexpGormColumn.FindStringSubmatch(s1)

	if len(matches) < 2 {
		return -1
	}

	s2 := strings.ToLower(matches[1])

	for idx, columnName := range columnNames {
		if strings.ToLower(columnName) == s2 {
			return idx
		}
	}

	return -1
}

func getColumnIdxByStructFieldName(fieldName string, columnNames []string) int {
	s1 := strings.ToLower(fieldName)

	for idx, columnName := range columnNames {
		s2 := strings.ReplaceAll(columnName, "-", "")
		s2 = strings.ReplaceAll(s2, "_", "")
		s2 = strings.ToLower(s2)

		if s2 == s1 {
			return idx
		}
	}

	return -1
}

func getParamsAndTimeout(args []interface{}) (params []interface{}, timeout time.Duration) {
	params = make([]interface{}, 0)

	for _, arg := range args {
		if d1, ok := arg.(time.Duration); ok {
			if d1 > 0 {
				timeout = d1
			}

			continue
		}

		if n1, ok := arg.(int); ok {
			if n1 > 0 {
				timeout = time.Duration(int64(n1)) * time.Second
			}

			continue
		}

		if s1, ok := arg.(string); ok {
			d1, err := time.ParseDuration(s1)

			if err == nil && d1 > 0 {
				timeout = d1
			}

			continue
		}

		if a1, ok := arg.([]interface{}); ok && len(a1) > 0 {
			params = a1
		}
	}

	return
}

func getFieldNamesAndTimeout(args []interface{}) (fieldNames []string, timeout time.Duration) {
	fieldNames = []string{}
	timeout = 0

	for _, arg := range args {
		if d1, ok := arg.(time.Duration); ok {
			if d1 > 0 {
				timeout = d1
			}

			continue
		}

		if n1, ok := arg.(int); ok {
			if n1 > 0 {
				timeout = time.Duration(int64(n1)) * time.Second
			}

			continue
		}

		if a1, ok := arg.([]string); ok {
			if len(a1) > 0 {
				fieldNames = a1
			}

			continue
		}

		if s1, ok := arg.(string); ok && s1 != "" {
			fieldNames = regexpCommaSep.Split(s1, -1)
		}
	}

	return
}

func getColumnNameBySturctField(tableName, fieldName string, tag reflect.StructTag) string {
	gormTag := tag.Get("gorm")

	if gormTag != "" {
		groups := regexpGormColumn.FindStringSubmatch(gormTag)

		if len(groups) > 1 {
			return groups[1]
		}
	}

	schemas, ok := tableSchemas[tableName]

	if ok && len(schemas) > 0 {
		s1 := strings.ToLower(fieldName)

		for _, item := range schemas {
			s2 := strings.ReplaceAll(item.FieldName, "-", "")
			s2 = strings.ReplaceAll(s2, "_", "")
			s2 = strings.ToLower(s2)

			if s2 == s1 {
				return item.FieldName
			}
		}
	}

	return lcfirst(fieldName)
}

func isPkField(tableName, columnName string, tag reflect.StructTag) bool {
	gormTag := tag.Get("gorm")

	if gormTag != "" && strings.Contains(gormTag, "primary_key") {
		return true
	}

	schemas, ok := tableSchemas[tableName]

	if !ok || len(schemas) < 1 {
		return false
	}

	for _, item := range schemas {
		if item.FieldName == columnName && item.IsPrimaryKey {
			return true
		}
	}

	return false
}

func autoAddCreateTime(tableName string, data map[string]interface{}) {
	if strings.Contains(tableName, ".") {
		tableName = substringAfter(tableName, ".")
	}

	tableName = strings.ReplaceAll(tableName, "`", "")
	schemas, ok := tableSchemas[tableName]

	if !ok {
		return
	}

	fieldNames := []string{
		"ctime",
		"create_at",
		"createAt",
		"create_time",
		"createTime",
	}

	for _, item := range schemas {
		if slicex.InStringSlice(item.FieldName, fieldNames) && item.FieldType == "datetime" {
			data[item.FieldName] = time.Now().Format(dateFormatFull)
			break
		}
	}
}

func autoAddUpdateTime(tableName string, data map[string]interface{}) {
	if strings.Contains(tableName, ".") {
		tableName = substringAfter(tableName, ".")
	}

	tableName = strings.ReplaceAll(tableName, "`", "")
	schemas, ok := tableSchemas[tableName]

	if !ok {
		return
	}

	fieldNames := []string{
		"update_at",
		"updateAt",
	}

	for _, item := range schemas {
		if slicex.InStringSlice(item.FieldName, fieldNames) && item.FieldType == "datetime" {
			data[item.FieldName] = time.Now().Format(dateFormatFull)
			return
		}
	}
}

func substringBefore(str, delimiter string, last ...bool) string {
	var idx int

	if len(last) > 0 && last[0] {
		idx = strings.LastIndex(str, delimiter)
	} else {
		strings.Index(str, delimiter)
	}

	if idx < 1 {
		return ""
	}

	return str[:idx]
}

func substringAfter(str, delimiter string, last ...bool) string {
	var idx int

	if len(last) > 0 && last[0] {
		idx = strings.LastIndex(str, delimiter)
	} else {
		strings.Index(str, delimiter)
	}

	if idx < 0 {
		return ""
	}

	idx += len(delimiter)

	if idx >= len(str) {
		return ""
	}

	return str[idx:]
}

func ensureLeft(str, prefix string) string {
	if str == "" || strings.HasPrefix(str, prefix) {
		return str
	}

	return prefix + str
}

func ensureRight(str, suffix string) string {
	if str == "" || strings.HasSuffix(str, suffix) {
		return str
	}

	return str + suffix
}

func lcfirst(str string) string {
	if str == "" {
		return ""
	}

	if len(str) < 2 {
		return strings.ToLower(str)
	}

	return strings.ToLower(str[:1]) + str[1:]
}

func toString(arg0 interface{}) string {
	if arg0 == nil {
		return ""
	}

	switch rv := arg0.(type) {
	case int8:
		return strconv.FormatInt(int64(rv), 10)
	case int16:
		return strconv.FormatInt(int64(rv), 10)
	case int32:
		return strconv.FormatInt(int64(rv), 10)
	case int:
		return strconv.FormatInt(int64(rv), 10)
	case int64:
		return strconv.FormatInt(rv, 10)
	case uint8:
		return strconv.FormatUint(uint64(rv), 10)
	case uint16:
		return strconv.FormatUint(uint64(rv), 10)
	case uint32:
		return strconv.FormatUint(uint64(rv), 10)
	case uint64:
		return strconv.FormatUint(rv, 10)
	case bool:
		if arg0.(bool) {
			return "true"
		}

		return "false"
	case string:
		return arg0.(string)
	}

	return ""
}

func toInt(arg0 interface{}, defaultValue ...int) int {
	var _defaultValue int

	if len(defaultValue) > 0 {
		_defaultValue = defaultValue[0]
	}

	if arg0 == nil {
		return _defaultValue
	}

	switch num := arg0.(type) {
	case int8:
		return int(num)
	case int16:
		return int(num)
	case int32:
		return int(num)
	case int:
		return num
	case int64:
		s1 := fmt.Sprintf("%d", num)

		if n1, err := strconv.Atoi(s1); err == nil {
			return n1
		}

		return _defaultValue
	case uint8:
		return int(num)
	case uint16:
		return int(num)
	case uint32:
		return int(num)
	case uint64:
		return int(num)
	case float32:
		parts := strings.Split(fmt.Sprintf("%0.2f", num), ".")

		if n1, err := strconv.Atoi(parts[0]); err == nil {
			return n1
		}

		return _defaultValue
	case float64:
		parts := strings.Split(fmt.Sprintf("%0.2f", num), ".")

		if n1, err := strconv.Atoi(parts[0]); err == nil {
			return n1
		}

		return _defaultValue
	case string:
		if n1, err := strconv.Atoi(num); err == nil {
			return n1
		}
	}

	return _defaultValue
}

func toFloat64(arg0 interface{}, defaultValue ...float64) float64 {
	var _defaultValue float64
	
	if len(defaultValue) > 0 {
		_defaultValue = defaultValue[0]
	}
	
	if arg0 == nil {
		return _defaultValue
	}

	switch num := arg0.(type) {
	case int8:
		return float64(num)
	case int16:
		return float64(num)
	case int32:
		return float64(num)
	case int:
		return float64(num)
	case int64:
		return float64(num)
	case uint8:
		return float64(num)
	case uint16:
		return float64(num)
	case uint32:
		return float64(num)
	case uint64:
		return float64(num)
	case float32:
		return float64(num)
	case float64:
		return num
	case string:
		if n1, err := strconv.ParseFloat(num, 64); err == nil {
			return n1
		}
	}
	
	return _defaultValue
}

func toSlice(arg0 interface{}) []interface{} {
	if lst, ok := arg0.([]interface{}); ok {
		return lst
	}

	rt := reflect.TypeOf(arg0)
	rv := reflect.ValueOf(arg0)

	if rt.Kind() == reflect.Ptr {
		rt = rt.Elem()
		rv = rv.Elem()
	}

	list := make([]interface{}, 0)

	if rt.Kind() != reflect.Slice || rv.Len() < 1 {
		return list
	}

	for i := 0; i < rv.Len(); i++ {
		list = append(list, rv.Index(i).Interface())
	}

	return list
}

func toDecimalString(arg0 interface{}) string {
	str := fmt.Sprintf("%0.12f", toFloat64(arg0))
	parts := strings.Split(str, ".")
	p1 := parts[0]
	p2 := parts[1][:2]
	p2 = strings.TrimSuffix(p2, "0")
	
	if p2 == "" {
		return p1
	}

	return p1 + "." + p2
}

func inStringSlice(needle string, array []string, ignoreCase ...bool) bool {
	if needle == "" || len(array) < 1 {
		return false
	}

	_ignoreCase := false

	if len(ignoreCase) > 0 {
		_ignoreCase = ignoreCase[0]
	}

	var s1 string

	if _ignoreCase {
		s1 = strings.ToLower(needle)
	} else {
		s1 = needle
	}

	for _, s2 := range array {
		if _ignoreCase {
			s2 = strings.ToLower(s2)
		}

		if s1 == s2 {
			return true
		}
	}

	return false
}
