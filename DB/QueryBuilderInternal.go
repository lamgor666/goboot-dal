package DB

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/lamgor666/goboot-common/util/slicex"
	"reflect"
	"strings"
	"time"
)

func (qb *queryBuilder) addTable(tableName string) *queryBuilder {
	if len(qb.tables) < 1 {
		qb.tables = make([]table, 0)
	}

	name, alias := parseToNameAndAlias(tableName)
	idx := -1

	for i, item := range qb.tables {
		if item.name == name {
			idx = i
			break
		}
	}

	item := table{name: name, alias: alias}

	if idx >= 0 {
		qb.tables[idx] = item
	} else {
		qb.tables = append(qb.tables, item)
	}

	return qb
}

func (qb *queryBuilder) addColumn(columnName string) *queryBuilder {
	if len(qb.columns) < 1 {
		qb.columns = make([]column, 0)
	}

	name, alias := parseToNameAndAlias(columnName)
	idx := -1

	for i, item := range qb.columns {
		if item.name == name {
			idx = i
			break
		}
	}

	item := column{name: name, alias: alias}

	if idx >= 0 {
		qb.columns[idx] = item
	} else {
		qb.columns = append(qb.columns, item)
	}

	return qb
}

func (qb *queryBuilder) addJoinClause(clause joinClause) *queryBuilder {
	qb.joinClauses = append(qb.joinClauses, clause)
	return qb
}

func (qb *queryBuilder) addCondition(condition string, or ...bool) *queryBuilder {
	n1 := len(qb.conditions)

	if len(or) < 1 || !or[0] || n1 < 1 {
		qb.conditions = append(qb.conditions, condition)
		return qb
	}

	lastCondition := qb.conditions[n1-1]
	condition = fmt.Sprintf("(%s OR %s)", lastCondition, condition)
	qb.conditions[n1-1] = condition
	return qb
}

func (qb *queryBuilder) addBindValues(args ...interface{}) *queryBuilder {
	if len(args) < 1 {
		return qb
	}

	qb.bindValues = append(qb.bindValues, args...)
	return qb
}

func (qb *queryBuilder) addOrderBy(orderBy ...string) *queryBuilder {
	if len(orderBy) < 1 {
		return qb
	}

	for _, _orderBy := range orderBy {
		if _orderBy == "" {
			continue
		}

		qb.orderBy = append(qb.orderBy, parseOrderBy(_orderBy))
	}

	return qb
}

func (qb *queryBuilder) addGroupBy(groupBy ...string) *queryBuilder {
	if len(groupBy) < 1 {
		return qb
	}

	for _, _groupBy := range groupBy {
		if _groupBy == "" {
			continue
		}

		qb.groupBy = append(qb.groupBy, quote(_groupBy))
	}

	return qb
}

func (qb *queryBuilder) buildSelectFields() string {
	if len(qb.columns) < 1 {
		return "*"
	}

	sb := strings.Builder{}

	for idx, item := range qb.columns {
		if idx > 0 {
			sb.WriteString(", ")
		}

		sb.WriteString(item.nameWithAlias())
	}

	return sb.String()
}

func (qb *queryBuilder) buildJoinStatements() string {
	if len(qb.joinClauses) < 1 {
		return ""
	}

	sb := strings.Builder{}

	for idx, item := range qb.joinClauses {
		if idx > 0 {
			sb.WriteString(" ")
		}

		sb.WriteString(item.joinType)
		sb.WriteString(" JOIN ")
		sb.WriteString(item.tbl.nameWithAlias())
		sb.WriteString(" ON ")
		sb.WriteString(item.joinOn)
	}

	return sb.String()
}

func (qb *queryBuilder) buildLimitStatement() string {
	if len(qb.limit) < 1 {
		return ""
	}

	if len(qb.limit) > 1 {
		return fmt.Sprintf("LIMIT %d, %d", qb.limit[0], qb.limit[1])
	}

	return fmt.Sprintf("LIMIT %d", qb.limit[0])
}

func (qb *queryBuilder) buildSelectSql() (query string, params []interface{}) {
	params = make([]interface{}, 0)

	if len(qb.tables) < 1 {
		return
	}

	sb := strings.Builder{}
	sb.WriteString("SELECT ")
	sb.WriteString(qb.buildSelectFields())
	sb.WriteString(" FROM ")
	sb.WriteString(qb.tables[0].nameWithAlias())
	joins := qb.buildJoinStatements()

	if joins != "" {
		sb.WriteString(" " + joins)
	}

	if len(qb.conditions) > 0 {
		sb.WriteString(" WHERE ")
		sb.WriteString(strings.Join(qb.conditions, " AND "))
	}

	if len(qb.groupBy) > 0 {
		sb.WriteString(" GROUP BY ")
		sb.WriteString(strings.Join(qb.groupBy, ", "))
	}

	if len(qb.orderBy) > 0 {
		sb.WriteString(" ORDER BY ")
		sb.WriteString(strings.Join(qb.orderBy, ", "))
	}

	limits := qb.buildLimitStatement()

	if limits != "" {
		sb.WriteString(" " + limits)
	}

	query = sb.String()

	if len(qb.bindValues) > 0 {
		params = append(params, qb.bindValues...)
	}

	return
}

func (qb *queryBuilder) buildCountSql(countField string) (query string, params []interface{}) {
	params = make([]interface{}, 0)

	if len(qb.tables) < 1 {
		return
	}

	if countField != "*" {
		countField = quote(countField)
	}

	sb := strings.Builder{}
	sb.WriteString(fmt.Sprintf("SELECT COUNT(%s) FROM ", countField))
	sb.WriteString(qb.tables[0].nameWithAlias())
	joins := qb.buildJoinStatements()

	if joins != "" {
		sb.WriteString(" " + joins)
	}

	if len(qb.conditions) > 0 {
		sb.WriteString(" WHERE ")
		sb.WriteString(strings.Join(qb.conditions, " AND "))
	}

	query = sb.String()

	if len(qb.bindValues) > 0 {
		params = append(params, qb.bindValues...)
	}

	return
}

func (qb *queryBuilder) buildSumSql(fieldName string) (query string, params []interface{}) {
	params = make([]interface{}, 0)

	if len(qb.tables) < 1 {
		return
	}

	sb := strings.Builder{}
	sb.WriteString(fmt.Sprintf("SELECT SUM(%s) FROM ", quote(fieldName)))
	sb.WriteString(qb.tables[0].nameWithAlias())
	joins := qb.buildJoinStatements()

	if joins != "" {
		sb.WriteString(" " + joins)
	}

	if len(qb.conditions) > 0 {
		sb.WriteString(" WHERE ")
		sb.WriteString(strings.Join(qb.conditions, " AND "))
	}

	sb.WriteString(" LIMIT 1")
	query = sb.String()

	if len(qb.bindValues) > 0 {
		params = append(params, qb.bindValues...)
	}

	return
}

func (qb *queryBuilder) buildInsertSqlByMap(data map[string]interface{}) (query string, params []interface{}) {
	params = make([]interface{}, 0)

	if len(qb.tables) < 1 || len(data) < 1 {
		return
	}

	autoAddCreateTime(qb.tables[0].name, data)
	var columns []string
	var values []string

	for columnName, bindValue := range data {
		columns = append(columns, quote(columnName))
		bindValue = indirect(bindValue)

		if bindValue == nil {
			values = append(values, "null")
			continue
		}

		if v, ok := bindValue.(rawSql); ok {
			values = append(values, v.expr)
			continue
		}

		values = append(values, "?")
		params = append(params, bindValue)
	}

	sb := strings.Builder{}
	sb.WriteString("INSERT INTO ")
	sb.WriteString(qb.tables[0].nameWithAlias())
	sb.WriteString(" (")
	sb.WriteString(strings.Join(columns, ", "))
	sb.WriteString(")")
	sb.WriteString(" VALUES (")
	sb.WriteString(strings.Join(values, ", "))
	sb.WriteString(")")
	query = sb.String()
	return
}

func (qb *queryBuilder) buildInsertSqlByModel(
	rt reflect.Type,
	rv reflect.Value,
) (query, pkField string, params []interface{}) {
	defer func() {
		qb.includeFields = []string{}
		qb.excludeFields = []string{}
	}()

	tableName := qb.tables[0].name
	data := map[string]interface{}{}

	for i := 0; i < rt.NumField(); i++ {
		fieldName := rt.Field(i).Name

		if len(qb.includeFields) > 0 && !inStringSlice(fieldName, qb.includeFields) {
			continue
		}

		if len(qb.excludeFields) > 0 && inStringSlice(fieldName, qb.excludeFields) {
			continue
		}

		tag := rt.Field(i).Tag
		columnName := getColumnNameBySturctField(tableName, fieldName, tag)

		if columnName == "" {
			continue
		}

		if pkField == "" && isPkField(tableName, columnName, tag) {
			pkField = fieldName
			continue
		}

		field := rv.Field(i)

		if t1, ok := field.Interface().(time.Time); ok {
			s1 := qb.handleDatetimeFieldInModel(tableName, columnName, &t1)

			if inStringSlice(s1, []string{"NotExists", "NotMatched", "NotNullable"}) {
				continue
			}

			if s1 == "nil" {
				data[columnName] = nil
			} else {
				data[columnName] = s1
			}

			continue
		}

		if t1, ok := field.Interface().(*time.Time); ok {
			s1 := qb.handleDatetimeFieldInModel(tableName, columnName, t1)

			if inStringSlice(s1, []string{"NotExists", "NotMatched", "NotNullable"}) {
				continue
			}

			if s1 == "nil" {
				data[columnName] = nil
			} else {
				data[columnName] = s1
			}

			continue
		}

		if field.Kind() == reflect.Ptr {
			if field.IsNil() {
				data[columnName] = nil
			} else {
				data[columnName] = field.Elem().Interface()
			}

			continue
		}

		data[columnName] = field.Interface()
	}

	query, params = qb.buildInsertSqlByMap(data)
	return
}

func (qb *queryBuilder) buildUpdateSqlByMap(data map[string]interface{}) (query string, params []interface{}) {
	params = make([]interface{}, 0)

	if len(qb.tables) < 1 || len(data) < 1 {
		return
	}

	autoAddUpdateTime(qb.tables[0].name, data)
	var updateSet []string

	for columnName, bindValue := range data {
		bindValue = indirect(bindValue)

		if bindValue == nil {
			updateSet = append(updateSet, quote(columnName) + " = null")
			continue
		}

		if v, ok := bindValue.(rawSql); ok {
			updateSet = append(updateSet, quote(columnName) + " = " + v.expr)
			continue
		}

		updateSet = append(updateSet, quote(columnName) + " = ?")
		params = append(params, bindValue)
	}

	sb := strings.Builder{}
	sb.WriteString("UPDATE ")
	sb.WriteString(qb.tables[0].nameWithAlias())
	sb.WriteString(" SET ")
	sb.WriteString(strings.Join(updateSet, ", "))

	if len(qb.conditions) > 0 {
		sb.WriteString(" WHERE ")
		sb.WriteString(strings.Join(qb.conditions, " AND "))
	}

	if len(qb.bindValues) > 0 {
		params = append(params, qb.bindValues...)
	}

	query = sb.String()
	return
}

func (qb *queryBuilder) buildUpdateSqlByModel(rt reflect.Type, rv reflect.Value) (query string, params []interface{}) {
	defer func() {
		qb.includeFields = []string{}
		qb.excludeFields = []string{}
	}()

	tableName := qb.tables[0].name
	var pkField string
	data := map[string]interface{}{}

	for i := 0; i < rt.NumField(); i++ {
		fieldName := rt.Field(i).Name

		if len(qb.includeFields) > 0 && !inStringSlice(fieldName, qb.includeFields) {
			continue
		}

		if len(qb.excludeFields) > 0 && inStringSlice(fieldName, qb.excludeFields) {
			continue
		}

		tag := rt.Field(i).Tag
		columnName := getColumnNameBySturctField(tableName, fieldName, tag)

		if columnName == "" {
			continue
		}

		field := rv.Field(i)

		if pkField == "" && isPkField(tableName, columnName, tag) {
			var pkValue interface{}

			if field.Kind() == reflect.Ptr {
				pkValue = field.Elem().Interface()
			} else {
				pkValue = field.Interface()
			}

			qb.conditions = []string{}
			qb.bindValues = []interface{}{}
			qb.Where(columnName, pkValue)
			pkField = fieldName
			continue
		}

		if t1, ok := field.Interface().(time.Time); ok {
			s1 := qb.handleDatetimeFieldInModel(tableName, columnName, &t1)

			if inStringSlice(s1, []string{"NotExists", "NotMatched", "NotNullable"}) {
				continue
			}

			if s1 == "nil" {
				data[columnName] = nil
			} else {
				data[columnName] = s1
			}

			continue
		}

		if t1, ok := field.Interface().(*time.Time); ok {
			s1 := qb.handleDatetimeFieldInModel(tableName, columnName, t1)

			if inStringSlice(s1, []string{"NotExists", "NotMatched", "NotNullable"}) {
				continue
			}

			if s1 == "nil" {
				data[columnName] = nil
			} else {
				data[columnName] = s1
			}

			continue
		}

		if field.Kind() == reflect.Ptr {
			if field.IsNil() {
				data[columnName] = nil
			} else {
				data[columnName] = field.Elem().Interface()
			}

			continue
		}

		data[columnName] = field.Interface()
	}

	query, params = qb.buildUpdateSqlByMap(data)
	return
}

func (qb *queryBuilder) buildDeleteSql() (query string, params []interface{}) {
	params = make([]interface{}, 0)

	if len(qb.tables) < 1 {
		return
	}

	sb := strings.Builder{}
	sb.WriteString("DELETE FROM ")
	sb.WriteString(qb.tables[0].nameWithAlias())

	if len(qb.conditions) > 0 {
		sb.WriteString(" WHERE ")
		sb.WriteString(strings.Join(qb.conditions, " AND "))
	}

	query = sb.String()

	if len(qb.bindValues) > 0 {
		params = append(params, qb.bindValues...)
	}

	return
}

func (qb *queryBuilder) getForMapList(tx *sql.Tx, fieldNames ...interface{}) ([]map[string]interface{}, error) {
	defer func() {
		qb.timeout = 0
	}()

	if len(fieldNames) > 0 {
		qb.Select(fieldNames[0])
	}

	query, params := qb.buildSelectSql()

	if tx != nil {
		return TxSelectBySql(tx, query, params, qb.getTimeout())
	}

	return SelectBySql(query, params, qb.getTimeout())
}

func (qb *queryBuilder) getForModels(tx *sql.Tx, model interface{}, eachFn func(interface{})) error {
	defer func() {
		qb.timeout = 0
	}()

	err1 := NewException("model is not struct pointer")

	if model == nil {
		return err1
	}

	rt := reflect.TypeOf(model)

	if rt.Kind() != reflect.Ptr || rt.Elem().Kind() != reflect.Struct {
		return err1
	}

	rt = rt.Elem()

	query, params := qb.buildSelectSql()
	logSql(query, params)
	var err error

	if pool == nil {
		err = NewException("database connection pool is nil")
		writeLog("error", err)
		return err
	}

	ctx, cancel := context.WithTimeout(context.TODO(), qb.getTimeout())
	defer cancel()
	var rs *sql.Rows

	if tx != nil {
		rs, err = tx.QueryContext(ctx, query, params...)
	} else {
		rs, err = pool.QueryContext(ctx, query, params...)
	}

	if err != nil {
		writeLog("error", err)
		return toDbException(err)
	}

	if ctx.Err() != nil {
		writeLog("error", ctx.Err())
		return toDbException(ctx.Err())
	}

	defer rs.Close()

	for rs.Next() {
		rv := reflect.New(rt)

		if err = scanIntoModel(rs, rv.Interface()); err != nil {
			break
		}

		eachFn(rv.Elem().Interface())
	}

	if err != nil {
		writeLog("error", err)
	}

	return err
}

func (qb *queryBuilder) firstForMap(tx *sql.Tx, fieldNames ...interface{}) (map[string]interface{}, error) {
	defer func() {
		qb.timeout = 0
	}()

	if len(fieldNames) > 0 {
		qb.Select(fieldNames[0])
	}

	qb.limit = []int{1}
	list, err := qb.getForMapList(tx, fieldNames...)

	if err != nil {
		return nil, err
	}

	if len(list) < 1 {
		return nil, nil
	}

	return list[0], nil
}

func (qb *queryBuilder) firstForModel(tx *sql.Tx, model interface{}) error {
	defer func() {
		qb.timeout = 0
	}()

	qb.limit = []int{1}
	query, params := qb.buildSelectSql()
	logSql(query, params)
	var err error

	if pool == nil {
		err = NewException("database connection pool is nil")
		writeLog("error", err)
		return err
	}

	ctx, cancel := context.WithTimeout(context.TODO(), qb.getTimeout())
	defer cancel()
	var rs *sql.Rows

	if tx != nil {
		rs, err = tx.QueryContext(ctx, query, params...)
	} else {
		rs, err = pool.QueryContext(ctx, query, params...)
	}

	if err != nil {
		writeLog("error", err)
		return toDbException(err)
	}

	if ctx.Err() != nil {
		writeLog("error", ctx.Err())
		return toDbException(ctx.Err())
	}

	defer rs.Close()

	for rs.Next() {
		err = scanIntoModel(rs, model)
		break
	}

	if err != nil {
		writeLog("error", err)
	}

	return err
}

func (qb *queryBuilder) getColumnValue(
	tx *sql.Tx, columnName string,
	defaultValue ...interface{},
) (interface{}, error) {
	var _defaultValue interface{}

	if len(defaultValue) > 0 {
		_defaultValue = defaultValue[0]
	}

	data, err := qb.firstForMap(tx, columnName)

	if err != nil {
		return _defaultValue, err
	}

	if len(data) < 1 {
		return _defaultValue, nil
	}

	value, ok := data[columnName]

	if !ok || value == nil {
		return _defaultValue, nil
	}

	return value, nil
}

func (qb *queryBuilder) getStringValue(tx *sql.Tx, columnName string, defaultValue ...string) (string, error) {
	var _defaultValue string

	if len(defaultValue) > 0 {
		_defaultValue = defaultValue[0]
	}

	value, err := qb.getColumnValue(tx, columnName)

	if err != nil {
		return _defaultValue, err
	}

	s1 := toString(value)

	if s1 == "" {
		return _defaultValue, nil
	}

	return s1, nil
}

func (qb *queryBuilder) getIntValue(tx *sql.Tx, columnName string, defaultValue ...int) (int, error) {
	var _defaultValue int

	if len(defaultValue) > 0 {
		_defaultValue = defaultValue[0]
	}

	value, err := qb.getColumnValue(tx, columnName)

	if err != nil {
		return _defaultValue, err
	}

	return toInt(value, _defaultValue), nil
}

func (qb *queryBuilder) count(tx *sql.Tx, countField string) (int, error) {
	defer func() {
		qb.timeout = 0
	}()

	query, params := qb.buildCountSql(countField)
	logSql(query, params)
	var err error

	if pool == nil {
		err = NewException("database connection pool is nil")
		writeLog("error", err)
		return 0, err
	}

	ctx, cancel := context.WithTimeout(context.TODO(), qb.getTimeout())
	defer cancel()
	var rows *sql.Rows

	if tx != nil {
		rows, err = tx.QueryContext(ctx, query, params...)
	} else {
		rows, err = pool.QueryContext(ctx, query, params...)
	}

	if err != nil {
		writeLog("error", err)
		return 0, toDbException(err)
	}

	if ctx.Err() != nil {
		writeLog("error", ctx.Err())
		return 0, toDbException(ctx.Err())
	}

	defer rows.Close()
	var n1 int

	for rows.Next() {
		err = rows.Scan(&n1)
		break
	}

	if err != nil {
		writeLog("error", err)
		return 0, toDbException(err)
	}

	return n1, nil
}

func (qb *queryBuilder) sumForInt(tx *sql.Tx, fieldName string) (int, error) {
	defer func() {
		qb.timeout = 0
	}()

	query, params := qb.buildSumSql(fieldName)
	logSql(query, params)
	var err error

	if pool == nil {
		err = NewException("database connection pool is nil")
		writeLog("error", err)
		return 0, err
	}

	ctx, cancel := context.WithTimeout(context.TODO(), qb.getTimeout())
	defer cancel()
	var rows *sql.Rows

	if tx != nil {
		rows, err = tx.QueryContext(ctx, query, params...)
	} else {
		rows, err = pool.QueryContext(ctx, query, params...)
	}

	if err != nil {
		writeLog("error", err)
		return 0, toDbException(err)
	}

	if ctx.Err() != nil {
		writeLog("error", ctx.Err())
		return 0, toDbException(ctx.Err())
	}

	defer rows.Close()
	var n1 int

	for rows.Next() {
		err = rows.Scan(&n1)
		break
	}

	if err != nil {
		writeLog("error", err)
		return 0, toDbException(err)
	}

	return n1, nil
}

func (qb *queryBuilder) sumForFloat(tx *sql.Tx, fieldName string) (float64, error) {
	defer func() {
		qb.timeout = 0
	}()

	query, params := qb.buildSumSql(fieldName)
	logSql(query, params)
	var err error

	if pool == nil {
		err = NewException("database connection pool is nil")
		writeLog("error", err)
		return 0, err
	}

	ctx, cancel := context.WithTimeout(context.TODO(), qb.getTimeout())
	defer cancel()
	var rows *sql.Rows

	if tx != nil {
		rows, err = tx.QueryContext(ctx, query, params...)
	} else {
		rows, err = pool.QueryContext(ctx, query, params...)
	}

	if err != nil {
		writeLog("error", err)
		return 0, toDbException(err)
	}

	if ctx.Err() != nil {
		writeLog("error", ctx.Err())
		return 0, toDbException(ctx.Err())
	}

	defer rows.Close()
	var n1 float64

	for rows.Next() {
		err = rows.Scan(&n1)
		break
	}

	if err != nil {
		writeLog("error", err)
		return 0, toDbException(err)
	}

	return n1, nil
}

func (qb *queryBuilder) insertByMap(tx *sql.Tx, data map[string]interface{}) (int64, error) {
	defer func() {
		qb.timeout = 0
	}()

	query, params := qb.buildInsertSqlByMap(data)

	if tx != nil {
		return TxInsertBySql(tx, query, params, qb.getTimeout())
	}

	return InsertBySql(query, params, qb.getTimeout())
}

func (qb *queryBuilder) insertByModel(tx *sql.Tx, model interface{}) (int64, error) {
	defer func() {
		qb.timeout = 0
	}()

	err1 := NewException("param [model] must be a struct pointer")

	if model == nil {
		return 0, err1
	}

	rt := reflect.TypeOf(model)

	if rt.Kind() != reflect.Ptr || rt.Elem().Kind() != reflect.Struct {
		return 0, err1
	}

	rt = rt.Elem()
	rv := reflect.ValueOf(model).Elem()
	query, pkField, params := qb.buildInsertSqlByModel(rt, rv)
	var n1 int64
	var err error

	if tx != nil {
		n1, err = TxInsertBySql(tx, query, params, qb.getTimeout())
	} else {
		n1, err = InsertBySql(query, params, qb.getTimeout())
	}

	if err == nil && n1 > 0 && pkField != "" {
		rv.FieldByName(pkField).Set(reflect.ValueOf(n1))
	}

	return n1, err
}

func (qb *queryBuilder) updateByMap(tx *sql.Tx, data map[string]interface{}) (int64, error) {
	defer func() {
		qb.timeout = 0
	}()

	query, params := qb.buildUpdateSqlByMap(data)

	if tx != nil {
		return TxUpdateBySql(tx, query, params, qb.getTimeout())
	}

	return UpdateBySql(query, params, qb.getTimeout())
}

func (qb *queryBuilder) updateByModel(tx *sql.Tx, model interface{}) (int64, error) {
	defer func() {
		qb.timeout = 0
	}()

	err1 := NewException("param [model] must be a struct pointer")

	if model == nil {
		return 0, err1
	}

	rt := reflect.TypeOf(model)

	if rt.Kind() != reflect.Ptr || rt.Elem().Kind() != reflect.Struct {
		return 0, err1
	}

	rt = rt.Elem()
	rv := reflect.ValueOf(model).Elem()

	query, params := qb.buildUpdateSqlByModel(rt, rv)

	if tx != nil {
		return TxUpdateBySql(tx, query, params, qb.getTimeout())
	}

	return UpdateBySql(query, params, qb.getTimeout())
}

func (qb *queryBuilder) delete(tx *sql.Tx) (int64, error) {
	defer func() {
		qb.timeout = 0
	}()

	query, params := qb.buildDeleteSql()

	if tx != nil {
		return TxDeleteBySql(tx, query, params, qb.getTimeout())
	}

	return DeleteBySql(query, params, qb.getTimeout())
}

func (qb *queryBuilder) softDelete(tx *sql.Tx) (int64, error) {
	defer func() {
		qb.timeout = 0
	}()

	map1 := map[string]interface{}{}
	tableName := qb.tables[0].name

	if strings.Contains(tableName, ".") {
		tableName = substringAfter(tableName, ".")
	}

	tableName = strings.ReplaceAll(tableName, "`", "")

	schemas, ok := tableSchemas[tableName]

	if !ok {
		return 0, nil
	}

	fieldNames := []string{
		"del_flag",
		"delFlag",
	}

	for _, item := range schemas {
		if slicex.InStringSlice(item.FieldName, fieldNames) && strings.Contains(item.FieldType, "int") {
			map1[item.FieldName] = 1
			break
		}
	}

	if len(map1) < 1 {
		fieldNames := []string{
			"delete_at",
			"deleteAt",
		}

		for _, item := range schemas {
			if slicex.InStringSlice(item.FieldName, fieldNames) && item.FieldType == "datetime" {
				map1[item.FieldName] = time.Now().Format(dateFormatFull)
			}
		}
	}

	if len(map1) < 1 {
		return 0, nil
	}

	query, params := qb.buildUpdateSqlByMap(map1)

	if tx != nil {
		return TxUpdateBySql(tx, query, params, qb.getTimeout())
	}

	return UpdateBySql(query, params, qb.getTimeout())
}

func (qb *queryBuilder) handleDatetimeFieldInModel(tableName, columnName string, t1 *time.Time) string {
	schemas := tableSchemas[tableName]

	if len(schemas) < 1 {
		return "NotExists"
	}

	var fieldInfo tableFieldInfo

	for _, item := range schemas {
		if item.FieldName == columnName {
			fieldInfo = item
			break
		}
	}

	if fieldInfo.FieldName != columnName {
		return "NotExists"
	}

	if strings.Contains(fieldInfo.FieldType, "datetime") {
		if t1 == nil {
			if fieldInfo.Nullable {
				return "nil"
			}

			return "NotNullable"
		}

		return t1.Format(dateFormatFull)
	}

	if strings.Contains(fieldInfo.FieldType, "date") {
		if t1 == nil {
			if fieldInfo.Nullable {
				return "nil"
			}

			return "NotNullable"
		}

		return t1.Format(dateFormatDateOnly)
	}

	return "NotMatched"
}

func (qb *queryBuilder) getTimeout() time.Duration {
	timeout := qb.timeout

	if timeout < time.Second {
		timeout = 5 * time.Second
	}

	return timeout
}
