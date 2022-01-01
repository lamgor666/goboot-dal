package DB

import (
	"database/sql"
	"fmt"
	"github.com/lamgor666/goboot-common/util/slicex"
	"strconv"
	"strings"
	"time"
)

type queryBuilder struct {
	tables        []table
	columns       []column
	joinClauses   []joinClause
	conditions    []string
	bindValues    []interface{}
	orderBy       []string
	groupBy       []string
	limit         []int
	includeFields []string
	excludeFields []string
	timeout       time.Duration
}

func (qb *queryBuilder) WithIncludeFields(stringOrStringSlice interface{}) *queryBuilder {
	var fields []string

	if a1, ok := stringOrStringSlice.([]string); ok && len(a1) > 0 {
		fields = a1
	} else if s1, ok := stringOrStringSlice.(string); ok && s1 != "" {
		fields = regexpCommaSep.Split(s1, -1)
	}

	if len(fields) > 0 {
		qb.includeFields = fields
	}

	return qb
}

func (qb *queryBuilder) WithExcludeFields(stringOrStringSlice interface{}) *queryBuilder {
	var fields []string

	if a1, ok := stringOrStringSlice.([]string); ok && len(a1) > 0 {
		fields = a1
	} else if s1, ok := stringOrStringSlice.(string); ok && s1 != "" {
		fields = regexpCommaSep.Split(s1, -1)
	}

	if len(fields) > 0 {
		qb.excludeFields = fields
	}

	return qb
}

func (qb *queryBuilder) WithTimeout(timeout time.Duration) *queryBuilder {
	if timeout >= time.Second {
		qb.timeout = timeout
	}

	return qb
}

func (qb *queryBuilder) Select(fieldNames interface{}) *queryBuilder {
	var columnNames []string

	if a1, ok := fieldNames.([]string); ok && len(a1) > 0 {
		columnNames = a1
	} else if s1, ok := fieldNames.(string); ok && s1 != "" {
		columnNames = regexpCommaSep.Split(s1, -1)
	}

	if len(columnNames) < 0 {
		return qb
	}

	if len(qb.columns) > 0 {
		qb.columns = []column{}
	}

	for _, columnName := range columnNames {
		qb.addColumn(columnName)
	}

	return qb
}

func (qb *queryBuilder) Join(tableName string, args ...string) *queryBuilder {
	joinType := "INNER"
	var joinOn string

	switch len(args) {
	case 1:
		joinOn = args[0]
	case 2:
		joinOn = args[0]
		joinType = strings.ToUpper(args[1])
	case 3:
		joinOn = strings.Join(args, " ")
	case 4:
		joinOn = strings.Join(args[0:3], " ")
		joinType = strings.ToUpper(args[3])
	default:
		return qb
	}

	if joinType == "" || joinOn == "" {
		return qb
	}

	name, alias := parseToNameAndAlias(tableName)

	item := joinClause{
		tbl:      table{name: name, alias: alias},
		joinType: joinType,
		joinOn:   joinOn,
	}

	qb.addJoinClause(item)
	return qb
}

func (qb *queryBuilder) LeftJoin(tableName string, args ...string) *queryBuilder {
	switch len(args) {
	case 1, 3:
		args = append(args, "LEFT")
	default:
		return qb
	}

	return qb.Join(tableName, args...)
}

func (qb *queryBuilder) RightJoin(tableName string, args ...string) *queryBuilder {
	switch len(args) {
	case 1, 3:
		args = append(args, "RIGHT")
	default:
		return qb
	}

	return qb.Join(tableName, args...)
}

func (qb *queryBuilder) CrossJoin(tableName string, args ...string) *queryBuilder {
	switch len(args) {
	case 1, 3:
		args = append(args, "CROSS")
	default:
		return qb
	}

	return qb.Join(tableName, args...)
}

func (qb *queryBuilder) OuterJoin(tableName string, args ...string) *queryBuilder {
	switch len(args) {
	case 1, 3:
		args = append(args, "OUTER")
	default:
		return qb
	}

	return qb.Join(tableName, args...)
}

func (qb *queryBuilder) LeftOuterJoin(tableName string, args ...string) *queryBuilder {
	switch len(args) {
	case 1, 3:
		args = append(args, "LEFT OUTER")
	default:
		return qb
	}

	return qb.Join(tableName, args...)
}

func (qb *queryBuilder) RightOuterJoin(tableName string, args ...string) *queryBuilder {
	switch len(args) {
	case 1, 3:
		args = append(args, "RIGHT OUTER")
	default:
		return qb
	}

	return qb.Join(tableName, args...)
}

func (qb *queryBuilder) Where(columnName string, args ...interface{}) *queryBuilder {
	operator := "="
	var bindValue interface{}

	if len(args) > 1 {
		if arg0, ok := args[0].(string); ok && arg0 != "" {
			operator = arg0
			bindValue = args[1]
		}
	} else if len(args) == 1 {
		bindValue = args[0]
	}

	bindValue = indirect(bindValue)

	if operator == "" || bindValue == nil {
		return qb
	}

	var condition string

	if v, ok := bindValue.(rawSql); ok {
		condition = strings.Join([]string{quote(columnName), operator, v.expr}, " ")
	} else {
		condition = strings.Join([]string{quote(columnName), operator, "?"}, " ")
		qb.addBindValues(bindValue)
	}

	return qb.addCondition(condition)
}

func (qb *queryBuilder) WhereIn(columnName string, values interface{}, not ...bool) *queryBuilder {
	var whList []string
	var bindValues []interface{}

	if a1, ok := values.([]string); ok {
		for _, s1 := range a1 {
			if s1 == "" {
				continue
			}

			whList = append(whList, "?")
			bindValues = append(bindValues, s1)
		}
	} else if a1, ok := values.([]int); ok {
		for _, n1 := range a1 {
			whList = append(whList, "?")
			bindValues = append(bindValues, n1)
		}
	} else if a1 := toSlice(values); len(a1) > 0 {
		for _, value := range a1 {
			value = indirect(value)

			if value == nil {
				continue
			}

			switch v := value.(type) {
			case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64, string:
				whList = append(whList, "?")
				bindValues = append(bindValues, v)
				break
			default:
				continue
			}
		}
	}

	if len(whList) < 1 {
		return qb
	}

	var operator string

	if len(not) > 0 && not[0] {
		operator = "NOT IN"
	} else {
		operator = "IN"
	}

	condition := fmt.Sprintf("%s %s (%s)", quote(columnName), operator, strings.Join(whList, ", "))
	qb.addCondition(condition)
	qb.addBindValues(bindValues...)
	return qb
}

func (qb *queryBuilder) WhereNotIn(columnName string, values interface{}) *queryBuilder {
	return qb.WhereIn(columnName, values, true)
}

func (qb *queryBuilder) WhereBetween(columnName string, start, end interface{}, not ...bool) *queryBuilder {
	start = indirect(start)
	end = indirect(end)

	if start == nil || end == nil {
		return qb
	}

	var operator string

	if len(not) > 0 && not[0] {
		operator = "NOT BETWEEN"
	} else {
		operator = "BETWEEN"
	}

	qb.addCondition(fmt.Sprintf("%s %s ? AND ?", quote(columnName), operator))
	qb.addBindValues(start, end)
	return qb
}

func (qb *queryBuilder) WhereNotBetween(columnName string, start, end interface{}) *queryBuilder {
	return qb.WhereBetween(columnName, start, end, true)
}

func (qb *queryBuilder) WhereLike(columnName string, keyword string, not ...bool) *queryBuilder {
	var condition string

	if len(not) > 0 && not[0] {
		condition = "%s NOT LIKE ?"
	} else {
		condition = "%s LIKE ?"
	}

	keyword = ensureLeft(keyword, "%")
	keyword = ensureRight(keyword, "%")
	qb.addCondition(fmt.Sprintf(condition, quote(columnName)))
	qb.addBindValues(keyword)
	return qb
}

func (qb *queryBuilder) WhereNotLike(columnName string, keyword string) *queryBuilder {
	return qb.WhereLike(columnName, keyword, true)
}

func (qb *queryBuilder) WhereRegexp(columnName string, regex string, not ...bool) *queryBuilder {
	var condition string

	if len(not) > 0 && not[0] {
		condition = "%s NOT REGEXP ?"
	} else {
		condition = "%s REGEXP ?"
	}

	qb.addCondition(fmt.Sprintf(condition, quote(columnName)))
	qb.addBindValues(regex)
	return qb
}

func (qb *queryBuilder) WhereNotRegexp(columnName string, regex string) *queryBuilder {
	return qb.WhereRegexp(columnName, regex, true)
}

func (qb *queryBuilder) WhereDate(columnName string, args ...string) *queryBuilder {
	operator := "="
	var bindValue string

	if len(args) > 1 {
		operator = args[0]
		bindValue = args[1]
	} else if len(args) == 1 {
		bindValue = args[0]
	}

	if operator == "" || bindValue == "" {
		return qb
	}

	condition := fmt.Sprintf("DATE(%s) %s ?", quote(columnName), operator)
	qb.addCondition(condition)
	qb.addBindValues(bindValue)
	return qb
}

func (qb *queryBuilder) WhereDateBetween(columnName, d1, d2 string, or ...bool) *queryBuilder {
	d1 = strings.ReplaceAll(strings.TrimSpace(d1), "/", "-")
	var t1 *time.Time

	if t, err := time.Parse(dateFormatDateOnly, d1); err == nil {
		t1 = &t
	} else if t, err := time.Parse(dateFormatFull, d1); err == nil {
		t1 = &t
	}

	if t1 == nil {
		return qb
	}

	d2 = strings.ReplaceAll(strings.TrimSpace(d2), "/", "-")
	var t2 *time.Time

	if t, err := time.Parse(dateFormatDateOnly, d2); err == nil {
		t2 = &t
	} else if t, err := time.Parse(dateFormatFull, d2); err == nil {
		t2 = &t
	}

	if t2 == nil {
		return qb
	}

	d1 = t1.Format(dateFormatDateOnly) + " 00:00:00"
	d2 = t2.Format(dateFormatDateOnly) + " 23:59:59"

	if len(or) > 0 && or[0] {
		return qb.OrWhereBetween(columnName, d1, d2)
	}

	return qb.WhereBetween(columnName, d1, d2)
}

func (qb *queryBuilder) WhereNull(columnName string, not ...bool) *queryBuilder {
	var expr string

	if len(not) > 0 && not[0] {
		expr = strings.Join([]string{quote(columnName), "IS NOT NULL"}, " ")
	} else {
		expr = strings.Join([]string{quote(columnName), "IS NULL"}, " ")
	}

	qb.addCondition(expr)
	return qb
}

func (qb *queryBuilder) WhereNotNull(columnName string) *queryBuilder {
	return qb.WhereNull(columnName, true)
}

func (qb *queryBuilder) WhereBlank(columnName string, not ...bool) *queryBuilder {
	columnName = quote(columnName)
	sb := strings.Builder{}

	if len(not) > 0 && not[0] {
		sb.WriteString(columnName)
		sb.WriteString(" IS NOT NULL AND ")
		sb.WriteString(columnName)
		sb.WriteString(" <> ''")
	} else {
		sb.WriteString("(")
		sb.WriteString(columnName)
		sb.WriteString(" IS NULL OR ")
		sb.WriteString(columnName)
		sb.WriteString(" = '')")
	}

	qb.addCondition(sb.String())
	return qb
}

func (qb *queryBuilder) WhereNotBlank(columnName string) *queryBuilder {
	return qb.WhereBlank(columnName, true)
}

func (qb *queryBuilder) WhereSoftDelete(flag bool) *queryBuilder {
	tableName := qb.tables[0].name

	if strings.Contains(tableName, ".") {
		tableName = substringAfter(tableName, ".")
	}

	tableName = strings.ReplaceAll(tableName, "`", "")

	schemas, ok := tableSchemas[tableName]

	if !ok {
		return qb
	}

	fieldNames := []string{
		"del_flag",
		"delFlag",
	}

	for _, item := range schemas {
		if slicex.InStringSlice(item.FieldName, fieldNames) && strings.Contains(item.FieldType, "int") {
			if flag {
				return qb.Where(item.FieldName, 1)
			} else {
				return qb.Where(item.FieldName, 0)
			}
		}
	}

	fieldNames = []string{
		"delete_at",
		"deleteAt",
	}

	for _, item := range schemas {
		if slicex.InStringSlice(item.FieldName, fieldNames) && item.FieldType == "datetime" {
			if flag {
				return qb.WhereNotNull(item.FieldName)
			} else {
				return qb.WhereNull(item.FieldName)
			}
		}
	}

	return qb
}

func (qb *queryBuilder) WhereRaw(rawSql string) *queryBuilder {
	qb.addCondition(rawSql)
	return qb
}

func (qb *queryBuilder) OrWhere(columnName string, args ...interface{}) *queryBuilder {
	operator := "="
	var bindValue interface{}

	if len(args) > 1 {
		if arg0, ok := args[0].(string); ok && arg0 != "" {
			operator = arg0
			bindValue = args[1]
		}
	} else if len(args) == 1 {
		bindValue = args[0]
	}

	bindValue = indirect(bindValue)

	if operator == "" || bindValue == nil {
		return qb
	}

	var condition string

	if v, ok := bindValue.(rawSql); ok {
		condition = strings.Join([]string{quote(columnName), operator, v.expr}, " ")
	} else {
		condition = strings.Join([]string{quote(columnName), operator, "?"}, " ")
		qb.addBindValues(bindValue)
	}

	qb.addCondition(condition, true)
	return qb
}

func (qb *queryBuilder) OrWhereIn(columnName string, values interface{}, not ...bool) *queryBuilder {
	var whList []string
	var bindValues []interface{}

	if a1, ok := values.([]string); ok {
		for _, s1 := range a1 {
			if s1 == "" {
				continue
			}

			whList = append(whList, "?")
			bindValues = append(bindValues, s1)
		}
	} else if a1, ok := values.([]int); ok {
		for _, n1 := range a1 {
			whList = append(whList, "?")
			bindValues = append(bindValues, n1)
		}
	} else if a1 := toSlice(values); len(a1) > 0 {
		for _, value := range a1 {
			value = indirect(value)

			if value == nil {
				continue
			}

			switch v := value.(type) {
			case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64, string:
				whList = append(whList, "?")
				bindValues = append(bindValues, v)
				break
			default:
				continue
			}
		}
	}

	if len(whList) < 1 {
		return qb
	}

	var operator string

	if len(not) > 0 && not[0] {
		operator = "NOT IN"
	} else {
		operator = "IN"
	}

	condition := fmt.Sprintf("%s %s (%s)", quote(columnName), operator, strings.Join(whList, ", "))
	qb.addCondition(condition, true)
	qb.addBindValues(bindValues...)
	return qb
}

func (qb *queryBuilder) OrWhereNotIn(columnName string, values interface{}) *queryBuilder {
	return qb.OrWhereIn(columnName, values, true)
}

func (qb *queryBuilder) OrWhereBetween(columnName string, start, end interface{}, not ...bool) *queryBuilder {
	start = indirect(start)
	end = indirect(end)

	if start == nil || end == nil {
		return qb
	}

	var operator string

	if len(not) > 0 && not[0] {
		operator = "NOT BETWEEN"
	} else {
		operator = "BETWEEN"
	}

	qb.addCondition(fmt.Sprintf("%s %s ? AND ?", quote(columnName), operator), true)
	qb.addBindValues(start, end)
	return qb
}

func (qb *queryBuilder) OrWhereNotBetween(columnName string, start, end interface{}) *queryBuilder {
	return qb.OrWhereBetween(columnName, start, end, true)
}

func (qb *queryBuilder) OrWhereLike(columnName string, keyword string, not ...bool) *queryBuilder {
	var condition string

	if len(not) > 0 && not[0] {
		condition = "%s NOT LIKE ?"
	} else {
		condition = "%s LIKE ?"
	}

	keyword = ensureLeft(keyword, "%")
	keyword = ensureRight(keyword, "%")
	qb.addCondition(fmt.Sprintf(condition, quote(columnName)), true)
	qb.addBindValues(keyword)
	return qb
}

func (qb *queryBuilder) OrWhereNotLike(columnName string, keyword string) *queryBuilder {
	return qb.OrWhereLike(columnName, keyword, true)
}

func (qb *queryBuilder) OrWhereRegexp(columnName string, regex string, not ...bool) *queryBuilder {
	var condition string

	if len(not) > 0 && not[0] {
		condition = "%s NOT REGEXP ?"
	} else {
		condition = "%s REGEXP ?"
	}

	qb.addCondition(fmt.Sprintf(condition, quote(columnName)), true)
	qb.addBindValues(regex)
	return qb
}

func (qb *queryBuilder) OrWhereNotRegexp(columnName string, regex string) *queryBuilder {
	return qb.OrWhereRegexp(columnName, regex, true)
}

func (qb *queryBuilder) OrWhereDate(columnName string, args ...string) *queryBuilder {
	operator := "="
	var bindValue string

	if len(args) > 1 {
		operator = args[0]
		bindValue = args[1]
	} else if len(args) == 1 {
		bindValue = args[0]
	}

	if operator == "" || bindValue == "" {
		return qb
	}

	condition := fmt.Sprintf("DATE(%s) %s ?", quote(columnName), operator)
	qb.addCondition(condition, true)
	qb.addBindValues(bindValue)
	return qb
}

func (qb *queryBuilder) OrWhereDateBetween(columnName, d1, d2 string) *queryBuilder {
	return qb.WhereDateBetween(columnName, d1, d2, true)
}

func (qb *queryBuilder) OrWhereNull(columnName string, not ...bool) *queryBuilder {
	var expr string

	if len(not) > 0 && not[0] {
		expr = strings.Join([]string{quote(columnName), "IS NOT NULL"}, " ")
	} else {
		expr = strings.Join([]string{quote(columnName), "IS NULL"}, " ")
	}

	qb.addCondition(expr, true)
	return qb
}

func (qb *queryBuilder) OrWhereNotNull(columnName string) *queryBuilder {
	return qb.OrWhereNull(columnName, true)
}

func (qb *queryBuilder) OrWhereBlank(columnName string, not ...bool) *queryBuilder {
	columnName = quote(columnName)
	sb := strings.Builder{}

	if len(not) > 0 && not[0] {
		sb.WriteString(columnName)
		sb.WriteString(" IS NOT NULL AND ")
		sb.WriteString(columnName)
		sb.WriteString(" <> ''")
	} else {
		sb.WriteString(columnName)
		sb.WriteString(" IS NULL OR ")
		sb.WriteString(columnName)
		sb.WriteString(" = ''")
	}

	qb.addCondition(sb.String(), true)
	return qb
}

func (qb *queryBuilder) OrWhereNotBlank(columnName string) *queryBuilder {
	return qb.OrWhereBlank(columnName, true)
}

func (qb *queryBuilder) OrWhereSoftDelete(flag bool) *queryBuilder {
	tableName := qb.tables[0].name

	if strings.Contains(tableName, ".") {
		tableName = substringAfter(tableName, ".")
	}

	tableName = strings.ReplaceAll(tableName, "`", "")

	schemas, ok := tableSchemas[tableName]

	if !ok {
		return qb
	}

	fieldNames := []string{
		"del_flag",
		"delFlag",
	}

	for _, item := range schemas {
		if slicex.InStringSlice(item.FieldName, fieldNames) && strings.Contains(item.FieldType, "int") {
			if flag {
				return qb.OrWhere(item.FieldName, 1)
			} else {
				return qb.OrWhere(item.FieldName, 0)
			}
		}
	}

	fieldNames = []string{
		"delete_at",
		"deleteAt",
	}

	for _, item := range schemas {
		if slicex.InStringSlice(item.FieldName, fieldNames) && item.FieldType == "datetime" {
			if flag {
				return qb.OrWhereNotNull(item.FieldName)
			} else {
				return qb.OrWhereNull(item.FieldName)
			}
		}
	}

	return qb
}

func (qb *queryBuilder) OrWhereRaw(rawSql string) *queryBuilder {
	qb.addCondition(rawSql, true)
	return qb
}

func (qb *queryBuilder) OrderBy(stringOrStringSlice interface{}) *queryBuilder {
	if s1, ok := stringOrStringSlice.(string); ok {
		if s1 == "" {
			return qb
		}

		return qb.OrderBy(regexpCommaSep.Split(s1, -1))
	}

	if a1, ok := stringOrStringSlice.([]string); ok {
		for _, s1 := range a1 {
			if s1 == "" {
				continue
			}

			qb.addOrderBy(s1)
		}
	}

	return qb
}

func (qb *queryBuilder) GroupBy(stringOrStringSlice interface{}) *queryBuilder {
	if s1, ok := stringOrStringSlice.(string); ok {
		if s1 == "" {
			return qb
		}

		return qb.GroupBy(regexpCommaSep.Split(s1, -1))
	}

	if a1, ok := stringOrStringSlice.([]string); ok {
		for _, s1 := range a1 {
			if s1 == "" {
				continue
			}

			qb.addGroupBy(s1)
		}
	}

	return qb
}

func (qb *queryBuilder) Limit(args ...interface{}) *queryBuilder {
	if len(args) > 1 {
		n1, ok1 := args[0].(int)
		n2, ok2 := args[1].(int)

		if ok1 && ok2 && n1 >= 0 && n2 > 0 {
			qb.limit = []int{n1, n2}
		}

		return qb
	}

	if len(args) != 1 {
		return qb
	}

	if n1, ok := args[0].(int); ok {
		if n1 > 0 {
			qb.limit = []int{0, n1}
		}

		return qb
	}

	s1, _ := args[0].(string)

	if s1 == "" {
		return qb
	}

	parts := regexpCommaSep.Split(s1, -1)

	if len(parts) == 1 {
		n1 := toInt(parts[0], -1)

		if n1 > 0 {
			qb.limit = []int{0, n1}
		}

		return qb
	}

	if len(parts) < 2 {
		return qb
	}

	n1 := toInt(parts[0], -1)
	n2 := toInt(parts[1], -1)

	if n1 >= 0 && n2 > 0 {
		qb.limit = []int{n1, n2}
	}

	return qb
}

func (qb *queryBuilder) ForPage(page, pageSize int) *queryBuilder {
	if page < 1 || pageSize < 1 {
		return qb
	}

	return qb.Limit((page-1) * pageSize, pageSize)
}

func (qb *queryBuilder) Get(fieldNames ...interface{}) ([]map[string]interface{}, error) {
	return qb.getForMapList(nil, fieldNames...)
}

func (qb *queryBuilder) TxGet(tx *sql.Tx, fieldNames ...interface{}) ([]map[string]interface{}, error) {
	return qb.getForMapList(tx, fieldNames...)
}

func (qb *queryBuilder) GetForModels(model interface{}, eachFn func(interface{})) error {
	return qb.getForModels(nil, model, eachFn)
}

func (qb *queryBuilder) TxGetForModels(tx *sql.Tx, model interface{}, eachFn func(interface{})) error {
	return qb.getForModels(tx, model, eachFn)
}

func (qb *queryBuilder) First(fieldNames ...interface{}) (map[string]interface{}, error) {
	return qb.firstForMap(nil, fieldNames...)
}

func (qb *queryBuilder) TxFirst(tx *sql.Tx, fieldNames ...interface{}) (map[string]interface{}, error) {
	return qb.firstForMap(tx, fieldNames...)
}

func (qb *queryBuilder) FirstForModel(model interface{}) error {
	return qb.firstForModel(nil, model)
}

func (qb *queryBuilder) TxFirstForModel(tx *sql.Tx, model interface{}) error {
	return qb.firstForModel(tx, model)
}

func (qb *queryBuilder) Value(columnName string, defaultValue ...interface{}) (interface{}, error) {
	return qb.getColumnValue(nil, columnName, defaultValue...)
}

func (qb *queryBuilder) TxValue(tx *sql.Tx, columnName string, defaultValue ...interface{}) (interface{}, error) {
	return qb.getColumnValue(tx, columnName, defaultValue...)
}

func (qb *queryBuilder) StringValue(columnName string, defaultValue ...string) (string, error) {
	return qb.getStringValue(nil, columnName, defaultValue...)
}

func (qb *queryBuilder) TxStringValue(tx *sql.Tx, columnName string, defaultValue ...string) (string, error) {
	return qb.getStringValue(tx, columnName, defaultValue...)
}

func (qb *queryBuilder) IntValue(columnName string, defaultValue ...int) (int, error) {
	return qb.getIntValue(nil, columnName, defaultValue...)
}

func (qb *queryBuilder) TxIntValue(tx *sql.Tx, columnName string, defaultValue ...int) (int, error) {
	return qb.getIntValue(tx, columnName, defaultValue...)
}

func (qb *queryBuilder) Count(countField ...string) (int, error) {
	fieldName := "*"

	if len(countField) > 0 && countField[0] != "" {
		fieldName = countField[0]
	}

	return qb.count(nil, fieldName)
}

func (qb *queryBuilder) TxCount(tx *sql.Tx, countField ...string) (int, error) {
	fieldName := "*"

	if len(countField) > 0 && countField[0] != "" {
		fieldName = countField[0]
	}

	return qb.count(tx, fieldName)
}

func (qb *queryBuilder) Exists(countField ...string) (bool, error) {
	n1, err := qb.Count(countField...)

	if err != nil {
		return false, err
	}

	return n1 > 0, nil
}

func (qb *queryBuilder) TxExists(tx *sql.Tx, countField ...string) (bool, error) {
	n1, err := qb.TxCount(tx, countField...)

	if err != nil {
		return false, err
	}

	return n1 > 0, nil
}

func (qb *queryBuilder) Insert(data map[string]interface{}) (int64, error) {
	return qb.insertByMap(nil, data)
}

func (qb *queryBuilder) TxInsert(tx *sql.Tx, data map[string]interface{}) (int64, error) {
	return qb.insertByMap(tx, data)
}

func (qb *queryBuilder) InsertByModel(model interface{}) (int64, error) {
	return qb.insertByModel(nil, model)
}

func (qb *queryBuilder) TxInsertByModel(tx *sql.Tx, model interface{}) (int64, error) {
	return qb.insertByModel(tx, model)
}

func (qb *queryBuilder) Update(data map[string]interface{}) (int64, error) {
	return qb.updateByMap(nil, data)
}

func (qb *queryBuilder) TxUpdate(tx *sql.Tx, data map[string]interface{}) (int64, error) {
	return qb.updateByMap(tx, data)
}

func (qb *queryBuilder) UpdateByModel(model interface{}) (int64, error) {
	return qb.updateByModel(nil, model)
}

func (qb *queryBuilder) TxUpdateByModel(tx *sql.Tx, model interface{}) (int64, error) {
	return qb.updateByModel(tx, model)
}

func (qb *queryBuilder) Delete() (int64, error) {
	return qb.delete(nil)
}

func (qb *queryBuilder) TxDelete(tx *sql.Tx) (int64, error) {
	return qb.delete(tx)
}

func (qb *queryBuilder) SoftDelete() (int64, error) {
	return qb.softDelete(nil)
}

func (qb *queryBuilder) TxSoftDelete(tx *sql.Tx) (int64, error) {
	return qb.softDelete(tx)
}

func (qb *queryBuilder) Incr(fieldName string, num interface{}) (int64, error) {
	expr := fieldName + " + "

	if n1, ok := num.(float64); ok && n1 > 0 {
		expr += toDecimalString(n1)
	} else if n1, ok := num.(float32); ok && n1 > 0 {
		expr += toDecimalString(n1)
	} else if s1, ok := num.(string); ok && s1 != "" {
		n1, err := strconv.ParseFloat(s1, 64)

		if err != nil || n1 < 0 {
			return 0, nil
		}

		expr += toDecimalString(n1)
	} else if n1, err := strconv.Atoi(toString(num)); err == nil && n1 > 0 {
		expr += fmt.Sprintf("%d", n1)
	} else {
		return 0, nil
	}

	data := map[string]interface{}{fieldName: Raw(expr)}
	return qb.updateByMap(nil, data)
}

func (qb *queryBuilder) TxIncr(tx *sql.Tx, fieldName string, num interface{}) (int64, error) {
	expr := fieldName + " + "

	if n1, ok := num.(float64); ok && n1 > 0 {
		expr += toDecimalString(n1)
	} else if n1, ok := num.(float32); ok && n1 > 0 {
		expr += toDecimalString(n1)
	} else if s1, ok := num.(string); ok && s1 != "" {
		n1, err := strconv.ParseFloat(s1, 64)

		if err != nil || n1 < 0 {
			return 0, nil
		}

		expr += toDecimalString(n1)
	} else if n1, err := strconv.Atoi(toString(num)); err == nil && n1 > 0 {
		expr += fmt.Sprintf("%d", n1)
	} else {
		return 0, nil
	}

	data := map[string]interface{}{fieldName: Raw(expr)}
	return qb.updateByMap(tx, data)
}

func (qb *queryBuilder) Decr(fieldName string, num interface{}) (int64, error) {
	expr := fieldName + " - "

	if n1, ok := num.(float64); ok && n1 > 0 {
		expr += toDecimalString(n1)
	} else if n1, ok := num.(float32); ok && n1 > 0 {
		expr += toDecimalString(n1)
	} else if s1, ok := num.(string); ok && s1 != "" {
		n1, err := strconv.ParseFloat(s1, 64)

		if err != nil || n1 < 0 {
			return 0, nil
		}

		expr += toDecimalString(n1)
	} else if n1, err := strconv.Atoi(toString(num)); err == nil && n1 > 0 {
		expr += fmt.Sprintf("%d", n1)
	} else {
		return 0, nil
	}

	data := map[string]interface{}{fieldName: Raw(expr)}
	return qb.updateByMap(nil, data)
}

func (qb *queryBuilder) TxDecr(tx *sql.Tx, fieldName string, num interface{}) (int64, error) {
	expr := fieldName + " - "

	if n1, ok := num.(float64); ok && n1 > 0 {
		expr += toDecimalString(n1)
	} else if n1, ok := num.(float32); ok && n1 > 0 {
		expr += toDecimalString(n1)
	} else if s1, ok := num.(string); ok && s1 != "" {
		n1, err := strconv.ParseFloat(s1, 64)

		if err != nil || n1 < 0 {
			return 0, nil
		}

		expr += toDecimalString(n1)
	} else if n1, err := strconv.Atoi(toString(num)); err == nil && n1 > 0 {
		expr += fmt.Sprintf("%d", n1)
	} else {
		return 0, nil
	}

	data := map[string]interface{}{fieldName: Raw(expr)}
	return qb.updateByMap(tx, data)
}

func (qb *queryBuilder) SumForInt(fieldName string) (int, error) {
	return qb.sumForInt(nil, fieldName)
}

func (qb *queryBuilder) TxSumForInt(tx *sql.Tx, fieldName string) (int, error) {
	return qb.sumForInt(tx, fieldName)
}

func (qb *queryBuilder) SumForFloat(fieldName string) (float64, error) {
	return qb.sumForFloat(nil, fieldName)
}

func (qb *queryBuilder) TxSumForFloat(tx *sql.Tx, fieldName string) (float64, error) {
	return qb.sumForFloat(tx, fieldName)
}
