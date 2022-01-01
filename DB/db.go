package DB

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/go-errors/errors"
	"github.com/lamgor666/goboot-common/util/slicex"
	"strings"
	"unicode/utf8"
)

func Raw(expr string) *rawSql {
	return &rawSql{expr: expr}
}

func Table(name string) *queryBuilder {
	qb := &queryBuilder{}
	qb.addTable(name)
	return qb
}

func IsFieldValueExists(tblName, fieldName string, fieldValue interface{}, pkValue ...interface{}) bool {
	qb := Table(tblName)

	if len(pkValue) > 0 && pkValue[0] != nil {
		qb.Where("id", "<>", pkValue[0])
	}

	qb.Where(fieldName, fieldValue)
	yes, _ := qb.Exists()
	return yes
}

func CheckRecordTotal(qb *queryBuilder, countField ...string) int {
	n1, _ := qb.Count(countField...)

	if n1 < 1 {
		panic(NoDataException{})
	}

	return n1
}

func SelectBySql(query string, args ...interface{}) ([]map[string]interface{}, error) {
	return doSelectBySql(nil, query, args...)
}

func TxSelectBySql(tx *sql.Tx, query string, args ...interface{}) ([]map[string]interface{}, error) {
	return doSelectBySql(tx, query, args...)
}

func InsertBySql(query string, args ...interface{}) (int64, error) {
	return doInsertBySql(nil, query, args...)
}

func TxInsertBySql(tx *sql.Tx, query string, args ...interface{}) (int64, error) {
	return doInsertBySql(tx, query, args...)
}

func UpdateBySql(query string, args ...interface{}) (int64, error) {
	return doUpdateBySql(nil, query, args...)
}

func TxUpdateBySql(tx *sql.Tx, query string, args ...interface{}) (int64, error) {
	return doUpdateBySql(tx, query, args...)
}

func DeleteBySql(query string, args ...interface{}) (int64, error) {
	return doUpdateBySql(nil, query, args...)
}

func TxDeleteBySql(tx *sql.Tx, query string, args ...interface{}) (int64, error) {
	return doUpdateBySql(tx, query, args...)
}

func ExecuteSql(query string, args ...interface{}) error {
	return doExecuteSql(nil, query, args...)
}

func TxExecuteSql(tx *sql.Tx, query string, args ...interface{}) error {
	return doExecuteSql(tx, query, args...)
}

func Transations(fn func(tx *sql.Tx) error, opts ...*sql.TxOptions) error {
	if pool == nil {
		err := NewException("database connection pool is nil")
		writeLog("error", err)
		return err
	}

	var _opts *sql.TxOptions

	if len(opts) > 0 {
		_opts = opts[0]
	}

	if _opts == nil {
		_opts = &sql.TxOptions{}
	}

	tx, err := pool.BeginTx(context.TODO(), _opts)

	if err != nil {
		writeLog("error", err)
		return toDbException(err)
	}

	if err := fn(tx); err != nil {
		tx.Rollback()
		return toDbException(err)
	}

	err = tx.Commit()

	if err != nil {
		writeLog("error", err)
		return toDbException(err)
	}

	return nil
}

func BuildTableSchemas() {
	if pool == nil {
		return
	}

	tableSchemas = map[string][]tableFieldInfo{}
	rows, err := pool.Query("SHOW TABLES")

	if err != nil {
		return
	}

	defer rows.Close()
	var tableNames []string

	for rows.Next() {
		var tableName string
		err := rows.Scan(&tableName)

		if err != nil || tableName == "" {
			continue
		}

		tableNames = append(tableNames, tableName)
	}

	if len(tableNames) < 1 {
		return
	}

	fieldNames := []string{
		"ctime",
		"create_at",
		"createAt",
		"create_time",
		"createTime",
		"update_at",
		"updateAt",
		"delete_at",
		"deleteAt",
		"del_flag",
		"delFlag",
	}

	for _, tableName := range tableNames {
		rs, err := pool.Query(fmt.Sprintf("DESC `%s`", tableName))

		if err != nil {
			continue
		}

		infoList := make([]tableFieldInfo, 0)

		for rs.Next() {
			var fField string
			var fType string
			var fNull string
			var fKey string
			var fDefault interface{}
			var fExtra string

			err := rs.Scan(&fField, &fType, &fNull, &fKey, &fDefault, &fExtra)

			if err != nil {
				continue
			}

			if fField == "" || !slicex.InStringSlice(fField, fieldNames) {
				continue
			}

			nullable := strings.Contains(strings.ToUpper(fNull), "YES")
			isPrimaryKey := strings.Contains(strings.ToUpper(fKey), "PRI")
			defaultValue := toString(fDefault)
			autoIncrement := strings.Contains(fExtra, "auto_increment")
			unsigned := strings.Contains(fType, "unsigned")
			var fieldType string
			var fieldSize string

			if strings.Contains(fType, " ") {
				fieldType = substringBefore(fType, " ")
			} else {
				fieldType = fType
			}

			if strings.HasSuffix(fieldType, ")") {
				fieldSize = substringAfter(fieldType, "(")
				fieldSize = strings.TrimSuffix(fieldSize, ")")
				fieldType = substringBefore(fieldType, "(")
			} else {
				fieldSize = "0"
			}

			infoList = append(infoList, tableFieldInfo{
				FieldName:     fField,
				FieldType:     fieldType,
				FieldSize:     toInt(fieldSize),
				Unsigned:      unsigned,
				Nullable:      nullable,
				DefaultValue:  defaultValue,
				AutoIncrement: autoIncrement,
				IsPrimaryKey:  isPrimaryKey,
			})
		}

		rs.Close()

		if len(infoList) > 0 {
			tableSchemas[tableName] = infoList
		}
	}
}

func GetTableSchemas() map[string][]tableFieldInfo {
	if len(tableSchemas) < 1 {
		return map[string][]tableFieldInfo{}
	}

	return tableSchemas
}

func doSelectBySql(tx *sql.Tx, query string, args ...interface{}) ([]map[string]interface{}, error) {
	emptyList := make([]map[string]interface{}, 0)

	if pool == nil {
		err := NewException("database connection pool is nil")
		writeLog("error", err)
		return emptyList, err
	}

	params, timeout := getParamsAndTimeout(args)
	logSql(query, params)
	ctx, cancel := context.WithTimeout(context.TODO(), timeout)
	defer cancel()
	var rows *sql.Rows
	var err error

	if tx != nil {
		rows, err = tx.QueryContext(ctx, query, params...)
	} else {
		rows, err = pool.QueryContext(ctx, query, params...)
	}

	if err != nil {
		writeLog("error", err)
		return emptyList, toDbException(err)
	}

	if ctx.Err() != nil {
		writeLog("error", ctx.Err())
		return emptyList, toDbException(ctx.Err())
	}

	defer rows.Close()
	list, err := scanIntoMapList(rows)

	if err != nil {
		return emptyList, toDbException(ctx.Err())
	}

	return list, nil
}

func doInsertBySql(tx *sql.Tx, query string, args ...interface{}) (int64, error) {
	if pool == nil {
		err := NewException("database connection pool is nil")
		writeLog("error", err)
		return 0, err
	}

	params, timeout := getParamsAndTimeout(args)
	logSql(query, params)
	ctx, cancel := context.WithTimeout(context.TODO(), timeout)
	defer cancel()
	var result sql.Result
	var err error

	if tx != nil {
		result, err = tx.ExecContext(ctx, query, params...)
	} else {
		result, err = pool.ExecContext(ctx, query, params...)
	}

	if err != nil {
		writeLog("error", err)
		return 0, toDbException(err)
	}

	if ctx.Err() != nil {
		writeLog("error", ctx.Err())
		return 0, toDbException(ctx.Err())
	}

	n1, err := result.LastInsertId()

	if err != nil {
		writeLog("error", err)
		return 0, toDbException(err)
	}

	return n1, nil
}

func doUpdateBySql(tx *sql.Tx, query string, args ...interface{}) (int64, error) {
	if pool == nil {
		err := NewException("database connection pool is nil")
		writeLog("error", err)
		return 0, err
	}

	params, timeout := getParamsAndTimeout(args)
	logSql(query, params)
	ctx, cancel := context.WithTimeout(context.TODO(), timeout)
	defer cancel()
	var result sql.Result
	n1 := int64(-1)
	var err error

	if tx != nil {
		result, err = tx.ExecContext(ctx, query, params...)
	} else {
		result, err = pool.ExecContext(ctx, query, params...)
	}

	if err != nil {
		writeLog("error", err)
		return n1, toDbException(err)
	}

	if ctx.Err() != nil {
		writeLog("error", ctx.Err())
		return n1, toDbException(ctx.Err())
	}

	n1, err = result.RowsAffected()

	if err != nil {
		writeLog("error", err)
		return -1, toDbException(err)
	}

	return n1, nil
}

func doExecuteSql(tx *sql.Tx, query string, args ...interface{}) error {
	if pool == nil {
		err := NewException("database connection pool is nil")
		writeLog("error", err)
		return err
	}

	params, timeout := getParamsAndTimeout(args)
	logSql(query, params)
	ctx, cancel := context.WithTimeout(context.TODO(), timeout)
	defer cancel()
	var err error

	if tx != nil {
		_, err = tx.ExecContext(ctx, query, params...)
	} else {
		_, err = pool.ExecContext(ctx, query, params...)
	}

	if err != nil {
		writeLog("error", err)
		return toDbException(err)
	}

	if ctx.Err() != nil {
		writeLog("error", ctx.Err())
		return toDbException(ctx.Err())
	}

	return nil
}

func logSql(sql string, params ...[]interface{}) {
	if !DebugModeEnabled() || logger == nil {
		return
	}

	logger.Debug(sql)

	if len(params) < 1 || len(params[0]) < 1 {
		return
	}

	buf, _ := json.Marshal(handleParams(params[0]))

	if len(buf) < 1 {
		return
	}

	logger.Debug("params: " + string(buf))
}

func handleParams(params []interface{}) []interface{} {
	ret := make([]interface{}, 0, len(params))

	for _, p := range params {
		s1, ok := p.(string)

		if !ok || utf8.RuneCountInString(s1) <= 64 {
			ret = append(ret, p)
			continue
		}

		ret = append(ret, mbTruncateString(s1, 64))
	}

	return ret
}

func mbTruncateString(s string, len int) string {
	n1 := 1
	endPos := 0

	strings.FieldsFunc(s, func(r rune) bool {
		if n1 <= len {
			endPos += utf8.RuneLen(r)
			n1++
		}

		return true
	})

	return s[:endPos] + "..."
}

func writeLog(level string, msg interface{}) {
	if logger == nil {
		return
	}

	var _msg string

	if err, ok := msg.(error); ok {
		_msg = getStacktrace(err)
	} else if s1, ok := msg.(string); ok {
		_msg = s1
	}

	if _msg == "" {
		return
	}

	logger.Log(level, _msg)
}

func getStacktrace(err error) string {
	s1 := errors.New(err).ErrorStack()

	if s1 == "" {
		return ""
	}

	s1 = strings.ReplaceAll(s1, "\r", "")
	lines := strings.Split(s1, "\n")
	var sb []string

	if strings.Contains(s1, "src/runtime/panic.go") {
		n1 := -1

		for i := 0; i < len(lines); i++ {
			if i == 0 {
				sb = append(sb, lines[i])
				continue
			}

			if strings.Contains(lines[i], "src/runtime/panic.go") {
				n1 = i
				continue
			}

			if strings.Contains(lines[i], "src/runtime/proc.go") ||
				strings.Contains(lines[i], "src/runtime/asm_amd64") {
				break
			}

			if n1 < 0 || i <= n1 + 1 {
				continue
			}

			sb = append(sb, lines[i])
		}
	} else {
		for i := 0; i < len(lines); i++ {
			if strings.Contains(lines[i], "src/runtime/proc.go") ||
				strings.Contains(lines[i], "src/runtime/asm_amd64") {
				break
			}

			sb = append(sb, lines[i])
		}
	}

	return strings.Join(sb, "\n")
}
