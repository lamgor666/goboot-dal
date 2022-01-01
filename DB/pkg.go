package DB

import (
	"database/sql"
	"github.com/lamgor666/goboot-common/logx"
	"time"
)

var pool *sql.DB
var logger logx.Logger
var debugMode bool

type tableFieldInfo struct {
	FieldName     string
	FieldType     string
	FieldSize     int
	Unsigned      bool
	Nullable      bool
	DefaultValue  string
	AutoIncrement bool
	IsPrimaryKey  bool
}

type table struct {
	name  string
	alias string
}

func (t *table) nameWithAlias() string {
	name := quote(t.name)

	if t.alias == "" {
		return name
	}

	return name + " AS " + t.alias
}

type column struct {
	name  string
	alias string
}

func (c *column) nameWithAlias() string {
	name := quote(c.name)

	if c.alias == "" {
		return name
	}

	return name + " AS " + c.alias
}

type joinClause struct {
	tbl      table
	joinType string
	joinOn   string
}

type rawSql struct {
	expr string
}

type scanField struct {
	TypeName       string
	NullStringVal  sql.NullString
	NullBoolVal    sql.NullBool
	NullInt32Val   sql.NullInt32
	NullInt64Val   sql.NullInt64
	NullFloat64Val sql.NullFloat64
	NullTimeVal    sql.NullTime
	StringVal      string
	BoolVal        bool
	IntVal         int
	Int64Val       int64
	Float64Val     float64
	TimeVal        time.Time
	InterfaceVal   interface{}
}

var tableSchemas map[string][]tableFieldInfo

func WithPool(arg0 *sql.DB) {
	pool = arg0
}

func WithLogger(arg0 logx.Logger) {
	logger = arg0
}

func DebugModeEnabled(args ...bool) bool {
	if len(args) > 0 {
		debugMode = args[0]
		return false
	}

	return debugMode
}
