package DbPool

import (
	"database/sql"
	"fmt"
	"github.com/go-sql-driver/mysql"
	"github.com/lamgor666/goboot-common/AppConf"
	"github.com/lamgor666/goboot-common/util/castx"
	"time"
)

var pool *sql.DB

func Init(settings ...map[string]interface{}) {
	var _settings map[string]interface{}

	if len(settings) > 0 && len(settings[0]) > 0 {
		_settings = settings[0]
	}

	if len(_settings) < 1 {
		_settings = AppConf.GetMap("datasource")
	}

	dsn := buildDsn(_settings)
	var err error
	pool, err = sql.Open("mysql", dsn)

	if err != nil {
		panic(err)
	}

	maxIdle := castx.ToInt(_settings["maxIdle"])

	if maxIdle < 1 {
		maxIdle = 10
	}

	maxOpen := castx.ToInt(_settings["maxOpen"])

	if maxOpen < 1 {
		maxOpen = 20
	}

	if maxOpen <= maxIdle {
		maxOpen = maxIdle + 10
	}

	maxLifetime := castx.ToDuration(_settings["maxLifeTime"])

	if maxLifetime <= 0 {
		maxLifetime = 30 * time.Minute
	}

	pool.SetMaxIdleConns(maxIdle)
	pool.SetMaxOpenConns(maxOpen)
	pool.SetConnMaxLifetime(maxLifetime)
}

func Get() *sql.DB {
	return pool
}

func Close() {
	pool.Close()
}

func buildDsn(settings map[string]interface{}) string {
	cfg := mysql.NewConfig()
	cfg.User = castx.ToString(settings["username"])
	cfg.Passwd = castx.ToString(settings["password"])
	cfg.DBName = castx.ToString(settings["database"])
	cfg.Collation = castx.ToString(settings["collation"])
	cfg.ParseTime = true

	host := castx.ToString(settings["host"])

	if host == "" {
		host = "127.0.0.1"
	}

	port := castx.ToInt(settings["port"])

	if port < 1 {
		port = 3306
	}

	cfg.Addr = fmt.Sprintf("%s:%d", host, port)

	if loc, err := time.LoadLocation(castx.ToString(settings["loc"])); err == nil {
		cfg.Loc = loc
	}

	params := map[string]string{
		"charset": castx.ToString(settings["charset"]),
	}

	cfg.Params = params
	return cfg.FormatDSN()
}
