package poolx

import (
	"context"
	"fmt"
	"github.com/gomodule/redigo/redis"
	"github.com/lamgor666/goboot-common/AppConf"
	"github.com/lamgor666/goboot-common/util/castx"
	"time"
)

var pool *redis.Pool

func Init(settings ...map[string]interface{}) {
	var _settings map[string]interface{}

	if len(settings) > 0 && len(settings[0]) > 0 {
		_settings = settings[0]
	}

	if len(_settings) < 1 {
		_settings = AppConf.GetMap("redis")
	}

	host := castx.ToString(_settings["host"])

	if host == "" {
		host = "127.0.0.1"
	}

	port := castx.ToInt(_settings["port"])

	if port < 1 {
		port = 6379
	}

	address := fmt.Sprintf("%s:%d", host, port)
	connectTimeout := castx.ToDuration(_settings["connectTimeout"])

	if connectTimeout <= 0 {
		connectTimeout = time.Second
	}

	readTimeout := castx.ToDuration(_settings["readTimeout"])

	if readTimeout <= 0 {
		readTimeout = 2 * time.Second
	}

	dialOptions := []redis.DialOption{
		redis.DialConnectTimeout(connectTimeout),
	}

	password := castx.ToString(_settings["password"])

	if password != "" {
		dialOptions = append(dialOptions, redis.DialPassword(password))
	}

	database := castx.ToInt(_settings["database"])

	if database > 0 {
		dialOptions = append(dialOptions, redis.DialDatabase(database))
	}

	maxIdle := castx.ToInt(_settings["maxIdle"])

	if maxIdle < 1 {
		maxIdle = 10
	}

	maxActive := castx.ToInt(_settings["maxActive"])

	if maxActive < 1 {
		maxActive = 20
	}

	if maxActive <= maxIdle {
		maxActive = maxIdle + 10
	}

	maxLifetime := castx.ToDuration(_settings["maxLifetime"])

	if maxLifetime <= 0 {
		maxLifetime = 24 * time.Hour
	}

	idleTimeout := castx.ToDuration(_settings["idleTimeout"])

	if idleTimeout <= 0 {
		idleTimeout = maxLifetime / 2
	}

	pool = &redis.Pool{
		Dial: func() (conn redis.Conn, err error) {
			return redis.Dial("tcp", address, dialOptions...)
		},
		DialContext: func(ctx context.Context) (conn redis.Conn, err error) {
			return redis.DialContext(ctx, "tcp", address, dialOptions...)
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
		MaxIdle:         maxIdle,
		MaxActive:       maxActive,
		IdleTimeout:     idleTimeout,
		Wait:            true,
		MaxConnLifetime: maxLifetime,
	}
}

func Get() *redis.Pool {
	return pool
}

func GetConn(ctx ...context.Context) (redis.Conn, error) {
	if len(ctx) > 0 {
		return pool.GetContext(ctx[0])
	}

	return pool.Get(), nil
}

func Close() {
	pool.Close()
}
