package RateLimiter

import (
	"fmt"
	"github.com/gomodule/redigo/redis"
	"github.com/lamgor666/goboot-common/AppConf"
	"github.com/lamgor666/goboot-common/enum/DatetimeFormat"
	"github.com/lamgor666/goboot-common/util/castx"
	"github.com/lamgor666/goboot-common/util/fsx"
	"github.com/lamgor666/goboot-common/util/numberx"
	"github.com/lamgor666/goboot-common/util/securityx"
	"github.com/lamgor666/goboot-dal/poolx"
	"io/ioutil"
	"math/big"
	"os"
	"strings"
	"time"
)

type options struct {
	luaFile string
	cacheDir string
}

type Limiter struct {
	id       string
	count    int
	duration time.Duration
	opts     *options
}

func NewOptions(args ...string) *options {
	var luaFile string
	var cacheDir string

	if len(args) > 0 {
		luaFile = args[0]
	}

	if len(args) > 1 {
		cacheDir = args[1]
	}

	opts := &options{luaFile: luaFile}

	if cacheDir != "" {
		if stat, err := os.Stat(cacheDir); err == nil && stat.IsDir() {
			opts.cacheDir = cacheDir
		}
	}

	return opts
}

func New(id string, count int, duration interface{}, opts ...*options) *Limiter {
	var _duration time.Duration

	if d1, ok := duration.(time.Duration); ok {
		_duration = d1
	} else if s1, ok := duration.(string); ok && s1 != "" {
		_duration = castx.ToDuration(s1)
	}

	if _duration < 1 {
		_duration = time.Second
	}

	var _opts *options

	if len(opts) > 0 {
		_opts = opts[0]
	}

	if _opts == nil {
		_opts = &options{}
	}

	return &Limiter{
		id:       id,
		count:    count,
		duration: _duration,
		opts:     _opts,
	}
}

func (o *options) WithLuaFile(fpath string) *options {
	if stat, err := os.Stat(fpath); err == nil && !stat.IsDir() {
		o.luaFile = fpath
	}

	return o
}

func (o *options) WithCacheDir(dir string) *options {
	if stat, err := os.Stat(dir); err == nil && stat.IsDir() {
		o.cacheDir = dir
	}

	return o
}

func (l *Limiter) GetLimit() map[string]interface{} {
	conn, err := poolx.GetRedisConnection()

	if err != nil {
		return map[string]interface{}{}
	}

	defer conn.Close()
	luaSha := l.ensureLuaShaExists(conn)

	if luaSha == "" {
		return map[string]interface{}{}
	}

	t1 := time.Now()
	resetAt := time.Now().Add(l.duration).Unix() + 2
	duration := castx.ToInt64(l.duration.Seconds())
	id := fmt.Sprintf("ratelimiter@%s@%d@%ds", securityx.Md5(l.id), l.count, duration)

	nums, err := redis.Int64s(conn.Do(
		"EVALSHA",
		luaSha,
		1,
		id,
		castx.ToString(l.count),
		castx.ToString(duration * 1000),
		castx.ToString(resetAt),
	))

	if err != nil || len(nums) < 4 {
		return map[string]interface{}{}
	}

	remaining := castx.ToInt(nums[0])

	map1 := map[string]interface{}{
		"total":     castx.ToInt(nums[1]),
		"remaining": remaining,
	}

	if remaining >= 0 {
		return map1
	}

	t2 := time.Unix(nums[3], 0)
	map1["resetAt"] = t2.Format(DatetimeFormat.Full)
	map1["retryAfter"] = l.parseRetryAfter(t2.Sub(t1))
	return map1
}

func (l *Limiter) IsReachRateLimit() bool {
	map1 := l.GetLimit()

	if remaining, ok := map1["remaining"].(int); !ok || remaining < 0 {
		return true
	}

	return false
}

func (l *Limiter) ensureLuaShaExists(conn redis.Conn) string {
	datadir := AppConf.GetDataDir()
	var cacheFile string
	cacheDir := l.opts.cacheDir

	if cacheDir == "" {
		cacheDir = fsx.GetRealpath(datadir, "cache")
	}

	if cacheDir != "" {
		cacheFile = fmt.Sprintf("%s/luasha.ratelimiter.dat", l.opts.cacheDir)
	}

	if cacheFile != "" {
		buf, _ := ioutil.ReadFile(cacheFile)

		if len(buf) > 0 {
			return string(buf)
		}
	}

	luaFile := l.opts.luaFile

	if luaFile == "" {
		luaFile = fsx.GetRealpath(datadir, "redislua/ratelimiter.lua")
	}

	if luaFile == "" {
		return ""
	}

	buf, _ := ioutil.ReadFile(l.opts.luaFile)

	if len(buf) < 1 {
		return ""
	}

	contents := strings.TrimSpace(string(buf))
	luaSha, _ := redis.String(conn.Do("SCRIPT", "LOAD", contents))

	if luaSha != "" && cacheFile != "" {
		ioutil.WriteFile(cacheFile, []byte(luaSha), 0644)
	}

	return luaSha
}

func (l *Limiter) parseRetryAfter(d time.Duration) string {
	n1 := d.Milliseconds()

	if n1 < 1000 {
		return fmt.Sprintf("%dms", n1)
	}

	n2 := big.NewFloat(castx.ToFloat64(n1))
	n3 := n2.Quo(n2, big.NewFloat(1000.0))
	n4, _ := n3.Float64()
	return numberx.ToDecimalString(n4, 3) + "s"
}
