package RedisLock

import (
	"fmt"
	"github.com/gomodule/redigo/redis"
	"github.com/lamgor666/goboot-common/AppConf"
	"github.com/lamgor666/goboot-common/util/castx"
	"github.com/lamgor666/goboot-common/util/fsx"
	"github.com/lamgor666/goboot-common/util/stringx"
	"github.com/lamgor666/goboot-dal/RedisPool"
	"io/ioutil"
	"os"
	"strings"
	"sync"
	"time"
)

type options struct {
	luaFile string
	cacheDir string
}

type Lock struct {
	key      string
	contents string
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

func New(key string, opts ...*options) *Lock {
	var _opts *options

	if len(opts) > 0 {
		_opts = opts[0]
	}

	if _opts == nil {
		_opts = &options{}
	}

	return &Lock{
		key:      key,
		contents: stringx.GetRandomString(16),
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

func (l *Lock) TryLock(waitTimeout, ttl time.Duration) bool {
	if waitTimeout < 1 {
		waitTimeout = 5 * time.Second
	}

	if ttl < 1 {
		ttl = 30 * time.Second
	}

	conn, err := RedisPool.GetConn()

	if err != nil {
		return false
	}

	defer conn.Close()
	luaSha := l.ensureLuaShaExists(conn, "lock")

	if luaSha == "" {
		return false
	}

	key := "redislock@" + l.key
	ttlMills := castx.ToString(ttl.Milliseconds())
	wg := &sync.WaitGroup{}
	var success bool

	go func(wg *sync.WaitGroup) {
		execStart := time.Now()

		for {
			n1, _ := redis.Int(conn.Do("EVALSHA", luaSha, 1, key, l.contents, ttlMills))

			if n1 > 0 {
				success = true
				break
			}

			if time.Now().Sub(execStart) > waitTimeout {
				break
			}

			time.Sleep(20 * time.Millisecond)
		}

		wg.Done()
	}(wg)

	wg.Wait()
	return success
}

func (l *Lock) Release() {
	conn, err := RedisPool.GetConn()

	if err != nil {
		return
	}

	defer conn.Close()
	luaSha := l.ensureLuaShaExists(conn, "unlock")

	if luaSha == "" {
		return
	}

	key := "redislock@" + l.key
	conn.Do("EVALSHA", luaSha, 1, key, l.contents)
}

func (l *Lock) ensureLuaShaExists(conn redis.Conn, actionType string) string {
	datadir := AppConf.GetDataDir()
	var cacheFile string
	cacheDir := l.opts.cacheDir

	if cacheDir == "" {
		cacheDir = fsx.GetRealpath(datadir, "cache")
	}

	if cacheDir != "" {
		cacheFile = fmt.Sprintf("%s/luasha.redislock.%s.dat", l.opts.cacheDir, actionType)
	}

	if cacheFile != "" {
		buf, _ := ioutil.ReadFile(cacheFile)

		if len(buf) > 0 {
			return string(buf)
		}
	}

	luaFile := l.opts.luaFile

	if luaFile == "" {
		luaFile = fsx.GetRealpath(datadir, fmt.Sprintf("redislua/redislock.%s.lua", actionType))
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
