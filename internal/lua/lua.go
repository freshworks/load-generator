package lua

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/freshworks/load-generator/internal/stats"
	"github.com/sirupsen/logrus"

	gopher_lua_libs "github.com/vadv/gopher-lua-libs"
	lua "github.com/yuin/gopher-lua"
	"github.com/yuin/gopher-lua/parse"
	luar "layeh.com/gopher-luar"
)

type Generator struct {
	Script    string
	ScriptDir string
	Args      []string
	L         *lua.LState
	LG        *LG

	o           GeneratorOptions
	requestrate int
	//debug              bool
	log      *logrus.Entry
	initFn   lua.LValue
	tickFn   lua.LValue
	finishFn lua.LValue
}

type GeneratorOptions struct {
	Script string
	Args   []string
	Debug  bool
}

func NewOptions() *GeneratorOptions {
	return &GeneratorOptions{}
}

func NewGenerator(o GeneratorOptions, id int, requestrate int, concurrency int, ctx context.Context, s *stats.Stats) *Generator {
	log := logrus.WithFields(logrus.Fields{"Id": id})

	return &Generator{
		Script:      o.Script,
		Args:        o.Args,
		log:         log,
		requestrate: requestrate,
		LG:          NewLG(id, requestrate, concurrency, ctx, o.Script, s, log),
		o:           o,
	}
}

// Compiled lua byte code, shared across all goroutines
var luaCompiled *lua.FunctionProto
var loadLuaOnce sync.Once
var luaCompileMux sync.Mutex
var luaCtx context.Context
var luaCtxCancelFunc context.CancelFunc
var luaArgs *lua.LTable

func init() {
	luaCtx, luaCtxCancelFunc = context.WithCancel(context.Background())
}

func Cancel() {
	luaCtxCancelFunc()
}

func (l *Generator) Init() error {
	var err error

	if l.Script == "" {
		return errors.New("for --script option, must provide a script")
	}

	l.log.Debugf("Loading Lua script %s", l.Script)

	l.L, err = l.newLState()
	if err != nil {
		return err
	}

	// Load(+call global scope functions) the script
	err = l.loadScript(l.Script)
	if err != nil {
		return err
	}

	l.log.Debug("Finished loading lua script")

	// Call init function
	start := time.Now()
	err = l.callInitFn()
	l.log.Debugf("Initialization done, took %v", time.Since(start))

	return err
}

func (l *Generator) InitDone() error {
	l.LG.init()
	return nil
}

func (l *Generator) Tick() error {
	d := l.LG.getTickData()
	return l.callTickFn(d)
}

func (l *Generator) Finish() error {
	l.LG.finish()
	return l.callFinishFn()
}

func (l *Generator) loadScript(luaScript string) error {
	err := l.updateScript(luaScript)
	if err != nil {
		return err
	}

	l.LG.ScriptArgs = luaArgs

	lfunc := l.L.NewFunctionFromProto(luaCompiled)
	l.L.Push(lfunc)
	err = l.L.PCall(0, lua.MultRet, nil)
	if err != nil {
		return fmt.Errorf("lua: execution failed: %s", err)
	}

	l.initFn = l.L.GetGlobal("init")
	if l.initFn != lua.LNil && l.initFn.Type() != lua.LTFunction {
		return fmt.Errorf("lua: init is not a function")
	}

	l.tickFn = l.L.GetGlobal("tick")
	if l.tickFn.Type() != lua.LTFunction {
		return fmt.Errorf("lua: tick function is not defined or is not a function type")
	}

	l.finishFn = l.L.GetGlobal("finish")
	if l.finishFn != lua.LNil && l.finishFn.Type() != lua.LTFunction {
		return fmt.Errorf("lua: finish is not a function")
	}

	return nil
}

func (l *Generator) updateScript(luaScript string) error {
	var err error
	luaCompileMux.Lock()
	defer luaCompileMux.Unlock()
	if luaCompiled != nil {
		return nil
	}

	luaCompiled, err = compileLua(luaScript)
	if err != nil {
		return fmt.Errorf("Lua compilation error: %v\n", err)
	}
	luaArgs, err = l.handleScriptArgs(l.Args)
	if err != nil {
		return fmt.Errorf("Lua args handling error: %v\n", err)
	}

	return nil
}

func (l *Generator) callInitFn() error {
	if l.initFn == lua.LNil {
		return nil
	}

	if err := l.L.CallByParam(lua.P{
		Fn:      l.initFn,
		NRet:    1,
		Protect: true,
	}); err != nil {
		return fmt.Errorf("init function execution failed: %v", err)
	}

	lv := l.L.Get(-1) // returned value
	l.L.Pop(1)        // remove received value

	if lv != lua.LNil {
		return errors.New("script initialization failed")
	}

	return nil
}

func (l *Generator) callTickFn(data interface{}) error {
	param := luar.New(l.L, data)
	err := l.L.CallByParam(lua.P{Fn: l.tickFn, NRet: 1, Protect: true}, param)
	if err != nil {
		return fmt.Errorf("tick function execution failed: %v", err)
	}

	lv := l.L.Get(-1)
	l.L.Pop(1)
	if lv != lua.LNil {
		return fmt.Errorf("script tick function asking to quit (%v)", lv.String())
	}

	return nil
}

func (l *Generator) callFinishFn() error {
	if l.finishFn == lua.LNil {
		return nil
	}

	err := l.L.CallByParam(lua.P{Fn: l.finishFn, NRet: 0, Protect: true})
	if err != nil {
		return fmt.Errorf("lua: finish function execution failed: %v", err)
	}
	l.log.Debug("finish function returned")
	return nil
}

func (l *Generator) newLState() (*lua.LState, error) {
	ls := lua.NewState(lua.Options{
		IncludeGoStackTrace: true,
		// RegistrySize:        1024 * 20,
		// RegistryMaxSize:     1024 * 80,
		// RegistryGrowStep:    32,
		// CallStackSize:       120,
		MinimizeStackMemory: true,
	})

	var err error

	// Setup script load paths
	m := ls.GetField(ls.Get(lua.EnvironIndex), "package")
	p := ls.GetField(m, "path").String()
	l.ScriptDir, err = filepath.Abs(filepath.Dir(l.Script))
	if err != nil {
		return nil, fmt.Errorf("failed to find script (%s) directory path, Lua requires may fail: %v", l.Script, err)
	}
	p = p + ";" + l.ScriptDir + "/?.lua"
	ls.SetField(m, "path", lua.LString(p))

	ls.SetGlobal("LG", luar.New(ls, l.LG))
	ls.SetGlobal("Log", luar.New(ls, l.log))

	if l.o.Debug {
		l.log.Infof("Enabling cancelable context for Lua")
		ls.SetContext(luaCtx)
	}

	gopher_lua_libs.Preload(ls)

	// Expose all the generators to Lua script
	l.LG.preloadModules(ls)
	preloadExtraModules(l.LG.ctx, ls)

	return ls, nil
}

// CompileLua reads the passed lua file from disk and compiles it.
func compileLua(filePath string) (*lua.FunctionProto, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	reader := bufio.NewReader(file)
	chunk, err := parse.Parse(reader, filePath)
	if err != nil {
		return nil, err
	}
	proto, err := lua.Compile(chunk, filePath)
	if err != nil {
		return nil, err
	}
	return proto, nil
}

// We will handle args in a separate LState and discard the state as it is
// only needed once. The resultant args table will be passed to all other
// goroutines/lstates later
func (l *Generator) handleScriptArgs(args []string) (*lua.LTable, error) {
	L, err := l.newLState()
	if err != nil {
		return nil, err
	}
	defer L.Close()

	lfunc := L.NewFunctionFromProto(luaCompiled)
	L.Push(lfunc)
	err = L.PCall(0, lua.MultRet, nil)
	if err != nil {
		return nil, fmt.Errorf("lua load failed: %s", err)
	}

	argsFn := L.GetGlobal("args")
	if argsFn != lua.LNil {
		if argsFn.Type() != lua.LTFunction {
			return nil, fmt.Errorf("lua: \"args\" is not a function")
		}

		// Build up args
		a := L.NewTable()
		for i, v := range args {
			a.RawSetInt(i+1, lua.LString(v))
		}

		// Mark global table as readonly
		L.SetFuncs(L.G.Global, map[string]lua.LGFunction{
			"__newindex": func(ls *lua.LState) int {
				ls.RaiseError("modifying readonly table")
				return 0
			},
		})
		L.SetMetatable(L.G.Global, L.G.Global)

		if err := L.CallByParam(lua.P{
			Fn:      argsFn,
			NRet:    1,
			Protect: true,
		}, a); err != nil {
			return nil, fmt.Errorf("args function execution failed: %v", err)
		}

		lv := L.Get(-1)
		L.Pop(1)
		if lv.Type() != lua.LTTable {
			return nil, fmt.Errorf("args function must return a table but returned: %s", lv.Type())
		}

		// Mark the args table read-only
		lt := lv.(*lua.LTable)
		markTableReadOnly(L, lt)
		return lt, nil
	}

	lt := L.NewTable()
	markTableReadOnly(L, lt)
	return lt, nil
}

func markTableReadOnly(L *lua.LState, lt *lua.LTable) {
	L.SetFuncs(lt, map[string]lua.LGFunction{
		"__newindex": func(ls *lua.LState) int {
			ls.RaiseError("modifying readonly table")
			return 0
		},
	})
	L.SetMetatable(lt, lt)
}
