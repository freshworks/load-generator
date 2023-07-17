package lua

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/csv"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Davmuz/gqt"
	"github.com/bdwilliams/go-jsonify/jsonify"
	"github.com/google/uuid"
	lua "github.com/yuin/gopher-lua"
	luajson "layeh.com/gopher-json"
	luar "layeh.com/gopher-luar"
)

type counter uint64

var counterMap sync.Map

func getCounter(name string) *counter {
	var c counter
	v, _ := counterMap.LoadOrStore(name, &c)
	return v.(*counter)
}

func (c *counter) Increment() uint64 {
	return atomic.AddUint64((*uint64)(c), 1)
}

func (c *counter) Decrement() uint64 {
	return atomic.AddUint64((*uint64)(c), ^uint64(0))
}

func (c *counter) Get() uint64 {
	return *(*uint64)(c)
}

func preloadExtraModules(ctx context.Context, L *lua.LState) {
	luajson.Preload(L)

	L.PreloadModule("csv", func(L *lua.LState) int {
		mod := L.NewTable()
		mod.RawSetString("Open", luar.New(L,
			func(file string) (*csv.Reader, error) {
				f, err := os.Open(file)
				if err != nil {
					return nil, err
				}

				return csv.NewReader(bufio.NewReader(f)), nil
			}))
		L.Push(mod)
		return 1
	})

	L.PreloadModule("sqlutils", func(L *lua.LState) int {
		mod := L.NewTable()
		mod.RawSetString("NewTemplateRepository", luar.New(L,
			func() *gqt.Repository {
				return gqt.NewRepository()
			}))
		mod.RawSetString("GetRow", luar.New(L, GetRow))
		mod.RawSetString("RowsToJson", luar.New(L, RowsToJson))
		L.Push(mod)
		return 1
	})

	L.PreloadModule("utils", func(L *lua.LState) int {
		mod := L.NewTable()
		mod.RawSetString("base64", luar.New(L, base64.StdEncoding))
		mod.RawSetString("GenerateRandomWords", luar.New(L,
			func(numWords int) string {
				return markovChain.Generate(numWords)
			}))
		mod.RawSetString("GenerateRandomText", luar.New(L,
			func(size, paraEveryNumWord int) string {
				return markovChain.GenerateText(size, paraEveryNumWord)
			}))
		mod.RawSetString("IoUtilReadAll", luar.New(L, ioutil.ReadAll))
		mod.RawSetString("BufferedReader", luar.New(L,
			func(r io.Reader) *bufio.Reader {
				return bufio.NewReader(r)
			}))
		mod.RawSetString("ByteToString", luar.New(L,
			func(b []byte) string {
				return string(b)
			}))
		mod.RawSetString("Sleep", luar.New(L,
			func(duration time.Duration) error {
				// Sleep within our quit context, need to come out of (larger)
				// sleeps immediately if we need to quit the app
				select {
				case <-time.After(duration * time.Millisecond):
				case <-ctx.Done():
					return fmt.Errorf("context canceled")
				}
				return nil
			}))
		mod.RawSetString("NewUUID", luar.New(L,
			func() string {
				return uuid.New().String()
			}))

		mod.RawSetString("GetCounter", luar.New(L, getCounter))

		L.Push(mod)
		return 1
	})
}

// sql.Rows.Scan requires pointer args, not sure how to pass that from Lua
// world, via luar (we can deference a pointer by using '-', not sure how to
// instruct luar to pass something by pointer). So for now, we can have a
// helper method that returns a row as an array.
func GetRow(rows *sql.Rows) ([]string, error) {
	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	colArgs := make([]interface{}, len(cols))
	for i := range colArgs {
		colArgs[i] = &sql.NullString{}
	}

	err = rows.Scan(colArgs...)

	out := make([]string, len(cols))
	for i, a := range colArgs {
		out[i] = a.(*sql.NullString).String
	}

	return out, err
}

func RowsToJson(rows *sql.Rows) []string {
	return jsonify.Jsonify(rows)
}
