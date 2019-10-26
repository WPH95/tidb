package executor

import (
	"fmt"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/util/chunk"
	"golang.org/x/net/context"
)

type CsvReaderExecutor struct {
	baseExecutor
	Table   *model.TableInfo
	Columns []*model.ColumnInfo

	result *chunk.Chunk
	cursor int
	pos    int

	tp           string
	topic        string
	variableName string
}

func (e *CsvReaderExecutor) Open(ctx context.Context) error {
	fmt.Print("open")
	e.pos = 0
	return nil
}

func (e *CsvReaderExecutor) Next(ctx context.Context, chk *chunk.Chunk) error {
	chk.Reset()
	if e.pos > 5 {
		return nil
	}
	fmt.Println("hello is working")
	chk.AppendInt64(0,int64(e.pos))
	e.pos += 1
	return nil
}

func (e *CsvReaderExecutor) Close() error {
	return nil
}
