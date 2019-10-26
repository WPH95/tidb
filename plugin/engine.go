package plugin

import (
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/util/chunk"
	"golang.org/x/net/context"
)

type EngineManifest struct {
	Manifest
	OnInsertOpen  func(ctx context.Context, meta *ExecutorMeta) error
	OnInsertNext  func(ctx context.Context, rows [][]expression.Expression, meta *ExecutorMeta) error
	OnInsertClose func(meta *ExecutorMeta) error

	OnReaderOpen  func(ctx context.Context, meta *ExecutorMeta) error
	OnReaderNext  func(ctx context.Context, chk *chunk.Chunk, meta *ExecutorMeta) error
	OnReaderClose func(meta ExecutorMeta)

	OnDropTable   func(tb *model.TableInfo) error
	OnCreateTable func(tb *model.TableInfo) error
}

type ExecutorMeta struct {
	Table *model.TableInfo
}

func HasEngine(name string) bool {
	p := Get(Engine, name)
	if p != nil {
		return true
	}

	return false
}
