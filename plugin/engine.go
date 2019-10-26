package plugin

import (
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/util/chunk"
	"golang.org/x/net/context"
)

type EngineManifest struct {
	Manifest
	OnReaderOpen  func(ctx context.Context, meta *ExecutorMeta)
	OnReaderNext  func(ctx context.Context, rows [][]expression.Expression, meta *ExecutorMeta) error
	OnReaderClose func(meta ExecutorMeta)

	OnInsertOpen func(ctx context.Context, meta *ExecutorMeta)
	OnInsertNext func(ctx context.Context, chk *chunk.Chunk, meta *ExecutorMeta) error
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
