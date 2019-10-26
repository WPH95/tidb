package plugin

import (
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/util/chunk"
	"golang.org/x/net/context"
)

type EngineManifest struct {
	Manifest
	OnReaderOpen  func(ctx context.Context, meta *ExecutorMeta)
	OnReaderNext  func(ctx context.Context, chk *chunk.Chunk, meta *ExecutorMeta) error
	OnReaderClose func(meta ExecutorMeta)
}

type ExecutorMeta struct {
	Table *model.TableInfo
}
