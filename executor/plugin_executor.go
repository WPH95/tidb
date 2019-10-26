package executor

import (
	"fmt"
	"github.com/davecgh/go-spew/spew"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/plugin"
	"github.com/pingcap/tidb/util/chunk"
	"golang.org/x/net/context"
)

type PluginExecutor struct {
	baseExecutor
	Table   *model.TableInfo
	Columns []*model.ColumnInfo
	Plugin  *plugin.Plugin
	pm      *plugin.EngineManifest
	meta    *plugin.ExecutorMeta
}

func (e *PluginExecutor) Open(ctx context.Context) error {
	e.pm = plugin.DeclareEngineManifest(e.Plugin.Manifest)
	e.meta = &plugin.ExecutorMeta{
		Table: e.Table,
	}
	e.pm.OnReaderOpen(ctx, e.meta)
	return nil
}

func (e *PluginExecutor) Next(ctx context.Context, chk *chunk.Chunk) error {
	err := e.pm.OnReaderNext(ctx, chk, e.meta)
	fmt.Println("pe next finished", spew.Sdump(err))
	return err
}

func (e *PluginExecutor) Close() error {
	return nil
}
