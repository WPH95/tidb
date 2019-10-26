package executor

import (
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/plugin"
	"github.com/pingcap/tidb/util/chunk"
	"golang.org/x/net/context"
)

type PluginScanExecutor struct {
	baseExecutor
	Table   *model.TableInfo
	Columns []*model.ColumnInfo
	Plugin  *plugin.Plugin
	pm      *plugin.EngineManifest
	meta    *plugin.ExecutorMeta
}

func (e *PluginScanExecutor) Open(ctx context.Context) error {
	e.pm = plugin.DeclareEngineManifest(e.Plugin.Manifest)
	e.meta = &plugin.ExecutorMeta{
		Table: e.Table,
	}
	return e.pm.OnReaderOpen(ctx, e.meta)
}

func (e *PluginScanExecutor) Next(ctx context.Context, chk *chunk.Chunk) error {
	chk.Reset()
	err := e.pm.OnReaderNext(ctx, chk, e.meta)
	return err
}

func (e *PluginScanExecutor) Close() error {
	return nil
}

type PluginInsertExec struct {
	baseExecutor
	Plugin  *plugin.Plugin
	pm      *plugin.EngineManifest
	InsertE *InsertExec
	meta    *plugin.ExecutorMeta
}

func (e *PluginInsertExec) Open(ctx context.Context) error {
	e.pm = plugin.DeclareEngineManifest(e.Plugin.Manifest)
	e.meta = &plugin.ExecutorMeta{
		Table: e.InsertE.Table.Meta(),
	}
	return e.pm.OnInsertOpen(ctx, e.meta)
}


func (e *PluginInsertExec) Next(ctx context.Context, req *chunk.Chunk) error {
	return e.pm.OnInsertNext(ctx, e.InsertE.Lists, e.meta)
}

func (e *PluginInsertExec) Close() error {
	return e.pm.OnInsertClose(e.meta)
}

