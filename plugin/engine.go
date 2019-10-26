package plugin

import (
	"context"
	"github.com/pingcap/tidb/sessionctx/variable"
)

type EngineManifest struct {
	Manifest
	OnTableCreate func(ctx context.Context, sctx *variable.SessionVars, event GeneralEvent, cmd string)
}

