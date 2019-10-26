
package main

import (
	"github.com/pingcap/tidb/plugin"
	"github.com/pingcap/tidb/sessionctx/variable"
)

func PluginManifest() *plugin.Manifest {
	return plugin.ExportManifest(&plugin.EngineManifest{
		Manifest: plugin.Manifest{
			Kind:           plugin.Engine,
			Name:           "dashbase",
			Description:    "just a test",
			Version:        1,
			RequireVersion: map[string]uint16{},
			License:        "",
			BuildTime:      "2019-10-26 19:31:18.8915 +0800 CST m=+0.000586230",
			SysVars: map[string]*variable.SysVar{
			    
				"dashbase_example_test_variable": {
					Scope: variable.ScopeGlobal,
					Name:  "dashbase_example_test_variable",
					Value: "2",
				},
				
				"dashbase_example_test_variable2": {
					Scope: variable.ScopeSession,
					Name:  "dashbase_example_test_variable2",
					Value: "2",
				},
				
			},
			
				Validate:   Validate,
			
			
				OnInit:     OnInit,
			
			
				OnShutdown: OnShutdown,
			
			
		},
		
		SetReaderExecutor: SetReaderExecutor,
		
	})
}
