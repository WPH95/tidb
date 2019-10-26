package plugin

import "github.com/pingcap/tidb/expression"

type UDFManifest struct {
	Manifest
	GetUserDefinedFuncClass func() UDFMeta
}

type UDFMeta struct {
	Func     expression.BuiltinFunc
	FuncName string
	MaxArgs  int
	MinArgs  int
}
