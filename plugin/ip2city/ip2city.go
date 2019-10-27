package ip2city


// package main

import (
	"context"
	"fmt"
	. "github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/plugin"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"strings"
)

// Validate implements TiDB plugin's Validate SPI.
func Validate(ctx context.Context, m *plugin.Manifest) error {
	fmt.Println("udf trim plugin validate")
	return nil
}

// OnInit implements TiDB plugin's OnInit SPI.
func OnInit(ctx context.Context, manifest *plugin.Manifest) error {
	fmt.Println("udf trim init called")
	return nil
}

// OnShutdown implements TiDB plugin's OnShutdown SPI.
func OnShutdown(ctx context.Context, manifest *plugin.Manifest) error {
	fmt.Println("udf trim shutdown called")
	return nil
}

type TrimFunction struct {
	BaseBuiltinFunc
}

func (b *TrimFunction) Clone() BuiltinFunc {
	newSig := &TrimFunction{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *TrimFunction) Initial(ctx sessionctx.Context, args []Expression) BuiltinFunc {
	var argTps []types.EvalType
	argTps = []types.EvalType{types.ETString}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETString, argTps...)
	argType := args[0].GetType()
	bf.Tp.Flen = argType.Flen
	SetBinFlagOrBinStr(argType, bf.Tp)
	b.BaseBuiltinFunc = bf
	return b
}

func (b *TrimFunction) EvalString(row chunk.Row) (d string, isNull bool, err error) {
	d, isNull, err = b.Args[0].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return d, isNull, err
	}

	if strings.HasPrefix(d, "1.") {
		return "BEIJING", false, nil
	}
	if strings.HasPrefix(d, "2.") {
		return "SHANGHAI", false, nil
	}

	return "UNKNOWN", false, nil
}

func GetUdfIp2city() plugin.UDFMeta {
	return plugin.UDFMeta{FuncName: "UDF_IP2CITY", Func: &TrimFunction{}, MinArgs: 1, MaxArgs: 1}
}
