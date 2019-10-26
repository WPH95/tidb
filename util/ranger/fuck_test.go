package ranger_test

import (
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/types"
	. "github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/chunk"
	"strings"
)

const spaceChars = " "

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
	switch len(args) {
	case 1:
		argTps = []types.EvalType{types.ETString}
	case 2:
		argTps = []types.EvalType{types.ETString, types.ETString}
	case 3:
		argTps = []types.EvalType{types.ETString, types.ETString, types.ETInt}
	}

	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETString, argTps...)
	argType := args[0].GetType()
	bf.Tp.Flen = argType.Flen
	SetBinFlagOrBinStr(argType, bf.Tp)
	b.BaseBuiltinFunc = bf
	return b
}

func (b *TrimFunction) EvalString(row chunk.Row) (d string, isNull bool, err error) {
	switch len(b.Args) {
	case 1:
		d, isNull, err = b.Args[0].EvalString(b.Ctx, row)
		if isNull || err != nil {
			return d, isNull, err
		}
		return strings.Trim(d, spaceChars), false, nil
	case 2:
		var str, remstr string

		str, isNull, err = b.Args[0].EvalString(b.Ctx, row)
		if isNull || err != nil {
			return d, isNull, err
		}
		remstr, isNull, err = b.Args[1].EvalString(b.Ctx, row)
		if isNull || err != nil {
			return d, isNull, err
		}
		d = trimLeft(str, remstr)
		d = trimRight(d, remstr)
		return d, false, nil
	case 3:
		var (
			str, remstr  string
			x            int64
			direction    ast.TrimDirectionType
			isRemStrNull bool
		)
		str, isNull, err = b.Args[0].EvalString(b.Ctx, row)
		if isNull || err != nil {
			return d, isNull, err
		}
		remstr, isRemStrNull, err = b.Args[1].EvalString(b.Ctx, row)
		if err != nil {
			return d, isNull, err
		}
		x, isNull, err = b.Args[2].EvalInt(b.Ctx, row)
		if isNull || err != nil {
			return d, isNull, err
		}
		direction = ast.TrimDirectionType(x)
		if direction == ast.TrimLeading {
			if isRemStrNull {
				d = strings.TrimLeft(str, spaceChars)
			} else {
				d = trimLeft(str, remstr)
			}
		} else if direction == ast.TrimTrailing {
			if isRemStrNull {
				d = strings.TrimRight(str, spaceChars)
			} else {
				d = trimRight(str, remstr)
			}
		} else {
			if isRemStrNull {
				d = strings.Trim(str, spaceChars)
			} else {
				d = trimLeft(str, remstr)
				d = trimRight(d, remstr)
			}
		}
		return d, false, nil
	}

	return d, isNull, err
}

func trimLeft(str, remstr string) string {
	for {
		x := strings.TrimPrefix(str, remstr)
		if len(x) == len(str) {
			return x
		}
		str = x
	}
}

func trimRight(str, remstr string) string {
	for {
		x := strings.TrimSuffix(str, remstr)
		if len(x) == len(str) {
			return x
		}
		str = x
	}
}
