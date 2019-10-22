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

type TrimFunctionClass struct {
	BaseFunctionClass
}

// getFunction sets trim built-in function signature.
// The syntax of trim in mysql is 'TRIM([{BOTH | LEADING | TRAILING} [remstr] FROM] str), TRIM([remstr FROM] str)',
// but we wil convert it into trim(str), trim(str, remstr) and trim(str, remstr, direction) in AST.
func (c *TrimFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}

	switch len(args) {
	case 1:
		bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETString, types.ETString)
		argType := args[0].GetType()
		bf.Tp.Flen = argType.Flen
		SetBinFlagOrBinStr(argType, bf.Tp)
		sig := &BuiltinTrim1ArgSig{bf}
		return sig, nil

	case 2:
		bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETString, types.ETString, types.ETString)
		argType := args[0].GetType()
		SetBinFlagOrBinStr(argType, bf.Tp)
		sig := &BuiltinTrim2ArgsSig{bf}
		return sig, nil

	case 3:
		bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETString, types.ETString, types.ETString, types.ETInt)
		argType := args[0].GetType()
		bf.Tp.Flen = argType.Flen
		SetBinFlagOrBinStr(argType, bf.Tp)
		sig := &BuiltinTrim3ArgsSig{bf}
		return sig, nil

	default:
		return nil, c.VerifyArgs(args)
	}
}

type BuiltinTrim1ArgSig struct {
	BaseBuiltinFunc
}

func (b *BuiltinTrim1ArgSig) Clone() BuiltinFunc {
	newSig := &BuiltinTrim1ArgSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalString evals a builtinTrim1ArgSig, corresponding to trim(str)
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_trim
func (b *BuiltinTrim1ArgSig) EvalString(row chunk.Row) (d string, isNull bool, err error) {
	d, isNull, err = b.Args[0].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return d, isNull, err
	}
	return strings.Trim(d, spaceChars), false, nil
}

type BuiltinTrim2ArgsSig struct {
	BaseBuiltinFunc
}

func (b *BuiltinTrim2ArgsSig) Clone() BuiltinFunc {
	newSig := &BuiltinTrim2ArgsSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalString evals a builtinTrim2ArgsSig, corresponding to trim(str, remstr)
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_trim
func (b *BuiltinTrim2ArgsSig) EvalString(row chunk.Row) (d string, isNull bool, err error) {
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
}

type BuiltinTrim3ArgsSig struct {
	BaseBuiltinFunc
}

func (b *BuiltinTrim3ArgsSig) Clone() BuiltinFunc {
	newSig := &BuiltinTrim3ArgsSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalString evals a builtinTrim3ArgsSig, corresponding to trim(str, remstr, direction)
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_trim
func (b *BuiltinTrim3ArgsSig) EvalString(row chunk.Row) (d string, isNull bool, err error) {
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
