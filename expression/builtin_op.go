// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package expression

import (
	"fmt"
	"math"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/opcode"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tipb/go-tipb"
)

var (
	_ FunctionClass = &logicAndFunctionClass{}
	_ FunctionClass = &logicOrFunctionClass{}
	_ FunctionClass = &logicXorFunctionClass{}
	_ FunctionClass = &isTrueOrFalseFunctionClass{}
	_ FunctionClass = &unaryMinusFunctionClass{}
	_ FunctionClass = &isNullFunctionClass{}
	_ FunctionClass = &unaryNotFunctionClass{}
)

var (
	_ BuiltinFunc = &builtinLogicAndSig{}
	_ BuiltinFunc = &builtinLogicOrSig{}
	_ BuiltinFunc = &builtinLogicXorSig{}
	_ BuiltinFunc = &builtinRealIsTrueSig{}
	_ BuiltinFunc = &builtinDecimalIsTrueSig{}
	_ BuiltinFunc = &builtinIntIsTrueSig{}
	_ BuiltinFunc = &builtinRealIsFalseSig{}
	_ BuiltinFunc = &builtinDecimalIsFalseSig{}
	_ BuiltinFunc = &builtinIntIsFalseSig{}
	_ BuiltinFunc = &builtinUnaryMinusIntSig{}
	_ BuiltinFunc = &builtinDecimalIsNullSig{}
	_ BuiltinFunc = &builtinDurationIsNullSig{}
	_ BuiltinFunc = &builtinIntIsNullSig{}
	_ BuiltinFunc = &builtinRealIsNullSig{}
	_ BuiltinFunc = &builtinStringIsNullSig{}
	_ BuiltinFunc = &builtinTimeIsNullSig{}
	_ BuiltinFunc = &builtinUnaryNotRealSig{}
	_ BuiltinFunc = &builtinUnaryNotDecimalSig{}
	_ BuiltinFunc = &builtinUnaryNotIntSig{}
)

type logicAndFunctionClass struct {
	BaseFunctionClass
}

func (c *logicAndFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	err := c.VerifyArgs(args)
	if err != nil {
		return nil, err
	}
	args[0], err = wrapWithIsTrue(ctx, true, args[0])
	if err != nil {
		return nil, errors.Trace(err)
	}
	args[1], err = wrapWithIsTrue(ctx, true, args[1])
	if err != nil {
		return nil, errors.Trace(err)
	}

	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETInt, types.ETInt)
	sig := &builtinLogicAndSig{bf}
	sig.SetPbCode(tipb.ScalarFuncSig_LogicalAnd)
	sig.Tp.Flen = 1
	return sig, nil
}

type builtinLogicAndSig struct {
	BaseBuiltinFunc
}

func (b *builtinLogicAndSig) Clone() BuiltinFunc {
	newSig := &builtinLogicAndSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinLogicAndSig) evalInt(row chunk.Row) (int64, bool, error) {
	arg0, isNull0, err := b.Args[0].EvalInt(b.Ctx, row)
	if err != nil || (!isNull0 && arg0 == 0) {
		return 0, err != nil, err
	}
	arg1, isNull1, err := b.Args[1].EvalInt(b.Ctx, row)
	if err != nil || (!isNull1 && arg1 == 0) {
		return 0, err != nil, err
	}
	if isNull0 || isNull1 {
		return 0, true, nil
	}
	return 1, false, nil
}

type logicOrFunctionClass struct {
	BaseFunctionClass
}

func (c *logicOrFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	err := c.VerifyArgs(args)
	if err != nil {
		return nil, err
	}
	args[0], err = wrapWithIsTrue(ctx, true, args[0])
	if err != nil {
		return nil, errors.Trace(err)
	}
	args[1], err = wrapWithIsTrue(ctx, true, args[1])
	if err != nil {
		return nil, errors.Trace(err)
	}

	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETInt, types.ETInt)
	bf.Tp.Flen = 1
	sig := &builtinLogicOrSig{bf}
	sig.SetPbCode(tipb.ScalarFuncSig_LogicalOr)
	return sig, nil
}

type builtinLogicOrSig struct {
	BaseBuiltinFunc
}

func (b *builtinLogicOrSig) Clone() BuiltinFunc {
	newSig := &builtinLogicOrSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinLogicOrSig) evalInt(row chunk.Row) (int64, bool, error) {
	arg0, isNull0, err := b.Args[0].EvalInt(b.Ctx, row)
	if err != nil {
		return 0, true, err
	}
	if !isNull0 && arg0 != 0 {
		return 1, false, nil
	}
	arg1, isNull1, err := b.Args[1].EvalInt(b.Ctx, row)
	if err != nil {
		return 0, true, err
	}
	if !isNull1 && arg1 != 0 {
		return 1, false, nil
	}
	if isNull0 || isNull1 {
		return 0, true, nil
	}
	return 0, false, nil
}

type logicXorFunctionClass struct {
	BaseFunctionClass
}

func (c *logicXorFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	err := c.VerifyArgs(args)
	if err != nil {
		return nil, err
	}

	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETInt, types.ETInt)
	sig := &builtinLogicXorSig{bf}
	sig.SetPbCode(tipb.ScalarFuncSig_LogicalXor)
	sig.Tp.Flen = 1
	return sig, nil
}

type builtinLogicXorSig struct {
	BaseBuiltinFunc
}

func (b *builtinLogicXorSig) Clone() BuiltinFunc {
	newSig := &builtinLogicXorSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinLogicXorSig) evalInt(row chunk.Row) (int64, bool, error) {
	arg0, isNull, err := b.Args[0].EvalInt(b.Ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	arg1, isNull, err := b.Args[1].EvalInt(b.Ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	if (arg0 != 0 && arg1 != 0) || (arg0 == 0 && arg1 == 0) {
		return 0, false, nil
	}
	return 1, false, nil
}

type bitAndFunctionClass struct {
	BaseFunctionClass
}

func (c *bitAndFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	err := c.VerifyArgs(args)
	if err != nil {
		return nil, err
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETInt, types.ETInt)
	sig := &builtinBitAndSig{bf}
	sig.SetPbCode(tipb.ScalarFuncSig_BitAndSig)
	sig.Tp.Flag |= mysql.UnsignedFlag
	return sig, nil
}

type builtinBitAndSig struct {
	BaseBuiltinFunc
}

func (b *builtinBitAndSig) Clone() BuiltinFunc {
	newSig := &builtinBitAndSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinBitAndSig) evalInt(row chunk.Row) (int64, bool, error) {
	arg0, isNull, err := b.Args[0].EvalInt(b.Ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	arg1, isNull, err := b.Args[1].EvalInt(b.Ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	return arg0 & arg1, false, nil
}

type bitOrFunctionClass struct {
	BaseFunctionClass
}

func (c *bitOrFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	err := c.VerifyArgs(args)
	if err != nil {
		return nil, err
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETInt, types.ETInt)
	sig := &builtinBitOrSig{bf}
	sig.SetPbCode(tipb.ScalarFuncSig_BitOrSig)
	sig.Tp.Flag |= mysql.UnsignedFlag
	return sig, nil
}

type builtinBitOrSig struct {
	BaseBuiltinFunc
}

func (b *builtinBitOrSig) Clone() BuiltinFunc {
	newSig := &builtinBitOrSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinBitOrSig) evalInt(row chunk.Row) (int64, bool, error) {
	arg0, isNull, err := b.Args[0].EvalInt(b.Ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	arg1, isNull, err := b.Args[1].EvalInt(b.Ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	return arg0 | arg1, false, nil
}

type bitXorFunctionClass struct {
	BaseFunctionClass
}

func (c *bitXorFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	err := c.VerifyArgs(args)
	if err != nil {
		return nil, err
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETInt, types.ETInt)
	sig := &builtinBitXorSig{bf}
	sig.SetPbCode(tipb.ScalarFuncSig_BitXorSig)
	sig.Tp.Flag |= mysql.UnsignedFlag
	return sig, nil
}

type builtinBitXorSig struct {
	BaseBuiltinFunc
}

func (b *builtinBitXorSig) Clone() BuiltinFunc {
	newSig := &builtinBitXorSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinBitXorSig) evalInt(row chunk.Row) (int64, bool, error) {
	arg0, isNull, err := b.Args[0].EvalInt(b.Ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	arg1, isNull, err := b.Args[1].EvalInt(b.Ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	return arg0 ^ arg1, false, nil
}

type leftShiftFunctionClass struct {
	BaseFunctionClass
}

func (c *leftShiftFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	err := c.VerifyArgs(args)
	if err != nil {
		return nil, err
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETInt, types.ETInt)
	sig := &builtinLeftShiftSig{bf}
	sig.Tp.Flag |= mysql.UnsignedFlag
	return sig, nil
}

type builtinLeftShiftSig struct {
	BaseBuiltinFunc
}

func (b *builtinLeftShiftSig) Clone() BuiltinFunc {
	newSig := &builtinLeftShiftSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinLeftShiftSig) evalInt(row chunk.Row) (int64, bool, error) {
	arg0, isNull, err := b.Args[0].EvalInt(b.Ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	arg1, isNull, err := b.Args[1].EvalInt(b.Ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	return int64(uint64(arg0) << uint64(arg1)), false, nil
}

type rightShiftFunctionClass struct {
	BaseFunctionClass
}

func (c *rightShiftFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	err := c.VerifyArgs(args)
	if err != nil {
		return nil, err
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETInt, types.ETInt)
	sig := &builtinRightShiftSig{bf}
	sig.Tp.Flag |= mysql.UnsignedFlag
	return sig, nil
}

type builtinRightShiftSig struct {
	BaseBuiltinFunc
}

func (b *builtinRightShiftSig) Clone() BuiltinFunc {
	newSig := &builtinRightShiftSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinRightShiftSig) evalInt(row chunk.Row) (int64, bool, error) {
	arg0, isNull, err := b.Args[0].EvalInt(b.Ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	arg1, isNull, err := b.Args[1].EvalInt(b.Ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	return int64(uint64(arg0) >> uint64(arg1)), false, nil
}

type isTrueOrFalseFunctionClass struct {
	BaseFunctionClass
	op opcode.Op

	// keepNull indicates how this function treats a null input parameter.
	// If keepNull is true and the input parameter is null, the function will return null.
	// If keepNull is false, the null input parameter will be cast to 0.
	keepNull bool
}

func (c *isTrueOrFalseFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}

	argTp := args[0].GetType().EvalType()
	if argTp == types.ETTimestamp || argTp == types.ETDatetime || argTp == types.ETDuration {
		argTp = types.ETInt
	} else if argTp == types.ETJson || argTp == types.ETString {
		argTp = types.ETReal
	}

	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETInt, argTp)
	bf.Tp.Flen = 1

	var sig BuiltinFunc
	switch c.op {
	case opcode.IsTruth:
		switch argTp {
		case types.ETReal:
			sig = &builtinRealIsTrueSig{bf, c.keepNull}
			sig.SetPbCode(tipb.ScalarFuncSig_RealIsTrue)
		case types.ETDecimal:
			sig = &builtinDecimalIsTrueSig{bf, c.keepNull}
			sig.SetPbCode(tipb.ScalarFuncSig_DecimalIsTrue)
		case types.ETInt:
			sig = &builtinIntIsTrueSig{bf, c.keepNull}
			sig.SetPbCode(tipb.ScalarFuncSig_IntIsTrue)
		default:
			return nil, errors.Errorf("unexpected types.EvalType %v", argTp)
		}
	case opcode.IsFalsity:
		switch argTp {
		case types.ETReal:
			sig = &builtinRealIsFalseSig{bf, c.keepNull}
			sig.SetPbCode(tipb.ScalarFuncSig_RealIsFalse)
		case types.ETDecimal:
			sig = &builtinDecimalIsFalseSig{bf, c.keepNull}
			sig.SetPbCode(tipb.ScalarFuncSig_DecimalIsFalse)
		case types.ETInt:
			sig = &builtinIntIsFalseSig{bf, c.keepNull}
			sig.SetPbCode(tipb.ScalarFuncSig_IntIsFalse)
		default:
			return nil, errors.Errorf("unexpected types.EvalType %v", argTp)
		}
	}
	return sig, nil
}

type builtinRealIsTrueSig struct {
	BaseBuiltinFunc
	keepNull bool
}

func (b *builtinRealIsTrueSig) Clone() BuiltinFunc {
	newSig := &builtinRealIsTrueSig{keepNull: b.keepNull}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinRealIsTrueSig) evalInt(row chunk.Row) (int64, bool, error) {
	input, isNull, err := b.Args[0].EvalReal(b.Ctx, row)
	if err != nil {
		return 0, true, err
	}
	if b.keepNull && isNull {
		return 0, true, nil
	}
	if isNull || input == 0 {
		return 0, false, nil
	}
	return 1, false, nil
}

type builtinDecimalIsTrueSig struct {
	BaseBuiltinFunc
	keepNull bool
}

func (b *builtinDecimalIsTrueSig) Clone() BuiltinFunc {
	newSig := &builtinDecimalIsTrueSig{keepNull: b.keepNull}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinDecimalIsTrueSig) evalInt(row chunk.Row) (int64, bool, error) {
	input, isNull, err := b.Args[0].EvalDecimal(b.Ctx, row)
	if err != nil {
		return 0, true, err
	}
	if b.keepNull && isNull {
		return 0, true, nil
	}
	if isNull || input.IsZero() {
		return 0, false, nil
	}
	return 1, false, nil
}

type builtinIntIsTrueSig struct {
	BaseBuiltinFunc
	keepNull bool
}

func (b *builtinIntIsTrueSig) Clone() BuiltinFunc {
	newSig := &builtinIntIsTrueSig{keepNull: b.keepNull}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinIntIsTrueSig) evalInt(row chunk.Row) (int64, bool, error) {
	input, isNull, err := b.Args[0].EvalInt(b.Ctx, row)
	if err != nil {
		return 0, true, err
	}
	if b.keepNull && isNull {
		return 0, true, nil
	}
	if isNull || input == 0 {
		return 0, false, nil
	}
	return 1, false, nil
}

type builtinRealIsFalseSig struct {
	BaseBuiltinFunc
	keepNull bool
}

func (b *builtinRealIsFalseSig) Clone() BuiltinFunc {
	newSig := &builtinRealIsFalseSig{keepNull: b.keepNull}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinRealIsFalseSig) evalInt(row chunk.Row) (int64, bool, error) {
	input, isNull, err := b.Args[0].EvalReal(b.Ctx, row)
	if err != nil {
		return 0, true, err
	}
	if b.keepNull && isNull {
		return 0, true, nil
	}
	if isNull || input != 0 {
		return 0, false, nil
	}
	return 1, false, nil
}

type builtinDecimalIsFalseSig struct {
	BaseBuiltinFunc
	keepNull bool
}

func (b *builtinDecimalIsFalseSig) Clone() BuiltinFunc {
	newSig := &builtinDecimalIsFalseSig{keepNull: b.keepNull}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinDecimalIsFalseSig) evalInt(row chunk.Row) (int64, bool, error) {
	input, isNull, err := b.Args[0].EvalDecimal(b.Ctx, row)
	if err != nil {
		return 0, true, err
	}
	if b.keepNull && isNull {
		return 0, true, nil
	}
	if isNull || !input.IsZero() {
		return 0, false, nil
	}
	return 1, false, nil
}

type builtinIntIsFalseSig struct {
	BaseBuiltinFunc
	keepNull bool
}

func (b *builtinIntIsFalseSig) Clone() BuiltinFunc {
	newSig := &builtinIntIsFalseSig{keepNull: b.keepNull}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinIntIsFalseSig) evalInt(row chunk.Row) (int64, bool, error) {
	input, isNull, err := b.Args[0].EvalInt(b.Ctx, row)
	if err != nil {
		return 0, true, err
	}
	if b.keepNull && isNull {
		return 0, true, nil
	}
	if isNull || input != 0 {
		return 0, false, nil
	}
	return 1, false, nil
}

type bitNegFunctionClass struct {
	BaseFunctionClass
}

func (c *bitNegFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETInt)
	bf.Tp.Flag |= mysql.UnsignedFlag
	sig := &builtinBitNegSig{bf}
	sig.SetPbCode(tipb.ScalarFuncSig_BitNegSig)
	return sig, nil
}

type builtinBitNegSig struct {
	BaseBuiltinFunc
}

func (b *builtinBitNegSig) Clone() BuiltinFunc {
	newSig := &builtinBitNegSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinBitNegSig) evalInt(row chunk.Row) (int64, bool, error) {
	arg, isNull, err := b.Args[0].EvalInt(b.Ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	return ^arg, false, nil
}

type unaryNotFunctionClass struct {
	BaseFunctionClass
}

func (c *unaryNotFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}

	argTp := args[0].GetType().EvalType()
	if argTp == types.ETTimestamp || argTp == types.ETDatetime || argTp == types.ETDuration {
		argTp = types.ETInt
	} else if argTp == types.ETJson || argTp == types.ETString {
		argTp = types.ETReal
	}

	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETInt, argTp)
	bf.Tp.Flen = 1

	var sig BuiltinFunc
	switch argTp {
	case types.ETReal:
		sig = &builtinUnaryNotRealSig{bf}
		sig.SetPbCode(tipb.ScalarFuncSig_UnaryNotReal)
	case types.ETDecimal:
		sig = &builtinUnaryNotDecimalSig{bf}
		sig.SetPbCode(tipb.ScalarFuncSig_UnaryNotDecimal)
	case types.ETInt:
		sig = &builtinUnaryNotIntSig{bf}
		sig.SetPbCode(tipb.ScalarFuncSig_UnaryNotInt)
	default:
		return nil, errors.Errorf("unexpected types.EvalType %v", argTp)
	}
	return sig, nil
}

type builtinUnaryNotRealSig struct {
	BaseBuiltinFunc
}

func (b *builtinUnaryNotRealSig) Clone() BuiltinFunc {
	newSig := &builtinUnaryNotRealSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinUnaryNotRealSig) evalInt(row chunk.Row) (int64, bool, error) {
	arg, isNull, err := b.Args[0].EvalReal(b.Ctx, row)
	if isNull || err != nil {
		return 0, true, err
	}
	if arg == 0 {
		return 1, false, nil
	}
	return 0, false, nil
}

type builtinUnaryNotDecimalSig struct {
	BaseBuiltinFunc
}

func (b *builtinUnaryNotDecimalSig) Clone() BuiltinFunc {
	newSig := &builtinUnaryNotDecimalSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinUnaryNotDecimalSig) evalInt(row chunk.Row) (int64, bool, error) {
	arg, isNull, err := b.Args[0].EvalDecimal(b.Ctx, row)
	if isNull || err != nil {
		return 0, true, err
	}
	if arg.IsZero() {
		return 1, false, nil
	}
	return 0, false, nil
}

type builtinUnaryNotIntSig struct {
	BaseBuiltinFunc
}

func (b *builtinUnaryNotIntSig) Clone() BuiltinFunc {
	newSig := &builtinUnaryNotIntSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinUnaryNotIntSig) evalInt(row chunk.Row) (int64, bool, error) {
	arg, isNull, err := b.Args[0].EvalInt(b.Ctx, row)
	if isNull || err != nil {
		return 0, true, err
	}
	if arg == 0 {
		return 1, false, nil
	}
	return 0, false, nil
}

type unaryMinusFunctionClass struct {
	BaseFunctionClass
}

func (c *unaryMinusFunctionClass) handleIntOverflow(arg *Constant) (overflow bool) {
	if mysql.HasUnsignedFlag(arg.GetType().Flag) {
		uval := arg.Value.GetUint64()
		// -math.MinInt64 is 9223372036854775808, so if uval is more than 9223372036854775808, like
		// 9223372036854775809, -9223372036854775809 is less than math.MinInt64, overflow occurs.
		if uval > uint64(-math.MinInt64) {
			return true
		}
	} else {
		val := arg.Value.GetInt64()
		// The math.MinInt64 is -9223372036854775808, the math.MaxInt64 is 9223372036854775807,
		// which is less than abs(-9223372036854775808). When val == math.MinInt64, overflow occurs.
		if val == math.MinInt64 {
			return true
		}
	}
	return false
}

// typeInfer infers unaryMinus function return type. when the arg is an int constant and overflow,
// typerInfer will infers the return type as types.ETDecimal, not types.ETInt.
func (c *unaryMinusFunctionClass) typeInfer(argExpr Expression) (types.EvalType, bool) {
	tp := argExpr.GetType().EvalType()
	if tp != types.ETInt && tp != types.ETDecimal {
		tp = types.ETReal
	}

	overflow := false
	// TODO: Handle float overflow.
	if arg, ok := argExpr.(*Constant); ok && tp == types.ETInt {
		overflow = c.handleIntOverflow(arg)
		if overflow {
			tp = types.ETDecimal
		}
	}
	return tp, overflow
}

func (c *unaryMinusFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (sig BuiltinFunc, err error) {
	if err = c.VerifyArgs(args); err != nil {
		return nil, err
	}

	argExpr, argExprTp := args[0], args[0].GetType()
	_, intOverflow := c.typeInfer(argExpr)

	var bf BaseBuiltinFunc
	switch argExprTp.EvalType() {
	case types.ETInt:
		if intOverflow {
			bf = NewBaseBuiltinFuncWithTp(ctx, args, types.ETDecimal, types.ETDecimal)
			sig = &builtinUnaryMinusDecimalSig{bf, true}
			sig.SetPbCode(tipb.ScalarFuncSig_UnaryMinusDecimal)
		} else {
			bf = NewBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETInt)
			sig = &builtinUnaryMinusIntSig{bf}
			sig.SetPbCode(tipb.ScalarFuncSig_UnaryMinusInt)
		}
		bf.Tp.Decimal = 0
	case types.ETDecimal:
		bf = NewBaseBuiltinFuncWithTp(ctx, args, types.ETDecimal, types.ETDecimal)
		bf.Tp.Decimal = argExprTp.Decimal
		sig = &builtinUnaryMinusDecimalSig{bf, false}
		sig.SetPbCode(tipb.ScalarFuncSig_UnaryMinusDecimal)
	case types.ETReal:
		bf = NewBaseBuiltinFuncWithTp(ctx, args, types.ETReal, types.ETReal)
		sig = &builtinUnaryMinusRealSig{bf}
		sig.SetPbCode(tipb.ScalarFuncSig_UnaryMinusReal)
	default:
		tp := argExpr.GetType().Tp
		if types.IsTypeTime(tp) || tp == mysql.TypeDuration {
			bf = NewBaseBuiltinFuncWithTp(ctx, args, types.ETDecimal, types.ETDecimal)
			sig = &builtinUnaryMinusDecimalSig{bf, false}
			sig.SetPbCode(tipb.ScalarFuncSig_UnaryMinusDecimal)
		} else {
			bf = NewBaseBuiltinFuncWithTp(ctx, args, types.ETReal, types.ETReal)
			sig = &builtinUnaryMinusRealSig{bf}
			sig.SetPbCode(tipb.ScalarFuncSig_UnaryMinusReal)
		}
	}
	bf.Tp.Flen = argExprTp.Flen + 1
	return sig, err
}

type builtinUnaryMinusIntSig struct {
	BaseBuiltinFunc
}

func (b *builtinUnaryMinusIntSig) Clone() BuiltinFunc {
	newSig := &builtinUnaryMinusIntSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinUnaryMinusIntSig) evalInt(row chunk.Row) (res int64, isNull bool, err error) {
	var val int64
	val, isNull, err = b.Args[0].EvalInt(b.Ctx, row)
	if err != nil || isNull {
		return val, isNull, err
	}

	if mysql.HasUnsignedFlag(b.Args[0].GetType().Flag) {
		uval := uint64(val)
		if uval > uint64(-math.MinInt64) {
			return 0, false, types.ErrOverflow.GenWithStackByArgs("BIGINT", fmt.Sprintf("-%v", uval))
		} else if uval == uint64(-math.MinInt64) {
			return math.MinInt64, false, nil
		}
	} else if val == math.MinInt64 {
		return 0, false, types.ErrOverflow.GenWithStackByArgs("BIGINT", fmt.Sprintf("-%v", val))
	}
	return -val, false, nil
}

type builtinUnaryMinusDecimalSig struct {
	BaseBuiltinFunc

	constantArgOverflow bool
}

func (b *builtinUnaryMinusDecimalSig) Clone() BuiltinFunc {
	newSig := &builtinUnaryMinusDecimalSig{constantArgOverflow: b.constantArgOverflow}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinUnaryMinusDecimalSig) evalDecimal(row chunk.Row) (*types.MyDecimal, bool, error) {
	dec, isNull, err := b.Args[0].EvalDecimal(b.Ctx, row)
	if err != nil || isNull {
		return dec, isNull, err
	}
	return types.DecimalNeg(dec), false, nil
}

type builtinUnaryMinusRealSig struct {
	BaseBuiltinFunc
}

func (b *builtinUnaryMinusRealSig) Clone() BuiltinFunc {
	newSig := &builtinUnaryMinusRealSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinUnaryMinusRealSig) evalReal(row chunk.Row) (float64, bool, error) {
	val, isNull, err := b.Args[0].EvalReal(b.Ctx, row)
	return -val, isNull, err
}

type isNullFunctionClass struct {
	BaseFunctionClass
}

func (c *isNullFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	argTp := args[0].GetType().EvalType()
	if argTp == types.ETTimestamp {
		argTp = types.ETDatetime
	} else if argTp == types.ETJson {
		argTp = types.ETString
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETInt, argTp)
	bf.Tp.Flen = 1
	var sig BuiltinFunc
	switch argTp {
	case types.ETInt:
		sig = &builtinIntIsNullSig{bf}
		sig.SetPbCode(tipb.ScalarFuncSig_IntIsNull)
	case types.ETDecimal:
		sig = &builtinDecimalIsNullSig{bf}
		sig.SetPbCode(tipb.ScalarFuncSig_DecimalIsNull)
	case types.ETReal:
		sig = &builtinRealIsNullSig{bf}
		sig.SetPbCode(tipb.ScalarFuncSig_RealIsNull)
	case types.ETDatetime:
		sig = &builtinTimeIsNullSig{bf}
		sig.SetPbCode(tipb.ScalarFuncSig_TimeIsNull)
	case types.ETDuration:
		sig = &builtinDurationIsNullSig{bf}
		sig.SetPbCode(tipb.ScalarFuncSig_DurationIsNull)
	case types.ETString:
		sig = &builtinStringIsNullSig{bf}
		sig.SetPbCode(tipb.ScalarFuncSig_StringIsNull)
	default:
		panic("unexpected types.EvalType")
	}
	return sig, nil
}

type builtinDecimalIsNullSig struct {
	BaseBuiltinFunc
}

func (b *builtinDecimalIsNullSig) Clone() BuiltinFunc {
	newSig := &builtinDecimalIsNullSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func evalIsNull(isNull bool, err error) (int64, bool, error) {
	if err != nil {
		return 0, true, err
	}
	if isNull {
		return 1, false, nil
	}
	return 0, false, nil
}

func (b *builtinDecimalIsNullSig) evalInt(row chunk.Row) (int64, bool, error) {
	_, isNull, err := b.Args[0].EvalDecimal(b.Ctx, row)
	return evalIsNull(isNull, err)
}

type builtinDurationIsNullSig struct {
	BaseBuiltinFunc
}

func (b *builtinDurationIsNullSig) Clone() BuiltinFunc {
	newSig := &builtinDurationIsNullSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinDurationIsNullSig) evalInt(row chunk.Row) (int64, bool, error) {
	_, isNull, err := b.Args[0].EvalDuration(b.Ctx, row)
	return evalIsNull(isNull, err)
}

type builtinIntIsNullSig struct {
	BaseBuiltinFunc
}

func (b *builtinIntIsNullSig) Clone() BuiltinFunc {
	newSig := &builtinIntIsNullSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinIntIsNullSig) evalInt(row chunk.Row) (int64, bool, error) {
	_, isNull, err := b.Args[0].EvalInt(b.Ctx, row)
	return evalIsNull(isNull, err)
}

type builtinRealIsNullSig struct {
	BaseBuiltinFunc
}

func (b *builtinRealIsNullSig) Clone() BuiltinFunc {
	newSig := &builtinRealIsNullSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinRealIsNullSig) evalInt(row chunk.Row) (int64, bool, error) {
	_, isNull, err := b.Args[0].EvalReal(b.Ctx, row)
	return evalIsNull(isNull, err)
}

type builtinStringIsNullSig struct {
	BaseBuiltinFunc
}

func (b *builtinStringIsNullSig) Clone() BuiltinFunc {
	newSig := &builtinStringIsNullSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinStringIsNullSig) evalInt(row chunk.Row) (int64, bool, error) {
	_, isNull, err := b.Args[0].EvalString(b.Ctx, row)
	return evalIsNull(isNull, err)
}

type builtinTimeIsNullSig struct {
	BaseBuiltinFunc
}

func (b *builtinTimeIsNullSig) Clone() BuiltinFunc {
	newSig := &builtinTimeIsNullSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinTimeIsNullSig) evalInt(row chunk.Row) (int64, bool, error) {
	_, isNull, err := b.Args[0].EvalTime(b.Ctx, row)
	return evalIsNull(isNull, err)
}
