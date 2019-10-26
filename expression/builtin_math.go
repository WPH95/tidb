// Copyright 2013 The ql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSES/QL-LICENSE file.

// Copyright 2015 PingCAP, Inc.
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
	"hash/crc32"
	"math"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cznic/mathutil"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tipb/go-tipb"
)

var (
	_ FunctionClass = &absFunctionClass{}
	_ FunctionClass = &roundFunctionClass{}
	_ FunctionClass = &ceilFunctionClass{}
	_ FunctionClass = &floorFunctionClass{}
	_ FunctionClass = &logFunctionClass{}
	_ FunctionClass = &log2FunctionClass{}
	_ FunctionClass = &log10FunctionClass{}
	_ FunctionClass = &randFunctionClass{}
	_ FunctionClass = &powFunctionClass{}
	_ FunctionClass = &convFunctionClass{}
	_ FunctionClass = &crc32FunctionClass{}
	_ FunctionClass = &signFunctionClass{}
	_ FunctionClass = &sqrtFunctionClass{}
	_ FunctionClass = &acosFunctionClass{}
	_ FunctionClass = &asinFunctionClass{}
	_ FunctionClass = &atanFunctionClass{}
	_ FunctionClass = &cosFunctionClass{}
	_ FunctionClass = &cotFunctionClass{}
	_ FunctionClass = &degreesFunctionClass{}
	_ FunctionClass = &expFunctionClass{}
	_ FunctionClass = &piFunctionClass{}
	_ FunctionClass = &radiansFunctionClass{}
	_ FunctionClass = &sinFunctionClass{}
	_ FunctionClass = &tanFunctionClass{}
	_ FunctionClass = &truncateFunctionClass{}
)

var (
	_ BuiltinFunc = &builtinAbsRealSig{}
	_ BuiltinFunc = &builtinAbsIntSig{}
	_ BuiltinFunc = &builtinAbsUIntSig{}
	_ BuiltinFunc = &builtinAbsDecSig{}
	_ BuiltinFunc = &builtinRoundRealSig{}
	_ BuiltinFunc = &builtinRoundIntSig{}
	_ BuiltinFunc = &builtinRoundDecSig{}
	_ BuiltinFunc = &builtinRoundWithFracRealSig{}
	_ BuiltinFunc = &builtinRoundWithFracIntSig{}
	_ BuiltinFunc = &builtinRoundWithFracDecSig{}
	_ BuiltinFunc = &builtinCeilRealSig{}
	_ BuiltinFunc = &builtinCeilIntToDecSig{}
	_ BuiltinFunc = &builtinCeilIntToIntSig{}
	_ BuiltinFunc = &builtinCeilDecToIntSig{}
	_ BuiltinFunc = &builtinCeilDecToDecSig{}
	_ BuiltinFunc = &builtinFloorRealSig{}
	_ BuiltinFunc = &builtinFloorIntToDecSig{}
	_ BuiltinFunc = &builtinFloorIntToIntSig{}
	_ BuiltinFunc = &builtinFloorDecToIntSig{}
	_ BuiltinFunc = &builtinFloorDecToDecSig{}
	_ BuiltinFunc = &builtinLog1ArgSig{}
	_ BuiltinFunc = &builtinLog2ArgsSig{}
	_ BuiltinFunc = &builtinLog2Sig{}
	_ BuiltinFunc = &builtinLog10Sig{}
	_ BuiltinFunc = &builtinRandSig{}
	_ BuiltinFunc = &builtinRandWithSeedSig{}
	_ BuiltinFunc = &builtinPowSig{}
	_ BuiltinFunc = &builtinConvSig{}
	_ BuiltinFunc = &builtinCRC32Sig{}
	_ BuiltinFunc = &builtinSignSig{}
	_ BuiltinFunc = &builtinSqrtSig{}
	_ BuiltinFunc = &builtinAcosSig{}
	_ BuiltinFunc = &builtinAsinSig{}
	_ BuiltinFunc = &builtinAtan1ArgSig{}
	_ BuiltinFunc = &builtinAtan2ArgsSig{}
	_ BuiltinFunc = &builtinCosSig{}
	_ BuiltinFunc = &builtinCotSig{}
	_ BuiltinFunc = &builtinDegreesSig{}
	_ BuiltinFunc = &builtinExpSig{}
	_ BuiltinFunc = &builtinPISig{}
	_ BuiltinFunc = &builtinRadiansSig{}
	_ BuiltinFunc = &builtinSinSig{}
	_ BuiltinFunc = &builtinTanSig{}
	_ BuiltinFunc = &builtinTruncateIntSig{}
	_ BuiltinFunc = &builtinTruncateRealSig{}
	_ BuiltinFunc = &builtinTruncateDecimalSig{}
	_ BuiltinFunc = &builtinTruncateUintSig{}
)

type absFunctionClass struct {
	BaseFunctionClass
}

func (c *absFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, c.VerifyArgs(args)
	}

	argFieldTp := args[0].GetType()
	argTp := argFieldTp.EvalType()
	if argTp != types.ETInt && argTp != types.ETDecimal {
		argTp = types.ETReal
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, argTp, argTp)
	if mysql.HasUnsignedFlag(argFieldTp.Flag) {
		bf.Tp.Flag |= mysql.UnsignedFlag
	}
	if argTp == types.ETReal {
		bf.Tp.Flen, bf.Tp.Decimal = mysql.GetDefaultFieldLengthAndDecimal(mysql.TypeDouble)
	} else {
		bf.Tp.Flen = argFieldTp.Flen
		bf.Tp.Decimal = argFieldTp.Decimal
	}
	var sig BuiltinFunc
	switch argTp {
	case types.ETInt:
		if mysql.HasUnsignedFlag(argFieldTp.Flag) {
			sig = &builtinAbsUIntSig{bf}
			sig.SetPbCode(tipb.ScalarFuncSig_AbsUInt)
		} else {
			sig = &builtinAbsIntSig{bf}
			sig.SetPbCode(tipb.ScalarFuncSig_AbsInt)
		}
	case types.ETDecimal:
		sig = &builtinAbsDecSig{bf}
		sig.SetPbCode(tipb.ScalarFuncSig_AbsDecimal)
	case types.ETReal:
		sig = &builtinAbsRealSig{bf}
		sig.SetPbCode(tipb.ScalarFuncSig_AbsReal)
	default:
		panic("unexpected argTp")
	}
	return sig, nil
}

type builtinAbsRealSig struct {
	BaseBuiltinFunc
}

func (b *builtinAbsRealSig) Clone() BuiltinFunc {
	newSig := &builtinAbsRealSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalReal evals ABS(value).
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_abs
func (b *builtinAbsRealSig) evalReal(row chunk.Row) (float64, bool, error) {
	val, isNull, err := b.Args[0].EvalReal(b.Ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	return math.Abs(val), false, nil
}

type builtinAbsIntSig struct {
	BaseBuiltinFunc
}

func (b *builtinAbsIntSig) Clone() BuiltinFunc {
	newSig := &builtinAbsIntSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalInt evals ABS(value).
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_abs
func (b *builtinAbsIntSig) evalInt(row chunk.Row) (int64, bool, error) {
	val, isNull, err := b.Args[0].EvalInt(b.Ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	if val >= 0 {
		return val, false, nil
	}
	if val == math.MinInt64 {
		return 0, false, types.ErrOverflow.GenWithStackByArgs("BIGINT", fmt.Sprintf("abs(%d)", val))
	}
	return -val, false, nil
}

type builtinAbsUIntSig struct {
	BaseBuiltinFunc
}

func (b *builtinAbsUIntSig) Clone() BuiltinFunc {
	newSig := &builtinAbsUIntSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalInt evals ABS(value).
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_abs
func (b *builtinAbsUIntSig) evalInt(row chunk.Row) (int64, bool, error) {
	return b.Args[0].EvalInt(b.Ctx, row)
}

type builtinAbsDecSig struct {
	BaseBuiltinFunc
}

func (b *builtinAbsDecSig) Clone() BuiltinFunc {
	newSig := &builtinAbsDecSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalDecimal evals ABS(value).
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_abs
func (b *builtinAbsDecSig) evalDecimal(row chunk.Row) (*types.MyDecimal, bool, error) {
	val, isNull, err := b.Args[0].EvalDecimal(b.Ctx, row)
	if isNull || err != nil {
		return nil, isNull, err
	}
	to := new(types.MyDecimal)
	if !val.IsNegative() {
		*to = *val
	} else {
		if err = types.DecimalSub(new(types.MyDecimal), val, to); err != nil {
			return nil, true, err
		}
	}
	return to, false, nil
}

func (c *roundFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, c.VerifyArgs(args)
	}
	argTp := args[0].GetType().EvalType()
	if argTp != types.ETInt && argTp != types.ETDecimal {
		argTp = types.ETReal
	}
	argTps := []types.EvalType{argTp}
	if len(args) > 1 {
		argTps = append(argTps, types.ETInt)
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, argTp, argTps...)
	argFieldTp := args[0].GetType()
	if mysql.HasUnsignedFlag(argFieldTp.Flag) {
		bf.Tp.Flag |= mysql.UnsignedFlag
	}

	bf.Tp.Flen = argFieldTp.Flen
	bf.Tp.Decimal = calculateDecimal4RoundAndTruncate(ctx, args, argTp)

	var sig BuiltinFunc
	if len(args) > 1 {
		switch argTp {
		case types.ETInt:
			sig = &builtinRoundWithFracIntSig{bf}
		case types.ETDecimal:
			sig = &builtinRoundWithFracDecSig{bf}
		case types.ETReal:
			sig = &builtinRoundWithFracRealSig{bf}
		default:
			panic("unexpected argTp")
		}
	} else {
		switch argTp {
		case types.ETInt:
			sig = &builtinRoundIntSig{bf}
		case types.ETDecimal:
			sig = &builtinRoundDecSig{bf}
		case types.ETReal:
			sig = &builtinRoundRealSig{bf}
		default:
			panic("unexpected argTp")
		}
	}
	return sig, nil
}

// calculateDecimal4RoundAndTruncate calculates tp.decimals of round/truncate func.
func calculateDecimal4RoundAndTruncate(ctx sessionctx.Context, args []Expression, retType types.EvalType) int {
	if retType == types.ETInt || len(args) <= 1 {
		return 0
	}
	secondConst, secondIsConst := args[1].(*Constant)
	if !secondIsConst {
		return args[0].GetType().Decimal
	}
	argDec, isNull, err := secondConst.EvalInt(ctx, chunk.Row{})
	if err != nil || isNull || argDec < 0 {
		return 0
	}
	if argDec > mysql.MaxDecimalScale {
		return mysql.MaxDecimalScale
	}
	return int(argDec)
}

type builtinRoundRealSig struct {
	BaseBuiltinFunc
}

func (b *builtinRoundRealSig) Clone() BuiltinFunc {
	newSig := &builtinRoundRealSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalReal evals ROUND(value).
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_round
func (b *builtinRoundRealSig) evalReal(row chunk.Row) (float64, bool, error) {
	val, isNull, err := b.Args[0].EvalReal(b.Ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	return types.Round(val, 0), false, nil
}

type builtinRoundIntSig struct {
	BaseBuiltinFunc
}

func (b *builtinRoundIntSig) Clone() BuiltinFunc {
	newSig := &builtinRoundIntSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalInt evals ROUND(value).
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_round
func (b *builtinRoundIntSig) evalInt(row chunk.Row) (int64, bool, error) {
	return b.Args[0].EvalInt(b.Ctx, row)
}

type builtinRoundDecSig struct {
	BaseBuiltinFunc
}

func (b *builtinRoundDecSig) Clone() BuiltinFunc {
	newSig := &builtinRoundDecSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalDecimal evals ROUND(value).
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_round
func (b *builtinRoundDecSig) evalDecimal(row chunk.Row) (*types.MyDecimal, bool, error) {
	val, isNull, err := b.Args[0].EvalDecimal(b.Ctx, row)
	if isNull || err != nil {
		return nil, isNull, err
	}
	to := new(types.MyDecimal)
	if err = val.Round(to, 0, types.ModeHalfEven); err != nil {
		return nil, true, err
	}
	return to, false, nil
}

type builtinRoundWithFracRealSig struct {
	BaseBuiltinFunc
}

func (b *builtinRoundWithFracRealSig) Clone() BuiltinFunc {
	newSig := &builtinRoundWithFracRealSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalReal evals ROUND(value, frac).
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_round
func (b *builtinRoundWithFracRealSig) evalReal(row chunk.Row) (float64, bool, error) {
	val, isNull, err := b.Args[0].EvalReal(b.Ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	frac, isNull, err := b.Args[1].EvalInt(b.Ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	return types.Round(val, int(frac)), false, nil
}

type builtinRoundWithFracIntSig struct {
	BaseBuiltinFunc
}

func (b *builtinRoundWithFracIntSig) Clone() BuiltinFunc {
	newSig := &builtinRoundWithFracIntSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalInt evals ROUND(value, frac).
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_round
func (b *builtinRoundWithFracIntSig) evalInt(row chunk.Row) (int64, bool, error) {
	val, isNull, err := b.Args[0].EvalInt(b.Ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	frac, isNull, err := b.Args[1].EvalInt(b.Ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	return int64(types.Round(float64(val), int(frac))), false, nil
}

type builtinRoundWithFracDecSig struct {
	BaseBuiltinFunc
}

func (b *builtinRoundWithFracDecSig) Clone() BuiltinFunc {
	newSig := &builtinRoundWithFracDecSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalDecimal evals ROUND(value, frac).
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_round
func (b *builtinRoundWithFracDecSig) evalDecimal(row chunk.Row) (*types.MyDecimal, bool, error) {
	val, isNull, err := b.Args[0].EvalDecimal(b.Ctx, row)
	if isNull || err != nil {
		return nil, isNull, err
	}
	frac, isNull, err := b.Args[1].EvalInt(b.Ctx, row)
	if isNull || err != nil {
		return nil, isNull, err
	}
	to := new(types.MyDecimal)
	if err = val.Round(to, mathutil.Min(int(frac), b.Tp.Decimal), types.ModeHalfEven); err != nil {
		return nil, true, err
	}
	return to, false, nil
}

type ceilFunctionClass struct {
	BaseFunctionClass
}

func (c *ceilFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (sig BuiltinFunc, err error) {
	if err = c.VerifyArgs(args); err != nil {
		return nil, err
	}

	retTp, argTp := getEvalTp4FloorAndCeil(args[0])
	bf := NewBaseBuiltinFuncWithTp(ctx, args, retTp, argTp)
	setFlag4FloorAndCeil(bf.Tp, args[0])
	argFieldTp := args[0].GetType()
	bf.Tp.Flen, bf.Tp.Decimal = argFieldTp.Flen, 0

	switch argTp {
	case types.ETInt:
		if retTp == types.ETInt {
			sig = &builtinCeilIntToIntSig{bf}
			sig.SetPbCode(tipb.ScalarFuncSig_CeilIntToInt)
		} else {
			sig = &builtinCeilIntToDecSig{bf}
			sig.SetPbCode(tipb.ScalarFuncSig_CeilIntToDec)
		}
	case types.ETDecimal:
		if retTp == types.ETInt {
			sig = &builtinCeilDecToIntSig{bf}
			sig.SetPbCode(tipb.ScalarFuncSig_CeilDecToInt)
		} else {
			sig = &builtinCeilDecToDecSig{bf}
			sig.SetPbCode(tipb.ScalarFuncSig_CeilDecToDec)
		}
	default:
		sig = &builtinCeilRealSig{bf}
		sig.SetPbCode(tipb.ScalarFuncSig_CeilReal)
	}
	return sig, nil
}

type builtinCeilRealSig struct {
	BaseBuiltinFunc
}

func (b *builtinCeilRealSig) Clone() BuiltinFunc {
	newSig := &builtinCeilRealSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalReal evals a builtinCeilRealSig.
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_ceil
func (b *builtinCeilRealSig) evalReal(row chunk.Row) (float64, bool, error) {
	val, isNull, err := b.Args[0].EvalReal(b.Ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	return math.Ceil(val), false, nil
}

type builtinCeilIntToIntSig struct {
	BaseBuiltinFunc
}

func (b *builtinCeilIntToIntSig) Clone() BuiltinFunc {
	newSig := &builtinCeilIntToIntSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalInt evals a builtinCeilIntToIntSig.
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_ceil
func (b *builtinCeilIntToIntSig) evalInt(row chunk.Row) (int64, bool, error) {
	return b.Args[0].EvalInt(b.Ctx, row)
}

type builtinCeilIntToDecSig struct {
	BaseBuiltinFunc
}

func (b *builtinCeilIntToDecSig) Clone() BuiltinFunc {
	newSig := &builtinCeilIntToDecSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalDecimal evals a builtinCeilIntToDecSig.
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_Ceil
func (b *builtinCeilIntToDecSig) evalDecimal(row chunk.Row) (*types.MyDecimal, bool, error) {
	val, isNull, err := b.Args[0].EvalInt(b.Ctx, row)
	if isNull || err != nil {
		return nil, true, err
	}

	if mysql.HasUnsignedFlag(b.Args[0].GetType().Flag) || val >= 0 {
		return types.NewDecFromUint(uint64(val)), false, nil
	}
	return types.NewDecFromInt(val), false, nil
}

type builtinCeilDecToIntSig struct {
	BaseBuiltinFunc
}

func (b *builtinCeilDecToIntSig) Clone() BuiltinFunc {
	newSig := &builtinCeilDecToIntSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalInt evals a builtinCeilDecToIntSig.
// Ceil receives
func (b *builtinCeilDecToIntSig) evalInt(row chunk.Row) (int64, bool, error) {
	val, isNull, err := b.Args[0].EvalDecimal(b.Ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	// err here will only be ErrOverFlow(will never happen) or ErrTruncate(can be ignored).
	res, err := val.ToInt()
	if err == types.ErrTruncated {
		err = nil
		if !val.IsNegative() {
			res = res + 1
		}
	}
	return res, false, err
}

type builtinCeilDecToDecSig struct {
	BaseBuiltinFunc
}

func (b *builtinCeilDecToDecSig) Clone() BuiltinFunc {
	newSig := &builtinCeilDecToDecSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalDecimal evals a builtinCeilDecToDecSig.
func (b *builtinCeilDecToDecSig) evalDecimal(row chunk.Row) (*types.MyDecimal, bool, error) {
	val, isNull, err := b.Args[0].EvalDecimal(b.Ctx, row)
	if isNull || err != nil {
		return nil, isNull, err
	}

	res := new(types.MyDecimal)
	if val.IsNegative() {
		err = val.Round(res, 0, types.ModeTruncate)
		return res, err != nil, err
	}

	err = val.Round(res, 0, types.ModeTruncate)
	if err != nil || res.Compare(val) == 0 {
		return res, err != nil, err
	}

	err = types.DecimalAdd(res, types.NewDecFromInt(1), res)
	return res, err != nil, err
}

type floorFunctionClass struct {
	BaseFunctionClass
}

// getEvalTp4FloorAndCeil gets the types.EvalType of FLOOR and CEIL.
func getEvalTp4FloorAndCeil(arg Expression) (retTp, argTp types.EvalType) {
	fieldTp := arg.GetType()
	retTp, argTp = types.ETInt, fieldTp.EvalType()
	switch argTp {
	case types.ETInt:
		if fieldTp.Tp == mysql.TypeLonglong {
			retTp = types.ETDecimal
		}
	case types.ETDecimal:
		if fieldTp.Flen-fieldTp.Decimal > mysql.MaxIntWidth-2 { // len(math.MaxInt64) - 1
			retTp = types.ETDecimal
		}
	default:
		retTp, argTp = types.ETReal, types.ETReal
	}
	return retTp, argTp
}

// setFlag4FloorAndCeil sets return flag of FLOOR and CEIL.
func setFlag4FloorAndCeil(tp *types.FieldType, arg Expression) {
	fieldTp := arg.GetType()
	if (fieldTp.Tp == mysql.TypeLong || fieldTp.Tp == mysql.TypeNewDecimal) && mysql.HasUnsignedFlag(fieldTp.Flag) {
		tp.Flag |= mysql.UnsignedFlag
	}
	// TODO: when argument type is timestamp, add not null flag.
}

func (c *floorFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (sig BuiltinFunc, err error) {
	if err = c.VerifyArgs(args); err != nil {
		return nil, err
	}

	retTp, argTp := getEvalTp4FloorAndCeil(args[0])
	bf := NewBaseBuiltinFuncWithTp(ctx, args, retTp, argTp)
	setFlag4FloorAndCeil(bf.Tp, args[0])
	bf.Tp.Flen, bf.Tp.Decimal = args[0].GetType().Flen, 0
	switch argTp {
	case types.ETInt:
		if retTp == types.ETInt {
			sig = &builtinFloorIntToIntSig{bf}
			sig.SetPbCode(tipb.ScalarFuncSig_FloorIntToInt)
		} else {
			sig = &builtinFloorIntToDecSig{bf}
			sig.SetPbCode(tipb.ScalarFuncSig_FloorIntToDec)
		}
	case types.ETDecimal:
		if retTp == types.ETInt {
			sig = &builtinFloorDecToIntSig{bf}
			sig.SetPbCode(tipb.ScalarFuncSig_FloorDecToInt)
		} else {
			sig = &builtinFloorDecToDecSig{bf}
			sig.SetPbCode(tipb.ScalarFuncSig_FloorDecToDec)
		}
	default:
		sig = &builtinFloorRealSig{bf}
		sig.SetPbCode(tipb.ScalarFuncSig_FloorReal)
	}
	return sig, nil
}

type builtinFloorRealSig struct {
	BaseBuiltinFunc
}

func (b *builtinFloorRealSig) Clone() BuiltinFunc {
	newSig := &builtinFloorRealSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalReal evals a builtinFloorRealSig.
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_floor
func (b *builtinFloorRealSig) evalReal(row chunk.Row) (float64, bool, error) {
	val, isNull, err := b.Args[0].EvalReal(b.Ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	return math.Floor(val), false, nil
}

type builtinFloorIntToIntSig struct {
	BaseBuiltinFunc
}

func (b *builtinFloorIntToIntSig) Clone() BuiltinFunc {
	newSig := &builtinFloorIntToIntSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalInt evals a builtinFloorIntToIntSig.
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_floor
func (b *builtinFloorIntToIntSig) evalInt(row chunk.Row) (int64, bool, error) {
	return b.Args[0].EvalInt(b.Ctx, row)
}

type builtinFloorIntToDecSig struct {
	BaseBuiltinFunc
}

func (b *builtinFloorIntToDecSig) Clone() BuiltinFunc {
	newSig := &builtinFloorIntToDecSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalDecimal evals a builtinFloorIntToDecSig.
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_floor
func (b *builtinFloorIntToDecSig) evalDecimal(row chunk.Row) (*types.MyDecimal, bool, error) {
	val, isNull, err := b.Args[0].EvalInt(b.Ctx, row)
	if isNull || err != nil {
		return nil, true, err
	}

	if mysql.HasUnsignedFlag(b.Args[0].GetType().Flag) || val >= 0 {
		return types.NewDecFromUint(uint64(val)), false, nil
	}
	return types.NewDecFromInt(val), false, nil
}

type builtinFloorDecToIntSig struct {
	BaseBuiltinFunc
}

func (b *builtinFloorDecToIntSig) Clone() BuiltinFunc {
	newSig := &builtinFloorDecToIntSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalInt evals a builtinFloorDecToIntSig.
// floor receives
func (b *builtinFloorDecToIntSig) evalInt(row chunk.Row) (int64, bool, error) {
	val, isNull, err := b.Args[0].EvalDecimal(b.Ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	// err here will only be ErrOverFlow(will never happen) or ErrTruncate(can be ignored).
	res, err := val.ToInt()
	if err == types.ErrTruncated {
		err = nil
		if val.IsNegative() {
			res--
		}
	}
	return res, false, err
}

type builtinFloorDecToDecSig struct {
	BaseBuiltinFunc
}

func (b *builtinFloorDecToDecSig) Clone() BuiltinFunc {
	newSig := &builtinFloorDecToDecSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalDecimal evals a builtinFloorDecToDecSig.
func (b *builtinFloorDecToDecSig) evalDecimal(row chunk.Row) (*types.MyDecimal, bool, error) {
	val, isNull, err := b.Args[0].EvalDecimal(b.Ctx, row)
	if isNull || err != nil {
		return nil, true, err
	}

	res := new(types.MyDecimal)
	if !val.IsNegative() {
		err = val.Round(res, 0, types.ModeTruncate)
		return res, err != nil, err
	}

	err = val.Round(res, 0, types.ModeTruncate)
	if err != nil || res.Compare(val) == 0 {
		return res, err != nil, err
	}

	err = types.DecimalSub(res, types.NewDecFromInt(1), res)
	return res, err != nil, err
}

type logFunctionClass struct {
	BaseFunctionClass
}

func (c *logFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	var (
		sig     BuiltinFunc
		bf      BaseBuiltinFunc
		argsLen   = len(args)
	)

	if argsLen == 1 {
		bf = NewBaseBuiltinFuncWithTp(ctx, args, types.ETReal, types.ETReal)
	} else {
		bf = NewBaseBuiltinFuncWithTp(ctx, args, types.ETReal, types.ETReal, types.ETReal)
	}

	if argsLen == 1 {
		sig = &builtinLog1ArgSig{bf}
	} else {
		sig = &builtinLog2ArgsSig{bf}
	}

	return sig, nil
}

type builtinLog1ArgSig struct {
	BaseBuiltinFunc
}

func (b *builtinLog1ArgSig) Clone() BuiltinFunc {
	newSig := &builtinLog1ArgSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalReal evals a builtinLog1ArgSig, corresponding to log(x).
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_log
func (b *builtinLog1ArgSig) evalReal(row chunk.Row) (float64, bool, error) {
	val, isNull, err := b.Args[0].EvalReal(b.Ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	if val <= 0 {
		return 0, true, nil
	}
	return math.Log(val), false, nil
}

type builtinLog2ArgsSig struct {
	BaseBuiltinFunc
}

func (b *builtinLog2ArgsSig) Clone() BuiltinFunc {
	newSig := &builtinLog2ArgsSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalReal evals a builtinLog2ArgsSig, corresponding to log(b, x).
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_log
func (b *builtinLog2ArgsSig) evalReal(row chunk.Row) (float64, bool, error) {
	val1, isNull, err := b.Args[0].EvalReal(b.Ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}

	val2, isNull, err := b.Args[1].EvalReal(b.Ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}

	if val1 <= 0 || val1 == 1 || val2 <= 0 {
		return 0, true, nil
	}

	return math.Log(val2) / math.Log(val1), false, nil
}

type log2FunctionClass struct {
	BaseFunctionClass
}

func (c *log2FunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETReal, types.ETReal)
	sig := &builtinLog2Sig{bf}
	return sig, nil
}

type builtinLog2Sig struct {
	BaseBuiltinFunc
}

func (b *builtinLog2Sig) Clone() BuiltinFunc {
	newSig := &builtinLog2Sig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalReal evals a builtinLog2Sig.
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_log2
func (b *builtinLog2Sig) evalReal(row chunk.Row) (float64, bool, error) {
	val, isNull, err := b.Args[0].EvalReal(b.Ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	if val <= 0 {
		return 0, true, nil
	}
	return math.Log2(val), false, nil
}

type log10FunctionClass struct {
	BaseFunctionClass
}

func (c *log10FunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETReal, types.ETReal)
	sig := &builtinLog10Sig{bf}
	return sig, nil
}

type builtinLog10Sig struct {
	BaseBuiltinFunc
}

func (b *builtinLog10Sig) Clone() BuiltinFunc {
	newSig := &builtinLog10Sig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalReal evals a builtinLog10Sig.
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_log10
func (b *builtinLog10Sig) evalReal(row chunk.Row) (float64, bool, error) {
	val, isNull, err := b.Args[0].EvalReal(b.Ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	if val <= 0 {
		return 0, true, nil
	}
	return math.Log10(val), false, nil
}

type randFunctionClass struct {
	BaseFunctionClass
}

func (c *randFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	var sig BuiltinFunc
	var argTps []types.EvalType
	if len(args) > 0 {
		argTps = []types.EvalType{types.ETInt}
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETReal, argTps...)
	bt := bf
	if len(args) == 0 {
		seed := time.Now().UnixNano()
		sig = &builtinRandSig{bt, &sync.Mutex{}, rand.New(rand.NewSource(seed))}
	} else if _, isConstant := args[0].(*Constant); isConstant {
		// According to MySQL manual:
		// If an integer argument N is specified, it is used as the seed value:
		// With a constant initializer argument, the seed is initialized once
		// when the statement is prepared, prior to execution.
		seed, isNull, err := args[0].EvalInt(ctx, chunk.Row{})
		if err != nil {
			return nil, err
		}
		if isNull {
			seed = time.Now().UnixNano()
		}
		sig = &builtinRandSig{bt, &sync.Mutex{}, rand.New(rand.NewSource(seed))}
	} else {
		sig = &builtinRandWithSeedSig{bt}
	}
	return sig, nil
}

type builtinRandSig struct {
	BaseBuiltinFunc
	mu      *sync.Mutex
	randGen *rand.Rand
}

func (b *builtinRandSig) Clone() BuiltinFunc {
	newSig := &builtinRandSig{randGen: b.randGen, mu: b.mu}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalReal evals RAND().
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_rand
func (b *builtinRandSig) evalReal(row chunk.Row) (float64, bool, error) {
	b.mu.Lock()
	res := b.randGen.Float64()
	b.mu.Unlock()
	return res, false, nil
}

type builtinRandWithSeedSig struct {
	BaseBuiltinFunc
}

func (b *builtinRandWithSeedSig) Clone() BuiltinFunc {
	newSig := &builtinRandWithSeedSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalReal evals RAND(N).
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_rand
func (b *builtinRandWithSeedSig) evalReal(row chunk.Row) (float64, bool, error) {
	seed, isNull, err := b.Args[0].EvalInt(b.Ctx, row)
	if err != nil {
		return 0, true, err
	}
	// b.args[0] is promised to be a non-constant(such as a column name) in
	// builtinRandWithSeedSig, the seed is initialized with the value for each
	// invocation of RAND().
	var randGen *rand.Rand
	if isNull {
		randGen = rand.New(rand.NewSource(time.Now().UnixNano()))
	} else {
		randGen = rand.New(rand.NewSource(seed))
	}
	return randGen.Float64(), false, nil
}

type powFunctionClass struct {
	BaseFunctionClass
}

func (c *powFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETReal, types.ETReal, types.ETReal)
	sig := &builtinPowSig{bf}
	return sig, nil
}

type builtinPowSig struct {
	BaseBuiltinFunc
}

func (b *builtinPowSig) Clone() BuiltinFunc {
	newSig := &builtinPowSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalReal evals POW(x, y).
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_pow
func (b *builtinPowSig) evalReal(row chunk.Row) (float64, bool, error) {
	x, isNull, err := b.Args[0].EvalReal(b.Ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	y, isNull, err := b.Args[1].EvalReal(b.Ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	power := math.Pow(x, y)
	if math.IsInf(power, -1) || math.IsInf(power, 1) || math.IsNaN(power) {
		return 0, false, types.ErrOverflow.GenWithStackByArgs("DOUBLE", fmt.Sprintf("pow(%s, %s)", strconv.FormatFloat(x, 'f', -1, 64), strconv.FormatFloat(y, 'f', -1, 64)))
	}
	return power, false, nil
}

type roundFunctionClass struct {
	BaseFunctionClass
}

type convFunctionClass struct {
	BaseFunctionClass
}

func (c *convFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}

	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETString, types.ETString, types.ETInt, types.ETInt)
	bf.Tp.Flen = 64
	sig := &builtinConvSig{bf}
	return sig, nil
}

type builtinConvSig struct {
	BaseBuiltinFunc
}

func (b *builtinConvSig) Clone() BuiltinFunc {
	newSig := &builtinConvSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalString evals CONV(N,from_base,to_base).
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_conv.
func (b *builtinConvSig) EvalString(row chunk.Row) (res string, isNull bool, err error) {
	n, isNull, err := b.Args[0].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}

	fromBase, isNull, err := b.Args[1].EvalInt(b.Ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}

	toBase, isNull, err := b.Args[2].EvalInt(b.Ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}

	var (
		signed     bool
		negative   bool
		ignoreSign bool
	)
	if fromBase < 0 {
		fromBase = -fromBase
		signed = true
	}

	if toBase < 0 {
		toBase = -toBase
		ignoreSign = true
	}

	if fromBase > 36 || fromBase < 2 || toBase > 36 || toBase < 2 {
		return res, true, nil
	}

	n = getValidPrefix(strings.TrimSpace(n), fromBase)
	if len(n) == 0 {
		return "0", false, nil
	}

	if n[0] == '-' {
		negative = true
		n = n[1:]
	}

	val, err := strconv.ParseUint(n, int(fromBase), 64)
	if err != nil {
		return res, false, types.ErrOverflow.GenWithStackByArgs("BIGINT UNSINGED", n)
	}
	if signed {
		if negative && val > -math.MinInt64 {
			val = -math.MinInt64
		}
		if !negative && val > math.MaxInt64 {
			val = math.MaxInt64
		}
	}
	if negative {
		val = -val
	}

	if int64(val) < 0 {
		negative = true
	} else {
		negative = false
	}
	if ignoreSign && negative {
		val = 0 - val
	}

	s := strconv.FormatUint(val, int(toBase))
	if negative && ignoreSign {
		s = "-" + s
	}
	res = strings.ToUpper(s)
	return res, false, nil
}

type crc32FunctionClass struct {
	BaseFunctionClass
}

func (c *crc32FunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETString)
	bf.Tp.Flen = 10
	bf.Tp.Flag |= mysql.UnsignedFlag
	sig := &builtinCRC32Sig{bf}
	return sig, nil
}

type builtinCRC32Sig struct {
	BaseBuiltinFunc
}

func (b *builtinCRC32Sig) Clone() BuiltinFunc {
	newSig := &builtinCRC32Sig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalInt evals a CRC32(expr).
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_crc32
func (b *builtinCRC32Sig) evalInt(row chunk.Row) (int64, bool, error) {
	x, isNull, err := b.Args[0].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	r := crc32.ChecksumIEEE([]byte(x))
	return int64(r), false, nil
}

type signFunctionClass struct {
	BaseFunctionClass
}

func (c *signFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETReal)
	sig := &builtinSignSig{bf}
	return sig, nil
}

type builtinSignSig struct {
	BaseBuiltinFunc
}

func (b *builtinSignSig) Clone() BuiltinFunc {
	newSig := &builtinSignSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalInt evals SIGN(v).
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_sign
func (b *builtinSignSig) evalInt(row chunk.Row) (int64, bool, error) {
	val, isNull, err := b.Args[0].EvalReal(b.Ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	if val > 0 {
		return 1, false, nil
	} else if val == 0 {
		return 0, false, nil
	} else {
		return -1, false, nil
	}
}

type sqrtFunctionClass struct {
	BaseFunctionClass
}

func (c *sqrtFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETReal, types.ETReal)
	sig := &builtinSqrtSig{bf}
	return sig, nil
}

type builtinSqrtSig struct {
	BaseBuiltinFunc
}

func (b *builtinSqrtSig) Clone() BuiltinFunc {
	newSig := &builtinSqrtSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalReal evals a SQRT(x).
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_sqrt
func (b *builtinSqrtSig) evalReal(row chunk.Row) (float64, bool, error) {
	val, isNull, err := b.Args[0].EvalReal(b.Ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	if val < 0 {
		return 0, true, nil
	}
	return math.Sqrt(val), false, nil
}

type acosFunctionClass struct {
	BaseFunctionClass
}

func (c *acosFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETReal, types.ETReal)
	sig := &builtinAcosSig{bf}
	return sig, nil
}

type builtinAcosSig struct {
	BaseBuiltinFunc
}

func (b *builtinAcosSig) Clone() BuiltinFunc {
	newSig := &builtinAcosSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalReal evals a builtinAcosSig.
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_acos
func (b *builtinAcosSig) evalReal(row chunk.Row) (float64, bool, error) {
	val, isNull, err := b.Args[0].EvalReal(b.Ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	if val < -1 || val > 1 {
		return 0, true, nil
	}

	return math.Acos(val), false, nil
}

type asinFunctionClass struct {
	BaseFunctionClass
}

func (c *asinFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETReal, types.ETReal)
	sig := &builtinAsinSig{bf}
	return sig, nil
}

type builtinAsinSig struct {
	BaseBuiltinFunc
}

func (b *builtinAsinSig) Clone() BuiltinFunc {
	newSig := &builtinAsinSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalReal evals a builtinAsinSig.
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_asin
func (b *builtinAsinSig) evalReal(row chunk.Row) (float64, bool, error) {
	val, isNull, err := b.Args[0].EvalReal(b.Ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}

	if val < -1 || val > 1 {
		return 0, true, nil
	}

	return math.Asin(val), false, nil
}

type atanFunctionClass struct {
	BaseFunctionClass
}

func (c *atanFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	var (
		sig     BuiltinFunc
		bf      BaseBuiltinFunc
		argsLen   = len(args)
	)

	if argsLen == 1 {
		bf = NewBaseBuiltinFuncWithTp(ctx, args, types.ETReal, types.ETReal)
	} else {
		bf = NewBaseBuiltinFuncWithTp(ctx, args, types.ETReal, types.ETReal, types.ETReal)
	}

	if argsLen == 1 {
		sig = &builtinAtan1ArgSig{bf}
	} else {
		sig = &builtinAtan2ArgsSig{bf}
	}

	return sig, nil
}

type builtinAtan1ArgSig struct {
	BaseBuiltinFunc
}

func (b *builtinAtan1ArgSig) Clone() BuiltinFunc {
	newSig := &builtinAtan1ArgSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalReal evals a builtinAtan1ArgSig, corresponding to atan(x).
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_atan
func (b *builtinAtan1ArgSig) evalReal(row chunk.Row) (float64, bool, error) {
	val, isNull, err := b.Args[0].EvalReal(b.Ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}

	return math.Atan(val), false, nil
}

type builtinAtan2ArgsSig struct {
	BaseBuiltinFunc
}

func (b *builtinAtan2ArgsSig) Clone() BuiltinFunc {
	newSig := &builtinAtan2ArgsSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalReal evals a builtinAtan1ArgSig, corresponding to atan(y, x).
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_atan
func (b *builtinAtan2ArgsSig) evalReal(row chunk.Row) (float64, bool, error) {
	val1, isNull, err := b.Args[0].EvalReal(b.Ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}

	val2, isNull, err := b.Args[1].EvalReal(b.Ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}

	return math.Atan2(val1, val2), false, nil
}

type cosFunctionClass struct {
	BaseFunctionClass
}

func (c *cosFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETReal, types.ETReal)
	sig := &builtinCosSig{bf}
	return sig, nil
}

type builtinCosSig struct {
	BaseBuiltinFunc
}

func (b *builtinCosSig) Clone() BuiltinFunc {
	newSig := &builtinCosSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalReal evals a builtinCosSig.
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_cos
func (b *builtinCosSig) evalReal(row chunk.Row) (float64, bool, error) {
	val, isNull, err := b.Args[0].EvalReal(b.Ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	return math.Cos(val), false, nil
}

type cotFunctionClass struct {
	BaseFunctionClass
}

func (c *cotFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETReal, types.ETReal)
	sig := &builtinCotSig{bf}
	return sig, nil
}

type builtinCotSig struct {
	BaseBuiltinFunc
}

func (b *builtinCotSig) Clone() BuiltinFunc {
	newSig := &builtinCotSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalReal evals a builtinCotSig.
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_cot
func (b *builtinCotSig) evalReal(row chunk.Row) (float64, bool, error) {
	val, isNull, err := b.Args[0].EvalReal(b.Ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}

	tan := math.Tan(val)
	if tan != 0 {
		cot := 1 / tan
		if !math.IsInf(cot, 0) && !math.IsNaN(cot) {
			return cot, false, nil
		}
	}
	return 0, false, types.ErrOverflow.GenWithStackByArgs("DOUBLE", fmt.Sprintf("cot(%s)", strconv.FormatFloat(val, 'f', -1, 64)))
}

type degreesFunctionClass struct {
	BaseFunctionClass
}

func (c *degreesFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETReal, types.ETReal)
	sig := &builtinDegreesSig{bf}
	return sig, nil
}

type builtinDegreesSig struct {
	BaseBuiltinFunc
}

func (b *builtinDegreesSig) Clone() BuiltinFunc {
	newSig := &builtinDegreesSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalReal evals a builtinDegreesSig.
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_degrees
func (b *builtinDegreesSig) evalReal(row chunk.Row) (float64, bool, error) {
	val, isNull, err := b.Args[0].EvalReal(b.Ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	res := val * 180 / math.Pi
	return res, false, nil
}

type expFunctionClass struct {
	BaseFunctionClass
}

func (c *expFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETReal, types.ETReal)
	sig := &builtinExpSig{bf}
	return sig, nil
}

type builtinExpSig struct {
	BaseBuiltinFunc
}

func (b *builtinExpSig) Clone() BuiltinFunc {
	newSig := &builtinExpSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalReal evals a builtinExpSig.
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_exp
func (b *builtinExpSig) evalReal(row chunk.Row) (float64, bool, error) {
	val, isNull, err := b.Args[0].EvalReal(b.Ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	exp := math.Exp(val)
	if math.IsInf(exp, 0) || math.IsNaN(exp) {
		s := fmt.Sprintf("exp(%s)", b.Args[0].String())
		return 0, false, types.ErrOverflow.GenWithStackByArgs("DOUBLE", s)
	}
	return exp, false, nil
}

type piFunctionClass struct {
	BaseFunctionClass
}

func (c *piFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	var (
		bf  BaseBuiltinFunc
		sig BuiltinFunc
	)

	bf = NewBaseBuiltinFuncWithTp(ctx, args, types.ETReal)
	bf.Tp.Decimal = 6
	bf.Tp.Flen = 8
	sig = &builtinPISig{bf}
	return sig, nil
}

type builtinPISig struct {
	BaseBuiltinFunc
}

func (b *builtinPISig) Clone() BuiltinFunc {
	newSig := &builtinPISig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalReal evals a builtinPISig.
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_pi
func (b *builtinPISig) evalReal(_ chunk.Row) (float64, bool, error) {
	return float64(math.Pi), false, nil
}

type radiansFunctionClass struct {
	BaseFunctionClass
}

func (c *radiansFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETReal, types.ETReal)
	sig := &builtinRadiansSig{bf}
	return sig, nil
}

type builtinRadiansSig struct {
	BaseBuiltinFunc
}

func (b *builtinRadiansSig) Clone() BuiltinFunc {
	newSig := &builtinRadiansSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalReal evals RADIANS(X).
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_radians
func (b *builtinRadiansSig) evalReal(row chunk.Row) (float64, bool, error) {
	x, isNull, err := b.Args[0].EvalReal(b.Ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	return x * math.Pi / 180, false, nil
}

type sinFunctionClass struct {
	BaseFunctionClass
}

func (c *sinFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETReal, types.ETReal)
	sig := &builtinSinSig{bf}
	return sig, nil
}

type builtinSinSig struct {
	BaseBuiltinFunc
}

func (b *builtinSinSig) Clone() BuiltinFunc {
	newSig := &builtinSinSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalReal evals a builtinSinSig.
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_sin
func (b *builtinSinSig) evalReal(row chunk.Row) (float64, bool, error) {
	val, isNull, err := b.Args[0].EvalReal(b.Ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	return math.Sin(val), false, nil
}

type tanFunctionClass struct {
	BaseFunctionClass
}

func (c *tanFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETReal, types.ETReal)
	sig := &builtinTanSig{bf}
	return sig, nil
}

type builtinTanSig struct {
	BaseBuiltinFunc
}

func (b *builtinTanSig) Clone() BuiltinFunc {
	newSig := &builtinTanSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalReal evals a builtinTanSig.
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_tan
func (b *builtinTanSig) evalReal(row chunk.Row) (float64, bool, error) {
	val, isNull, err := b.Args[0].EvalReal(b.Ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	return math.Tan(val), false, nil
}

type truncateFunctionClass struct {
	BaseFunctionClass
}

func (c *truncateFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}

	argTp := args[0].GetType().EvalType()
	if argTp == types.ETTimestamp || argTp == types.ETDatetime || argTp == types.ETDuration || argTp == types.ETString {
		argTp = types.ETReal
	}

	bf := NewBaseBuiltinFuncWithTp(ctx, args, argTp, argTp, types.ETInt)

	bf.Tp.Decimal = calculateDecimal4RoundAndTruncate(ctx, args, argTp)
	bf.Tp.Flen = args[0].GetType().Flen - args[0].GetType().Decimal + bf.Tp.Decimal
	bf.Tp.Flag |= args[0].GetType().Flag

	var sig BuiltinFunc
	switch argTp {
	case types.ETInt:
		if mysql.HasUnsignedFlag(args[0].GetType().Flag) {
			sig = &builtinTruncateUintSig{bf}
		} else {
			sig = &builtinTruncateIntSig{bf}
		}
	case types.ETReal:
		sig = &builtinTruncateRealSig{bf}
	case types.ETDecimal:
		sig = &builtinTruncateDecimalSig{bf}
	}

	return sig, nil
}

type builtinTruncateDecimalSig struct {
	BaseBuiltinFunc
}

func (b *builtinTruncateDecimalSig) Clone() BuiltinFunc {
	newSig := &builtinTruncateDecimalSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalDecimal evals a TRUNCATE(X,D).
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_truncate
func (b *builtinTruncateDecimalSig) evalDecimal(row chunk.Row) (*types.MyDecimal, bool, error) {
	x, isNull, err := b.Args[0].EvalDecimal(b.Ctx, row)
	if isNull || err != nil {
		return nil, isNull, err
	}

	d, isNull, err := b.Args[1].EvalInt(b.Ctx, row)
	if isNull || err != nil {
		return nil, isNull, err
	}

	result := new(types.MyDecimal)
	if err := x.Round(result, mathutil.Min(int(d), b.getRetTp().Decimal), types.ModeTruncate); err != nil {
		return nil, true, err
	}
	return result, false, nil
}

type builtinTruncateRealSig struct {
	BaseBuiltinFunc
}

func (b *builtinTruncateRealSig) Clone() BuiltinFunc {
	newSig := &builtinTruncateRealSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalReal evals a TRUNCATE(X,D).
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_truncate
func (b *builtinTruncateRealSig) evalReal(row chunk.Row) (float64, bool, error) {
	x, isNull, err := b.Args[0].EvalReal(b.Ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}

	d, isNull, err := b.Args[1].EvalInt(b.Ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}

	return types.Truncate(x, int(d)), false, nil
}

type builtinTruncateIntSig struct {
	BaseBuiltinFunc
}

func (b *builtinTruncateIntSig) Clone() BuiltinFunc {
	newSig := &builtinTruncateIntSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalInt evals a TRUNCATE(X,D).
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_truncate
func (b *builtinTruncateIntSig) evalInt(row chunk.Row) (int64, bool, error) {
	x, isNull, err := b.Args[0].EvalInt(b.Ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}

	d, isNull, err := b.Args[1].EvalInt(b.Ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}

	if d >= 0 {
		return x, false, nil
	}
	shift := int64(math.Pow10(int(-d)))
	return x / shift * shift, false, nil
}

func (b *builtinTruncateUintSig) Clone() BuiltinFunc {
	newSig := &builtinTruncateUintSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

type builtinTruncateUintSig struct {
	BaseBuiltinFunc
}

// evalInt evals a TRUNCATE(X,D).
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_truncate
func (b *builtinTruncateUintSig) evalInt(row chunk.Row) (int64, bool, error) {
	x, isNull, err := b.Args[0].EvalInt(b.Ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	uintx := uint64(x)

	d, isNull, err := b.Args[1].EvalInt(b.Ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	if d >= 0 {
		return x, false, nil
	}
	shift := uint64(math.Pow10(int(-d)))
	return int64(uintx / shift * shift), false, nil
}
