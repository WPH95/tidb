// Copyright 2017 PingCAP, Inc.
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

// We implement 6 CastAsXXFunctionClass for `cast` built-in functions.
// XX means the return type of the `cast` built-in functions.
// XX contains the following 6 types:
// Int, Decimal, Real, String, Time, Duration.

// We implement 6 CastYYAsXXSig built-in function signatures for every CastAsXXFunctionClass.
// builtinCastXXAsYYSig takes a argument of type XX and returns a value of type YY.

package expression

import (
	"math"
	"strconv"
	"strings"

	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/charset"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/types/json"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tipb/go-tipb"
)

var (
	_ FunctionClass = &castAsIntFunctionClass{}
	_ FunctionClass = &castAsRealFunctionClass{}
	_ FunctionClass = &castAsStringFunctionClass{}
	_ FunctionClass = &castAsDecimalFunctionClass{}
	_ FunctionClass = &castAsTimeFunctionClass{}
	_ FunctionClass = &castAsDurationFunctionClass{}
	_ FunctionClass = &castAsJSONFunctionClass{}
)

var (
	_ BuiltinFunc = &builtinCastIntAsIntSig{}
	_ BuiltinFunc = &builtinCastIntAsRealSig{}
	_ BuiltinFunc = &builtinCastIntAsStringSig{}
	_ BuiltinFunc = &builtinCastIntAsDecimalSig{}
	_ BuiltinFunc = &builtinCastIntAsTimeSig{}
	_ BuiltinFunc = &builtinCastIntAsDurationSig{}
	_ BuiltinFunc = &builtinCastIntAsJSONSig{}

	_ BuiltinFunc = &builtinCastRealAsIntSig{}
	_ BuiltinFunc = &builtinCastRealAsRealSig{}
	_ BuiltinFunc = &builtinCastRealAsStringSig{}
	_ BuiltinFunc = &builtinCastRealAsDecimalSig{}
	_ BuiltinFunc = &builtinCastRealAsTimeSig{}
	_ BuiltinFunc = &builtinCastRealAsDurationSig{}
	_ BuiltinFunc = &builtinCastRealAsJSONSig{}

	_ BuiltinFunc = &builtinCastDecimalAsIntSig{}
	_ BuiltinFunc = &builtinCastDecimalAsRealSig{}
	_ BuiltinFunc = &builtinCastDecimalAsStringSig{}
	_ BuiltinFunc = &builtinCastDecimalAsDecimalSig{}
	_ BuiltinFunc = &builtinCastDecimalAsTimeSig{}
	_ BuiltinFunc = &builtinCastDecimalAsDurationSig{}
	_ BuiltinFunc = &builtinCastDecimalAsJSONSig{}

	_ BuiltinFunc = &builtinCastStringAsIntSig{}
	_ BuiltinFunc = &builtinCastStringAsRealSig{}
	_ BuiltinFunc = &builtinCastStringAsStringSig{}
	_ BuiltinFunc = &builtinCastStringAsDecimalSig{}
	_ BuiltinFunc = &builtinCastStringAsTimeSig{}
	_ BuiltinFunc = &builtinCastStringAsDurationSig{}
	_ BuiltinFunc = &builtinCastStringAsJSONSig{}

	_ BuiltinFunc = &builtinCastTimeAsIntSig{}
	_ BuiltinFunc = &builtinCastTimeAsRealSig{}
	_ BuiltinFunc = &builtinCastTimeAsStringSig{}
	_ BuiltinFunc = &builtinCastTimeAsDecimalSig{}
	_ BuiltinFunc = &builtinCastTimeAsTimeSig{}
	_ BuiltinFunc = &builtinCastTimeAsDurationSig{}
	_ BuiltinFunc = &builtinCastTimeAsJSONSig{}

	_ BuiltinFunc = &builtinCastDurationAsIntSig{}
	_ BuiltinFunc = &builtinCastDurationAsRealSig{}
	_ BuiltinFunc = &builtinCastDurationAsStringSig{}
	_ BuiltinFunc = &builtinCastDurationAsDecimalSig{}
	_ BuiltinFunc = &builtinCastDurationAsTimeSig{}
	_ BuiltinFunc = &builtinCastDurationAsDurationSig{}
	_ BuiltinFunc = &builtinCastDurationAsJSONSig{}

	_ BuiltinFunc = &builtinCastJSONAsIntSig{}
	_ BuiltinFunc = &builtinCastJSONAsRealSig{}
	_ BuiltinFunc = &builtinCastJSONAsStringSig{}
	_ BuiltinFunc = &builtinCastJSONAsDecimalSig{}
	_ BuiltinFunc = &builtinCastJSONAsTimeSig{}
	_ BuiltinFunc = &builtinCastJSONAsDurationSig{}
	_ BuiltinFunc = &builtinCastJSONAsJSONSig{}
)

type castAsIntFunctionClass struct {
	BaseFunctionClass

	tp *types.FieldType
}

func (c *castAsIntFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (sig BuiltinFunc, err error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	bf := newBaseBuiltinCastFunc(newBaseBuiltinFunc(ctx, args), ctx.Value(inUnionCastContext) != nil)
	bf.Tp = c.tp
	if args[0].GetType().Hybrid() || IsBinaryLiteral(args[0]) {
		sig = &builtinCastIntAsIntSig{bf}
		sig.SetPbCode(tipb.ScalarFuncSig_CastIntAsInt)
		return sig, nil
	}
	argTp := args[0].GetType().EvalType()
	switch argTp {
	case types.ETInt:
		sig = &builtinCastIntAsIntSig{bf}
		sig.SetPbCode(tipb.ScalarFuncSig_CastIntAsInt)
	case types.ETReal:
		sig = &builtinCastRealAsIntSig{bf}
		sig.SetPbCode(tipb.ScalarFuncSig_CastRealAsInt)
	case types.ETDecimal:
		sig = &builtinCastDecimalAsIntSig{bf}
		sig.SetPbCode(tipb.ScalarFuncSig_CastDecimalAsInt)
	case types.ETDatetime, types.ETTimestamp:
		sig = &builtinCastTimeAsIntSig{bf}
		sig.SetPbCode(tipb.ScalarFuncSig_CastTimeAsInt)
	case types.ETDuration:
		sig = &builtinCastDurationAsIntSig{bf}
		sig.SetPbCode(tipb.ScalarFuncSig_CastDurationAsInt)
	case types.ETJson:
		sig = &builtinCastJSONAsIntSig{bf}
		sig.SetPbCode(tipb.ScalarFuncSig_CastJsonAsInt)
	case types.ETString:
		sig = &builtinCastStringAsIntSig{bf}
		sig.SetPbCode(tipb.ScalarFuncSig_CastStringAsInt)
	default:
		panic("unsupported types.EvalType in castAsIntFunctionClass")
	}
	return sig, nil
}

type castAsRealFunctionClass struct {
	BaseFunctionClass

	tp *types.FieldType
}

func (c *castAsRealFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (sig BuiltinFunc, err error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	bf := newBaseBuiltinCastFunc(newBaseBuiltinFunc(ctx, args), ctx.Value(inUnionCastContext) != nil)
	bf.Tp = c.tp
	if IsBinaryLiteral(args[0]) {
		sig = &builtinCastRealAsRealSig{bf}
		sig.SetPbCode(tipb.ScalarFuncSig_CastRealAsReal)
		return sig, nil
	}
	var argTp types.EvalType
	if args[0].GetType().Hybrid() {
		argTp = types.ETInt
	} else {
		argTp = args[0].GetType().EvalType()
	}
	switch argTp {
	case types.ETInt:
		sig = &builtinCastIntAsRealSig{bf}
		sig.SetPbCode(tipb.ScalarFuncSig_CastIntAsReal)
	case types.ETReal:
		sig = &builtinCastRealAsRealSig{bf}
		sig.SetPbCode(tipb.ScalarFuncSig_CastRealAsReal)
	case types.ETDecimal:
		sig = &builtinCastDecimalAsRealSig{bf}
		sig.SetPbCode(tipb.ScalarFuncSig_CastDecimalAsReal)
	case types.ETDatetime, types.ETTimestamp:
		sig = &builtinCastTimeAsRealSig{bf}
		sig.SetPbCode(tipb.ScalarFuncSig_CastTimeAsReal)
	case types.ETDuration:
		sig = &builtinCastDurationAsRealSig{bf}
		sig.SetPbCode(tipb.ScalarFuncSig_CastDurationAsReal)
	case types.ETJson:
		sig = &builtinCastJSONAsRealSig{bf}
		sig.SetPbCode(tipb.ScalarFuncSig_CastJsonAsReal)
	case types.ETString:
		sig = &builtinCastStringAsRealSig{bf}
		sig.SetPbCode(tipb.ScalarFuncSig_CastStringAsReal)
	default:
		panic("unsupported types.EvalType in castAsRealFunctionClass")
	}
	return sig, nil
}

type castAsDecimalFunctionClass struct {
	BaseFunctionClass

	tp *types.FieldType
}

func (c *castAsDecimalFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (sig BuiltinFunc, err error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	bf := newBaseBuiltinCastFunc(newBaseBuiltinFunc(ctx, args), ctx.Value(inUnionCastContext) != nil)
	bf.Tp = c.tp
	if IsBinaryLiteral(args[0]) {
		sig = &builtinCastDecimalAsDecimalSig{bf}
		sig.SetPbCode(tipb.ScalarFuncSig_CastDecimalAsDecimal)
		return sig, nil
	}
	var argTp types.EvalType
	if args[0].GetType().Hybrid() {
		argTp = types.ETInt
	} else {
		argTp = args[0].GetType().EvalType()
	}
	switch argTp {
	case types.ETInt:
		sig = &builtinCastIntAsDecimalSig{bf}
		sig.SetPbCode(tipb.ScalarFuncSig_CastIntAsDecimal)
	case types.ETReal:
		sig = &builtinCastRealAsDecimalSig{bf}
		sig.SetPbCode(tipb.ScalarFuncSig_CastRealAsDecimal)
	case types.ETDecimal:
		sig = &builtinCastDecimalAsDecimalSig{bf}
		sig.SetPbCode(tipb.ScalarFuncSig_CastDecimalAsDecimal)
	case types.ETDatetime, types.ETTimestamp:
		sig = &builtinCastTimeAsDecimalSig{bf}
		sig.SetPbCode(tipb.ScalarFuncSig_CastTimeAsDecimal)
	case types.ETDuration:
		sig = &builtinCastDurationAsDecimalSig{bf}
		sig.SetPbCode(tipb.ScalarFuncSig_CastDurationAsDecimal)
	case types.ETJson:
		sig = &builtinCastJSONAsDecimalSig{bf}
		sig.SetPbCode(tipb.ScalarFuncSig_CastJsonAsDecimal)
	case types.ETString:
		sig = &builtinCastStringAsDecimalSig{bf}
		sig.SetPbCode(tipb.ScalarFuncSig_CastStringAsDecimal)
	default:
		panic("unsupported types.EvalType in castAsDecimalFunctionClass")
	}
	return sig, nil
}

type castAsStringFunctionClass struct {
	BaseFunctionClass

	tp *types.FieldType
}

func (c *castAsStringFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (sig BuiltinFunc, err error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	bf := newBaseBuiltinFunc(ctx, args)
	bf.Tp = c.tp
	if args[0].GetType().Hybrid() || IsBinaryLiteral(args[0]) {
		sig = &builtinCastStringAsStringSig{bf}
		sig.SetPbCode(tipb.ScalarFuncSig_CastStringAsString)
		return sig, nil
	}
	argTp := args[0].GetType().EvalType()
	switch argTp {
	case types.ETInt:
		sig = &builtinCastIntAsStringSig{bf}
		sig.SetPbCode(tipb.ScalarFuncSig_CastIntAsString)
	case types.ETReal:
		sig = &builtinCastRealAsStringSig{bf}
		sig.SetPbCode(tipb.ScalarFuncSig_CastRealAsString)
	case types.ETDecimal:
		sig = &builtinCastDecimalAsStringSig{bf}
		sig.SetPbCode(tipb.ScalarFuncSig_CastDecimalAsString)
	case types.ETDatetime, types.ETTimestamp:
		sig = &builtinCastTimeAsStringSig{bf}
		sig.SetPbCode(tipb.ScalarFuncSig_CastTimeAsString)
	case types.ETDuration:
		sig = &builtinCastDurationAsStringSig{bf}
		sig.SetPbCode(tipb.ScalarFuncSig_CastDurationAsString)
	case types.ETJson:
		sig = &builtinCastJSONAsStringSig{bf}
		sig.SetPbCode(tipb.ScalarFuncSig_CastJsonAsString)
	case types.ETString:
		sig = &builtinCastStringAsStringSig{bf}
		sig.SetPbCode(tipb.ScalarFuncSig_CastStringAsString)
	default:
		panic("unsupported types.EvalType in castAsStringFunctionClass")
	}
	return sig, nil
}

type castAsTimeFunctionClass struct {
	BaseFunctionClass

	tp *types.FieldType
}

func (c *castAsTimeFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (sig BuiltinFunc, err error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	bf := newBaseBuiltinFunc(ctx, args)
	bf.Tp = c.tp
	argTp := args[0].GetType().EvalType()
	switch argTp {
	case types.ETInt:
		sig = &builtinCastIntAsTimeSig{bf}
		sig.SetPbCode(tipb.ScalarFuncSig_CastIntAsTime)
	case types.ETReal:
		sig = &builtinCastRealAsTimeSig{bf}
		sig.SetPbCode(tipb.ScalarFuncSig_CastRealAsTime)
	case types.ETDecimal:
		sig = &builtinCastDecimalAsTimeSig{bf}
		sig.SetPbCode(tipb.ScalarFuncSig_CastDecimalAsTime)
	case types.ETDatetime, types.ETTimestamp:
		sig = &builtinCastTimeAsTimeSig{bf}
		sig.SetPbCode(tipb.ScalarFuncSig_CastTimeAsTime)
	case types.ETDuration:
		sig = &builtinCastDurationAsTimeSig{bf}
		sig.SetPbCode(tipb.ScalarFuncSig_CastDurationAsTime)
	case types.ETJson:
		sig = &builtinCastJSONAsTimeSig{bf}
		sig.SetPbCode(tipb.ScalarFuncSig_CastJsonAsTime)
	case types.ETString:
		sig = &builtinCastStringAsTimeSig{bf}
		sig.SetPbCode(tipb.ScalarFuncSig_CastStringAsTime)
	default:
		panic("unsupported types.EvalType in castAsTimeFunctionClass")
	}
	return sig, nil
}

type castAsDurationFunctionClass struct {
	BaseFunctionClass

	tp *types.FieldType
}

func (c *castAsDurationFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (sig BuiltinFunc, err error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	bf := newBaseBuiltinFunc(ctx, args)
	bf.Tp = c.tp
	argTp := args[0].GetType().EvalType()
	switch argTp {
	case types.ETInt:
		sig = &builtinCastIntAsDurationSig{bf}
		sig.SetPbCode(tipb.ScalarFuncSig_CastIntAsDuration)
	case types.ETReal:
		sig = &builtinCastRealAsDurationSig{bf}
		sig.SetPbCode(tipb.ScalarFuncSig_CastRealAsDuration)
	case types.ETDecimal:
		sig = &builtinCastDecimalAsDurationSig{bf}
		sig.SetPbCode(tipb.ScalarFuncSig_CastDecimalAsDuration)
	case types.ETDatetime, types.ETTimestamp:
		sig = &builtinCastTimeAsDurationSig{bf}
		sig.SetPbCode(tipb.ScalarFuncSig_CastTimeAsDuration)
	case types.ETDuration:
		sig = &builtinCastDurationAsDurationSig{bf}
		sig.SetPbCode(tipb.ScalarFuncSig_CastDurationAsDuration)
	case types.ETJson:
		sig = &builtinCastJSONAsDurationSig{bf}
		sig.SetPbCode(tipb.ScalarFuncSig_CastJsonAsDuration)
	case types.ETString:
		sig = &builtinCastStringAsDurationSig{bf}
		sig.SetPbCode(tipb.ScalarFuncSig_CastStringAsDuration)
	default:
		panic("unsupported types.EvalType in castAsDurationFunctionClass")
	}
	return sig, nil
}

type castAsJSONFunctionClass struct {
	BaseFunctionClass

	tp *types.FieldType
}

func (c *castAsJSONFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (sig BuiltinFunc, err error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	bf := newBaseBuiltinFunc(ctx, args)
	bf.Tp = c.tp
	argTp := args[0].GetType().EvalType()
	switch argTp {
	case types.ETInt:
		sig = &builtinCastIntAsJSONSig{bf}
		sig.SetPbCode(tipb.ScalarFuncSig_CastIntAsJson)
	case types.ETReal:
		sig = &builtinCastRealAsJSONSig{bf}
		sig.SetPbCode(tipb.ScalarFuncSig_CastRealAsJson)
	case types.ETDecimal:
		sig = &builtinCastDecimalAsJSONSig{bf}
		sig.SetPbCode(tipb.ScalarFuncSig_CastDecimalAsJson)
	case types.ETDatetime, types.ETTimestamp:
		sig = &builtinCastTimeAsJSONSig{bf}
		sig.SetPbCode(tipb.ScalarFuncSig_CastTimeAsJson)
	case types.ETDuration:
		sig = &builtinCastDurationAsJSONSig{bf}
		sig.SetPbCode(tipb.ScalarFuncSig_CastDurationAsJson)
	case types.ETJson:
		sig = &builtinCastJSONAsJSONSig{bf}
		sig.SetPbCode(tipb.ScalarFuncSig_CastJsonAsJson)
	case types.ETString:
		sig = &builtinCastStringAsJSONSig{bf}
		sig.getRetTp().Flag |= mysql.ParseToJSONFlag
		sig.SetPbCode(tipb.ScalarFuncSig_CastStringAsJson)
	default:
		panic("unsupported types.EvalType in castAsJSONFunctionClass")
	}
	return sig, nil
}

type builtinCastIntAsIntSig struct {
	baseBuiltinCastFunc
}

func (b *builtinCastIntAsIntSig) Clone() BuiltinFunc {
	newSig := &builtinCastIntAsIntSig{}
	newSig.cloneFrom(&b.baseBuiltinCastFunc)
	return newSig
}

func (b *builtinCastIntAsIntSig) evalInt(row chunk.Row) (res int64, isNull bool, err error) {
	res, isNull, err = b.Args[0].EvalInt(b.Ctx, row)
	if isNull || err != nil {
		return
	}
	if b.inUnion && mysql.HasUnsignedFlag(b.Tp.Flag) && res < 0 {
		res = 0
	}
	return
}

type builtinCastIntAsRealSig struct {
	baseBuiltinCastFunc
}

func (b *builtinCastIntAsRealSig) Clone() BuiltinFunc {
	newSig := &builtinCastIntAsRealSig{}
	newSig.cloneFrom(&b.baseBuiltinCastFunc)
	return newSig
}

func (b *builtinCastIntAsRealSig) evalReal(row chunk.Row) (res float64, isNull bool, err error) {
	val, isNull, err := b.Args[0].EvalInt(b.Ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	if !mysql.HasUnsignedFlag(b.Tp.Flag) && !mysql.HasUnsignedFlag(b.Args[0].GetType().Flag) {
		res = float64(val)
	} else if b.inUnion && val < 0 {
		res = 0
	} else {
		// recall that, int to float is different from uint to float
		res = float64(uint64(val))
	}
	return res, false, err
}

type builtinCastIntAsDecimalSig struct {
	baseBuiltinCastFunc
}

func (b *builtinCastIntAsDecimalSig) Clone() BuiltinFunc {
	newSig := &builtinCastIntAsDecimalSig{}
	newSig.cloneFrom(&b.baseBuiltinCastFunc)
	return newSig
}

func (b *builtinCastIntAsDecimalSig) evalDecimal(row chunk.Row) (res *types.MyDecimal, isNull bool, err error) {
	val, isNull, err := b.Args[0].EvalInt(b.Ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	if !mysql.HasUnsignedFlag(b.Tp.Flag) && !mysql.HasUnsignedFlag(b.Args[0].GetType().Flag) {
		res = types.NewDecFromInt(val)
	} else if b.inUnion && val < 0 {
		res = &types.MyDecimal{}
	} else {
		res = types.NewDecFromUint(uint64(val))
	}
	res, err = types.ProduceDecWithSpecifiedTp(res, b.Tp, b.Ctx.GetSessionVars().StmtCtx)
	return res, isNull, err
}

type builtinCastIntAsStringSig struct {
	BaseBuiltinFunc
}

func (b *builtinCastIntAsStringSig) Clone() BuiltinFunc {
	newSig := &builtinCastIntAsStringSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinCastIntAsStringSig) EvalString(row chunk.Row) (res string, isNull bool, err error) {
	val, isNull, err := b.Args[0].EvalInt(b.Ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	if !mysql.HasUnsignedFlag(b.Args[0].GetType().Flag) {
		res = strconv.FormatInt(val, 10)
	} else {
		res = strconv.FormatUint(uint64(val), 10)
	}
	res, err = types.ProduceStrWithSpecifiedTp(res, b.Tp, b.Ctx.GetSessionVars().StmtCtx, false)
	if err != nil {
		return res, false, err
	}
	return padZeroForBinaryType(res, b.Tp, b.Ctx)
}

type builtinCastIntAsTimeSig struct {
	BaseBuiltinFunc
}

func (b *builtinCastIntAsTimeSig) Clone() BuiltinFunc {
	newSig := &builtinCastIntAsTimeSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinCastIntAsTimeSig) evalTime(row chunk.Row) (res types.Time, isNull bool, err error) {
	val, isNull, err := b.Args[0].EvalInt(b.Ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	res, err = types.ParseTimeFromNum(b.Ctx.GetSessionVars().StmtCtx, val, b.Tp.Tp, int8(b.Tp.Decimal))
	if err != nil {
		return types.Time{}, true, handleInvalidTimeError(b.Ctx, err)
	}
	if b.Tp.Tp == mysql.TypeDate {
		// Truncate hh:mm:ss part if the type is Date.
		res.Time = types.FromDate(res.Time.Year(), res.Time.Month(), res.Time.Day(), 0, 0, 0, 0)
	}
	return res, false, nil
}

type builtinCastIntAsDurationSig struct {
	BaseBuiltinFunc
}

func (b *builtinCastIntAsDurationSig) Clone() BuiltinFunc {
	newSig := &builtinCastIntAsDurationSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinCastIntAsDurationSig) evalDuration(row chunk.Row) (res types.Duration, isNull bool, err error) {
	val, isNull, err := b.Args[0].EvalInt(b.Ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	dur, err := types.NumberToDuration(val, int8(b.Tp.Decimal))
	if err != nil {
		if types.ErrOverflow.Equal(err) {
			err = b.Ctx.GetSessionVars().StmtCtx.HandleOverflow(err, err)
		}
		return res, true, err
	}
	return dur, false, err
}

type builtinCastIntAsJSONSig struct {
	BaseBuiltinFunc
}

func (b *builtinCastIntAsJSONSig) Clone() BuiltinFunc {
	newSig := &builtinCastIntAsJSONSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinCastIntAsJSONSig) evalJSON(row chunk.Row) (res json.BinaryJSON, isNull bool, err error) {
	val, isNull, err := b.Args[0].EvalInt(b.Ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	if mysql.HasIsBooleanFlag(b.Args[0].GetType().Flag) {
		res = json.CreateBinary(val != 0)
	} else if mysql.HasUnsignedFlag(b.Args[0].GetType().Flag) {
		res = json.CreateBinary(uint64(val))
	} else {
		res = json.CreateBinary(val)
	}
	return res, false, nil
}

type builtinCastRealAsJSONSig struct {
	BaseBuiltinFunc
}

func (b *builtinCastRealAsJSONSig) Clone() BuiltinFunc {
	newSig := &builtinCastRealAsJSONSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinCastRealAsJSONSig) evalJSON(row chunk.Row) (res json.BinaryJSON, isNull bool, err error) {
	val, isNull, err := b.Args[0].EvalReal(b.Ctx, row)
	// FIXME: `select json_type(cast(1111.11 as json))` should return `DECIMAL`, we return `DOUBLE` now.
	return json.CreateBinary(val), isNull, err
}

type builtinCastDecimalAsJSONSig struct {
	BaseBuiltinFunc
}

func (b *builtinCastDecimalAsJSONSig) Clone() BuiltinFunc {
	newSig := &builtinCastDecimalAsJSONSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinCastDecimalAsJSONSig) evalJSON(row chunk.Row) (json.BinaryJSON, bool, error) {
	val, isNull, err := b.Args[0].EvalDecimal(b.Ctx, row)
	if isNull || err != nil {
		return json.BinaryJSON{}, true, err
	}
	// FIXME: `select json_type(cast(1111.11 as json))` should return `DECIMAL`, we return `DOUBLE` now.
	f64, err := val.ToFloat64()
	if err != nil {
		return json.BinaryJSON{}, true, err
	}
	return json.CreateBinary(f64), isNull, err
}

type builtinCastStringAsJSONSig struct {
	BaseBuiltinFunc
}

func (b *builtinCastStringAsJSONSig) Clone() BuiltinFunc {
	newSig := &builtinCastStringAsJSONSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinCastStringAsJSONSig) evalJSON(row chunk.Row) (res json.BinaryJSON, isNull bool, err error) {
	val, isNull, err := b.Args[0].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	if mysql.HasParseToJSONFlag(b.Tp.Flag) {
		res, err = json.ParseBinaryFromString(val)
	} else {
		res = json.CreateBinary(val)
	}
	return res, false, err
}

type builtinCastDurationAsJSONSig struct {
	BaseBuiltinFunc
}

func (b *builtinCastDurationAsJSONSig) Clone() BuiltinFunc {
	newSig := &builtinCastDurationAsJSONSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinCastDurationAsJSONSig) evalJSON(row chunk.Row) (res json.BinaryJSON, isNull bool, err error) {
	val, isNull, err := b.Args[0].EvalDuration(b.Ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	val.Fsp = types.MaxFsp
	return json.CreateBinary(val.String()), false, nil
}

type builtinCastTimeAsJSONSig struct {
	BaseBuiltinFunc
}

func (b *builtinCastTimeAsJSONSig) Clone() BuiltinFunc {
	newSig := &builtinCastTimeAsJSONSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinCastTimeAsJSONSig) evalJSON(row chunk.Row) (res json.BinaryJSON, isNull bool, err error) {
	val, isNull, err := b.Args[0].EvalTime(b.Ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	if val.Type == mysql.TypeDatetime || val.Type == mysql.TypeTimestamp {
		val.Fsp = types.MaxFsp
	}
	return json.CreateBinary(val.String()), false, nil
}

type builtinCastRealAsRealSig struct {
	baseBuiltinCastFunc
}

func (b *builtinCastRealAsRealSig) Clone() BuiltinFunc {
	newSig := &builtinCastRealAsRealSig{}
	newSig.cloneFrom(&b.baseBuiltinCastFunc)
	return newSig
}

func (b *builtinCastRealAsRealSig) evalReal(row chunk.Row) (res float64, isNull bool, err error) {
	res, isNull, err = b.Args[0].EvalReal(b.Ctx, row)
	if b.inUnion && mysql.HasUnsignedFlag(b.Tp.Flag) && res < 0 {
		res = 0
	}
	return
}

type builtinCastRealAsIntSig struct {
	baseBuiltinCastFunc
}

func (b *builtinCastRealAsIntSig) Clone() BuiltinFunc {
	newSig := &builtinCastRealAsIntSig{}
	newSig.cloneFrom(&b.baseBuiltinCastFunc)
	return newSig
}

func (b *builtinCastRealAsIntSig) evalInt(row chunk.Row) (res int64, isNull bool, err error) {
	val, isNull, err := b.Args[0].EvalReal(b.Ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	if !mysql.HasUnsignedFlag(b.Tp.Flag) {
		res, err = types.ConvertFloatToInt(val, types.IntergerSignedLowerBound(mysql.TypeLonglong), types.IntergerSignedUpperBound(mysql.TypeLonglong), mysql.TypeLonglong)
	} else if b.inUnion && val < 0 {
		res = 0
	} else {
		var uintVal uint64
		sc := b.Ctx.GetSessionVars().StmtCtx
		uintVal, err = types.ConvertFloatToUint(sc, val, types.IntergerUnsignedUpperBound(mysql.TypeLonglong), mysql.TypeLonglong)
		res = int64(uintVal)
	}
	return res, isNull, err
}

type builtinCastRealAsDecimalSig struct {
	baseBuiltinCastFunc
}

func (b *builtinCastRealAsDecimalSig) Clone() BuiltinFunc {
	newSig := &builtinCastRealAsDecimalSig{}
	newSig.cloneFrom(&b.baseBuiltinCastFunc)
	return newSig
}

func (b *builtinCastRealAsDecimalSig) evalDecimal(row chunk.Row) (res *types.MyDecimal, isNull bool, err error) {
	val, isNull, err := b.Args[0].EvalReal(b.Ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	res = new(types.MyDecimal)
	if !b.inUnion || val >= 0 {
		err = res.FromFloat64(val)
		if err != nil {
			return res, false, err
		}
	}
	res, err = types.ProduceDecWithSpecifiedTp(res, b.Tp, b.Ctx.GetSessionVars().StmtCtx)
	return res, false, err
}

type builtinCastRealAsStringSig struct {
	BaseBuiltinFunc
}

func (b *builtinCastRealAsStringSig) Clone() BuiltinFunc {
	newSig := &builtinCastRealAsStringSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinCastRealAsStringSig) EvalString(row chunk.Row) (res string, isNull bool, err error) {
	val, isNull, err := b.Args[0].EvalReal(b.Ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}

	bits := 64
	if b.Args[0].GetType().Tp == mysql.TypeFloat {
		// b.args[0].EvalReal() casts the value from float32 to float64, for example:
		// float32(208.867) is cast to float64(208.86700439)
		// If we strconv.FormatFloat the value with 64bits, the result is incorrect!
		bits = 32
	}
	res, err = types.ProduceStrWithSpecifiedTp(strconv.FormatFloat(val, 'f', -1, bits), b.Tp, b.Ctx.GetSessionVars().StmtCtx, false)
	if err != nil {
		return res, false, err
	}
	return padZeroForBinaryType(res, b.Tp, b.Ctx)
}

type builtinCastRealAsTimeSig struct {
	BaseBuiltinFunc
}

func (b *builtinCastRealAsTimeSig) Clone() BuiltinFunc {
	newSig := &builtinCastRealAsTimeSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinCastRealAsTimeSig) evalTime(row chunk.Row) (types.Time, bool, error) {
	val, isNull, err := b.Args[0].EvalReal(b.Ctx, row)
	if isNull || err != nil {
		return types.Time{}, true, err
	}
	sc := b.Ctx.GetSessionVars().StmtCtx
	res, err := types.ParseTime(sc, strconv.FormatFloat(val, 'f', -1, 64), b.Tp.Tp, int8(b.Tp.Decimal))
	if err != nil {
		return types.Time{}, true, handleInvalidTimeError(b.Ctx, err)
	}
	if b.Tp.Tp == mysql.TypeDate {
		// Truncate hh:mm:ss part if the type is Date.
		res.Time = types.FromDate(res.Time.Year(), res.Time.Month(), res.Time.Day(), 0, 0, 0, 0)
	}
	return res, false, nil
}

type builtinCastRealAsDurationSig struct {
	BaseBuiltinFunc
}

func (b *builtinCastRealAsDurationSig) Clone() BuiltinFunc {
	newSig := &builtinCastRealAsDurationSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinCastRealAsDurationSig) evalDuration(row chunk.Row) (res types.Duration, isNull bool, err error) {
	val, isNull, err := b.Args[0].EvalReal(b.Ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	res, err = types.ParseDuration(b.Ctx.GetSessionVars().StmtCtx, strconv.FormatFloat(val, 'f', -1, 64), int8(b.Tp.Decimal))
	return res, false, err
}

type builtinCastDecimalAsDecimalSig struct {
	baseBuiltinCastFunc
}

func (b *builtinCastDecimalAsDecimalSig) Clone() BuiltinFunc {
	newSig := &builtinCastDecimalAsDecimalSig{}
	newSig.cloneFrom(&b.baseBuiltinCastFunc)
	return newSig
}

func (b *builtinCastDecimalAsDecimalSig) evalDecimal(row chunk.Row) (res *types.MyDecimal, isNull bool, err error) {
	evalDecimal, isNull, err := b.Args[0].EvalDecimal(b.Ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	res = &types.MyDecimal{}
	if !(b.inUnion && mysql.HasUnsignedFlag(b.Tp.Flag) && evalDecimal.IsNegative()) {
		*res = *evalDecimal
	}
	sc := b.Ctx.GetSessionVars().StmtCtx
	res, err = types.ProduceDecWithSpecifiedTp(res, b.Tp, sc)
	return res, false, err
}

type builtinCastDecimalAsIntSig struct {
	baseBuiltinCastFunc
}

func (b *builtinCastDecimalAsIntSig) Clone() BuiltinFunc {
	newSig := &builtinCastDecimalAsIntSig{}
	newSig.cloneFrom(&b.baseBuiltinCastFunc)
	return newSig
}

func (b *builtinCastDecimalAsIntSig) evalInt(row chunk.Row) (res int64, isNull bool, err error) {
	val, isNull, err := b.Args[0].EvalDecimal(b.Ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}

	// Round is needed for both unsigned and signed.
	var to types.MyDecimal
	err = val.Round(&to, 0, types.ModeHalfEven)
	if err != nil {
		return 0, true, err
	}

	if !mysql.HasUnsignedFlag(b.Tp.Flag) {
		res, err = to.ToInt()
	} else if b.inUnion && to.IsNegative() {
		res = 0
	} else {
		var uintRes uint64
		uintRes, err = to.ToUint()
		res = int64(uintRes)
	}

	if types.ErrOverflow.Equal(err) {
		warnErr := types.ErrTruncatedWrongVal.GenWithStackByArgs("DECIMAL", val)
		err = b.Ctx.GetSessionVars().StmtCtx.HandleOverflow(err, warnErr)
	}

	return res, false, err
}

type builtinCastDecimalAsStringSig struct {
	BaseBuiltinFunc
}

func (b *builtinCastDecimalAsStringSig) Clone() BuiltinFunc {
	newSig := &builtinCastDecimalAsStringSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinCastDecimalAsStringSig) EvalString(row chunk.Row) (res string, isNull bool, err error) {
	val, isNull, err := b.Args[0].EvalDecimal(b.Ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	sc := b.Ctx.GetSessionVars().StmtCtx
	res, err = types.ProduceStrWithSpecifiedTp(string(val.ToString()), b.Tp, sc, false)
	if err != nil {
		return res, false, err
	}
	return padZeroForBinaryType(res, b.Tp, b.Ctx)
}

type builtinCastDecimalAsRealSig struct {
	baseBuiltinCastFunc
}

func (b *builtinCastDecimalAsRealSig) Clone() BuiltinFunc {
	newSig := &builtinCastDecimalAsRealSig{}
	newSig.cloneFrom(&b.baseBuiltinCastFunc)
	return newSig
}

func (b *builtinCastDecimalAsRealSig) evalReal(row chunk.Row) (res float64, isNull bool, err error) {
	val, isNull, err := b.Args[0].EvalDecimal(b.Ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	if b.inUnion && mysql.HasUnsignedFlag(b.Tp.Flag) && val.IsNegative() {
		res = 0
	} else {
		res, err = val.ToFloat64()
	}
	return res, false, err
}

type builtinCastDecimalAsTimeSig struct {
	BaseBuiltinFunc
}

func (b *builtinCastDecimalAsTimeSig) Clone() BuiltinFunc {
	newSig := &builtinCastDecimalAsTimeSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinCastDecimalAsTimeSig) evalTime(row chunk.Row) (res types.Time, isNull bool, err error) {
	val, isNull, err := b.Args[0].EvalDecimal(b.Ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	sc := b.Ctx.GetSessionVars().StmtCtx
	res, err = types.ParseTimeFromFloatString(sc, string(val.ToString()), b.Tp.Tp, int8(b.Tp.Decimal))
	if err != nil {
		return types.Time{}, true, handleInvalidTimeError(b.Ctx, err)
	}
	if b.Tp.Tp == mysql.TypeDate {
		// Truncate hh:mm:ss part if the type is Date.
		res.Time = types.FromDate(res.Time.Year(), res.Time.Month(), res.Time.Day(), 0, 0, 0, 0)
	}
	return res, false, err
}

type builtinCastDecimalAsDurationSig struct {
	BaseBuiltinFunc
}

func (b *builtinCastDecimalAsDurationSig) Clone() BuiltinFunc {
	newSig := &builtinCastDecimalAsDurationSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinCastDecimalAsDurationSig) evalDuration(row chunk.Row) (res types.Duration, isNull bool, err error) {
	val, isNull, err := b.Args[0].EvalDecimal(b.Ctx, row)
	if isNull || err != nil {
		return res, true, err
	}
	res, err = types.ParseDuration(b.Ctx.GetSessionVars().StmtCtx, string(val.ToString()), int8(b.Tp.Decimal))
	if types.ErrTruncatedWrongVal.Equal(err) {
		err = b.Ctx.GetSessionVars().StmtCtx.HandleTruncate(err)
		// ZeroDuration of error ErrTruncatedWrongVal needs to be considered NULL.
		if res == types.ZeroDuration {
			return res, true, err
		}
	}
	return res, false, err
}

type builtinCastStringAsStringSig struct {
	BaseBuiltinFunc
}

func (b *builtinCastStringAsStringSig) Clone() BuiltinFunc {
	newSig := &builtinCastStringAsStringSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinCastStringAsStringSig) EvalString(row chunk.Row) (res string, isNull bool, err error) {
	res, isNull, err = b.Args[0].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	sc := b.Ctx.GetSessionVars().StmtCtx
	res, err = types.ProduceStrWithSpecifiedTp(res, b.Tp, sc, false)
	if err != nil {
		return res, false, err
	}
	return padZeroForBinaryType(res, b.Tp, b.Ctx)
}

type builtinCastStringAsIntSig struct {
	baseBuiltinCastFunc
}

func (b *builtinCastStringAsIntSig) Clone() BuiltinFunc {
	newSig := &builtinCastStringAsIntSig{}
	newSig.cloneFrom(&b.baseBuiltinCastFunc)
	return newSig
}

// handleOverflow handles the overflow caused by cast string as int,
// see https://dev.mysql.com/doc/refman/5.7/en/out-of-range-and-overflow.html.
// When an out-of-range value is assigned to an integer column, MySQL stores the value representing the corresponding endpoint of the column data type range. If it is in select statement, it will return the
// endpoint value with a warning.
func (b *builtinCastStringAsIntSig) handleOverflow(origRes int64, origStr string, origErr error, isNegative bool) (res int64, err error) {
	res, err = origRes, origErr
	if err == nil {
		return
	}

	sc := b.Ctx.GetSessionVars().StmtCtx
	if sc.InSelectStmt && types.ErrOverflow.Equal(origErr) {
		if isNegative {
			res = math.MinInt64
		} else {
			uval := uint64(math.MaxUint64)
			res = int64(uval)
		}
		warnErr := types.ErrTruncatedWrongVal.GenWithStackByArgs("INTEGER", origStr)
		err = sc.HandleOverflow(origErr, warnErr)
	}
	return
}

func (b *builtinCastStringAsIntSig) evalInt(row chunk.Row) (res int64, isNull bool, err error) {
	if b.Args[0].GetType().Hybrid() || IsBinaryLiteral(b.Args[0]) {
		return b.Args[0].EvalInt(b.Ctx, row)
	}
	val, isNull, err := b.Args[0].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}

	val = strings.TrimSpace(val)
	isNegative := false
	if len(val) > 1 && val[0] == '-' { // negative number
		isNegative = true
	}

	var ures uint64
	sc := b.Ctx.GetSessionVars().StmtCtx
	if !isNegative {
		ures, err = types.StrToUint(sc, val)
		res = int64(ures)

		if err == nil && !mysql.HasUnsignedFlag(b.Tp.Flag) && ures > uint64(math.MaxInt64) {
			sc.AppendWarning(types.ErrCastAsSignedOverflow)
		}
	} else if b.inUnion && mysql.HasUnsignedFlag(b.Tp.Flag) {
		res = 0
	} else {
		res, err = types.StrToInt(sc, val)
		if err == nil && mysql.HasUnsignedFlag(b.Tp.Flag) {
			// If overflow, don't append this warnings
			sc.AppendWarning(types.ErrCastNegIntAsUnsigned)
		}
	}

	res, err = b.handleOverflow(res, val, err, isNegative)
	return res, false, err
}

type builtinCastStringAsRealSig struct {
	baseBuiltinCastFunc
}

func (b *builtinCastStringAsRealSig) Clone() BuiltinFunc {
	newSig := &builtinCastStringAsRealSig{}
	newSig.cloneFrom(&b.baseBuiltinCastFunc)
	return newSig
}

func (b *builtinCastStringAsRealSig) evalReal(row chunk.Row) (res float64, isNull bool, err error) {
	if IsBinaryLiteral(b.Args[0]) {
		return b.Args[0].EvalReal(b.Ctx, row)
	}
	val, isNull, err := b.Args[0].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	sc := b.Ctx.GetSessionVars().StmtCtx
	res, err = types.StrToFloat(sc, val)
	if err != nil {
		return 0, false, err
	}
	if b.inUnion && mysql.HasUnsignedFlag(b.Tp.Flag) && res < 0 {
		res = 0
	}
	res, err = types.ProduceFloatWithSpecifiedTp(res, b.Tp, sc)
	return res, false, err
}

type builtinCastStringAsDecimalSig struct {
	baseBuiltinCastFunc
}

func (b *builtinCastStringAsDecimalSig) Clone() BuiltinFunc {
	newSig := &builtinCastStringAsDecimalSig{}
	newSig.cloneFrom(&b.baseBuiltinCastFunc)
	return newSig
}

func (b *builtinCastStringAsDecimalSig) evalDecimal(row chunk.Row) (res *types.MyDecimal, isNull bool, err error) {
	if IsBinaryLiteral(b.Args[0]) {
		return b.Args[0].EvalDecimal(b.Ctx, row)
	}
	val, isNull, err := b.Args[0].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	res = new(types.MyDecimal)
	sc := b.Ctx.GetSessionVars().StmtCtx
	if !(b.inUnion && mysql.HasUnsignedFlag(b.Tp.Flag) && res.IsNegative()) {
		err = sc.HandleTruncate(res.FromString([]byte(val)))
		if err != nil {
			return res, false, err
		}
	}
	res, err = types.ProduceDecWithSpecifiedTp(res, b.Tp, sc)
	return res, false, err
}

type builtinCastStringAsTimeSig struct {
	BaseBuiltinFunc
}

func (b *builtinCastStringAsTimeSig) Clone() BuiltinFunc {
	newSig := &builtinCastStringAsTimeSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinCastStringAsTimeSig) evalTime(row chunk.Row) (res types.Time, isNull bool, err error) {
	val, isNull, err := b.Args[0].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	sc := b.Ctx.GetSessionVars().StmtCtx
	res, err = types.ParseTime(sc, val, b.Tp.Tp, int8(b.Tp.Decimal))
	if err != nil {
		return types.Time{}, true, handleInvalidTimeError(b.Ctx, err)
	}
	if b.Tp.Tp == mysql.TypeDate {
		// Truncate hh:mm:ss part if the type is Date.
		res.Time = types.FromDate(res.Time.Year(), res.Time.Month(), res.Time.Day(), 0, 0, 0, 0)
	}
	return res, false, nil
}

type builtinCastStringAsDurationSig struct {
	BaseBuiltinFunc
}

func (b *builtinCastStringAsDurationSig) Clone() BuiltinFunc {
	newSig := &builtinCastStringAsDurationSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinCastStringAsDurationSig) evalDuration(row chunk.Row) (res types.Duration, isNull bool, err error) {
	val, isNull, err := b.Args[0].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	res, err = types.ParseDuration(b.Ctx.GetSessionVars().StmtCtx, val, int8(b.Tp.Decimal))
	if types.ErrTruncatedWrongVal.Equal(err) {
		sc := b.Ctx.GetSessionVars().StmtCtx
		err = sc.HandleTruncate(err)
		// ZeroDuration of error ErrTruncatedWrongVal needs to be considered NULL.
		if res == types.ZeroDuration {
			return res, true, err
		}
	}
	return res, false, err
}

type builtinCastTimeAsTimeSig struct {
	BaseBuiltinFunc
}

func (b *builtinCastTimeAsTimeSig) Clone() BuiltinFunc {
	newSig := &builtinCastTimeAsTimeSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinCastTimeAsTimeSig) evalTime(row chunk.Row) (res types.Time, isNull bool, err error) {
	res, isNull, err = b.Args[0].EvalTime(b.Ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}

	sc := b.Ctx.GetSessionVars().StmtCtx
	if res, err = res.Convert(sc, b.Tp.Tp); err != nil {
		return types.Time{}, true, handleInvalidTimeError(b.Ctx, err)
	}
	res, err = res.RoundFrac(sc, int8(b.Tp.Decimal))
	if b.Tp.Tp == mysql.TypeDate {
		// Truncate hh:mm:ss part if the type is Date.
		res.Time = types.FromDate(res.Time.Year(), res.Time.Month(), res.Time.Day(), 0, 0, 0, 0)
		res.Type = b.Tp.Tp
	}
	return res, false, err
}

type builtinCastTimeAsIntSig struct {
	baseBuiltinCastFunc
}

func (b *builtinCastTimeAsIntSig) Clone() BuiltinFunc {
	newSig := &builtinCastTimeAsIntSig{}
	newSig.cloneFrom(&b.baseBuiltinCastFunc)
	return newSig
}

func (b *builtinCastTimeAsIntSig) evalInt(row chunk.Row) (res int64, isNull bool, err error) {
	val, isNull, err := b.Args[0].EvalTime(b.Ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	sc := b.Ctx.GetSessionVars().StmtCtx
	t, err := val.RoundFrac(sc, types.DefaultFsp)
	if err != nil {
		return res, false, err
	}
	res, err = t.ToNumber().ToInt()
	return res, false, err
}

type builtinCastTimeAsRealSig struct {
	baseBuiltinCastFunc
}

func (b *builtinCastTimeAsRealSig) Clone() BuiltinFunc {
	newSig := &builtinCastTimeAsRealSig{}
	newSig.cloneFrom(&b.baseBuiltinCastFunc)
	return newSig
}

func (b *builtinCastTimeAsRealSig) evalReal(row chunk.Row) (res float64, isNull bool, err error) {
	val, isNull, err := b.Args[0].EvalTime(b.Ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	res, err = val.ToNumber().ToFloat64()
	return res, false, err
}

type builtinCastTimeAsDecimalSig struct {
	baseBuiltinCastFunc
}

func (b *builtinCastTimeAsDecimalSig) Clone() BuiltinFunc {
	newSig := &builtinCastTimeAsDecimalSig{}
	newSig.cloneFrom(&b.baseBuiltinCastFunc)
	return newSig
}

func (b *builtinCastTimeAsDecimalSig) evalDecimal(row chunk.Row) (res *types.MyDecimal, isNull bool, err error) {
	val, isNull, err := b.Args[0].EvalTime(b.Ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	sc := b.Ctx.GetSessionVars().StmtCtx
	res, err = types.ProduceDecWithSpecifiedTp(val.ToNumber(), b.Tp, sc)
	return res, false, err
}

type builtinCastTimeAsStringSig struct {
	BaseBuiltinFunc
}

func (b *builtinCastTimeAsStringSig) Clone() BuiltinFunc {
	newSig := &builtinCastTimeAsStringSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinCastTimeAsStringSig) EvalString(row chunk.Row) (res string, isNull bool, err error) {
	val, isNull, err := b.Args[0].EvalTime(b.Ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	sc := b.Ctx.GetSessionVars().StmtCtx
	res, err = types.ProduceStrWithSpecifiedTp(val.String(), b.Tp, sc, false)
	if err != nil {
		return res, false, err
	}
	return padZeroForBinaryType(res, b.Tp, b.Ctx)
}

type builtinCastTimeAsDurationSig struct {
	BaseBuiltinFunc
}

func (b *builtinCastTimeAsDurationSig) Clone() BuiltinFunc {
	newSig := &builtinCastTimeAsDurationSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinCastTimeAsDurationSig) evalDuration(row chunk.Row) (res types.Duration, isNull bool, err error) {
	val, isNull, err := b.Args[0].EvalTime(b.Ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	res, err = val.ConvertToDuration()
	if err != nil {
		return res, false, err
	}
	res, err = res.RoundFrac(int8(b.Tp.Decimal))
	return res, false, err
}

type builtinCastDurationAsDurationSig struct {
	BaseBuiltinFunc
}

func (b *builtinCastDurationAsDurationSig) Clone() BuiltinFunc {
	newSig := &builtinCastDurationAsDurationSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinCastDurationAsDurationSig) evalDuration(row chunk.Row) (res types.Duration, isNull bool, err error) {
	res, isNull, err = b.Args[0].EvalDuration(b.Ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	res, err = res.RoundFrac(int8(b.Tp.Decimal))
	return res, false, err
}

type builtinCastDurationAsIntSig struct {
	baseBuiltinCastFunc
}

func (b *builtinCastDurationAsIntSig) Clone() BuiltinFunc {
	newSig := &builtinCastDurationAsIntSig{}
	newSig.cloneFrom(&b.baseBuiltinCastFunc)
	return newSig
}

func (b *builtinCastDurationAsIntSig) evalInt(row chunk.Row) (res int64, isNull bool, err error) {
	val, isNull, err := b.Args[0].EvalDuration(b.Ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	dur, err := val.RoundFrac(types.DefaultFsp)
	if err != nil {
		return res, false, err
	}
	res, err = dur.ToNumber().ToInt()
	return res, false, err
}

type builtinCastDurationAsRealSig struct {
	baseBuiltinCastFunc
}

func (b *builtinCastDurationAsRealSig) Clone() BuiltinFunc {
	newSig := &builtinCastDurationAsRealSig{}
	newSig.cloneFrom(&b.baseBuiltinCastFunc)
	return newSig
}

func (b *builtinCastDurationAsRealSig) evalReal(row chunk.Row) (res float64, isNull bool, err error) {
	val, isNull, err := b.Args[0].EvalDuration(b.Ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	res, err = val.ToNumber().ToFloat64()
	return res, false, err
}

type builtinCastDurationAsDecimalSig struct {
	baseBuiltinCastFunc
}

func (b *builtinCastDurationAsDecimalSig) Clone() BuiltinFunc {
	newSig := &builtinCastDurationAsDecimalSig{}
	newSig.cloneFrom(&b.baseBuiltinCastFunc)
	return newSig
}

func (b *builtinCastDurationAsDecimalSig) evalDecimal(row chunk.Row) (res *types.MyDecimal, isNull bool, err error) {
	val, isNull, err := b.Args[0].EvalDuration(b.Ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	sc := b.Ctx.GetSessionVars().StmtCtx
	res, err = types.ProduceDecWithSpecifiedTp(val.ToNumber(), b.Tp, sc)
	return res, false, err
}

type builtinCastDurationAsStringSig struct {
	BaseBuiltinFunc
}

func (b *builtinCastDurationAsStringSig) Clone() BuiltinFunc {
	newSig := &builtinCastDurationAsStringSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinCastDurationAsStringSig) EvalString(row chunk.Row) (res string, isNull bool, err error) {
	val, isNull, err := b.Args[0].EvalDuration(b.Ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	sc := b.Ctx.GetSessionVars().StmtCtx
	res, err = types.ProduceStrWithSpecifiedTp(val.String(), b.Tp, sc, false)
	if err != nil {
		return res, false, err
	}
	return padZeroForBinaryType(res, b.Tp, b.Ctx)
}

func padZeroForBinaryType(s string, tp *types.FieldType, ctx sessionctx.Context) (string, bool, error) {
	flen := tp.Flen
	if tp.Tp == mysql.TypeString && types.IsBinaryStr(tp) && len(s) < flen {
		sc := ctx.GetSessionVars().StmtCtx
		valStr, _ := ctx.GetSessionVars().GetSystemVar(variable.MaxAllowedPacket)
		maxAllowedPacket, err := strconv.ParseUint(valStr, 10, 64)
		if err != nil {
			return "", false, err
		}
		if uint64(flen) > maxAllowedPacket {
			sc.AppendWarning(errWarnAllowedPacketOverflowed.GenWithStackByArgs("cast_as_binary", maxAllowedPacket))
			return "", true, nil
		}
		padding := make([]byte, flen-len(s))
		s = string(append([]byte(s), padding...))
	}
	return s, false, nil
}

type builtinCastDurationAsTimeSig struct {
	BaseBuiltinFunc
}

func (b *builtinCastDurationAsTimeSig) Clone() BuiltinFunc {
	newSig := &builtinCastDurationAsTimeSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinCastDurationAsTimeSig) evalTime(row chunk.Row) (res types.Time, isNull bool, err error) {
	val, isNull, err := b.Args[0].EvalDuration(b.Ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	sc := b.Ctx.GetSessionVars().StmtCtx
	res, err = val.ConvertToTime(sc, b.Tp.Tp)
	if err != nil {
		return types.Time{}, true, handleInvalidTimeError(b.Ctx, err)
	}
	res, err = res.RoundFrac(sc, int8(b.Tp.Decimal))
	return res, false, err
}

type builtinCastJSONAsJSONSig struct {
	BaseBuiltinFunc
}

func (b *builtinCastJSONAsJSONSig) Clone() BuiltinFunc {
	newSig := &builtinCastJSONAsJSONSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinCastJSONAsJSONSig) evalJSON(row chunk.Row) (res json.BinaryJSON, isNull bool, err error) {
	return b.Args[0].EvalJSON(b.Ctx, row)
}

type builtinCastJSONAsIntSig struct {
	baseBuiltinCastFunc
}

func (b *builtinCastJSONAsIntSig) Clone() BuiltinFunc {
	newSig := &builtinCastJSONAsIntSig{}
	newSig.cloneFrom(&b.baseBuiltinCastFunc)
	return newSig
}

func (b *builtinCastJSONAsIntSig) evalInt(row chunk.Row) (res int64, isNull bool, err error) {
	val, isNull, err := b.Args[0].EvalJSON(b.Ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	sc := b.Ctx.GetSessionVars().StmtCtx
	res, err = types.ConvertJSONToInt(sc, val, mysql.HasUnsignedFlag(b.Tp.Flag))
	return
}

type builtinCastJSONAsRealSig struct {
	baseBuiltinCastFunc
}

func (b *builtinCastJSONAsRealSig) Clone() BuiltinFunc {
	newSig := &builtinCastJSONAsRealSig{}
	newSig.cloneFrom(&b.baseBuiltinCastFunc)
	return newSig
}

func (b *builtinCastJSONAsRealSig) evalReal(row chunk.Row) (res float64, isNull bool, err error) {
	val, isNull, err := b.Args[0].EvalJSON(b.Ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	sc := b.Ctx.GetSessionVars().StmtCtx
	res, err = types.ConvertJSONToFloat(sc, val)
	return
}

type builtinCastJSONAsDecimalSig struct {
	baseBuiltinCastFunc
}

func (b *builtinCastJSONAsDecimalSig) Clone() BuiltinFunc {
	newSig := &builtinCastJSONAsDecimalSig{}
	newSig.cloneFrom(&b.baseBuiltinCastFunc)
	return newSig
}

func (b *builtinCastJSONAsDecimalSig) evalDecimal(row chunk.Row) (res *types.MyDecimal, isNull bool, err error) {
	val, isNull, err := b.Args[0].EvalJSON(b.Ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	sc := b.Ctx.GetSessionVars().StmtCtx
	res, err = types.ConvertJSONToDecimal(sc, val)
	if err != nil {
		return res, false, err
	}
	res, err = types.ProduceDecWithSpecifiedTp(res, b.Tp, sc)
	return res, false, err
}

type builtinCastJSONAsStringSig struct {
	BaseBuiltinFunc
}

func (b *builtinCastJSONAsStringSig) Clone() BuiltinFunc {
	newSig := &builtinCastJSONAsStringSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinCastJSONAsStringSig) EvalString(row chunk.Row) (res string, isNull bool, err error) {
	val, isNull, err := b.Args[0].EvalJSON(b.Ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	return val.String(), false, nil
}

type builtinCastJSONAsTimeSig struct {
	BaseBuiltinFunc
}

func (b *builtinCastJSONAsTimeSig) Clone() BuiltinFunc {
	newSig := &builtinCastJSONAsTimeSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinCastJSONAsTimeSig) evalTime(row chunk.Row) (res types.Time, isNull bool, err error) {
	val, isNull, err := b.Args[0].EvalJSON(b.Ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	s, err := val.Unquote()
	if err != nil {
		return res, false, err
	}
	sc := b.Ctx.GetSessionVars().StmtCtx
	res, err = types.ParseTime(sc, s, b.Tp.Tp, int8(b.Tp.Decimal))
	if err != nil {
		return types.Time{}, true, handleInvalidTimeError(b.Ctx, err)
	}
	if b.Tp.Tp == mysql.TypeDate {
		// Truncate hh:mm:ss part if the type is Date.
		res.Time = types.FromDate(res.Time.Year(), res.Time.Month(), res.Time.Day(), 0, 0, 0, 0)
	}
	return
}

type builtinCastJSONAsDurationSig struct {
	BaseBuiltinFunc
}

func (b *builtinCastJSONAsDurationSig) Clone() BuiltinFunc {
	newSig := &builtinCastJSONAsDurationSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinCastJSONAsDurationSig) evalDuration(row chunk.Row) (res types.Duration, isNull bool, err error) {
	val, isNull, err := b.Args[0].EvalJSON(b.Ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	s, err := val.Unquote()
	if err != nil {
		return res, false, err
	}
	res, err = types.ParseDuration(b.Ctx.GetSessionVars().StmtCtx, s, int8(b.Tp.Decimal))
	if types.ErrTruncatedWrongVal.Equal(err) {
		sc := b.Ctx.GetSessionVars().StmtCtx
		err = sc.HandleTruncate(err)
	}
	return
}

// inCastContext is session key type that indicates whether executing
// in special cast context that negative unsigned num will be zero.
type inCastContext int

func (i inCastContext) String() string {
	return "__cast_ctx"
}

// inUnionCastContext is session key value that indicates whether executing in
// union cast context.
// @see BuildCastFunction4Union
const inUnionCastContext inCastContext = 0

// hasSpecialCast checks if this expr has its own special cast function.
// for example(#9713): when doing arithmetic using results of function DayName,
// "Monday" should be regarded as 0, "Tuesday" should be regarded as 1 and so on.
func hasSpecialCast(ctx sessionctx.Context, expr Expression, tp *types.FieldType) bool {
	switch f := expr.(type) {
	case *ScalarFunction:
		switch f.FuncName.L {
		case ast.DayName:
			switch tp.EvalType() {
			case types.ETInt, types.ETReal:
				return true
			}
		}
	}
	return false
}

// BuildCastFunction4Union build a implicitly CAST ScalarFunction from the Union
// Expression.
func BuildCastFunction4Union(ctx sessionctx.Context, expr Expression, tp *types.FieldType) (res Expression) {
	ctx.SetValue(inUnionCastContext, struct{}{})
	defer func() {
		ctx.SetValue(inUnionCastContext, nil)
	}()
	return BuildCastFunction(ctx, expr, tp)
}

// BuildCastFunction builds a CAST ScalarFunction from the Expression.
func BuildCastFunction(ctx sessionctx.Context, expr Expression, tp *types.FieldType) (res Expression) {
	if hasSpecialCast(ctx, expr, tp) {
		return expr
	}

	var fc FunctionClass
	switch tp.EvalType() {
	case types.ETInt:
		fc = &castAsIntFunctionClass{BaseFunctionClass{ast.Cast, 1, 1}, tp}
	case types.ETDecimal:
		fc = &castAsDecimalFunctionClass{BaseFunctionClass{ast.Cast, 1, 1}, tp}
	case types.ETReal:
		fc = &castAsRealFunctionClass{BaseFunctionClass{ast.Cast, 1, 1}, tp}
	case types.ETDatetime, types.ETTimestamp:
		fc = &castAsTimeFunctionClass{BaseFunctionClass{ast.Cast, 1, 1}, tp}
	case types.ETDuration:
		fc = &castAsDurationFunctionClass{BaseFunctionClass{ast.Cast, 1, 1}, tp}
	case types.ETJson:
		fc = &castAsJSONFunctionClass{BaseFunctionClass{ast.Cast, 1, 1}, tp}
	case types.ETString:
		fc = &castAsStringFunctionClass{BaseFunctionClass{ast.Cast, 1, 1}, tp}
	}
	f, err := fc.GetFunction(ctx, []Expression{expr})
	terror.Log(err)
	res = &ScalarFunction{
		FuncName: model.NewCIStr(ast.Cast),
		RetType:  tp,
		Function: f,
	}
	// We do not fold CAST if the eval type of this scalar function is ETJson
	// since we may reset the flag of the field type of CastAsJson later which
	// would affect the evaluation of it.
	if tp.EvalType() != types.ETJson {
		res = FoldConstant(res)
	}
	return res
}

// WrapWithCastAsInt wraps `expr` with `cast` if the return type of expr is not
// type int, otherwise, returns `expr` directly.
func WrapWithCastAsInt(ctx sessionctx.Context, expr Expression) Expression {
	if expr.GetType().EvalType() == types.ETInt {
		return expr
	}
	tp := types.NewFieldType(mysql.TypeLonglong)
	tp.Flen, tp.Decimal = expr.GetType().Flen, 0
	types.SetBinChsClnFlag(tp)
	tp.Flag |= expr.GetType().Flag & mysql.UnsignedFlag
	return BuildCastFunction(ctx, expr, tp)
}

// WrapWithCastAsReal wraps `expr` with `cast` if the return type of expr is not
// type real, otherwise, returns `expr` directly.
func WrapWithCastAsReal(ctx sessionctx.Context, expr Expression) Expression {
	if expr.GetType().EvalType() == types.ETReal {
		return expr
	}
	tp := types.NewFieldType(mysql.TypeDouble)
	tp.Flen, tp.Decimal = mysql.MaxRealWidth, types.UnspecifiedLength
	types.SetBinChsClnFlag(tp)
	tp.Flag |= expr.GetType().Flag & mysql.UnsignedFlag
	return BuildCastFunction(ctx, expr, tp)
}

// WrapWithCastAsDecimal wraps `expr` with `cast` if the return type of expr is
// not type decimal, otherwise, returns `expr` directly.
func WrapWithCastAsDecimal(ctx sessionctx.Context, expr Expression) Expression {
	if expr.GetType().EvalType() == types.ETDecimal {
		return expr
	}
	tp := types.NewFieldType(mysql.TypeNewDecimal)
	tp.Flen, tp.Decimal = expr.GetType().Flen, expr.GetType().Decimal
	if expr.GetType().EvalType() == types.ETInt {
		tp.Flen = mysql.MaxIntWidth
	}
	types.SetBinChsClnFlag(tp)
	tp.Flag |= expr.GetType().Flag & mysql.UnsignedFlag
	return BuildCastFunction(ctx, expr, tp)
}

// WrapWithCastAsString wraps `expr` with `cast` if the return type of expr is
// not type string, otherwise, returns `expr` directly.
func WrapWithCastAsString(ctx sessionctx.Context, expr Expression) Expression {
	exprTp := expr.GetType()
	if exprTp.EvalType() == types.ETString {
		return expr
	}
	argLen := exprTp.Flen
	// If expr is decimal, we should take the decimal point and negative sign
	// into consideration, so we set `expr.GetType().Flen + 2` as the `argLen`.
	// Since the length of float and double is not accurate, we do not handle
	// them.
	if exprTp.Tp == mysql.TypeNewDecimal && argLen != int(types.UnspecifiedFsp) {
		argLen += 2
	}
	if exprTp.EvalType() == types.ETInt {
		argLen = mysql.MaxIntWidth
	}
	tp := types.NewFieldType(mysql.TypeVarString)
	tp.Charset, tp.Collate = charset.GetDefaultCharsetAndCollate()
	tp.Flen, tp.Decimal = argLen, types.UnspecifiedLength
	return BuildCastFunction(ctx, expr, tp)
}

// WrapWithCastAsTime wraps `expr` with `cast` if the return type of expr is not
// same as type of the specified `tp` , otherwise, returns `expr` directly.
func WrapWithCastAsTime(ctx sessionctx.Context, expr Expression, tp *types.FieldType) Expression {
	exprTp := expr.GetType().Tp
	if tp.Tp == exprTp {
		return expr
	} else if (exprTp == mysql.TypeDate || exprTp == mysql.TypeTimestamp) && tp.Tp == mysql.TypeDatetime {
		return expr
	}
	switch x := expr.GetType(); x.Tp {
	case mysql.TypeDatetime, mysql.TypeTimestamp, mysql.TypeDate, mysql.TypeDuration:
		tp.Decimal = x.Decimal
	default:
		tp.Decimal = int(types.MaxFsp)
	}
	switch tp.Tp {
	case mysql.TypeDate:
		tp.Flen = mysql.MaxDateWidth
	case mysql.TypeDatetime, mysql.TypeTimestamp:
		tp.Flen = mysql.MaxDatetimeWidthNoFsp
		if tp.Decimal > 0 {
			tp.Flen = tp.Flen + 1 + tp.Decimal
		}
	}
	types.SetBinChsClnFlag(tp)
	return BuildCastFunction(ctx, expr, tp)
}

// WrapWithCastAsDuration wraps `expr` with `cast` if the return type of expr is
// not type duration, otherwise, returns `expr` directly.
func WrapWithCastAsDuration(ctx sessionctx.Context, expr Expression) Expression {
	if expr.GetType().Tp == mysql.TypeDuration {
		return expr
	}
	tp := types.NewFieldType(mysql.TypeDuration)
	switch x := expr.GetType(); x.Tp {
	case mysql.TypeDatetime, mysql.TypeTimestamp, mysql.TypeDate:
		tp.Decimal = x.Decimal
	default:
		tp.Decimal = int(types.MaxFsp)
	}
	tp.Flen = mysql.MaxDurationWidthNoFsp
	if tp.Decimal > 0 {
		tp.Flen = tp.Flen + 1 + tp.Decimal
	}
	return BuildCastFunction(ctx, expr, tp)
}

// WrapWithCastAsJSON wraps `expr` with `cast` if the return type of expr is not
// type json, otherwise, returns `expr` directly.
func WrapWithCastAsJSON(ctx sessionctx.Context, expr Expression) Expression {
	if expr.GetType().Tp == mysql.TypeJSON && !mysql.HasParseToJSONFlag(expr.GetType().Flag) {
		return expr
	}
	tp := &types.FieldType{
		Tp:      mysql.TypeJSON,
		Flen:    12582912, // FIXME: Here the Flen is not trusted.
		Decimal: 0,
		Charset: mysql.DefaultCharset,
		Collate: mysql.DefaultCollationName,
		Flag:    mysql.BinaryFlag,
	}
	return BuildCastFunction(ctx, expr, tp)
}
