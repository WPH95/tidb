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

package expression

import (
	"math"

	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/opcode"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/types/json"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tipb/go-tipb"
)

var (
	_ FunctionClass = &coalesceFunctionClass{}
	_ FunctionClass = &greatestFunctionClass{}
	_ FunctionClass = &leastFunctionClass{}
	_ FunctionClass = &intervalFunctionClass{}
	_ FunctionClass = &compareFunctionClass{}
)

var (
	_ BuiltinFunc = &builtinCoalesceIntSig{}
	_ BuiltinFunc = &builtinCoalesceRealSig{}
	_ BuiltinFunc = &builtinCoalesceDecimalSig{}
	_ BuiltinFunc = &builtinCoalesceStringSig{}
	_ BuiltinFunc = &builtinCoalesceTimeSig{}
	_ BuiltinFunc = &builtinCoalesceDurationSig{}

	_ BuiltinFunc = &builtinGreatestIntSig{}
	_ BuiltinFunc = &builtinGreatestRealSig{}
	_ BuiltinFunc = &builtinGreatestDecimalSig{}
	_ BuiltinFunc = &builtinGreatestStringSig{}
	_ BuiltinFunc = &builtinGreatestTimeSig{}
	_ BuiltinFunc = &builtinLeastIntSig{}
	_ BuiltinFunc = &builtinLeastRealSig{}
	_ BuiltinFunc = &builtinLeastDecimalSig{}
	_ BuiltinFunc = &builtinLeastStringSig{}
	_ BuiltinFunc = &builtinLeastTimeSig{}
	_ BuiltinFunc = &builtinIntervalIntSig{}
	_ BuiltinFunc = &builtinIntervalRealSig{}

	_ BuiltinFunc = &builtinLTIntSig{}
	_ BuiltinFunc = &builtinLTRealSig{}
	_ BuiltinFunc = &builtinLTDecimalSig{}
	_ BuiltinFunc = &builtinLTStringSig{}
	_ BuiltinFunc = &builtinLTDurationSig{}
	_ BuiltinFunc = &builtinLTTimeSig{}

	_ BuiltinFunc = &builtinLEIntSig{}
	_ BuiltinFunc = &builtinLERealSig{}
	_ BuiltinFunc = &builtinLEDecimalSig{}
	_ BuiltinFunc = &builtinLEStringSig{}
	_ BuiltinFunc = &builtinLEDurationSig{}
	_ BuiltinFunc = &builtinLETimeSig{}

	_ BuiltinFunc = &builtinGTIntSig{}
	_ BuiltinFunc = &builtinGTRealSig{}
	_ BuiltinFunc = &builtinGTDecimalSig{}
	_ BuiltinFunc = &builtinGTStringSig{}
	_ BuiltinFunc = &builtinGTTimeSig{}
	_ BuiltinFunc = &builtinGTDurationSig{}

	_ BuiltinFunc = &builtinGEIntSig{}
	_ BuiltinFunc = &builtinGERealSig{}
	_ BuiltinFunc = &builtinGEDecimalSig{}
	_ BuiltinFunc = &builtinGEStringSig{}
	_ BuiltinFunc = &builtinGETimeSig{}
	_ BuiltinFunc = &builtinGEDurationSig{}

	_ BuiltinFunc = &builtinNEIntSig{}
	_ BuiltinFunc = &builtinNERealSig{}
	_ BuiltinFunc = &builtinNEDecimalSig{}
	_ BuiltinFunc = &builtinNEStringSig{}
	_ BuiltinFunc = &builtinNETimeSig{}
	_ BuiltinFunc = &builtinNEDurationSig{}

	_ BuiltinFunc = &builtinNullEQIntSig{}
	_ BuiltinFunc = &builtinNullEQRealSig{}
	_ BuiltinFunc = &builtinNullEQDecimalSig{}
	_ BuiltinFunc = &builtinNullEQStringSig{}
	_ BuiltinFunc = &builtinNullEQTimeSig{}
	_ BuiltinFunc = &builtinNullEQDurationSig{}
)

// coalesceFunctionClass returns the first non-NULL value in the list,
// or NULL if there are no non-NULL values.
type coalesceFunctionClass struct {
	BaseFunctionClass
}

func (c *coalesceFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (sig BuiltinFunc, err error) {
	if err = c.VerifyArgs(args); err != nil {
		return nil, err
	}

	fieldTps := make([]*types.FieldType, 0, len(args))
	for _, arg := range args {
		fieldTps = append(fieldTps, arg.GetType())
	}

	// Use the aggregated field type as retType.
	resultFieldType := types.AggFieldType(fieldTps)
	resultEvalType := types.AggregateEvalType(fieldTps, &resultFieldType.Flag)
	retEvalTp := resultFieldType.EvalType()

	fieldEvalTps := make([]types.EvalType, 0, len(args))
	for range args {
		fieldEvalTps = append(fieldEvalTps, retEvalTp)
	}

	bf := NewBaseBuiltinFuncWithTp(ctx, args, retEvalTp, fieldEvalTps...)

	bf.Tp.Flag |= resultFieldType.Flag
	resultFieldType.Flen, resultFieldType.Decimal = 0, types.UnspecifiedLength

	// Set retType to BINARY(0) if all arguments are of type NULL.
	if resultFieldType.Tp == mysql.TypeNull {
		types.SetBinChsClnFlag(bf.Tp)
	} else {
		maxIntLen := 0
		maxFlen := 0

		// Find the max length of field in `maxFlen`,
		// and max integer-part length in `maxIntLen`.
		for _, argTp := range fieldTps {
			if argTp.Decimal > resultFieldType.Decimal {
				resultFieldType.Decimal = argTp.Decimal
			}
			argIntLen := argTp.Flen
			if argTp.Decimal > 0 {
				argIntLen -= argTp.Decimal + 1
			}

			// Reduce the sign bit if it is a signed integer/decimal
			if !mysql.HasUnsignedFlag(argTp.Flag) {
				argIntLen--
			}
			if argIntLen > maxIntLen {
				maxIntLen = argIntLen
			}
			if argTp.Flen > maxFlen || argTp.Flen == types.UnspecifiedLength {
				maxFlen = argTp.Flen
			}
		}
		// For integer, field length = maxIntLen + (1/0 for sign bit)
		// For decimal, field length = maxIntLen + maxDecimal + (1/0 for sign bit)
		if resultEvalType == types.ETInt || resultEvalType == types.ETDecimal {
			resultFieldType.Flen = maxIntLen + resultFieldType.Decimal
			if resultFieldType.Decimal > 0 {
				resultFieldType.Flen++
			}
			if !mysql.HasUnsignedFlag(resultFieldType.Flag) {
				resultFieldType.Flen++
			}
			bf.Tp = resultFieldType
		} else {
			bf.Tp.Flen = maxFlen
		}
		// Set the field length to maxFlen for other types.
		if bf.Tp.Flen > mysql.MaxDecimalWidth {
			bf.Tp.Flen = mysql.MaxDecimalWidth
		}
	}

	switch retEvalTp {
	case types.ETInt:
		sig = &builtinCoalesceIntSig{bf}
		sig.SetPbCode(tipb.ScalarFuncSig_CoalesceInt)
	case types.ETReal:
		sig = &builtinCoalesceRealSig{bf}
		sig.SetPbCode(tipb.ScalarFuncSig_CoalesceReal)
	case types.ETDecimal:
		sig = &builtinCoalesceDecimalSig{bf}
		sig.SetPbCode(tipb.ScalarFuncSig_CoalesceDecimal)
	case types.ETString:
		sig = &builtinCoalesceStringSig{bf}
		sig.SetPbCode(tipb.ScalarFuncSig_CoalesceString)
	case types.ETDatetime, types.ETTimestamp:
		sig = &builtinCoalesceTimeSig{bf}
		sig.SetPbCode(tipb.ScalarFuncSig_CoalesceTime)
	case types.ETDuration:
		bf.Tp.Decimal, err = getExpressionFsp(ctx, args[0])
		if err != nil {
			return nil, err
		}
		sig = &builtinCoalesceDurationSig{bf}
		sig.SetPbCode(tipb.ScalarFuncSig_CoalesceDuration)
	case types.ETJson:
		sig = &builtinCoalesceJSONSig{bf}
		sig.SetPbCode(tipb.ScalarFuncSig_CoalesceJson)
	}

	return sig, nil
}

// builtinCoalesceIntSig is buitin function coalesce signature which return type int
// See http://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#function_coalesce
type builtinCoalesceIntSig struct {
	BaseBuiltinFunc
}

func (b *builtinCoalesceIntSig) Clone() BuiltinFunc {
	newSig := &builtinCoalesceIntSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinCoalesceIntSig) evalInt(row chunk.Row) (res int64, isNull bool, err error) {
	for _, a := range b.getArgs() {
		res, isNull, err = a.EvalInt(b.Ctx, row)
		if err != nil || !isNull {
			break
		}
	}
	return res, isNull, err
}

// builtinCoalesceRealSig is buitin function coalesce signature which return type real
// See http://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#function_coalesce
type builtinCoalesceRealSig struct {
	BaseBuiltinFunc
}

func (b *builtinCoalesceRealSig) Clone() BuiltinFunc {
	newSig := &builtinCoalesceRealSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinCoalesceRealSig) evalReal(row chunk.Row) (res float64, isNull bool, err error) {
	for _, a := range b.getArgs() {
		res, isNull, err = a.EvalReal(b.Ctx, row)
		if err != nil || !isNull {
			break
		}
	}
	return res, isNull, err
}

// builtinCoalesceDecimalSig is buitin function coalesce signature which return type Decimal
// See http://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#function_coalesce
type builtinCoalesceDecimalSig struct {
	BaseBuiltinFunc
}

func (b *builtinCoalesceDecimalSig) Clone() BuiltinFunc {
	newSig := &builtinCoalesceDecimalSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinCoalesceDecimalSig) evalDecimal(row chunk.Row) (res *types.MyDecimal, isNull bool, err error) {
	for _, a := range b.getArgs() {
		res, isNull, err = a.EvalDecimal(b.Ctx, row)
		if err != nil || !isNull {
			break
		}
	}
	return res, isNull, err
}

// builtinCoalesceStringSig is buitin function coalesce signature which return type string
// See http://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#function_coalesce
type builtinCoalesceStringSig struct {
	BaseBuiltinFunc
}

func (b *builtinCoalesceStringSig) Clone() BuiltinFunc {
	newSig := &builtinCoalesceStringSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinCoalesceStringSig) EvalString(row chunk.Row) (res string, isNull bool, err error) {
	for _, a := range b.getArgs() {
		res, isNull, err = a.EvalString(b.Ctx, row)
		if err != nil || !isNull {
			break
		}
	}
	return res, isNull, err
}

// builtinCoalesceTimeSig is buitin function coalesce signature which return type time
// See http://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#function_coalesce
type builtinCoalesceTimeSig struct {
	BaseBuiltinFunc
}

func (b *builtinCoalesceTimeSig) Clone() BuiltinFunc {
	newSig := &builtinCoalesceTimeSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinCoalesceTimeSig) evalTime(row chunk.Row) (res types.Time, isNull bool, err error) {
	for _, a := range b.getArgs() {
		res, isNull, err = a.EvalTime(b.Ctx, row)
		if err != nil || !isNull {
			break
		}
	}
	return res, isNull, err
}

// builtinCoalesceDurationSig is buitin function coalesce signature which return type duration
// See http://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#function_coalesce
type builtinCoalesceDurationSig struct {
	BaseBuiltinFunc
}

func (b *builtinCoalesceDurationSig) Clone() BuiltinFunc {
	newSig := &builtinCoalesceDurationSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinCoalesceDurationSig) evalDuration(row chunk.Row) (res types.Duration, isNull bool, err error) {
	for _, a := range b.getArgs() {
		res, isNull, err = a.EvalDuration(b.Ctx, row)
		if err != nil || !isNull {
			break
		}
	}
	return res, isNull, err
}

// builtinCoalesceJSONSig is buitin function coalesce signature which return type json.
// See http://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#function_coalesce
type builtinCoalesceJSONSig struct {
	BaseBuiltinFunc
}

func (b *builtinCoalesceJSONSig) Clone() BuiltinFunc {
	newSig := &builtinCoalesceJSONSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinCoalesceJSONSig) evalJSON(row chunk.Row) (res json.BinaryJSON, isNull bool, err error) {
	for _, a := range b.getArgs() {
		res, isNull, err = a.EvalJSON(b.Ctx, row)
		if err != nil || !isNull {
			break
		}
	}
	return res, isNull, err
}

// temporalWithDateAsNumEvalType makes DATE, DATETIME, TIMESTAMP pretend to be numbers rather than strings.
func temporalWithDateAsNumEvalType(argTp *types.FieldType) (argEvalType types.EvalType, isStr bool, isTemporalWithDate bool) {
	argEvalType = argTp.EvalType()
	isStr, isTemporalWithDate = argEvalType.IsStringKind(), types.IsTemporalWithDate(argTp.Tp)
	if !isTemporalWithDate {
		return
	}
	if argTp.Decimal > 0 {
		argEvalType = types.ETDecimal
	} else {
		argEvalType = types.ETInt
	}
	return
}

// GetCmpTp4MinMax gets compare type for GREATEST and LEAST and BETWEEN (mainly for datetime).
func GetCmpTp4MinMax(args []Expression) (argTp types.EvalType) {
	datetimeFound, isAllStr := false, true
	cmpEvalType, isStr, isTemporalWithDate := temporalWithDateAsNumEvalType(args[0].GetType())
	if !isStr {
		isAllStr = false
	}
	if isTemporalWithDate {
		datetimeFound = true
	}
	lft := args[0].GetType()
	for i := range args {
		rft := args[i].GetType()
		var tp types.EvalType
		tp, isStr, isTemporalWithDate = temporalWithDateAsNumEvalType(rft)
		if isTemporalWithDate {
			datetimeFound = true
		}
		if !isStr {
			isAllStr = false
		}
		cmpEvalType = getBaseCmpType(cmpEvalType, tp, lft, rft)
		lft = rft
	}
	argTp = cmpEvalType
	if cmpEvalType.IsStringKind() {
		argTp = types.ETString
	}
	if isAllStr && datetimeFound {
		argTp = types.ETDatetime
	}
	return argTp
}

type greatestFunctionClass struct {
	BaseFunctionClass
}

func (c *greatestFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (sig BuiltinFunc, err error) {
	if err = c.VerifyArgs(args); err != nil {
		return nil, err
	}
	tp, cmpAsDatetime := GetCmpTp4MinMax(args), false
	if tp == types.ETDatetime {
		cmpAsDatetime = true
		tp = types.ETString
	}
	argTps := make([]types.EvalType, len(args))
	for i := range args {
		argTps[i] = tp
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, tp, argTps...)
	if cmpAsDatetime {
		tp = types.ETDatetime
	}
	switch tp {
	case types.ETInt:
		sig = &builtinGreatestIntSig{bf}
	case types.ETReal:
		sig = &builtinGreatestRealSig{bf}
	case types.ETDecimal:
		sig = &builtinGreatestDecimalSig{bf}
	case types.ETString:
		sig = &builtinGreatestStringSig{bf}
	case types.ETDatetime:
		sig = &builtinGreatestTimeSig{bf}
	}
	return sig, nil
}

type builtinGreatestIntSig struct {
	BaseBuiltinFunc
}

func (b *builtinGreatestIntSig) Clone() BuiltinFunc {
	newSig := &builtinGreatestIntSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalInt evals a builtinGreatestIntSig.
// See http://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#function_greatest
func (b *builtinGreatestIntSig) evalInt(row chunk.Row) (max int64, isNull bool, err error) {
	max, isNull, err = b.Args[0].EvalInt(b.Ctx, row)
	if isNull || err != nil {
		return max, isNull, err
	}
	for i := 1; i < len(b.Args); i++ {
		var v int64
		v, isNull, err = b.Args[i].EvalInt(b.Ctx, row)
		if isNull || err != nil {
			return max, isNull, err
		}
		if v > max {
			max = v
		}
	}
	return
}

type builtinGreatestRealSig struct {
	BaseBuiltinFunc
}

func (b *builtinGreatestRealSig) Clone() BuiltinFunc {
	newSig := &builtinGreatestRealSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalReal evals a builtinGreatestRealSig.
// See http://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#function_greatest
func (b *builtinGreatestRealSig) evalReal(row chunk.Row) (max float64, isNull bool, err error) {
	max, isNull, err = b.Args[0].EvalReal(b.Ctx, row)
	if isNull || err != nil {
		return max, isNull, err
	}
	for i := 1; i < len(b.Args); i++ {
		var v float64
		v, isNull, err = b.Args[i].EvalReal(b.Ctx, row)
		if isNull || err != nil {
			return max, isNull, err
		}
		if v > max {
			max = v
		}
	}
	return
}

type builtinGreatestDecimalSig struct {
	BaseBuiltinFunc
}

func (b *builtinGreatestDecimalSig) Clone() BuiltinFunc {
	newSig := &builtinGreatestDecimalSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalDecimal evals a builtinGreatestDecimalSig.
// See http://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#function_greatest
func (b *builtinGreatestDecimalSig) evalDecimal(row chunk.Row) (max *types.MyDecimal, isNull bool, err error) {
	max, isNull, err = b.Args[0].EvalDecimal(b.Ctx, row)
	if isNull || err != nil {
		return max, isNull, err
	}
	for i := 1; i < len(b.Args); i++ {
		var v *types.MyDecimal
		v, isNull, err = b.Args[i].EvalDecimal(b.Ctx, row)
		if isNull || err != nil {
			return max, isNull, err
		}
		if v.Compare(max) > 0 {
			max = v
		}
	}
	return
}

type builtinGreatestStringSig struct {
	BaseBuiltinFunc
}

func (b *builtinGreatestStringSig) Clone() BuiltinFunc {
	newSig := &builtinGreatestStringSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalString evals a builtinGreatestStringSig.
// See http://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#function_greatest
func (b *builtinGreatestStringSig) EvalString(row chunk.Row) (max string, isNull bool, err error) {
	max, isNull, err = b.Args[0].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return max, isNull, err
	}
	for i := 1; i < len(b.Args); i++ {
		var v string
		v, isNull, err = b.Args[i].EvalString(b.Ctx, row)
		if isNull || err != nil {
			return max, isNull, err
		}
		if types.CompareString(v, max) > 0 {
			max = v
		}
	}
	return
}

type builtinGreatestTimeSig struct {
	BaseBuiltinFunc
}

func (b *builtinGreatestTimeSig) Clone() BuiltinFunc {
	newSig := &builtinGreatestTimeSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalString evals a builtinGreatestTimeSig.
// See http://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#function_greatest
func (b *builtinGreatestTimeSig) EvalString(row chunk.Row) (_ string, isNull bool, err error) {
	var (
		v string
		t types.Time
	)
	max := types.ZeroDatetime
	sc := b.Ctx.GetSessionVars().StmtCtx
	for i := 0; i < len(b.Args); i++ {
		v, isNull, err = b.Args[i].EvalString(b.Ctx, row)
		if isNull || err != nil {
			return "", true, err
		}
		t, err = types.ParseDatetime(sc, v)
		if err != nil {
			if err = handleInvalidTimeError(b.Ctx, err); err != nil {
				return v, true, err
			}
			continue
		}
		if t.Compare(max) > 0 {
			max = t
		}
	}
	return max.String(), false, nil
}

type leastFunctionClass struct {
	BaseFunctionClass
}

func (c *leastFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (sig BuiltinFunc, err error) {
	if err = c.VerifyArgs(args); err != nil {
		return nil, err
	}
	tp, cmpAsDatetime := GetCmpTp4MinMax(args), false
	if tp == types.ETDatetime {
		cmpAsDatetime = true
		tp = types.ETString
	}
	argTps := make([]types.EvalType, len(args))
	for i := range args {
		argTps[i] = tp
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, tp, argTps...)
	if cmpAsDatetime {
		tp = types.ETDatetime
	}
	switch tp {
	case types.ETInt:
		sig = &builtinLeastIntSig{bf}
	case types.ETReal:
		sig = &builtinLeastRealSig{bf}
	case types.ETDecimal:
		sig = &builtinLeastDecimalSig{bf}
	case types.ETString:
		sig = &builtinLeastStringSig{bf}
	case types.ETDatetime:
		sig = &builtinLeastTimeSig{bf}
	}
	return sig, nil
}

type builtinLeastIntSig struct {
	BaseBuiltinFunc
}

func (b *builtinLeastIntSig) Clone() BuiltinFunc {
	newSig := &builtinLeastIntSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalInt evals a builtinLeastIntSig.
// See http://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#functionleast
func (b *builtinLeastIntSig) evalInt(row chunk.Row) (min int64, isNull bool, err error) {
	min, isNull, err = b.Args[0].EvalInt(b.Ctx, row)
	if isNull || err != nil {
		return min, isNull, err
	}
	for i := 1; i < len(b.Args); i++ {
		var v int64
		v, isNull, err = b.Args[i].EvalInt(b.Ctx, row)
		if isNull || err != nil {
			return min, isNull, err
		}
		if v < min {
			min = v
		}
	}
	return
}

type builtinLeastRealSig struct {
	BaseBuiltinFunc
}

func (b *builtinLeastRealSig) Clone() BuiltinFunc {
	newSig := &builtinLeastRealSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalReal evals a builtinLeastRealSig.
// See http://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#functionleast
func (b *builtinLeastRealSig) evalReal(row chunk.Row) (min float64, isNull bool, err error) {
	min, isNull, err = b.Args[0].EvalReal(b.Ctx, row)
	if isNull || err != nil {
		return min, isNull, err
	}
	for i := 1; i < len(b.Args); i++ {
		var v float64
		v, isNull, err = b.Args[i].EvalReal(b.Ctx, row)
		if isNull || err != nil {
			return min, isNull, err
		}
		if v < min {
			min = v
		}
	}
	return
}

type builtinLeastDecimalSig struct {
	BaseBuiltinFunc
}

func (b *builtinLeastDecimalSig) Clone() BuiltinFunc {
	newSig := &builtinLeastDecimalSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalDecimal evals a builtinLeastDecimalSig.
// See http://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#functionleast
func (b *builtinLeastDecimalSig) evalDecimal(row chunk.Row) (min *types.MyDecimal, isNull bool, err error) {
	min, isNull, err = b.Args[0].EvalDecimal(b.Ctx, row)
	if isNull || err != nil {
		return min, isNull, err
	}
	for i := 1; i < len(b.Args); i++ {
		var v *types.MyDecimal
		v, isNull, err = b.Args[i].EvalDecimal(b.Ctx, row)
		if isNull || err != nil {
			return min, isNull, err
		}
		if v.Compare(min) < 0 {
			min = v
		}
	}
	return
}

type builtinLeastStringSig struct {
	BaseBuiltinFunc
}

func (b *builtinLeastStringSig) Clone() BuiltinFunc {
	newSig := &builtinLeastStringSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalString evals a builtinLeastStringSig.
// See http://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#functionleast
func (b *builtinLeastStringSig) EvalString(row chunk.Row) (min string, isNull bool, err error) {
	min, isNull, err = b.Args[0].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return min, isNull, err
	}
	for i := 1; i < len(b.Args); i++ {
		var v string
		v, isNull, err = b.Args[i].EvalString(b.Ctx, row)
		if isNull || err != nil {
			return min, isNull, err
		}
		if types.CompareString(v, min) < 0 {
			min = v
		}
	}
	return
}

type builtinLeastTimeSig struct {
	BaseBuiltinFunc
}

func (b *builtinLeastTimeSig) Clone() BuiltinFunc {
	newSig := &builtinLeastTimeSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalString evals a builtinLeastTimeSig.
// See http://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#functionleast
func (b *builtinLeastTimeSig) EvalString(row chunk.Row) (res string, isNull bool, err error) {
	var (
		v string
		t types.Time
	)
	min := types.Time{
		Time: types.MaxDatetime,
		Type: mysql.TypeDatetime,
		Fsp:  types.MaxFsp,
	}
	findInvalidTime := false
	sc := b.Ctx.GetSessionVars().StmtCtx
	for i := 0; i < len(b.Args); i++ {
		v, isNull, err = b.Args[i].EvalString(b.Ctx, row)
		if isNull || err != nil {
			return "", true, err
		}
		t, err = types.ParseDatetime(sc, v)
		if err != nil {
			if err = handleInvalidTimeError(b.Ctx, err); err != nil {
				return v, true, err
			} else if !findInvalidTime {
				res = v
				findInvalidTime = true
			}
		}
		if t.Compare(min) < 0 {
			min = t
		}
	}
	if !findInvalidTime {
		res = min.String()
	}
	return res, false, nil
}

type intervalFunctionClass struct {
	BaseFunctionClass
}

func (c *intervalFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}

	allInt := true
	for i := range args {
		if args[i].GetType().EvalType() != types.ETInt {
			allInt = false
		}
	}

	argTps, argTp := make([]types.EvalType, 0, len(args)), types.ETReal
	if allInt {
		argTp = types.ETInt
	}
	for range args {
		argTps = append(argTps, argTp)
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETInt, argTps...)
	var sig BuiltinFunc
	if allInt {
		sig = &builtinIntervalIntSig{bf}
	} else {
		sig = &builtinIntervalRealSig{bf}
	}
	return sig, nil
}

type builtinIntervalIntSig struct {
	BaseBuiltinFunc
}

func (b *builtinIntervalIntSig) Clone() BuiltinFunc {
	newSig := &builtinIntervalIntSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalInt evals a builtinIntervalIntSig.
// See http://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#function_interval
func (b *builtinIntervalIntSig) evalInt(row chunk.Row) (int64, bool, error) {
	args0, isNull, err := b.Args[0].EvalInt(b.Ctx, row)
	if err != nil {
		return 0, true, err
	}
	if isNull {
		return -1, false, nil
	}
	idx, err := b.binSearch(args0, mysql.HasUnsignedFlag(b.Args[0].GetType().Flag), b.Args[1:], row)
	return int64(idx), err != nil, err
}

// binSearch is a binary search method.
// All arguments are treated as integers.
// It is required that arg[0] < args[1] < args[2] < ... < args[n] for this function to work correctly.
// This is because a binary search is used (very fast).
func (b *builtinIntervalIntSig) binSearch(target int64, isUint1 bool, args []Expression, row chunk.Row) (_ int, err error) {
	i, j, cmp := 0, len(args), false
	for i < j {
		mid := i + (j-i)/2
		v, isNull, err1 := args[mid].EvalInt(b.Ctx, row)
		if err1 != nil {
			err = err1
			break
		}
		if isNull {
			v = target
		}
		isUint2 := mysql.HasUnsignedFlag(args[mid].GetType().Flag)
		switch {
		case !isUint1 && !isUint2:
			cmp = target < v
		case isUint1 && isUint2:
			cmp = uint64(target) < uint64(v)
		case !isUint1 && isUint2:
			cmp = target < 0 || uint64(target) < uint64(v)
		case isUint1 && !isUint2:
			cmp = v > 0 && uint64(target) < uint64(v)
		}
		if !cmp {
			i = mid + 1
		} else {
			j = mid
		}
	}
	return i, err
}

type builtinIntervalRealSig struct {
	BaseBuiltinFunc
}

func (b *builtinIntervalRealSig) Clone() BuiltinFunc {
	newSig := &builtinIntervalRealSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalInt evals a builtinIntervalRealSig.
// See http://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#function_interval
func (b *builtinIntervalRealSig) evalInt(row chunk.Row) (int64, bool, error) {
	args0, isNull, err := b.Args[0].EvalReal(b.Ctx, row)
	if err != nil {
		return 0, true, err
	}
	if isNull {
		return -1, false, nil
	}
	idx, err := b.binSearch(args0, b.Args[1:], row)
	return int64(idx), err != nil, err
}

func (b *builtinIntervalRealSig) binSearch(target float64, args []Expression, row chunk.Row) (_ int, err error) {
	i, j := 0, len(args)
	for i < j {
		mid := i + (j-i)/2
		v, isNull, err1 := args[mid].EvalReal(b.Ctx, row)
		if err != nil {
			err = err1
			break
		}
		if isNull {
			i = mid + 1
		} else if cmp := target < v; !cmp {
			i = mid + 1
		} else {
			j = mid
		}
	}
	return i, err
}

type compareFunctionClass struct {
	BaseFunctionClass

	op opcode.Op
}

// getBaseCmpType gets the EvalType that the two args will be treated as when comparing.
func getBaseCmpType(lhs, rhs types.EvalType, lft, rft *types.FieldType) types.EvalType {
	if lft.Tp == mysql.TypeUnspecified || rft.Tp == mysql.TypeUnspecified {
		if lft.Tp == rft.Tp {
			return types.ETString
		}
		if lft.Tp == mysql.TypeUnspecified {
			lhs = rhs
		} else {
			rhs = lhs
		}
	}
	if lhs.IsStringKind() && rhs.IsStringKind() {
		return types.ETString
	} else if (lhs == types.ETInt || lft.Hybrid()) && (rhs == types.ETInt || rft.Hybrid()) {
		return types.ETInt
	} else if ((lhs == types.ETInt || lft.Hybrid()) || lhs == types.ETDecimal) &&
		((rhs == types.ETInt || rft.Hybrid()) || rhs == types.ETDecimal) {
		return types.ETDecimal
	}
	return types.ETReal
}

// GetAccurateCmpType uses a more complex logic to decide the EvalType of the two args when compare with each other than
// getBaseCmpType does.
func GetAccurateCmpType(lhs, rhs Expression) types.EvalType {
	lhsFieldType, rhsFieldType := lhs.GetType(), rhs.GetType()
	lhsEvalType, rhsEvalType := lhsFieldType.EvalType(), rhsFieldType.EvalType()
	cmpType := getBaseCmpType(lhsEvalType, rhsEvalType, lhsFieldType, rhsFieldType)
	if (lhsEvalType.IsStringKind() && rhsFieldType.Tp == mysql.TypeJSON) ||
		(lhsFieldType.Tp == mysql.TypeJSON && rhsEvalType.IsStringKind()) {
		cmpType = types.ETJson
	} else if cmpType == types.ETString && (types.IsTypeTime(lhsFieldType.Tp) || types.IsTypeTime(rhsFieldType.Tp)) {
		// date[time] <cmp> date[time]
		// string <cmp> date[time]
		// compare as time
		if lhsFieldType.Tp == rhsFieldType.Tp {
			cmpType = lhsFieldType.EvalType()
		} else {
			cmpType = types.ETDatetime
		}
	} else if lhsFieldType.Tp == mysql.TypeDuration && rhsFieldType.Tp == mysql.TypeDuration {
		// duration <cmp> duration
		// compare as duration
		cmpType = types.ETDuration
	} else if cmpType == types.ETReal || cmpType == types.ETString {
		_, isLHSConst := lhs.(*Constant)
		_, isRHSConst := rhs.(*Constant)
		if (lhsEvalType == types.ETDecimal && !isLHSConst && rhsEvalType.IsStringKind() && isRHSConst) ||
			(rhsEvalType == types.ETDecimal && !isRHSConst && lhsEvalType.IsStringKind() && isLHSConst) {
			/*
				<non-const decimal expression> <cmp> <const string expression>
				or
				<const string expression> <cmp> <non-const decimal expression>

				Do comparison as decimal rather than float, in order not to lose precision.
			)*/
			cmpType = types.ETDecimal
		} else if isTemporalColumn(lhs) && isRHSConst ||
			isTemporalColumn(rhs) && isLHSConst {
			/*
				<temporal column> <cmp> <non-temporal constant>
				or
				<non-temporal constant> <cmp> <temporal column>

				Convert the constant to temporal type.
			*/
			col, isLHSColumn := lhs.(*Column)
			if !isLHSColumn {
				col = rhs.(*Column)
			}
			if col.GetType().Tp == mysql.TypeDuration {
				cmpType = types.ETDuration
			} else {
				cmpType = types.ETDatetime
			}
		}
	}
	return cmpType
}

// GetCmpFunction get the compare function according to two arguments.
func GetCmpFunction(lhs, rhs Expression) CompareFunc {
	switch GetAccurateCmpType(lhs, rhs) {
	case types.ETInt:
		return CompareInt
	case types.ETReal:
		return CompareReal
	case types.ETDecimal:
		return CompareDecimal
	case types.ETString:
		return CompareString
	case types.ETDuration:
		return CompareDuration
	case types.ETDatetime, types.ETTimestamp:
		return CompareTime
	case types.ETJson:
		return CompareJSON
	}
	return nil
}

// isTemporalColumn checks if a expression is a temporal column,
// temporal column indicates time column or duration column.
func isTemporalColumn(expr Expression) bool {
	ft := expr.GetType()
	if _, isCol := expr.(*Column); !isCol {
		return false
	}
	if !types.IsTypeTime(ft.Tp) && ft.Tp != mysql.TypeDuration {
		return false
	}
	return true
}

// tryToConvertConstantInt tries to convert a constant with other type to a int constant.
// isExceptional indicates whether the 'int column [cmp] const' might be true/false.
// If isExceptional is true, ExecptionalVal is returned. Or, CorrectVal is returned.
// CorrectVal: The computed result. If the constant can be converted to int without exception, return the val. Else return 'con'(the input).
// ExceptionalVal : It is used to get more information to check whether 'int column [cmp] const' is true/false
// 					If the op == LT,LE,GT,GE and it gets an Overflow when converting, return inf/-inf.
// 					If the op == EQ,NullEQ and the constant can never be equal to the int column, return ‘con’(the input, a non-int constant).
func tryToConvertConstantInt(ctx sessionctx.Context, targetFieldType *types.FieldType, con *Constant) (_ *Constant, isExceptional bool) {
	if con.GetType().EvalType() == types.ETInt {
		return con, false
	}
	dt, err := con.Eval(chunk.Row{})
	if err != nil {
		return con, false
	}
	sc := ctx.GetSessionVars().StmtCtx

	dt, err = dt.ConvertTo(sc, targetFieldType)
	if err != nil {
		if terror.ErrorEqual(err, types.ErrOverflow) {
			return &Constant{
				Value:   dt,
				RetType: targetFieldType,
			}, true
		}
		return con, false
	}
	return &Constant{
		Value:        dt,
		RetType:      targetFieldType,
		DeferredExpr: con.DeferredExpr,
		ParamMarker:  con.ParamMarker,
	}, false
}

// RefineComparedConstant changes a non-integer constant argument to its ceiling or floor result by the given op.
// isExceptional indicates whether the 'int column [cmp] const' might be true/false.
// If isExceptional is true, ExecptionalVal is returned. Or, CorrectVal is returned.
// CorrectVal: The computed result. If the constant can be converted to int without exception, return the val. Else return 'con'(the input).
// ExceptionalVal : It is used to get more information to check whether 'int column [cmp] const' is true/false
// 					If the op == LT,LE,GT,GE and it gets an Overflow when converting, return inf/-inf.
// 					If the op == EQ,NullEQ and the constant can never be equal to the int column, return ‘con’(the input, a non-int constant).
func RefineComparedConstant(ctx sessionctx.Context, targetFieldType types.FieldType, con *Constant, op opcode.Op) (_ *Constant, isExceptional bool) {
	dt, err := con.Eval(chunk.Row{})
	if err != nil {
		return con, false
	}
	sc := ctx.GetSessionVars().StmtCtx

	if targetFieldType.Tp == mysql.TypeBit {
		targetFieldType = *types.NewFieldType(mysql.TypeLonglong)
	}
	var intDatum types.Datum
	intDatum, err = dt.ConvertTo(sc, &targetFieldType)
	if err != nil {
		if terror.ErrorEqual(err, types.ErrOverflow) {
			return &Constant{
				Value:   intDatum,
				RetType: &targetFieldType,
			}, true
		}
		return con, false
	}
	c, err := intDatum.CompareDatum(sc, &con.Value)
	if err != nil {
		return con, false
	}
	if c == 0 {
		return &Constant{
			Value:        intDatum,
			RetType:      &targetFieldType,
			DeferredExpr: con.DeferredExpr,
			ParamMarker:  con.ParamMarker,
		}, false
	}
	switch op {
	case opcode.LT, opcode.GE:
		resultExpr := NewFunctionInternal(ctx, ast.Ceil, types.NewFieldType(mysql.TypeUnspecified), con)
		if resultCon, ok := resultExpr.(*Constant); ok {
			return tryToConvertConstantInt(ctx, &targetFieldType, resultCon)
		}
	case opcode.LE, opcode.GT:
		resultExpr := NewFunctionInternal(ctx, ast.Floor, types.NewFieldType(mysql.TypeUnspecified), con)
		if resultCon, ok := resultExpr.(*Constant); ok {
			return tryToConvertConstantInt(ctx, &targetFieldType, resultCon)
		}
	case opcode.NullEQ, opcode.EQ:
		switch con.GetType().EvalType() {
		// An integer value equal or NULL-safe equal to a float value which contains
		// non-zero decimal digits is definitely false.
		// e.g.,
		//   1. "integer  =  1.1" is definitely false.
		//   2. "integer <=> 1.1" is definitely false.
		case types.ETReal, types.ETDecimal:
			return con, true
		case types.ETString:
			// We try to convert the string constant to double.
			// If the double result equals the int result, we can return the int result;
			// otherwise, the compare function will be false.
			var doubleDatum types.Datum
			doubleDatum, err = dt.ConvertTo(sc, types.NewFieldType(mysql.TypeDouble))
			if err != nil {
				return con, false
			}
			if c, err = doubleDatum.CompareDatum(sc, &intDatum); err != nil {
				return con, false
			}
			if c != 0 {
				return con, true
			}
			return &Constant{
				Value:        intDatum,
				RetType:      &targetFieldType,
				DeferredExpr: con.DeferredExpr,
				ParamMarker:  con.ParamMarker,
			}, false
		}
	}
	return con, false
}

// refineArgs will rewrite the arguments if the compare expression is `int column <cmp> non-int constant` or
// `non-int constant <cmp> int column`. E.g., `a < 1.1` will be rewritten to `a < 2`.
func (c *compareFunctionClass) refineArgs(ctx sessionctx.Context, args []Expression) []Expression {
	arg0Type, arg1Type := args[0].GetType(), args[1].GetType()
	arg0IsInt := arg0Type.EvalType() == types.ETInt
	arg1IsInt := arg1Type.EvalType() == types.ETInt
	arg0, arg0IsCon := args[0].(*Constant)
	arg1, arg1IsCon := args[1].(*Constant)
	isExceptional, finalArg0, finalArg1 := false, args[0], args[1]
	isPositiveInfinite, isNegativeInfinite := false, false
	// int non-constant [cmp] non-int constant
	if arg0IsInt && !arg0IsCon && !arg1IsInt && arg1IsCon {
		arg1, isExceptional = RefineComparedConstant(ctx, *arg0Type, arg1, c.op)
		finalArg1 = arg1
		if isExceptional && arg1.GetType().EvalType() == types.ETInt {
			// Judge it is inf or -inf
			// For int:
			//			inf:  01111111 & 1 == 1
			//		   -inf:  10000000 & 1 == 0
			// For uint:
			//			inf:  11111111 & 1 == 1
			//		   -inf:  00000000 & 0 == 0
			if arg1.Value.GetInt64()&1 == 1 {
				isPositiveInfinite = true
			} else {
				isNegativeInfinite = true
			}
		}
	}
	// non-int constant [cmp] int non-constant
	if arg1IsInt && !arg1IsCon && !arg0IsInt && arg0IsCon {
		arg0, isExceptional = RefineComparedConstant(ctx, *arg1Type, arg0, symmetricOp[c.op])
		finalArg0 = arg0
		if isExceptional && arg0.GetType().EvalType() == types.ETInt {
			if arg0.Value.GetInt64()&1 == 1 {
				isNegativeInfinite = true
			} else {
				isPositiveInfinite = true
			}
		}
	}

	if isExceptional && (c.op == opcode.EQ || c.op == opcode.NullEQ) {
		// This will always be false.
		return []Expression{Zero.Clone(), One.Clone()}
	}
	if isPositiveInfinite {
		// If the op is opcode.LT, opcode.LE
		// This will always be true.
		// If the op is opcode.GT, opcode.GE
		// This will always be false.
		return []Expression{Zero.Clone(), One.Clone()}
	}
	if isNegativeInfinite {
		// If the op is opcode.GT, opcode.GE
		// This will always be true.
		// If the op is opcode.LT, opcode.LE
		// This will always be false.
		return []Expression{One.Clone(), Zero.Clone()}
	}

	return []Expression{finalArg0, finalArg1}
}

// getFunction sets compare built-in function signatures for various types.
func (c *compareFunctionClass) GetFunction(ctx sessionctx.Context, rawArgs []Expression) (sig BuiltinFunc, err error) {
	if err = c.VerifyArgs(rawArgs); err != nil {
		return nil, err
	}
	args := c.refineArgs(ctx, rawArgs)
	cmpType := GetAccurateCmpType(args[0], args[1])
	sig, err = c.generateCmpSigs(ctx, args, cmpType)
	return sig, err
}

// generateCmpSigs generates compare function signatures.
func (c *compareFunctionClass) generateCmpSigs(ctx sessionctx.Context, args []Expression, tp types.EvalType) (sig BuiltinFunc, err error) {
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETInt, tp, tp)
	if tp == types.ETJson {
		// In compare, if we cast string to JSON, we shouldn't parse it.
		for i := range args {
			DisableParseJSONFlag4Expr(args[i])
		}
	}
	bf.Tp.Flen = 1
	switch tp {
	case types.ETInt:
		switch c.op {
		case opcode.LT:
			sig = &builtinLTIntSig{bf}
			sig.SetPbCode(tipb.ScalarFuncSig_LTInt)
		case opcode.LE:
			sig = &builtinLEIntSig{bf}
			sig.SetPbCode(tipb.ScalarFuncSig_LEInt)
		case opcode.GT:
			sig = &builtinGTIntSig{bf}
			sig.SetPbCode(tipb.ScalarFuncSig_GTInt)
		case opcode.EQ:
			sig = &builtinEQIntSig{bf}
			sig.SetPbCode(tipb.ScalarFuncSig_EQInt)
		case opcode.GE:
			sig = &builtinGEIntSig{bf}
			sig.SetPbCode(tipb.ScalarFuncSig_GEInt)
		case opcode.NE:
			sig = &builtinNEIntSig{bf}
			sig.SetPbCode(tipb.ScalarFuncSig_NEInt)
		case opcode.NullEQ:
			sig = &builtinNullEQIntSig{bf}
			sig.SetPbCode(tipb.ScalarFuncSig_NullEQInt)
		}
	case types.ETReal:
		switch c.op {
		case opcode.LT:
			sig = &builtinLTRealSig{bf}
			sig.SetPbCode(tipb.ScalarFuncSig_LTReal)
		case opcode.LE:
			sig = &builtinLERealSig{bf}
			sig.SetPbCode(tipb.ScalarFuncSig_LEReal)
		case opcode.GT:
			sig = &builtinGTRealSig{bf}
			sig.SetPbCode(tipb.ScalarFuncSig_GTReal)
		case opcode.GE:
			sig = &builtinGERealSig{bf}
			sig.SetPbCode(tipb.ScalarFuncSig_GEReal)
		case opcode.EQ:
			sig = &builtinEQRealSig{bf}
			sig.SetPbCode(tipb.ScalarFuncSig_EQReal)
		case opcode.NE:
			sig = &builtinNERealSig{bf}
			sig.SetPbCode(tipb.ScalarFuncSig_NEReal)
		case opcode.NullEQ:
			sig = &builtinNullEQRealSig{bf}
			sig.SetPbCode(tipb.ScalarFuncSig_NullEQReal)
		}
	case types.ETDecimal:
		switch c.op {
		case opcode.LT:
			sig = &builtinLTDecimalSig{bf}
			sig.SetPbCode(tipb.ScalarFuncSig_LTDecimal)
		case opcode.LE:
			sig = &builtinLEDecimalSig{bf}
			sig.SetPbCode(tipb.ScalarFuncSig_LEDecimal)
		case opcode.GT:
			sig = &builtinGTDecimalSig{bf}
			sig.SetPbCode(tipb.ScalarFuncSig_GTDecimal)
		case opcode.GE:
			sig = &builtinGEDecimalSig{bf}
			sig.SetPbCode(tipb.ScalarFuncSig_GEDecimal)
		case opcode.EQ:
			sig = &builtinEQDecimalSig{bf}
			sig.SetPbCode(tipb.ScalarFuncSig_EQDecimal)
		case opcode.NE:
			sig = &builtinNEDecimalSig{bf}
			sig.SetPbCode(tipb.ScalarFuncSig_NEDecimal)
		case opcode.NullEQ:
			sig = &builtinNullEQDecimalSig{bf}
			sig.SetPbCode(tipb.ScalarFuncSig_NullEQDecimal)
		}
	case types.ETString:
		switch c.op {
		case opcode.LT:
			sig = &builtinLTStringSig{bf}
			sig.SetPbCode(tipb.ScalarFuncSig_LTString)
		case opcode.LE:
			sig = &builtinLEStringSig{bf}
			sig.SetPbCode(tipb.ScalarFuncSig_LEString)
		case opcode.GT:
			sig = &builtinGTStringSig{bf}
			sig.SetPbCode(tipb.ScalarFuncSig_GTString)
		case opcode.GE:
			sig = &builtinGEStringSig{bf}
			sig.SetPbCode(tipb.ScalarFuncSig_GEString)
		case opcode.EQ:
			sig = &builtinEQStringSig{bf}
			sig.SetPbCode(tipb.ScalarFuncSig_EQString)
		case opcode.NE:
			sig = &builtinNEStringSig{bf}
			sig.SetPbCode(tipb.ScalarFuncSig_NEString)
		case opcode.NullEQ:
			sig = &builtinNullEQStringSig{bf}
			sig.SetPbCode(tipb.ScalarFuncSig_NullEQString)
		}
	case types.ETDuration:
		switch c.op {
		case opcode.LT:
			sig = &builtinLTDurationSig{bf}
			sig.SetPbCode(tipb.ScalarFuncSig_LTDuration)
		case opcode.LE:
			sig = &builtinLEDurationSig{bf}
			sig.SetPbCode(tipb.ScalarFuncSig_LEDuration)
		case opcode.GT:
			sig = &builtinGTDurationSig{bf}
			sig.SetPbCode(tipb.ScalarFuncSig_GTDuration)
		case opcode.GE:
			sig = &builtinGEDurationSig{bf}
			sig.SetPbCode(tipb.ScalarFuncSig_GEDuration)
		case opcode.EQ:
			sig = &builtinEQDurationSig{bf}
			sig.SetPbCode(tipb.ScalarFuncSig_EQDuration)
		case opcode.NE:
			sig = &builtinNEDurationSig{bf}
			sig.SetPbCode(tipb.ScalarFuncSig_NEDuration)
		case opcode.NullEQ:
			sig = &builtinNullEQDurationSig{bf}
			sig.SetPbCode(tipb.ScalarFuncSig_NullEQDuration)
		}
	case types.ETDatetime, types.ETTimestamp:
		switch c.op {
		case opcode.LT:
			sig = &builtinLTTimeSig{bf}
			sig.SetPbCode(tipb.ScalarFuncSig_LTTime)
		case opcode.LE:
			sig = &builtinLETimeSig{bf}
			sig.SetPbCode(tipb.ScalarFuncSig_LETime)
		case opcode.GT:
			sig = &builtinGTTimeSig{bf}
			sig.SetPbCode(tipb.ScalarFuncSig_GTTime)
		case opcode.GE:
			sig = &builtinGETimeSig{bf}
			sig.SetPbCode(tipb.ScalarFuncSig_GETime)
		case opcode.EQ:
			sig = &builtinEQTimeSig{bf}
			sig.SetPbCode(tipb.ScalarFuncSig_EQTime)
		case opcode.NE:
			sig = &builtinNETimeSig{bf}
			sig.SetPbCode(tipb.ScalarFuncSig_NETime)
		case opcode.NullEQ:
			sig = &builtinNullEQTimeSig{bf}
			sig.SetPbCode(tipb.ScalarFuncSig_NullEQTime)
		}
	case types.ETJson:
		switch c.op {
		case opcode.LT:
			sig = &builtinLTJSONSig{bf}
			sig.SetPbCode(tipb.ScalarFuncSig_LTJson)
		case opcode.LE:
			sig = &builtinLEJSONSig{bf}
			sig.SetPbCode(tipb.ScalarFuncSig_LEJson)
		case opcode.GT:
			sig = &builtinGTJSONSig{bf}
			sig.SetPbCode(tipb.ScalarFuncSig_GTJson)
		case opcode.GE:
			sig = &builtinGEJSONSig{bf}
			sig.SetPbCode(tipb.ScalarFuncSig_GEJson)
		case opcode.EQ:
			sig = &builtinEQJSONSig{bf}
			sig.SetPbCode(tipb.ScalarFuncSig_EQJson)
		case opcode.NE:
			sig = &builtinNEJSONSig{bf}
			sig.SetPbCode(tipb.ScalarFuncSig_NEJson)
		case opcode.NullEQ:
			sig = &builtinNullEQJSONSig{bf}
			sig.SetPbCode(tipb.ScalarFuncSig_NullEQJson)
		}
	}
	return
}

type builtinLTIntSig struct {
	BaseBuiltinFunc
}

func (b *builtinLTIntSig) Clone() BuiltinFunc {
	newSig := &builtinLTIntSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinLTIntSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	return resOfLT(CompareInt(b.Ctx, b.Args[0], b.Args[1], row, row))
}

type builtinLTRealSig struct {
	BaseBuiltinFunc
}

func (b *builtinLTRealSig) Clone() BuiltinFunc {
	newSig := &builtinLTRealSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinLTRealSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	return resOfLT(CompareReal(b.Ctx, b.Args[0], b.Args[1], row, row))
}

type builtinLTDecimalSig struct {
	BaseBuiltinFunc
}

func (b *builtinLTDecimalSig) Clone() BuiltinFunc {
	newSig := &builtinLTDecimalSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinLTDecimalSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	return resOfLT(CompareDecimal(b.Ctx, b.Args[0], b.Args[1], row, row))
}

type builtinLTStringSig struct {
	BaseBuiltinFunc
}

func (b *builtinLTStringSig) Clone() BuiltinFunc {
	newSig := &builtinLTStringSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinLTStringSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	return resOfLT(CompareString(b.Ctx, b.Args[0], b.Args[1], row, row))
}

type builtinLTDurationSig struct {
	BaseBuiltinFunc
}

func (b *builtinLTDurationSig) Clone() BuiltinFunc {
	newSig := &builtinLTDurationSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinLTDurationSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	return resOfLT(CompareDuration(b.Ctx, b.Args[0], b.Args[1], row, row))
}

type builtinLTTimeSig struct {
	BaseBuiltinFunc
}

func (b *builtinLTTimeSig) Clone() BuiltinFunc {
	newSig := &builtinLTTimeSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinLTTimeSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	return resOfLT(CompareTime(b.Ctx, b.Args[0], b.Args[1], row, row))
}

type builtinLTJSONSig struct {
	BaseBuiltinFunc
}

func (b *builtinLTJSONSig) Clone() BuiltinFunc {
	newSig := &builtinLTJSONSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinLTJSONSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	return resOfLT(CompareJSON(b.Ctx, b.Args[0], b.Args[1], row, row))
}

type builtinLEIntSig struct {
	BaseBuiltinFunc
}

func (b *builtinLEIntSig) Clone() BuiltinFunc {
	newSig := &builtinLEIntSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinLEIntSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	return resOfLE(CompareInt(b.Ctx, b.Args[0], b.Args[1], row, row))
}

type builtinLERealSig struct {
	BaseBuiltinFunc
}

func (b *builtinLERealSig) Clone() BuiltinFunc {
	newSig := &builtinLERealSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinLERealSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	return resOfLE(CompareReal(b.Ctx, b.Args[0], b.Args[1], row, row))
}

type builtinLEDecimalSig struct {
	BaseBuiltinFunc
}

func (b *builtinLEDecimalSig) Clone() BuiltinFunc {
	newSig := &builtinLEDecimalSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinLEDecimalSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	return resOfLE(CompareDecimal(b.Ctx, b.Args[0], b.Args[1], row, row))
}

type builtinLEStringSig struct {
	BaseBuiltinFunc
}

func (b *builtinLEStringSig) Clone() BuiltinFunc {
	newSig := &builtinLEStringSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinLEStringSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	return resOfLE(CompareString(b.Ctx, b.Args[0], b.Args[1], row, row))
}

type builtinLEDurationSig struct {
	BaseBuiltinFunc
}

func (b *builtinLEDurationSig) Clone() BuiltinFunc {
	newSig := &builtinLEDurationSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinLEDurationSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	return resOfLE(CompareDuration(b.Ctx, b.Args[0], b.Args[1], row, row))
}

type builtinLETimeSig struct {
	BaseBuiltinFunc
}

func (b *builtinLETimeSig) Clone() BuiltinFunc {
	newSig := &builtinLETimeSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinLETimeSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	return resOfLE(CompareTime(b.Ctx, b.Args[0], b.Args[1], row, row))
}

type builtinLEJSONSig struct {
	BaseBuiltinFunc
}

func (b *builtinLEJSONSig) Clone() BuiltinFunc {
	newSig := &builtinLEJSONSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinLEJSONSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	return resOfLE(CompareJSON(b.Ctx, b.Args[0], b.Args[1], row, row))
}

type builtinGTIntSig struct {
	BaseBuiltinFunc
}

func (b *builtinGTIntSig) Clone() BuiltinFunc {
	newSig := &builtinGTIntSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinGTIntSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	return resOfGT(CompareInt(b.Ctx, b.Args[0], b.Args[1], row, row))
}

type builtinGTRealSig struct {
	BaseBuiltinFunc
}

func (b *builtinGTRealSig) Clone() BuiltinFunc {
	newSig := &builtinGTRealSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinGTRealSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	return resOfGT(CompareReal(b.Ctx, b.Args[0], b.Args[1], row, row))
}

type builtinGTDecimalSig struct {
	BaseBuiltinFunc
}

func (b *builtinGTDecimalSig) Clone() BuiltinFunc {
	newSig := &builtinGTDecimalSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinGTDecimalSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	return resOfGT(CompareDecimal(b.Ctx, b.Args[0], b.Args[1], row, row))
}

type builtinGTStringSig struct {
	BaseBuiltinFunc
}

func (b *builtinGTStringSig) Clone() BuiltinFunc {
	newSig := &builtinGTStringSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinGTStringSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	return resOfGT(CompareString(b.Ctx, b.Args[0], b.Args[1], row, row))
}

type builtinGTDurationSig struct {
	BaseBuiltinFunc
}

func (b *builtinGTDurationSig) Clone() BuiltinFunc {
	newSig := &builtinGTDurationSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinGTDurationSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	return resOfGT(CompareDuration(b.Ctx, b.Args[0], b.Args[1], row, row))
}

type builtinGTTimeSig struct {
	BaseBuiltinFunc
}

func (b *builtinGTTimeSig) Clone() BuiltinFunc {
	newSig := &builtinGTTimeSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinGTTimeSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	return resOfGT(CompareTime(b.Ctx, b.Args[0], b.Args[1], row, row))
}

type builtinGTJSONSig struct {
	BaseBuiltinFunc
}

func (b *builtinGTJSONSig) Clone() BuiltinFunc {
	newSig := &builtinGTJSONSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinGTJSONSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	return resOfGT(CompareJSON(b.Ctx, b.Args[0], b.Args[1], row, row))
}

type builtinGEIntSig struct {
	BaseBuiltinFunc
}

func (b *builtinGEIntSig) Clone() BuiltinFunc {
	newSig := &builtinGEIntSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinGEIntSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	return resOfGE(CompareInt(b.Ctx, b.Args[0], b.Args[1], row, row))
}

type builtinGERealSig struct {
	BaseBuiltinFunc
}

func (b *builtinGERealSig) Clone() BuiltinFunc {
	newSig := &builtinGERealSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinGERealSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	return resOfGE(CompareReal(b.Ctx, b.Args[0], b.Args[1], row, row))
}

type builtinGEDecimalSig struct {
	BaseBuiltinFunc
}

func (b *builtinGEDecimalSig) Clone() BuiltinFunc {
	newSig := &builtinGEDecimalSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinGEDecimalSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	return resOfGE(CompareDecimal(b.Ctx, b.Args[0], b.Args[1], row, row))
}

type builtinGEStringSig struct {
	BaseBuiltinFunc
}

func (b *builtinGEStringSig) Clone() BuiltinFunc {
	newSig := &builtinGEStringSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinGEStringSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	return resOfGE(CompareString(b.Ctx, b.Args[0], b.Args[1], row, row))
}

type builtinGEDurationSig struct {
	BaseBuiltinFunc
}

func (b *builtinGEDurationSig) Clone() BuiltinFunc {
	newSig := &builtinGEDurationSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinGEDurationSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	return resOfGE(CompareDuration(b.Ctx, b.Args[0], b.Args[1], row, row))
}

type builtinGETimeSig struct {
	BaseBuiltinFunc
}

func (b *builtinGETimeSig) Clone() BuiltinFunc {
	newSig := &builtinGETimeSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinGETimeSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	return resOfGE(CompareTime(b.Ctx, b.Args[0], b.Args[1], row, row))
}

type builtinGEJSONSig struct {
	BaseBuiltinFunc
}

func (b *builtinGEJSONSig) Clone() BuiltinFunc {
	newSig := &builtinGEJSONSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinGEJSONSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	return resOfGE(CompareJSON(b.Ctx, b.Args[0], b.Args[1], row, row))
}

type builtinEQIntSig struct {
	BaseBuiltinFunc
}

func (b *builtinEQIntSig) Clone() BuiltinFunc {
	newSig := &builtinEQIntSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinEQIntSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	return resOfEQ(CompareInt(b.Ctx, b.Args[0], b.Args[1], row, row))
}

type builtinEQRealSig struct {
	BaseBuiltinFunc
}

func (b *builtinEQRealSig) Clone() BuiltinFunc {
	newSig := &builtinEQRealSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinEQRealSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	return resOfEQ(CompareReal(b.Ctx, b.Args[0], b.Args[1], row, row))
}

type builtinEQDecimalSig struct {
	BaseBuiltinFunc
}

func (b *builtinEQDecimalSig) Clone() BuiltinFunc {
	newSig := &builtinEQDecimalSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinEQDecimalSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	return resOfEQ(CompareDecimal(b.Ctx, b.Args[0], b.Args[1], row, row))
}

type builtinEQStringSig struct {
	BaseBuiltinFunc
}

func (b *builtinEQStringSig) Clone() BuiltinFunc {
	newSig := &builtinEQStringSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinEQStringSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	return resOfEQ(CompareString(b.Ctx, b.Args[0], b.Args[1], row, row))
}

type builtinEQDurationSig struct {
	BaseBuiltinFunc
}

func (b *builtinEQDurationSig) Clone() BuiltinFunc {
	newSig := &builtinEQDurationSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinEQDurationSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	return resOfEQ(CompareDuration(b.Ctx, b.Args[0], b.Args[1], row, row))
}

type builtinEQTimeSig struct {
	BaseBuiltinFunc
}

func (b *builtinEQTimeSig) Clone() BuiltinFunc {
	newSig := &builtinEQTimeSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinEQTimeSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	return resOfEQ(CompareTime(b.Ctx, b.Args[0], b.Args[1], row, row))
}

type builtinEQJSONSig struct {
	BaseBuiltinFunc
}

func (b *builtinEQJSONSig) Clone() BuiltinFunc {
	newSig := &builtinEQJSONSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinEQJSONSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	return resOfEQ(CompareJSON(b.Ctx, b.Args[0], b.Args[1], row, row))
}

type builtinNEIntSig struct {
	BaseBuiltinFunc
}

func (b *builtinNEIntSig) Clone() BuiltinFunc {
	newSig := &builtinNEIntSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinNEIntSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	return resOfNE(CompareInt(b.Ctx, b.Args[0], b.Args[1], row, row))
}

type builtinNERealSig struct {
	BaseBuiltinFunc
}

func (b *builtinNERealSig) Clone() BuiltinFunc {
	newSig := &builtinNERealSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinNERealSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	return resOfNE(CompareReal(b.Ctx, b.Args[0], b.Args[1], row, row))
}

type builtinNEDecimalSig struct {
	BaseBuiltinFunc
}

func (b *builtinNEDecimalSig) Clone() BuiltinFunc {
	newSig := &builtinNEDecimalSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinNEDecimalSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	return resOfNE(CompareDecimal(b.Ctx, b.Args[0], b.Args[1], row, row))
}

type builtinNEStringSig struct {
	BaseBuiltinFunc
}

func (b *builtinNEStringSig) Clone() BuiltinFunc {
	newSig := &builtinNEStringSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinNEStringSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	return resOfNE(CompareString(b.Ctx, b.Args[0], b.Args[1], row, row))
}

type builtinNEDurationSig struct {
	BaseBuiltinFunc
}

func (b *builtinNEDurationSig) Clone() BuiltinFunc {
	newSig := &builtinNEDurationSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinNEDurationSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	return resOfNE(CompareDuration(b.Ctx, b.Args[0], b.Args[1], row, row))
}

type builtinNETimeSig struct {
	BaseBuiltinFunc
}

func (b *builtinNETimeSig) Clone() BuiltinFunc {
	newSig := &builtinNETimeSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinNETimeSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	return resOfNE(CompareTime(b.Ctx, b.Args[0], b.Args[1], row, row))
}

type builtinNEJSONSig struct {
	BaseBuiltinFunc
}

func (b *builtinNEJSONSig) Clone() BuiltinFunc {
	newSig := &builtinNEJSONSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinNEJSONSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	return resOfNE(CompareJSON(b.Ctx, b.Args[0], b.Args[1], row, row))
}

type builtinNullEQIntSig struct {
	BaseBuiltinFunc
}

func (b *builtinNullEQIntSig) Clone() BuiltinFunc {
	newSig := &builtinNullEQIntSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinNullEQIntSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	arg0, isNull0, err := b.Args[0].EvalInt(b.Ctx, row)
	if err != nil {
		return 0, isNull0, err
	}
	arg1, isNull1, err := b.Args[1].EvalInt(b.Ctx, row)
	if err != nil {
		return 0, isNull1, err
	}
	isUnsigned0, isUnsigned1 := mysql.HasUnsignedFlag(b.Args[0].GetType().Flag), mysql.HasUnsignedFlag(b.Args[1].GetType().Flag)
	var res int64
	switch {
	case isNull0 && isNull1:
		res = 1
	case isNull0 != isNull1:
		break
	case isUnsigned0 && isUnsigned1 && types.CompareUint64(uint64(arg0), uint64(arg1)) == 0:
		res = 1
	case !isUnsigned0 && !isUnsigned1 && types.CompareInt64(arg0, arg1) == 0:
		res = 1
	case isUnsigned0 && !isUnsigned1:
		if arg1 < 0 || arg0 > math.MaxInt64 {
			break
		}
		if types.CompareInt64(arg0, arg1) == 0 {
			res = 1
		}
	case !isUnsigned0 && isUnsigned1:
		if arg0 < 0 || arg1 > math.MaxInt64 {
			break
		}
		if types.CompareInt64(arg0, arg1) == 0 {
			res = 1
		}
	}
	return res, false, nil
}

type builtinNullEQRealSig struct {
	BaseBuiltinFunc
}

func (b *builtinNullEQRealSig) Clone() BuiltinFunc {
	newSig := &builtinNullEQRealSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinNullEQRealSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	arg0, isNull0, err := b.Args[0].EvalReal(b.Ctx, row)
	if err != nil {
		return 0, true, err
	}
	arg1, isNull1, err := b.Args[1].EvalReal(b.Ctx, row)
	if err != nil {
		return 0, true, err
	}
	var res int64
	switch {
	case isNull0 && isNull1:
		res = 1
	case isNull0 != isNull1:
		break
	case types.CompareFloat64(arg0, arg1) == 0:
		res = 1
	}
	return res, false, nil
}

type builtinNullEQDecimalSig struct {
	BaseBuiltinFunc
}

func (b *builtinNullEQDecimalSig) Clone() BuiltinFunc {
	newSig := &builtinNullEQDecimalSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinNullEQDecimalSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	arg0, isNull0, err := b.Args[0].EvalDecimal(b.Ctx, row)
	if err != nil {
		return 0, true, err
	}
	arg1, isNull1, err := b.Args[1].EvalDecimal(b.Ctx, row)
	if err != nil {
		return 0, true, err
	}
	var res int64
	switch {
	case isNull0 && isNull1:
		res = 1
	case isNull0 != isNull1:
		break
	case arg0.Compare(arg1) == 0:
		res = 1
	}
	return res, false, nil
}

type builtinNullEQStringSig struct {
	BaseBuiltinFunc
}

func (b *builtinNullEQStringSig) Clone() BuiltinFunc {
	newSig := &builtinNullEQStringSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinNullEQStringSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	arg0, isNull0, err := b.Args[0].EvalString(b.Ctx, row)
	if err != nil {
		return 0, true, err
	}
	arg1, isNull1, err := b.Args[1].EvalString(b.Ctx, row)
	if err != nil {
		return 0, true, err
	}
	var res int64
	switch {
	case isNull0 && isNull1:
		res = 1
	case isNull0 != isNull1:
		break
	case types.CompareString(arg0, arg1) == 0:
		res = 1
	}
	return res, false, nil
}

type builtinNullEQDurationSig struct {
	BaseBuiltinFunc
}

func (b *builtinNullEQDurationSig) Clone() BuiltinFunc {
	newSig := &builtinNullEQDurationSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinNullEQDurationSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	arg0, isNull0, err := b.Args[0].EvalDuration(b.Ctx, row)
	if err != nil {
		return 0, true, err
	}
	arg1, isNull1, err := b.Args[1].EvalDuration(b.Ctx, row)
	if err != nil {
		return 0, true, err
	}
	var res int64
	switch {
	case isNull0 && isNull1:
		res = 1
	case isNull0 != isNull1:
		break
	case arg0.Compare(arg1) == 0:
		res = 1
	}
	return res, false, nil
}

type builtinNullEQTimeSig struct {
	BaseBuiltinFunc
}

func (b *builtinNullEQTimeSig) Clone() BuiltinFunc {
	newSig := &builtinNullEQTimeSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinNullEQTimeSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	arg0, isNull0, err := b.Args[0].EvalTime(b.Ctx, row)
	if err != nil {
		return 0, true, err
	}
	arg1, isNull1, err := b.Args[1].EvalTime(b.Ctx, row)
	if err != nil {
		return 0, true, err
	}
	var res int64
	switch {
	case isNull0 && isNull1:
		res = 1
	case isNull0 != isNull1:
		break
	case arg0.Compare(arg1) == 0:
		res = 1
	}
	return res, false, nil
}

type builtinNullEQJSONSig struct {
	BaseBuiltinFunc
}

func (b *builtinNullEQJSONSig) Clone() BuiltinFunc {
	newSig := &builtinNullEQJSONSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinNullEQJSONSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	arg0, isNull0, err := b.Args[0].EvalJSON(b.Ctx, row)
	if err != nil {
		return 0, true, err
	}
	arg1, isNull1, err := b.Args[1].EvalJSON(b.Ctx, row)
	if err != nil {
		return 0, true, err
	}
	var res int64
	switch {
	case isNull0 && isNull1:
		res = 1
	case isNull0 != isNull1:
		break
	default:
		cmpRes := json.CompareBinary(arg0, arg1)
		if cmpRes == 0 {
			res = 1
		}
	}
	return res, false, nil
}

func resOfLT(val int64, isNull bool, err error) (int64, bool, error) {
	if isNull || err != nil {
		return 0, isNull, err
	}
	if val < 0 {
		val = 1
	} else {
		val = 0
	}
	return val, false, nil
}

func resOfLE(val int64, isNull bool, err error) (int64, bool, error) {
	if isNull || err != nil {
		return 0, isNull, err
	}
	if val <= 0 {
		val = 1
	} else {
		val = 0
	}
	return val, false, nil
}

func resOfGT(val int64, isNull bool, err error) (int64, bool, error) {
	if isNull || err != nil {
		return 0, isNull, err
	}
	if val > 0 {
		val = 1
	} else {
		val = 0
	}
	return val, false, nil
}

func resOfGE(val int64, isNull bool, err error) (int64, bool, error) {
	if isNull || err != nil {
		return 0, isNull, err
	}
	if val >= 0 {
		val = 1
	} else {
		val = 0
	}
	return val, false, nil
}

func resOfEQ(val int64, isNull bool, err error) (int64, bool, error) {
	if isNull || err != nil {
		return 0, isNull, err
	}
	if val == 0 {
		val = 1
	} else {
		val = 0
	}
	return val, false, nil
}

func resOfNE(val int64, isNull bool, err error) (int64, bool, error) {
	if isNull || err != nil {
		return 0, isNull, err
	}
	if val != 0 {
		val = 1
	} else {
		val = 0
	}
	return val, false, nil
}

// compareNull compares null values based on the following rules.
// 1. NULL is considered to be equal to NULL
// 2. NULL is considered to be smaller than a non-NULL value.
// NOTE: (lhsIsNull == true) or (rhsIsNull == true) is required.
func compareNull(lhsIsNull, rhsIsNull bool) int64 {
	if lhsIsNull && rhsIsNull {
		return 0
	}
	if lhsIsNull {
		return -1
	}
	return 1
}

// CompareFunc defines the compare function prototype.
type CompareFunc = func(sctx sessionctx.Context, lhsArg, rhsArg Expression, lhsRow, rhsRow chunk.Row) (int64, bool, error)

// CompareInt compares two integers.
func CompareInt(sctx sessionctx.Context, lhsArg, rhsArg Expression, lhsRow, rhsRow chunk.Row) (int64, bool, error) {
	arg0, isNull0, err := lhsArg.EvalInt(sctx, lhsRow)
	if err != nil {
		return 0, true, err
	}

	arg1, isNull1, err := rhsArg.EvalInt(sctx, rhsRow)
	if err != nil {
		return 0, true, err
	}

	// compare null values.
	if isNull0 || isNull1 {
		return compareNull(isNull0, isNull1), true, nil
	}

	isUnsigned0, isUnsigned1 := mysql.HasUnsignedFlag(lhsArg.GetType().Flag), mysql.HasUnsignedFlag(rhsArg.GetType().Flag)
	var res int
	switch {
	case isUnsigned0 && isUnsigned1:
		res = types.CompareUint64(uint64(arg0), uint64(arg1))
	case isUnsigned0 && !isUnsigned1:
		if arg1 < 0 || uint64(arg0) > math.MaxInt64 {
			res = 1
		} else {
			res = types.CompareInt64(arg0, arg1)
		}
	case !isUnsigned0 && isUnsigned1:
		if arg0 < 0 || uint64(arg1) > math.MaxInt64 {
			res = -1
		} else {
			res = types.CompareInt64(arg0, arg1)
		}
	case !isUnsigned0 && !isUnsigned1:
		res = types.CompareInt64(arg0, arg1)
	}
	return int64(res), false, nil
}

// CompareString compares two strings.
func CompareString(sctx sessionctx.Context, lhsArg, rhsArg Expression, lhsRow, rhsRow chunk.Row) (int64, bool, error) {
	arg0, isNull0, err := lhsArg.EvalString(sctx, lhsRow)
	if err != nil {
		return 0, true, err
	}

	arg1, isNull1, err := rhsArg.EvalString(sctx, rhsRow)
	if err != nil {
		return 0, true, err
	}

	if isNull0 || isNull1 {
		return compareNull(isNull0, isNull1), true, nil
	}
	return int64(types.CompareString(arg0, arg1)), false, nil
}

// CompareReal compares two float-point values.
func CompareReal(sctx sessionctx.Context, lhsArg, rhsArg Expression, lhsRow, rhsRow chunk.Row) (int64, bool, error) {
	arg0, isNull0, err := lhsArg.EvalReal(sctx, lhsRow)
	if err != nil {
		return 0, true, err
	}

	arg1, isNull1, err := rhsArg.EvalReal(sctx, rhsRow)
	if err != nil {
		return 0, true, err
	}

	if isNull0 || isNull1 {
		return compareNull(isNull0, isNull1), true, nil
	}
	return int64(types.CompareFloat64(arg0, arg1)), false, nil
}

// CompareDecimal compares two decimals.
func CompareDecimal(sctx sessionctx.Context, lhsArg, rhsArg Expression, lhsRow, rhsRow chunk.Row) (int64, bool, error) {
	arg0, isNull0, err := lhsArg.EvalDecimal(sctx, lhsRow)
	if err != nil {
		return 0, true, err
	}

	arg1, isNull1, err := rhsArg.EvalDecimal(sctx, rhsRow)
	if err != nil {
		return 0, true, err
	}

	if isNull0 || isNull1 {
		return compareNull(isNull0, isNull1), true, nil
	}
	return int64(arg0.Compare(arg1)), false, nil
}

// CompareTime compares two datetime or timestamps.
func CompareTime(sctx sessionctx.Context, lhsArg, rhsArg Expression, lhsRow, rhsRow chunk.Row) (int64, bool, error) {
	arg0, isNull0, err := lhsArg.EvalTime(sctx, lhsRow)
	if err != nil {
		return 0, true, err
	}

	arg1, isNull1, err := rhsArg.EvalTime(sctx, rhsRow)
	if err != nil {
		return 0, true, err
	}

	if isNull0 || isNull1 {
		return compareNull(isNull0, isNull1), true, nil
	}
	return int64(arg0.Compare(arg1)), false, nil
}

// CompareDuration compares two durations.
func CompareDuration(sctx sessionctx.Context, lhsArg, rhsArg Expression, lhsRow, rhsRow chunk.Row) (int64, bool, error) {
	arg0, isNull0, err := lhsArg.EvalDuration(sctx, lhsRow)
	if err != nil {
		return 0, true, err
	}

	arg1, isNull1, err := rhsArg.EvalDuration(sctx, rhsRow)
	if err != nil {
		return 0, true, err
	}

	if isNull0 || isNull1 {
		return compareNull(isNull0, isNull1), true, nil
	}
	return int64(arg0.Compare(arg1)), false, nil
}

// CompareJSON compares two JSONs.
func CompareJSON(sctx sessionctx.Context, lhsArg, rhsArg Expression, lhsRow, rhsRow chunk.Row) (int64, bool, error) {
	arg0, isNull0, err := lhsArg.EvalJSON(sctx, lhsRow)
	if err != nil {
		return 0, true, err
	}

	arg1, isNull1, err := rhsArg.EvalJSON(sctx, rhsRow)
	if err != nil {
		return 0, true, err
	}

	if isNull0 || isNull1 {
		return compareNull(isNull0, isNull1), true, nil
	}
	return int64(json.CompareBinary(arg0, arg1)), false, nil
}
