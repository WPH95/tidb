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
	"github.com/cznic/mathutil"
	"github.com/pingcap/parser/charset"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/types/json"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tipb/go-tipb"
)

var (
	_ FunctionClass = &caseWhenFunctionClass{}
	_ FunctionClass = &ifFunctionClass{}
	_ FunctionClass = &ifNullFunctionClass{}
)

var (
	_ BuiltinFunc = &builtinCaseWhenIntSig{}
	_ BuiltinFunc = &builtinCaseWhenRealSig{}
	_ BuiltinFunc = &builtinCaseWhenDecimalSig{}
	_ BuiltinFunc = &builtinCaseWhenStringSig{}
	_ BuiltinFunc = &builtinCaseWhenTimeSig{}
	_ BuiltinFunc = &builtinCaseWhenDurationSig{}
	_ BuiltinFunc = &builtinCaseWhenJSONSig{}
	_ BuiltinFunc = &builtinIfNullIntSig{}
	_ BuiltinFunc = &builtinIfNullRealSig{}
	_ BuiltinFunc = &builtinIfNullDecimalSig{}
	_ BuiltinFunc = &builtinIfNullStringSig{}
	_ BuiltinFunc = &builtinIfNullTimeSig{}
	_ BuiltinFunc = &builtinIfNullDurationSig{}
	_ BuiltinFunc = &builtinIfNullJSONSig{}
	_ BuiltinFunc = &builtinIfIntSig{}
	_ BuiltinFunc = &builtinIfRealSig{}
	_ BuiltinFunc = &builtinIfDecimalSig{}
	_ BuiltinFunc = &builtinIfStringSig{}
	_ BuiltinFunc = &builtinIfTimeSig{}
	_ BuiltinFunc = &builtinIfDurationSig{}
	_ BuiltinFunc = &builtinIfJSONSig{}
)

// InferType4ControlFuncs infer result type for builtin IF, IFNULL, NULLIF, LEAD and LAG.
func InferType4ControlFuncs(lhs, rhs *types.FieldType) *types.FieldType {
	resultFieldType := &types.FieldType{}
	if lhs.Tp == mysql.TypeNull {
		*resultFieldType = *rhs
		// If both arguments are NULL, make resulting type BINARY(0).
		if rhs.Tp == mysql.TypeNull {
			resultFieldType.Tp = mysql.TypeString
			resultFieldType.Flen, resultFieldType.Decimal = 0, 0
			types.SetBinChsClnFlag(resultFieldType)
		}
	} else if rhs.Tp == mysql.TypeNull {
		*resultFieldType = *lhs
	} else {
		resultFieldType = types.AggFieldType([]*types.FieldType{lhs, rhs})
		evalType := types.AggregateEvalType([]*types.FieldType{lhs, rhs}, &resultFieldType.Flag)
		if evalType == types.ETInt {
			resultFieldType.Decimal = 0
		} else {
			if lhs.Decimal == types.UnspecifiedLength || rhs.Decimal == types.UnspecifiedLength {
				resultFieldType.Decimal = types.UnspecifiedLength
			} else {
				resultFieldType.Decimal = mathutil.Max(lhs.Decimal, rhs.Decimal)
			}
		}
		if types.IsNonBinaryStr(lhs) && !types.IsBinaryStr(rhs) {
			resultFieldType.Charset, resultFieldType.Collate, resultFieldType.Flag = charset.CharsetUTF8MB4, charset.CollationUTF8MB4, 0
			if mysql.HasBinaryFlag(lhs.Flag) || !types.IsNonBinaryStr(rhs) {
				resultFieldType.Flag |= mysql.BinaryFlag
			}
		} else if types.IsNonBinaryStr(rhs) && !types.IsBinaryStr(lhs) {
			resultFieldType.Charset, resultFieldType.Collate, resultFieldType.Flag = charset.CharsetUTF8MB4, charset.CollationUTF8MB4, 0
			if mysql.HasBinaryFlag(rhs.Flag) || !types.IsNonBinaryStr(lhs) {
				resultFieldType.Flag |= mysql.BinaryFlag
			}
		} else if types.IsBinaryStr(lhs) || types.IsBinaryStr(rhs) || !evalType.IsStringKind() {
			types.SetBinChsClnFlag(resultFieldType)
		} else {
			resultFieldType.Charset, resultFieldType.Collate, resultFieldType.Flag = mysql.DefaultCharset, mysql.DefaultCollationName, 0
		}
		if evalType == types.ETDecimal || evalType == types.ETInt {
			lhsUnsignedFlag, rhsUnsignedFlag := mysql.HasUnsignedFlag(lhs.Flag), mysql.HasUnsignedFlag(rhs.Flag)
			lhsFlagLen, rhsFlagLen := 0, 0
			if !lhsUnsignedFlag {
				lhsFlagLen = 1
			}
			if !rhsUnsignedFlag {
				rhsFlagLen = 1
			}
			lhsFlen := lhs.Flen - lhsFlagLen
			rhsFlen := rhs.Flen - rhsFlagLen
			if lhs.Decimal != types.UnspecifiedLength {
				lhsFlen -= lhs.Decimal
			}
			if lhs.Decimal != types.UnspecifiedLength {
				rhsFlen -= rhs.Decimal
			}
			resultFieldType.Flen = mathutil.Max(lhsFlen, rhsFlen) + resultFieldType.Decimal + 1
		} else {
			resultFieldType.Flen = mathutil.Max(lhs.Flen, rhs.Flen)
		}
	}
	// Fix decimal for int and string.
	resultEvalType := resultFieldType.EvalType()
	if resultEvalType == types.ETInt {
		resultFieldType.Decimal = 0
	} else if resultEvalType == types.ETString {
		if lhs.Tp != mysql.TypeNull || rhs.Tp != mysql.TypeNull {
			resultFieldType.Decimal = types.UnspecifiedLength
		}
	}
	return resultFieldType
}

type caseWhenFunctionClass struct {
	BaseFunctionClass
}

func (c *caseWhenFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (sig BuiltinFunc, err error) {
	if err = c.VerifyArgs(args); err != nil {
		return nil, err
	}
	l := len(args)
	// Fill in each 'THEN' clause parameter type.
	fieldTps := make([]*types.FieldType, 0, (l+1)/2)
	decimal, flen, isBinaryStr, isBinaryFlag := args[1].GetType().Decimal, 0, false, false
	for i := 1; i < l; i += 2 {
		fieldTps = append(fieldTps, args[i].GetType())
		decimal = mathutil.Max(decimal, args[i].GetType().Decimal)
		flen = mathutil.Max(flen, args[i].GetType().Flen)
		isBinaryStr = isBinaryStr || types.IsBinaryStr(args[i].GetType())
		isBinaryFlag = isBinaryFlag || !types.IsNonBinaryStr(args[i].GetType())
	}
	if l%2 == 1 {
		fieldTps = append(fieldTps, args[l-1].GetType())
		decimal = mathutil.Max(decimal, args[l-1].GetType().Decimal)
		flen = mathutil.Max(flen, args[l-1].GetType().Flen)
		isBinaryStr = isBinaryStr || types.IsBinaryStr(args[l-1].GetType())
		isBinaryFlag = isBinaryFlag || !types.IsNonBinaryStr(args[l-1].GetType())
	}

	fieldTp := types.AggFieldType(fieldTps)
	tp := fieldTp.EvalType()

	if tp == types.ETInt {
		decimal = 0
	}
	fieldTp.Decimal, fieldTp.Flen = decimal, flen
	if fieldTp.EvalType().IsStringKind() && !isBinaryStr {
		fieldTp.Charset, fieldTp.Collate = charset.CharsetUTF8MB4, charset.CollationUTF8MB4
	}
	if isBinaryFlag {
		fieldTp.Flag |= mysql.BinaryFlag
	}
	// Set retType to BINARY(0) if all arguments are of type NULL.
	if fieldTp.Tp == mysql.TypeNull {
		fieldTp.Flen, fieldTp.Decimal = 0, -1
		types.SetBinChsClnFlag(fieldTp)
	}
	argTps := make([]types.EvalType, 0, l)
	for i := 0; i < l-1; i += 2 {
		argTps = append(argTps, types.ETInt, tp)
	}
	if l%2 == 1 {
		argTps = append(argTps, tp)
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, tp, argTps...)
	bf.Tp = fieldTp

	switch tp {
	case types.ETInt:
		bf.Tp.Decimal = 0
		sig = &builtinCaseWhenIntSig{bf}
		sig.SetPbCode(tipb.ScalarFuncSig_CaseWhenInt)
	case types.ETReal:
		sig = &builtinCaseWhenRealSig{bf}
		sig.SetPbCode(tipb.ScalarFuncSig_CaseWhenReal)
	case types.ETDecimal:
		sig = &builtinCaseWhenDecimalSig{bf}
		sig.SetPbCode(tipb.ScalarFuncSig_CaseWhenDecimal)
	case types.ETString:
		bf.Tp.Decimal = types.UnspecifiedLength
		sig = &builtinCaseWhenStringSig{bf}
		sig.SetPbCode(tipb.ScalarFuncSig_CaseWhenString)
	case types.ETDatetime, types.ETTimestamp:
		sig = &builtinCaseWhenTimeSig{bf}
		sig.SetPbCode(tipb.ScalarFuncSig_CaseWhenTime)
	case types.ETDuration:
		sig = &builtinCaseWhenDurationSig{bf}
		sig.SetPbCode(tipb.ScalarFuncSig_CaseWhenDuration)
	case types.ETJson:
		sig = &builtinCaseWhenJSONSig{bf}
		sig.SetPbCode(tipb.ScalarFuncSig_CaseWhenJson)
	}
	return sig, nil
}

type builtinCaseWhenIntSig struct {
	BaseBuiltinFunc
}

func (b *builtinCaseWhenIntSig) Clone() BuiltinFunc {
	newSig := &builtinCaseWhenIntSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalInt evals a builtinCaseWhenIntSig.
// See https://dev.mysql.com/doc/refman/5.7/en/case.html
func (b *builtinCaseWhenIntSig) evalInt(row chunk.Row) (ret int64, isNull bool, err error) {
	var condition int64
	args, l := b.getArgs(), len(b.getArgs())
	for i := 0; i < l-1; i += 2 {
		condition, isNull, err = args[i].EvalInt(b.Ctx, row)
		if err != nil {
			return 0, isNull, err
		}
		if isNull || condition == 0 {
			continue
		}
		ret, isNull, err = args[i+1].EvalInt(b.Ctx, row)
		return ret, isNull, err
	}
	// when clause(condition, result) -> args[i], args[i+1]; (i >= 0 && i+1 < l-1)
	// else clause -> args[l-1]
	// If case clause has else clause, l%2 == 1.
	if l%2 == 1 {
		ret, isNull, err = args[l-1].EvalInt(b.Ctx, row)
		return ret, isNull, err
	}
	return ret, true, nil
}

type builtinCaseWhenRealSig struct {
	BaseBuiltinFunc
}

func (b *builtinCaseWhenRealSig) Clone() BuiltinFunc {
	newSig := &builtinCaseWhenRealSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalReal evals a builtinCaseWhenRealSig.
// See https://dev.mysql.com/doc/refman/5.7/en/case.html
func (b *builtinCaseWhenRealSig) evalReal(row chunk.Row) (ret float64, isNull bool, err error) {
	var condition int64
	args, l := b.getArgs(), len(b.getArgs())
	for i := 0; i < l-1; i += 2 {
		condition, isNull, err = args[i].EvalInt(b.Ctx, row)
		if err != nil {
			return 0, isNull, err
		}
		if isNull || condition == 0 {
			continue
		}
		ret, isNull, err = args[i+1].EvalReal(b.Ctx, row)
		return ret, isNull, err
	}
	// when clause(condition, result) -> args[i], args[i+1]; (i >= 0 && i+1 < l-1)
	// else clause -> args[l-1]
	// If case clause has else clause, l%2 == 1.
	if l%2 == 1 {
		ret, isNull, err = args[l-1].EvalReal(b.Ctx, row)
		return ret, isNull, err
	}
	return ret, true, nil
}

type builtinCaseWhenDecimalSig struct {
	BaseBuiltinFunc
}

func (b *builtinCaseWhenDecimalSig) Clone() BuiltinFunc {
	newSig := &builtinCaseWhenDecimalSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalDecimal evals a builtinCaseWhenDecimalSig.
// See https://dev.mysql.com/doc/refman/5.7/en/case.html
func (b *builtinCaseWhenDecimalSig) evalDecimal(row chunk.Row) (ret *types.MyDecimal, isNull bool, err error) {
	var condition int64
	args, l := b.getArgs(), len(b.getArgs())
	for i := 0; i < l-1; i += 2 {
		condition, isNull, err = args[i].EvalInt(b.Ctx, row)
		if err != nil {
			return nil, isNull, err
		}
		if isNull || condition == 0 {
			continue
		}
		ret, isNull, err = args[i+1].EvalDecimal(b.Ctx, row)
		return ret, isNull, err
	}
	// when clause(condition, result) -> args[i], args[i+1]; (i >= 0 && i+1 < l-1)
	// else clause -> args[l-1]
	// If case clause has else clause, l%2 == 1.
	if l%2 == 1 {
		ret, isNull, err = args[l-1].EvalDecimal(b.Ctx, row)
		return ret, isNull, err
	}
	return ret, true, nil
}

type builtinCaseWhenStringSig struct {
	BaseBuiltinFunc
}

func (b *builtinCaseWhenStringSig) Clone() BuiltinFunc {
	newSig := &builtinCaseWhenStringSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalString evals a builtinCaseWhenStringSig.
// See https://dev.mysql.com/doc/refman/5.7/en/case.html
func (b *builtinCaseWhenStringSig) EvalString(row chunk.Row) (ret string, isNull bool, err error) {
	var condition int64
	args, l := b.getArgs(), len(b.getArgs())
	for i := 0; i < l-1; i += 2 {
		condition, isNull, err = args[i].EvalInt(b.Ctx, row)
		if err != nil {
			return "", isNull, err
		}
		if isNull || condition == 0 {
			continue
		}
		ret, isNull, err = args[i+1].EvalString(b.Ctx, row)
		return ret, isNull, err
	}
	// when clause(condition, result) -> args[i], args[i+1]; (i >= 0 && i+1 < l-1)
	// else clause -> args[l-1]
	// If case clause has else clause, l%2 == 1.
	if l%2 == 1 {
		ret, isNull, err = args[l-1].EvalString(b.Ctx, row)
		return ret, isNull, err
	}
	return ret, true, nil
}

type builtinCaseWhenTimeSig struct {
	BaseBuiltinFunc
}

func (b *builtinCaseWhenTimeSig) Clone() BuiltinFunc {
	newSig := &builtinCaseWhenTimeSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalTime evals a builtinCaseWhenTimeSig.
// See https://dev.mysql.com/doc/refman/5.7/en/case.html
func (b *builtinCaseWhenTimeSig) evalTime(row chunk.Row) (ret types.Time, isNull bool, err error) {
	var condition int64
	args, l := b.getArgs(), len(b.getArgs())
	for i := 0; i < l-1; i += 2 {
		condition, isNull, err = args[i].EvalInt(b.Ctx, row)
		if err != nil {
			return ret, isNull, err
		}
		if isNull || condition == 0 {
			continue
		}
		ret, isNull, err = args[i+1].EvalTime(b.Ctx, row)
		return ret, isNull, err
	}
	// when clause(condition, result) -> args[i], args[i+1]; (i >= 0 && i+1 < l-1)
	// else clause -> args[l-1]
	// If case clause has else clause, l%2 == 1.
	if l%2 == 1 {
		ret, isNull, err = args[l-1].EvalTime(b.Ctx, row)
		return ret, isNull, err
	}
	return ret, true, nil
}

type builtinCaseWhenDurationSig struct {
	BaseBuiltinFunc
}

func (b *builtinCaseWhenDurationSig) Clone() BuiltinFunc {
	newSig := &builtinCaseWhenDurationSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalDuration evals a builtinCaseWhenDurationSig.
// See https://dev.mysql.com/doc/refman/5.7/en/case.html
func (b *builtinCaseWhenDurationSig) evalDuration(row chunk.Row) (ret types.Duration, isNull bool, err error) {
	var condition int64
	args, l := b.getArgs(), len(b.getArgs())
	for i := 0; i < l-1; i += 2 {
		condition, isNull, err = args[i].EvalInt(b.Ctx, row)
		if err != nil {
			return ret, true, err
		}
		if isNull || condition == 0 {
			continue
		}
		ret, isNull, err = args[i+1].EvalDuration(b.Ctx, row)
		return ret, isNull, err
	}
	// when clause(condition, result) -> args[i], args[i+1]; (i >= 0 && i+1 < l-1)
	// else clause -> args[l-1]
	// If case clause has else clause, l%2 == 1.
	if l%2 == 1 {
		ret, isNull, err = args[l-1].EvalDuration(b.Ctx, row)
		return ret, isNull, err
	}
	return ret, true, nil
}

type builtinCaseWhenJSONSig struct {
	BaseBuiltinFunc
}

func (b *builtinCaseWhenJSONSig) Clone() BuiltinFunc {
	newSig := &builtinCaseWhenJSONSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalJSON evals a builtinCaseWhenJSONSig.
// See https://dev.mysql.com/doc/refman/5.7/en/case.html
func (b *builtinCaseWhenJSONSig) evalJSON(row chunk.Row) (ret json.BinaryJSON, isNull bool, err error) {
	var condition int64
	args, l := b.getArgs(), len(b.getArgs())
	for i := 0; i < l-1; i += 2 {
		condition, isNull, err = args[i].EvalInt(b.Ctx, row)
		if err != nil {
			return
		}
		if isNull || condition == 0 {
			continue
		}
		return args[i+1].EvalJSON(b.Ctx, row)
	}
	// when clause(condition, result) -> args[i], args[i+1]; (i >= 0 && i+1 < l-1)
	// else clause -> args[l-1]
	// If case clause has else clause, l%2 == 1.
	if l%2 == 1 {
		return args[l-1].EvalJSON(b.Ctx, row)
	}
	return ret, true, nil
}

type ifFunctionClass struct {
	BaseFunctionClass
}

// getFunction see https://dev.mysql.com/doc/refman/5.7/en/control-flow-functions.html#function_if
func (c *ifFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (sig BuiltinFunc, err error) {
	if err = c.VerifyArgs(args); err != nil {
		return nil, err
	}
	retTp := InferType4ControlFuncs(args[1].GetType(), args[2].GetType())
	evalTps := retTp.EvalType()
	bf := NewBaseBuiltinFuncWithTp(ctx, args, evalTps, types.ETInt, evalTps, evalTps)
	retTp.Flag |= bf.Tp.Flag
	bf.Tp = retTp
	switch evalTps {
	case types.ETInt:
		sig = &builtinIfIntSig{bf}
		sig.SetPbCode(tipb.ScalarFuncSig_IfInt)
	case types.ETReal:
		sig = &builtinIfRealSig{bf}
		sig.SetPbCode(tipb.ScalarFuncSig_IfReal)
	case types.ETDecimal:
		sig = &builtinIfDecimalSig{bf}
		sig.SetPbCode(tipb.ScalarFuncSig_IfDecimal)
	case types.ETString:
		sig = &builtinIfStringSig{bf}
		sig.SetPbCode(tipb.ScalarFuncSig_IfString)
	case types.ETDatetime, types.ETTimestamp:
		sig = &builtinIfTimeSig{bf}
		sig.SetPbCode(tipb.ScalarFuncSig_IfTime)
	case types.ETDuration:
		sig = &builtinIfDurationSig{bf}
		sig.SetPbCode(tipb.ScalarFuncSig_IfDuration)
	case types.ETJson:
		sig = &builtinIfJSONSig{bf}
		sig.SetPbCode(tipb.ScalarFuncSig_IfJson)
	}
	return sig, nil
}

type builtinIfIntSig struct {
	BaseBuiltinFunc
}

func (b *builtinIfIntSig) Clone() BuiltinFunc {
	newSig := &builtinIfIntSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinIfIntSig) evalInt(row chunk.Row) (ret int64, isNull bool, err error) {
	arg0, isNull0, err := b.Args[0].EvalInt(b.Ctx, row)
	if err != nil {
		return 0, true, err
	}
	arg1, isNull1, err := b.Args[1].EvalInt(b.Ctx, row)
	if (!isNull0 && arg0 != 0) || err != nil {
		return arg1, isNull1, err
	}
	arg2, isNull2, err := b.Args[2].EvalInt(b.Ctx, row)
	return arg2, isNull2, err
}

type builtinIfRealSig struct {
	BaseBuiltinFunc
}

func (b *builtinIfRealSig) Clone() BuiltinFunc {
	newSig := &builtinIfRealSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinIfRealSig) evalReal(row chunk.Row) (ret float64, isNull bool, err error) {
	arg0, isNull0, err := b.Args[0].EvalInt(b.Ctx, row)
	if err != nil {
		return 0, true, err
	}
	arg1, isNull1, err := b.Args[1].EvalReal(b.Ctx, row)
	if (!isNull0 && arg0 != 0) || err != nil {
		return arg1, isNull1, err
	}
	arg2, isNull2, err := b.Args[2].EvalReal(b.Ctx, row)
	return arg2, isNull2, err
}

type builtinIfDecimalSig struct {
	BaseBuiltinFunc
}

func (b *builtinIfDecimalSig) Clone() BuiltinFunc {
	newSig := &builtinIfDecimalSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinIfDecimalSig) evalDecimal(row chunk.Row) (ret *types.MyDecimal, isNull bool, err error) {
	arg0, isNull0, err := b.Args[0].EvalInt(b.Ctx, row)
	if err != nil {
		return nil, true, err
	}
	arg1, isNull1, err := b.Args[1].EvalDecimal(b.Ctx, row)
	if (!isNull0 && arg0 != 0) || err != nil {
		return arg1, isNull1, err
	}
	arg2, isNull2, err := b.Args[2].EvalDecimal(b.Ctx, row)
	return arg2, isNull2, err
}

type builtinIfStringSig struct {
	BaseBuiltinFunc
}

func (b *builtinIfStringSig) Clone() BuiltinFunc {
	newSig := &builtinIfStringSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinIfStringSig) EvalString(row chunk.Row) (ret string, isNull bool, err error) {
	arg0, isNull0, err := b.Args[0].EvalInt(b.Ctx, row)
	if err != nil {
		return "", true, err
	}
	arg1, isNull1, err := b.Args[1].EvalString(b.Ctx, row)
	if (!isNull0 && arg0 != 0) || err != nil {
		return arg1, isNull1, err
	}
	arg2, isNull2, err := b.Args[2].EvalString(b.Ctx, row)
	return arg2, isNull2, err
}

type builtinIfTimeSig struct {
	BaseBuiltinFunc
}

func (b *builtinIfTimeSig) Clone() BuiltinFunc {
	newSig := &builtinIfTimeSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinIfTimeSig) evalTime(row chunk.Row) (ret types.Time, isNull bool, err error) {
	arg0, isNull0, err := b.Args[0].EvalInt(b.Ctx, row)
	if err != nil {
		return ret, true, err
	}
	arg1, isNull1, err := b.Args[1].EvalTime(b.Ctx, row)
	if (!isNull0 && arg0 != 0) || err != nil {
		return arg1, isNull1, err
	}
	arg2, isNull2, err := b.Args[2].EvalTime(b.Ctx, row)
	return arg2, isNull2, err
}

type builtinIfDurationSig struct {
	BaseBuiltinFunc
}

func (b *builtinIfDurationSig) Clone() BuiltinFunc {
	newSig := &builtinIfDurationSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinIfDurationSig) evalDuration(row chunk.Row) (ret types.Duration, isNull bool, err error) {
	arg0, isNull0, err := b.Args[0].EvalInt(b.Ctx, row)
	if err != nil {
		return ret, true, err
	}
	arg1, isNull1, err := b.Args[1].EvalDuration(b.Ctx, row)
	if (!isNull0 && arg0 != 0) || err != nil {
		return arg1, isNull1, err
	}
	arg2, isNull2, err := b.Args[2].EvalDuration(b.Ctx, row)
	return arg2, isNull2, err
}

type builtinIfJSONSig struct {
	BaseBuiltinFunc
}

func (b *builtinIfJSONSig) Clone() BuiltinFunc {
	newSig := &builtinIfJSONSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinIfJSONSig) evalJSON(row chunk.Row) (ret json.BinaryJSON, isNull bool, err error) {
	arg0, isNull0, err := b.Args[0].EvalInt(b.Ctx, row)
	if err != nil {
		return ret, true, err
	}
	arg1, isNull1, err := b.Args[1].EvalJSON(b.Ctx, row)
	if err != nil {
		return ret, true, err
	}
	arg2, isNull2, err := b.Args[2].EvalJSON(b.Ctx, row)
	if err != nil {
		return ret, true, err
	}
	switch {
	case isNull0 || arg0 == 0:
		ret, isNull = arg2, isNull2
	case arg0 != 0:
		ret, isNull = arg1, isNull1
	}
	return
}

type ifNullFunctionClass struct {
	BaseFunctionClass
}

func (c *ifNullFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (sig BuiltinFunc, err error) {
	if err = c.VerifyArgs(args); err != nil {
		return nil, err
	}
	lhs, rhs := args[0].GetType(), args[1].GetType()
	retTp := InferType4ControlFuncs(lhs, rhs)
	retTp.Flag |= (lhs.Flag & mysql.NotNullFlag) | (rhs.Flag & mysql.NotNullFlag)
	if lhs.Tp == mysql.TypeNull && rhs.Tp == mysql.TypeNull {
		retTp.Tp = mysql.TypeNull
		retTp.Flen, retTp.Decimal = 0, -1
		types.SetBinChsClnFlag(retTp)
	}
	evalTps := retTp.EvalType()
	bf := NewBaseBuiltinFuncWithTp(ctx, args, evalTps, evalTps, evalTps)
	bf.Tp = retTp
	switch evalTps {
	case types.ETInt:
		sig = &builtinIfNullIntSig{bf}
		sig.SetPbCode(tipb.ScalarFuncSig_IfNullInt)
	case types.ETReal:
		sig = &builtinIfNullRealSig{bf}
		sig.SetPbCode(tipb.ScalarFuncSig_IfNullReal)
	case types.ETDecimal:
		sig = &builtinIfNullDecimalSig{bf}
		sig.SetPbCode(tipb.ScalarFuncSig_IfNullDecimal)
	case types.ETString:
		sig = &builtinIfNullStringSig{bf}
		sig.SetPbCode(tipb.ScalarFuncSig_IfNullString)
	case types.ETDatetime, types.ETTimestamp:
		sig = &builtinIfNullTimeSig{bf}
		sig.SetPbCode(tipb.ScalarFuncSig_IfNullTime)
	case types.ETDuration:
		sig = &builtinIfNullDurationSig{bf}
		sig.SetPbCode(tipb.ScalarFuncSig_IfNullDuration)
	case types.ETJson:
		sig = &builtinIfNullJSONSig{bf}
		sig.SetPbCode(tipb.ScalarFuncSig_IfNullJson)
	}
	return sig, nil
}

type builtinIfNullIntSig struct {
	BaseBuiltinFunc
}

func (b *builtinIfNullIntSig) Clone() BuiltinFunc {
	newSig := &builtinIfNullIntSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinIfNullIntSig) evalInt(row chunk.Row) (int64, bool, error) {
	arg0, isNull, err := b.Args[0].EvalInt(b.Ctx, row)
	if !isNull || err != nil {
		return arg0, err != nil, err
	}
	arg1, isNull, err := b.Args[1].EvalInt(b.Ctx, row)
	return arg1, isNull || err != nil, err
}

type builtinIfNullRealSig struct {
	BaseBuiltinFunc
}

func (b *builtinIfNullRealSig) Clone() BuiltinFunc {
	newSig := &builtinIfNullRealSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinIfNullRealSig) evalReal(row chunk.Row) (float64, bool, error) {
	arg0, isNull, err := b.Args[0].EvalReal(b.Ctx, row)
	if !isNull || err != nil {
		return arg0, err != nil, err
	}
	arg1, isNull, err := b.Args[1].EvalReal(b.Ctx, row)
	return arg1, isNull || err != nil, err
}

type builtinIfNullDecimalSig struct {
	BaseBuiltinFunc
}

func (b *builtinIfNullDecimalSig) Clone() BuiltinFunc {
	newSig := &builtinIfNullDecimalSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinIfNullDecimalSig) evalDecimal(row chunk.Row) (*types.MyDecimal, bool, error) {
	arg0, isNull, err := b.Args[0].EvalDecimal(b.Ctx, row)
	if !isNull || err != nil {
		return arg0, err != nil, err
	}
	arg1, isNull, err := b.Args[1].EvalDecimal(b.Ctx, row)
	return arg1, isNull || err != nil, err
}

type builtinIfNullStringSig struct {
	BaseBuiltinFunc
}

func (b *builtinIfNullStringSig) Clone() BuiltinFunc {
	newSig := &builtinIfNullStringSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinIfNullStringSig) EvalString(row chunk.Row) (string, bool, error) {
	arg0, isNull, err := b.Args[0].EvalString(b.Ctx, row)
	if !isNull || err != nil {
		return arg0, err != nil, err
	}
	arg1, isNull, err := b.Args[1].EvalString(b.Ctx, row)
	return arg1, isNull || err != nil, err
}

type builtinIfNullTimeSig struct {
	BaseBuiltinFunc
}

func (b *builtinIfNullTimeSig) Clone() BuiltinFunc {
	newSig := &builtinIfNullTimeSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinIfNullTimeSig) evalTime(row chunk.Row) (types.Time, bool, error) {
	arg0, isNull, err := b.Args[0].EvalTime(b.Ctx, row)
	if !isNull || err != nil {
		return arg0, err != nil, err
	}
	arg1, isNull, err := b.Args[1].EvalTime(b.Ctx, row)
	return arg1, isNull || err != nil, err
}

type builtinIfNullDurationSig struct {
	BaseBuiltinFunc
}

func (b *builtinIfNullDurationSig) Clone() BuiltinFunc {
	newSig := &builtinIfNullDurationSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinIfNullDurationSig) evalDuration(row chunk.Row) (types.Duration, bool, error) {
	arg0, isNull, err := b.Args[0].EvalDuration(b.Ctx, row)
	if !isNull || err != nil {
		return arg0, err != nil, err
	}
	arg1, isNull, err := b.Args[1].EvalDuration(b.Ctx, row)
	return arg1, isNull || err != nil, err
}

type builtinIfNullJSONSig struct {
	BaseBuiltinFunc
}

func (b *builtinIfNullJSONSig) Clone() BuiltinFunc {
	newSig := &builtinIfNullJSONSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinIfNullJSONSig) evalJSON(row chunk.Row) (json.BinaryJSON, bool, error) {
	arg0, isNull, err := b.Args[0].EvalJSON(b.Ctx, row)
	if !isNull {
		return arg0, err != nil, err
	}
	arg1, isNull, err := b.Args[1].EvalJSON(b.Ctx, row)
	return arg1, isNull || err != nil, err
}
