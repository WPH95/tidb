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
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/types/json"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/stringutil"
	"github.com/pingcap/tipb/go-tipb"
)

var (
	_ FunctionClass = &inFunctionClass{}
	_ FunctionClass = &rowFunctionClass{}
	_ FunctionClass = &setVarFunctionClass{}
	_ FunctionClass = &getVarFunctionClass{}
	_ FunctionClass = &lockFunctionClass{}
	_ FunctionClass = &releaseLockFunctionClass{}
	_ FunctionClass = &valuesFunctionClass{}
	_ FunctionClass = &bitCountFunctionClass{}
	_ FunctionClass = &getParamFunctionClass{}
)

var (
	_ BuiltinFunc = &builtinSleepSig{}
	_ BuiltinFunc = &builtinInIntSig{}
	_ BuiltinFunc = &builtinInStringSig{}
	_ BuiltinFunc = &builtinInDecimalSig{}
	_ BuiltinFunc = &builtinInRealSig{}
	_ BuiltinFunc = &builtinInTimeSig{}
	_ BuiltinFunc = &builtinInDurationSig{}
	_ BuiltinFunc = &builtinInJSONSig{}
	_ BuiltinFunc = &builtinRowSig{}
	_ BuiltinFunc = &builtinSetVarSig{}
	_ BuiltinFunc = &builtinGetVarSig{}
	_ BuiltinFunc = &builtinLockSig{}
	_ BuiltinFunc = &builtinReleaseLockSig{}
	_ BuiltinFunc = &builtinValuesIntSig{}
	_ BuiltinFunc = &builtinValuesRealSig{}
	_ BuiltinFunc = &builtinValuesDecimalSig{}
	_ BuiltinFunc = &builtinValuesStringSig{}
	_ BuiltinFunc = &builtinValuesTimeSig{}
	_ BuiltinFunc = &builtinValuesDurationSig{}
	_ BuiltinFunc = &builtinValuesJSONSig{}
	_ BuiltinFunc = &builtinBitCountSig{}
	_ BuiltinFunc = &builtinGetParamStringSig{}
)

type inFunctionClass struct {
	BaseFunctionClass
}

func (c *inFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (sig BuiltinFunc, err error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	argTps := make([]types.EvalType, len(args))
	for i := range args {
		argTps[i] = args[0].GetType().EvalType()
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETInt, argTps...)
	bf.Tp.Flen = 1
	switch args[0].GetType().EvalType() {
	case types.ETInt:
		sig = &builtinInIntSig{BaseBuiltinFunc: bf}
		sig.SetPbCode(tipb.ScalarFuncSig_InInt)
	case types.ETString:
		sig = &builtinInStringSig{BaseBuiltinFunc: bf}
		sig.SetPbCode(tipb.ScalarFuncSig_InString)
	case types.ETReal:
		sig = &builtinInRealSig{BaseBuiltinFunc: bf}
		sig.SetPbCode(tipb.ScalarFuncSig_InReal)
	case types.ETDecimal:
		sig = &builtinInDecimalSig{BaseBuiltinFunc: bf}
		sig.SetPbCode(tipb.ScalarFuncSig_InDecimal)
	case types.ETDatetime, types.ETTimestamp:
		sig = &builtinInTimeSig{BaseBuiltinFunc: bf}
		sig.SetPbCode(tipb.ScalarFuncSig_InTime)
	case types.ETDuration:
		sig = &builtinInDurationSig{BaseBuiltinFunc: bf}
		sig.SetPbCode(tipb.ScalarFuncSig_InDuration)
	case types.ETJson:
		sig = &builtinInJSONSig{BaseBuiltinFunc: bf}
		sig.SetPbCode(tipb.ScalarFuncSig_InJson)
	}
	return sig, nil
}

// builtinInIntSig see https://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#function_in
type builtinInIntSig struct {
	BaseBuiltinFunc
}

func (b *builtinInIntSig) Clone() BuiltinFunc {
	newSig := &builtinInIntSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinInIntSig) evalInt(row chunk.Row) (int64, bool, error) {
	arg0, isNull0, err := b.Args[0].EvalInt(b.Ctx, row)
	if isNull0 || err != nil {
		return 0, isNull0, err
	}
	isUnsigned0 := mysql.HasUnsignedFlag(b.Args[0].GetType().Flag)
	var hasNull bool
	for _, arg := range b.Args[1:] {
		evaledArg, isNull, err := arg.EvalInt(b.Ctx, row)
		if err != nil {
			return 0, true, err
		}
		if isNull {
			hasNull = true
			continue
		}
		isUnsigned := mysql.HasUnsignedFlag(arg.GetType().Flag)
		if isUnsigned0 && isUnsigned {
			if evaledArg == arg0 {
				return 1, false, nil
			}
		} else if !isUnsigned0 && !isUnsigned {
			if evaledArg == arg0 {
				return 1, false, nil
			}
		} else if !isUnsigned0 && isUnsigned {
			if arg0 >= 0 && evaledArg == arg0 {
				return 1, false, nil
			}
		} else {
			if evaledArg >= 0 && evaledArg == arg0 {
				return 1, false, nil
			}
		}
	}
	return 0, hasNull, nil
}

// builtinInStringSig see https://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#function_in
type builtinInStringSig struct {
	BaseBuiltinFunc
}

func (b *builtinInStringSig) Clone() BuiltinFunc {
	newSig := &builtinInStringSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinInStringSig) evalInt(row chunk.Row) (int64, bool, error) {
	arg0, isNull0, err := b.Args[0].EvalString(b.Ctx, row)
	if isNull0 || err != nil {
		return 0, isNull0, err
	}
	var hasNull bool
	for _, arg := range b.Args[1:] {
		evaledArg, isNull, err := arg.EvalString(b.Ctx, row)
		if err != nil {
			return 0, true, err
		}
		if isNull {
			hasNull = true
			continue
		}
		if arg0 == evaledArg {
			return 1, false, nil
		}
	}
	return 0, hasNull, nil
}

// builtinInRealSig see https://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#function_in
type builtinInRealSig struct {
	BaseBuiltinFunc
}

func (b *builtinInRealSig) Clone() BuiltinFunc {
	newSig := &builtinInRealSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinInRealSig) evalInt(row chunk.Row) (int64, bool, error) {
	arg0, isNull0, err := b.Args[0].EvalReal(b.Ctx, row)
	if isNull0 || err != nil {
		return 0, isNull0, err
	}
	var hasNull bool
	for _, arg := range b.Args[1:] {
		evaledArg, isNull, err := arg.EvalReal(b.Ctx, row)
		if err != nil {
			return 0, true, err
		}
		if isNull {
			hasNull = true
			continue
		}
		if arg0 == evaledArg {
			return 1, false, nil
		}
	}
	return 0, hasNull, nil
}

// builtinInDecimalSig see https://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#function_in
type builtinInDecimalSig struct {
	BaseBuiltinFunc
}

func (b *builtinInDecimalSig) Clone() BuiltinFunc {
	newSig := &builtinInDecimalSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinInDecimalSig) evalInt(row chunk.Row) (int64, bool, error) {
	arg0, isNull0, err := b.Args[0].EvalDecimal(b.Ctx, row)
	if isNull0 || err != nil {
		return 0, isNull0, err
	}
	var hasNull bool
	for _, arg := range b.Args[1:] {
		evaledArg, isNull, err := arg.EvalDecimal(b.Ctx, row)
		if err != nil {
			return 0, true, err
		}
		if isNull {
			hasNull = true
			continue
		}
		if arg0.Compare(evaledArg) == 0 {
			return 1, false, nil
		}
	}
	return 0, hasNull, nil
}

// builtinInTimeSig see https://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#function_in
type builtinInTimeSig struct {
	BaseBuiltinFunc
}

func (b *builtinInTimeSig) Clone() BuiltinFunc {
	newSig := &builtinInTimeSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinInTimeSig) evalInt(row chunk.Row) (int64, bool, error) {
	arg0, isNull0, err := b.Args[0].EvalTime(b.Ctx, row)
	if isNull0 || err != nil {
		return 0, isNull0, err
	}
	var hasNull bool
	for _, arg := range b.Args[1:] {
		evaledArg, isNull, err := arg.EvalTime(b.Ctx, row)
		if err != nil {
			return 0, true, err
		}
		if isNull {
			hasNull = true
			continue
		}
		if arg0.Compare(evaledArg) == 0 {
			return 1, false, nil
		}
	}
	return 0, hasNull, nil
}

// builtinInDurationSig see https://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#function_in
type builtinInDurationSig struct {
	BaseBuiltinFunc
}

func (b *builtinInDurationSig) Clone() BuiltinFunc {
	newSig := &builtinInDurationSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinInDurationSig) evalInt(row chunk.Row) (int64, bool, error) {
	arg0, isNull0, err := b.Args[0].EvalDuration(b.Ctx, row)
	if isNull0 || err != nil {
		return 0, isNull0, err
	}
	var hasNull bool
	for _, arg := range b.Args[1:] {
		evaledArg, isNull, err := arg.EvalDuration(b.Ctx, row)
		if err != nil {
			return 0, true, err
		}
		if isNull {
			hasNull = true
			continue
		}
		if arg0.Compare(evaledArg) == 0 {
			return 1, false, nil
		}
	}
	return 0, hasNull, nil
}

// builtinInJSONSig see https://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#function_in
type builtinInJSONSig struct {
	BaseBuiltinFunc
}

func (b *builtinInJSONSig) Clone() BuiltinFunc {
	newSig := &builtinInJSONSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinInJSONSig) evalInt(row chunk.Row) (int64, bool, error) {
	arg0, isNull0, err := b.Args[0].EvalJSON(b.Ctx, row)
	if isNull0 || err != nil {
		return 0, isNull0, err
	}
	var hasNull bool
	for _, arg := range b.Args[1:] {
		evaledArg, isNull, err := arg.EvalJSON(b.Ctx, row)
		if err != nil {
			return 0, true, err
		}
		if isNull {
			hasNull = true
			continue
		}
		result := json.CompareBinary(evaledArg, arg0)
		if result == 0 {
			return 1, false, nil
		}
	}
	return 0, hasNull, nil
}

type rowFunctionClass struct {
	BaseFunctionClass
}

func (c *rowFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (sig BuiltinFunc, err error) {
	if err = c.VerifyArgs(args); err != nil {
		return nil, err
	}
	argTps := make([]types.EvalType, len(args))
	for i := range argTps {
		argTps[i] = args[i].GetType().EvalType()
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETString, argTps...)
	sig = &builtinRowSig{bf}
	return sig, nil
}

type builtinRowSig struct {
	BaseBuiltinFunc
}

func (b *builtinRowSig) Clone() BuiltinFunc {
	newSig := &builtinRowSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalString rowFunc should always be flattened in expression rewrite phrase.
func (b *builtinRowSig) EvalString(row chunk.Row) (string, bool, error) {
	panic("builtinRowSig.evalString() should never be called.")
}

type setVarFunctionClass struct {
	BaseFunctionClass
}

func (c *setVarFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (sig BuiltinFunc, err error) {
	if err = c.VerifyArgs(args); err != nil {
		return nil, err
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETString, types.ETString, types.ETString)
	bf.Tp.Flen = args[1].GetType().Flen
	// TODO: we should consider the type of the argument, but not take it as string for all situations.
	sig = &builtinSetVarSig{bf}
	return sig, err
}

type builtinSetVarSig struct {
	BaseBuiltinFunc
}

func (b *builtinSetVarSig) Clone() BuiltinFunc {
	newSig := &builtinSetVarSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinSetVarSig) EvalString(row chunk.Row) (res string, isNull bool, err error) {
	var varName string
	sessionVars := b.Ctx.GetSessionVars()
	varName, isNull, err = b.Args[0].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	res, isNull, err = b.Args[1].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	varName = strings.ToLower(varName)
	sessionVars.UsersLock.Lock()
	sessionVars.Users[varName] = stringutil.Copy(res)
	sessionVars.UsersLock.Unlock()
	return res, false, nil
}

type getVarFunctionClass struct {
	BaseFunctionClass
}

func (c *getVarFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (sig BuiltinFunc, err error) {
	if err = c.VerifyArgs(args); err != nil {
		return nil, err
	}
	// TODO: we should consider the type of the argument, but not take it as string for all situations.
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETString, types.ETString)
	bf.Tp.Flen = mysql.MaxFieldVarCharLength
	sig = &builtinGetVarSig{bf}
	return sig, nil
}

type builtinGetVarSig struct {
	BaseBuiltinFunc
}

func (b *builtinGetVarSig) Clone() BuiltinFunc {
	newSig := &builtinGetVarSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinGetVarSig) EvalString(row chunk.Row) (string, bool, error) {
	sessionVars := b.Ctx.GetSessionVars()
	varName, isNull, err := b.Args[0].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	varName = strings.ToLower(varName)
	sessionVars.UsersLock.RLock()
	defer sessionVars.UsersLock.RUnlock()
	if v, ok := sessionVars.Users[varName]; ok {
		return v, false, nil
	}
	return "", true, nil
}

type valuesFunctionClass struct {
	BaseFunctionClass

	offset int
	tp     *types.FieldType
}

func (c *valuesFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (sig BuiltinFunc, err error) {
	if err = c.VerifyArgs(args); err != nil {
		return nil, err
	}
	bf := newBaseBuiltinFunc(ctx, args)
	bf.Tp = c.tp
	switch c.tp.EvalType() {
	case types.ETInt:
		sig = &builtinValuesIntSig{bf, c.offset}
	case types.ETReal:
		sig = &builtinValuesRealSig{bf, c.offset}
	case types.ETDecimal:
		sig = &builtinValuesDecimalSig{bf, c.offset}
	case types.ETString:
		sig = &builtinValuesStringSig{bf, c.offset}
	case types.ETDatetime, types.ETTimestamp:
		sig = &builtinValuesTimeSig{bf, c.offset}
	case types.ETDuration:
		sig = &builtinValuesDurationSig{bf, c.offset}
	case types.ETJson:
		sig = &builtinValuesJSONSig{bf, c.offset}
	}
	return sig, nil
}

type builtinValuesIntSig struct {
	BaseBuiltinFunc

	offset int
}

func (b *builtinValuesIntSig) Clone() BuiltinFunc {
	newSig := &builtinValuesIntSig{offset: b.offset}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalInt evals a builtinValuesIntSig.
// See https://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_values
func (b *builtinValuesIntSig) evalInt(_ chunk.Row) (int64, bool, error) {
	if !b.Ctx.GetSessionVars().StmtCtx.InInsertStmt {
		return 0, true, nil
	}
	row := b.Ctx.GetSessionVars().CurrInsertValues
	if row.IsEmpty() {
		return 0, true, errors.New("Session current insert values is nil")
	}
	if b.offset < row.Len() {
		if row.IsNull(b.offset) {
			return 0, true, nil
		}
		return row.GetInt64(b.offset), false, nil
	}
	return 0, true, errors.Errorf("Session current insert values len %d and column's offset %v don't match", row.Len(), b.offset)
}

type builtinValuesRealSig struct {
	BaseBuiltinFunc

	offset int
}

func (b *builtinValuesRealSig) Clone() BuiltinFunc {
	newSig := &builtinValuesRealSig{offset: b.offset}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalReal evals a builtinValuesRealSig.
// See https://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_values
func (b *builtinValuesRealSig) evalReal(_ chunk.Row) (float64, bool, error) {
	if !b.Ctx.GetSessionVars().StmtCtx.InInsertStmt {
		return 0, true, nil
	}
	row := b.Ctx.GetSessionVars().CurrInsertValues
	if row.IsEmpty() {
		return 0, true, errors.New("Session current insert values is nil")
	}
	if b.offset < row.Len() {
		if row.IsNull(b.offset) {
			return 0, true, nil
		}
		if b.getRetTp().Tp == mysql.TypeFloat {
			return float64(row.GetFloat32(b.offset)), false, nil
		}
		return row.GetFloat64(b.offset), false, nil
	}
	return 0, true, errors.Errorf("Session current insert values len %d and column's offset %v don't match", row.Len(), b.offset)
}

type builtinValuesDecimalSig struct {
	BaseBuiltinFunc

	offset int
}

func (b *builtinValuesDecimalSig) Clone() BuiltinFunc {
	newSig := &builtinValuesDecimalSig{offset: b.offset}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalDecimal evals a builtinValuesDecimalSig.
// See https://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_values
func (b *builtinValuesDecimalSig) evalDecimal(_ chunk.Row) (*types.MyDecimal, bool, error) {
	if !b.Ctx.GetSessionVars().StmtCtx.InInsertStmt {
		return nil, true, nil
	}
	row := b.Ctx.GetSessionVars().CurrInsertValues
	if row.IsEmpty() {
		return nil, true, errors.New("Session current insert values is nil")
	}
	if b.offset < row.Len() {
		if row.IsNull(b.offset) {
			return nil, true, nil
		}
		return row.GetMyDecimal(b.offset), false, nil
	}
	return nil, true, errors.Errorf("Session current insert values len %d and column's offset %v don't match", row.Len(), b.offset)
}

type builtinValuesStringSig struct {
	BaseBuiltinFunc

	offset int
}

func (b *builtinValuesStringSig) Clone() BuiltinFunc {
	newSig := &builtinValuesStringSig{offset: b.offset}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalString evals a builtinValuesStringSig.
// See https://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_values
func (b *builtinValuesStringSig) EvalString(_ chunk.Row) (string, bool, error) {
	if !b.Ctx.GetSessionVars().StmtCtx.InInsertStmt {
		return "", true, nil
	}
	row := b.Ctx.GetSessionVars().CurrInsertValues
	if row.IsEmpty() {
		return "", true, errors.New("Session current insert values is nil")
	}
	if b.offset >= row.Len() {
		return "", true, errors.Errorf("Session current insert values len %d and column's offset %v don't match", row.Len(), b.offset)
	}

	if row.IsNull(b.offset) {
		return "", true, nil
	}

	// Specially handle the ENUM/SET/BIT input value.
	if retType := b.getRetTp(); retType.Hybrid() {
		val := row.GetDatum(b.offset, retType)
		res, err := val.ToString()
		return res, err != nil, err
	}

	return row.GetString(b.offset), false, nil
}

type builtinValuesTimeSig struct {
	BaseBuiltinFunc

	offset int
}

func (b *builtinValuesTimeSig) Clone() BuiltinFunc {
	newSig := &builtinValuesTimeSig{offset: b.offset}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalTime evals a builtinValuesTimeSig.
// See https://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_values
func (b *builtinValuesTimeSig) evalTime(_ chunk.Row) (types.Time, bool, error) {
	if !b.Ctx.GetSessionVars().StmtCtx.InInsertStmt {
		return types.Time{}, true, nil
	}
	row := b.Ctx.GetSessionVars().CurrInsertValues
	if row.IsEmpty() {
		return types.Time{}, true, errors.New("Session current insert values is nil")
	}
	if b.offset < row.Len() {
		if row.IsNull(b.offset) {
			return types.Time{}, true, nil
		}
		return row.GetTime(b.offset), false, nil
	}
	return types.Time{}, true, errors.Errorf("Session current insert values len %d and column's offset %v don't match", row.Len(), b.offset)
}

type builtinValuesDurationSig struct {
	BaseBuiltinFunc

	offset int
}

func (b *builtinValuesDurationSig) Clone() BuiltinFunc {
	newSig := &builtinValuesDurationSig{offset: b.offset}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalDuration evals a builtinValuesDurationSig.
// See https://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_values
func (b *builtinValuesDurationSig) evalDuration(_ chunk.Row) (types.Duration, bool, error) {
	if !b.Ctx.GetSessionVars().StmtCtx.InInsertStmt {
		return types.Duration{}, true, nil
	}
	row := b.Ctx.GetSessionVars().CurrInsertValues
	if row.IsEmpty() {
		return types.Duration{}, true, errors.New("Session current insert values is nil")
	}
	if b.offset < row.Len() {
		if row.IsNull(b.offset) {
			return types.Duration{}, true, nil
		}
		duration := row.GetDuration(b.offset, b.getRetTp().Decimal)
		return duration, false, nil
	}
	return types.Duration{}, true, errors.Errorf("Session current insert values len %d and column's offset %v don't match", row.Len(), b.offset)
}

type builtinValuesJSONSig struct {
	BaseBuiltinFunc

	offset int
}

func (b *builtinValuesJSONSig) Clone() BuiltinFunc {
	newSig := &builtinValuesJSONSig{offset: b.offset}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalJSON evals a builtinValuesJSONSig.
// See https://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_values
func (b *builtinValuesJSONSig) evalJSON(_ chunk.Row) (json.BinaryJSON, bool, error) {
	if !b.Ctx.GetSessionVars().StmtCtx.InInsertStmt {
		return json.BinaryJSON{}, true, nil
	}
	row := b.Ctx.GetSessionVars().CurrInsertValues
	if row.IsEmpty() {
		return json.BinaryJSON{}, true, errors.New("Session current insert values is nil")
	}
	if b.offset < row.Len() {
		if row.IsNull(b.offset) {
			return json.BinaryJSON{}, true, nil
		}
		return row.GetJSON(b.offset), false, nil
	}
	return json.BinaryJSON{}, true, errors.Errorf("Session current insert values len %d and column's offset %v don't match", row.Len(), b.offset)
}

type bitCountFunctionClass struct {
	BaseFunctionClass
}

func (c *bitCountFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETInt)
	bf.Tp.Flen = 2
	sig := &builtinBitCountSig{bf}
	return sig, nil
}

type builtinBitCountSig struct {
	BaseBuiltinFunc
}

func (b *builtinBitCountSig) Clone() BuiltinFunc {
	newSig := &builtinBitCountSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalInt evals BIT_COUNT(N).
// See https://dev.mysql.com/doc/refman/5.7/en/bit-functions.html#function_bit-count
func (b *builtinBitCountSig) evalInt(row chunk.Row) (int64, bool, error) {
	n, isNull, err := b.Args[0].EvalInt(b.Ctx, row)
	if err != nil || isNull {
		if err != nil && types.ErrOverflow.Equal(err) {
			return 64, false, nil
		}
		return 0, true, err
	}

	var count int64
	for ; n != 0; n = (n - 1) & n {
		count++
	}
	return count, false, nil
}

// getParamFunctionClass for plan cache of prepared statements
type getParamFunctionClass struct {
	BaseFunctionClass
}

// getFunction gets function
// TODO: more typed functions will be added when typed parameters are supported.
func (c *getParamFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETString, types.ETInt)
	bf.Tp.Flen = mysql.MaxFieldVarCharLength
	sig := &builtinGetParamStringSig{bf}
	return sig, nil
}

type builtinGetParamStringSig struct {
	BaseBuiltinFunc
}

func (b *builtinGetParamStringSig) Clone() BuiltinFunc {
	newSig := &builtinGetParamStringSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinGetParamStringSig) EvalString(row chunk.Row) (string, bool, error) {
	sessionVars := b.Ctx.GetSessionVars()
	idx, isNull, err := b.Args[0].EvalInt(b.Ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	v := sessionVars.PreparedParams[idx]

	str, err := v.ToString()
	if err != nil {
		return "", true, nil
	}
	return str, false, nil
}
