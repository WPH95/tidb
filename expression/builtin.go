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

//go:generate go run generator/control_vec.go
//go:generate go run generator/time_vec.go

package expression

import (
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/charset"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/opcode"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/types/json"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tipb/go-tipb"
)

// baseBuiltinFunc will be contained in every struct that implement builtinFunc interface.
type BaseBuiltinFunc struct {
	bufAllocator columnBufferAllocator
	Args         []Expression
	Ctx          sessionctx.Context
	Tp           *types.FieldType
	pbCode       tipb.ScalarFuncSig

	childrenVectorizedOnce *sync.Once
	childrenVectorized     bool
}

func (b *BaseBuiltinFunc) PbCode() tipb.ScalarFuncSig {
	return b.pbCode
}

// implicitArgs returns the implicit arguments of this function.
// implicit arguments means some functions contain extra inner fields which will not
// contain in `tipb.Expr.children` but must be pushed down to coprocessor
func (b *BaseBuiltinFunc) implicitArgs() []types.Datum {
	// We will not use a field to store them because of only
	// a few functions contain implicit parameters
	return nil
}

func (b *BaseBuiltinFunc) SetPbCode(c tipb.ScalarFuncSig) {
	b.pbCode = c
}

func newBaseBuiltinFunc(ctx sessionctx.Context, args []Expression) BaseBuiltinFunc {
	if ctx == nil {
		panic("ctx should not be nil")
	}
	return BaseBuiltinFunc{
		bufAllocator:           newLocalSliceBuffer(len(args)),
		childrenVectorizedOnce: new(sync.Once),

		Args: args,
		Ctx:  ctx,
		Tp:   types.NewFieldType(mysql.TypeUnspecified),
	}
}

// newBaseBuiltinFuncWithTp creates a built-in function signature with specified types of arguments and the return type of the function.
// argTps indicates the types of the args, retType indicates the return type of the built-in function.
// Every built-in function needs determined argTps and retType when we create it.
func NewBaseBuiltinFuncWithTp(ctx sessionctx.Context, args []Expression, retType types.EvalType, argTps ...types.EvalType) (bf BaseBuiltinFunc) {
	if len(args) != len(argTps) {
		panic("unexpected length of args and argTps")
	}
	if ctx == nil {
		panic("ctx should not be nil")
	}
	for i := range args {
		switch argTps[i] {
		case types.ETInt:
			args[i] = WrapWithCastAsInt(ctx, args[i])
		case types.ETReal:
			args[i] = WrapWithCastAsReal(ctx, args[i])
		case types.ETDecimal:
			args[i] = WrapWithCastAsDecimal(ctx, args[i])
		case types.ETString:
			args[i] = WrapWithCastAsString(ctx, args[i])
		case types.ETDatetime:
			args[i] = WrapWithCastAsTime(ctx, args[i], types.NewFieldType(mysql.TypeDatetime))
		case types.ETTimestamp:
			args[i] = WrapWithCastAsTime(ctx, args[i], types.NewFieldType(mysql.TypeTimestamp))
		case types.ETDuration:
			args[i] = WrapWithCastAsDuration(ctx, args[i])
		case types.ETJson:
			args[i] = WrapWithCastAsJSON(ctx, args[i])
		}
	}
	var fieldType *types.FieldType
	switch retType {
	case types.ETInt:
		fieldType = &types.FieldType{
			Tp:      mysql.TypeLonglong,
			Flen:    mysql.MaxIntWidth,
			Decimal: 0,
			Flag:    mysql.BinaryFlag,
		}
	case types.ETReal:
		fieldType = &types.FieldType{
			Tp:      mysql.TypeDouble,
			Flen:    mysql.MaxRealWidth,
			Decimal: types.UnspecifiedLength,
			Flag:    mysql.BinaryFlag,
		}
	case types.ETDecimal:
		fieldType = &types.FieldType{
			Tp:      mysql.TypeNewDecimal,
			Flen:    11,
			Decimal: 0,
			Flag:    mysql.BinaryFlag,
		}
	case types.ETString:
		fieldType = &types.FieldType{
			Tp:      mysql.TypeVarString,
			Flen:    0,
			Decimal: types.UnspecifiedLength,
		}
	case types.ETDatetime:
		fieldType = &types.FieldType{
			Tp:      mysql.TypeDatetime,
			Flen:    mysql.MaxDatetimeWidthWithFsp,
			Decimal: int(types.MaxFsp),
			Flag:    mysql.BinaryFlag,
		}
	case types.ETTimestamp:
		fieldType = &types.FieldType{
			Tp:      mysql.TypeTimestamp,
			Flen:    mysql.MaxDatetimeWidthWithFsp,
			Decimal: int(types.MaxFsp),
			Flag:    mysql.BinaryFlag,
		}
	case types.ETDuration:
		fieldType = &types.FieldType{
			Tp:      mysql.TypeDuration,
			Flen:    mysql.MaxDurationWidthWithFsp,
			Decimal: int(types.MaxFsp),
			Flag:    mysql.BinaryFlag,
		}
	case types.ETJson:
		fieldType = &types.FieldType{
			Tp:      mysql.TypeJSON,
			Flen:    mysql.MaxBlobWidth,
			Decimal: 0,
			Charset: mysql.DefaultCharset,
			Collate: mysql.DefaultCollationName,
			Flag:    mysql.BinaryFlag,
		}
	}
	if mysql.HasBinaryFlag(fieldType.Flag) && fieldType.Tp != mysql.TypeJSON {
		fieldType.Charset, fieldType.Collate = charset.CharsetBin, charset.CollationBin
	} else {
		fieldType.Charset, fieldType.Collate = charset.GetDefaultCharsetAndCollate()
	}
	return BaseBuiltinFunc{
		bufAllocator:           newLocalSliceBuffer(len(args)),
		childrenVectorizedOnce: new(sync.Once),

		Args: args,
		Ctx:  ctx,
		Tp:   fieldType,
	}
}

func (b *BaseBuiltinFunc) getArgs() []Expression {
	return b.Args
}

func (b *BaseBuiltinFunc) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("baseBuiltinFunc.vecEvalInt() should never be called, please contact the TiDB team for help")
}

func (b *BaseBuiltinFunc) vecEvalReal(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("baseBuiltinFunc.vecEvalReal() should never be called, please contact the TiDB team for help")
}

func (b *BaseBuiltinFunc) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("baseBuiltinFunc.vecEvalString() should never be called, please contact the TiDB team for help")
}

func (b *BaseBuiltinFunc) vecEvalDecimal(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("baseBuiltinFunc.vecEvalDecimal() should never be called, please contact the TiDB team for help")
}

func (b *BaseBuiltinFunc) vecEvalTime(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("baseBuiltinFunc.vecEvalTime() should never be called, please contact the TiDB team for help")
}

func (b *BaseBuiltinFunc) vecEvalDuration(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("baseBuiltinFunc.vecEvalDuration() should never be called, please contact the TiDB team for help")
}

func (b *BaseBuiltinFunc) vecEvalJSON(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("baseBuiltinFunc.vecEvalJSON() should never be called, please contact the TiDB team for help")
}

func (b *BaseBuiltinFunc) evalInt(row chunk.Row) (int64, bool, error) {
	return 0, false, errors.Errorf("baseBuiltinFunc.evalInt() should never be called, please contact the TiDB team for help")
}

func (b *BaseBuiltinFunc) evalReal(row chunk.Row) (float64, bool, error) {
	return 0, false, errors.Errorf("baseBuiltinFunc.evalReal() should never be called, please contact the TiDB team for help")
}

func (b *BaseBuiltinFunc) EvalString(row chunk.Row) (string, bool, error) {
	return "", false, errors.Errorf("baseBuiltinFunc.evalString() should never be called, please contact the TiDB team for help")
}

func (b *BaseBuiltinFunc) evalDecimal(row chunk.Row) (*types.MyDecimal, bool, error) {
	return nil, false, errors.Errorf("baseBuiltinFunc.evalDecimal() should never be called, please contact the TiDB team for help")
}

func (b *BaseBuiltinFunc) evalTime(row chunk.Row) (types.Time, bool, error) {
	return types.Time{}, false, errors.Errorf("baseBuiltinFunc.evalTime() should never be called, please contact the TiDB team for help")
}

func (b *BaseBuiltinFunc) evalDuration(row chunk.Row) (types.Duration, bool, error) {
	return types.Duration{}, false, errors.Errorf("baseBuiltinFunc.evalDuration() should never be called, please contact the TiDB team for help")
}

func (b *BaseBuiltinFunc) evalJSON(row chunk.Row) (json.BinaryJSON, bool, error) {
	return json.BinaryJSON{}, false, errors.Errorf("baseBuiltinFunc.evalJSON() should never be called, please contact the TiDB team for help")
}

func (b *BaseBuiltinFunc) vectorized() bool {
	return false
}

func (b *BaseBuiltinFunc) isChildrenVectorized() bool {
	b.childrenVectorizedOnce.Do(func() {
		b.childrenVectorized = true
		for _, arg := range b.Args {
			if !arg.Vectorized() {
				b.childrenVectorized = false
				break
			}
		}
	})
	return b.childrenVectorized
}

func (b *BaseBuiltinFunc) getRetTp() *types.FieldType {
	switch b.Tp.EvalType() {
	case types.ETString:
		if b.Tp.Flen >= mysql.MaxBlobWidth {
			b.Tp.Tp = mysql.TypeLongBlob
		} else if b.Tp.Flen >= 65536 {
			b.Tp.Tp = mysql.TypeMediumBlob
		}
		if len(b.Tp.Charset) <= 0 {
			b.Tp.Charset, b.Tp.Collate = charset.GetDefaultCharsetAndCollate()
		}
	}
	return b.Tp
}

func (b *BaseBuiltinFunc) equal(fun BuiltinFunc) bool {
	funArgs := fun.getArgs()
	if len(funArgs) != len(b.Args) {
		return false
	}
	for i := range b.Args {
		if !b.Args[i].Equal(b.Ctx, funArgs[i]) {
			return false
		}
	}
	return true
}

func (b *BaseBuiltinFunc) getCtx() sessionctx.Context {
	return b.Ctx
}

func (b *BaseBuiltinFunc) CloneFrom(from *BaseBuiltinFunc) {
	b.Args = make([]Expression, 0, len(b.Args))
	for _, arg := range from.Args {
		b.Args = append(b.Args, arg.Clone())
	}
	b.Ctx = from.Ctx
	b.Tp = from.Tp
	b.pbCode = from.pbCode
	b.bufAllocator = newLocalSliceBuffer(len(b.Args))
	b.childrenVectorizedOnce = new(sync.Once)
}

func (b *BaseBuiltinFunc) Clone() BuiltinFunc {
	panic("you should not call this method.")
}

// baseBuiltinCastFunc will be contained in every struct that implement cast builtinFunc.
type baseBuiltinCastFunc struct {
	BaseBuiltinFunc

	// inUnion indicates whether cast is in union context.
	inUnion bool
}

// implicitArgs returns the implicit arguments of cast functions
func (b *baseBuiltinCastFunc) implicitArgs() []types.Datum {
	args := b.BaseBuiltinFunc.implicitArgs()
	if b.inUnion {
		args = append(args, types.NewIntDatum(1))
	} else {
		args = append(args, types.NewIntDatum(0))
	}
	return args
}

func (b *baseBuiltinCastFunc) cloneFrom(from *baseBuiltinCastFunc) {
	b.BaseBuiltinFunc.CloneFrom(&from.BaseBuiltinFunc)
	b.inUnion = from.inUnion
}

func newBaseBuiltinCastFunc(builtinFunc BaseBuiltinFunc, inUnion bool) baseBuiltinCastFunc {
	return baseBuiltinCastFunc{
		BaseBuiltinFunc: builtinFunc,
		inUnion:         inUnion,
	}
}

// vecBuiltinFunc contains all vectorized methods for a builtin function.
type vecBuiltinFunc interface {
	// vectorized returns if this builtin function itself supports vectorized evaluation.
	vectorized() bool

	// isChildrenVectorized returns if its all children support vectorized evaluation.
	isChildrenVectorized() bool

	// vecEvalInt evaluates this builtin function in a vectorized manner.
	vecEvalInt(input *chunk.Chunk, result *chunk.Column) error

	// vecEvalReal evaluates this builtin function in a vectorized manner.
	vecEvalReal(input *chunk.Chunk, result *chunk.Column) error

	// vecEvalString evaluates this builtin function in a vectorized manner.
	vecEvalString(input *chunk.Chunk, result *chunk.Column) error

	// vecEvalDecimal evaluates this builtin function in a vectorized manner.
	vecEvalDecimal(input *chunk.Chunk, result *chunk.Column) error

	// vecEvalTime evaluates this builtin function in a vectorized manner.
	vecEvalTime(input *chunk.Chunk, result *chunk.Column) error

	// vecEvalDuration evaluates this builtin function in a vectorized manner.
	vecEvalDuration(input *chunk.Chunk, result *chunk.Column) error

	// vecEvalJSON evaluates this builtin function in a vectorized manner.
	vecEvalJSON(input *chunk.Chunk, result *chunk.Column) error
}

// builtinFunc stands for a particular function signature.
type BuiltinFunc interface {
	vecBuiltinFunc

	// evalInt evaluates int result of builtinFunc by given row.
	evalInt(row chunk.Row) (val int64, isNull bool, err error)
	// evalReal evaluates real representation of builtinFunc by given row.
	evalReal(row chunk.Row) (val float64, isNull bool, err error)
	// evalString evaluates string representation of builtinFunc by given row.
	EvalString(row chunk.Row) (val string, isNull bool, err error)
	// evalDecimal evaluates decimal representation of builtinFunc by given row.
	evalDecimal(row chunk.Row) (val *types.MyDecimal, isNull bool, err error)
	// evalTime evaluates DATE/DATETIME/TIMESTAMP representation of builtinFunc by given row.
	evalTime(row chunk.Row) (val types.Time, isNull bool, err error)
	// evalDuration evaluates duration representation of builtinFunc by given row.
	evalDuration(row chunk.Row) (val types.Duration, isNull bool, err error)
	// evalJSON evaluates JSON representation of builtinFunc by given row.
	evalJSON(row chunk.Row) (val json.BinaryJSON, isNull bool, err error)
	// getArgs returns the arguments expressions.
	getArgs() []Expression
	// equal check if this function equals to another function.
	equal(BuiltinFunc) bool
	// getCtx returns this function's context.
	getCtx() sessionctx.Context
	// getRetTp returns the return type of the built-in function.
	getRetTp() *types.FieldType
	// setPbCode sets pbCode for signature.
	SetPbCode(tipb.ScalarFuncSig)
	// PbCode returns PbCode of this signature.
	PbCode() tipb.ScalarFuncSig
	// implicitArgs returns the implicit parameters of a function.
	// implicit arguments means some functions contain extra inner fields which will not
	// contain in `tipb.Expr.children` but must be pushed down to coprocessor
	implicitArgs() []types.Datum
	// Clone returns a copy of itself.
	Clone() BuiltinFunc
}

// baseFunctionClass will be contained in every struct that implement functionClass interface.
type BaseFunctionClass struct {
	FuncName string
	MinArgs  int
	MaxArgs  int
}

func (b *BaseFunctionClass) VerifyArgs(args []Expression) error {
	l := len(args)
	if l < b.MinArgs || (b.MaxArgs != -1 && l > b.MaxArgs) {
		return ErrIncorrectParameterCount.GenWithStackByArgs(b.FuncName)
	}
	return nil
}

// functionClass is the interface for a function which may contains multiple functions.
type FunctionClass interface {
	// getFunction gets a function signature by the types and the counts of given arguments.
	GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error)
}

// funcs holds all registered builtin functions. When new function is added,
// check expression/function_traits.go to see if it should be appended to
// any set there.
var funcs = map[string]FunctionClass{
	// common functions
	ast.Coalesce: &coalesceFunctionClass{BaseFunctionClass{ast.Coalesce, 1, -1}},
	ast.IsNull:   &isNullFunctionClass{BaseFunctionClass{ast.IsNull, 1, 1}},
	ast.Greatest: &greatestFunctionClass{BaseFunctionClass{ast.Greatest, 2, -1}},
	ast.Least:    &leastFunctionClass{BaseFunctionClass{ast.Least, 2, -1}},
	ast.Interval: &intervalFunctionClass{BaseFunctionClass{ast.Interval, 2, -1}},

	// math functions
	ast.Abs:      &absFunctionClass{BaseFunctionClass{ast.Abs, 1, 1}},
	ast.Acos:     &acosFunctionClass{BaseFunctionClass{ast.Acos, 1, 1}},
	ast.Asin:     &asinFunctionClass{BaseFunctionClass{ast.Asin, 1, 1}},
	ast.Atan:     &atanFunctionClass{BaseFunctionClass{ast.Atan, 1, 2}},
	ast.Atan2:    &atanFunctionClass{BaseFunctionClass{ast.Atan2, 2, 2}},
	ast.Ceil:     &ceilFunctionClass{BaseFunctionClass{ast.Ceil, 1, 1}},
	ast.Ceiling:  &ceilFunctionClass{BaseFunctionClass{ast.Ceiling, 1, 1}},
	ast.Conv:     &convFunctionClass{BaseFunctionClass{ast.Conv, 3, 3}},
	ast.Cos:      &cosFunctionClass{BaseFunctionClass{ast.Cos, 1, 1}},
	ast.Cot:      &cotFunctionClass{BaseFunctionClass{ast.Cot, 1, 1}},
	ast.CRC32:    &crc32FunctionClass{BaseFunctionClass{ast.CRC32, 1, 1}},
	ast.Degrees:  &degreesFunctionClass{BaseFunctionClass{ast.Degrees, 1, 1}},
	ast.Exp:      &expFunctionClass{BaseFunctionClass{ast.Exp, 1, 1}},
	ast.Floor:    &floorFunctionClass{BaseFunctionClass{ast.Floor, 1, 1}},
	ast.Ln:       &logFunctionClass{BaseFunctionClass{ast.Ln, 1, 1}},
	ast.Log:      &logFunctionClass{BaseFunctionClass{ast.Log, 1, 2}},
	ast.Log2:     &log2FunctionClass{BaseFunctionClass{ast.Log2, 1, 1}},
	ast.Log10:    &log10FunctionClass{BaseFunctionClass{ast.Log10, 1, 1}},
	ast.PI:       &piFunctionClass{BaseFunctionClass{ast.PI, 0, 0}},
	ast.Pow:      &powFunctionClass{BaseFunctionClass{ast.Pow, 2, 2}},
	ast.Power:    &powFunctionClass{BaseFunctionClass{ast.Power, 2, 2}},
	ast.Radians:  &radiansFunctionClass{BaseFunctionClass{ast.Radians, 1, 1}},
	ast.Rand:     &randFunctionClass{BaseFunctionClass{ast.Rand, 0, 1}},
	ast.Round:    &roundFunctionClass{BaseFunctionClass{ast.Round, 1, 2}},
	ast.Sign:     &signFunctionClass{BaseFunctionClass{ast.Sign, 1, 1}},
	ast.Sin:      &sinFunctionClass{BaseFunctionClass{ast.Sin, 1, 1}},
	ast.Sqrt:     &sqrtFunctionClass{BaseFunctionClass{ast.Sqrt, 1, 1}},
	ast.Tan:      &tanFunctionClass{BaseFunctionClass{ast.Tan, 1, 1}},
	ast.Truncate: &truncateFunctionClass{BaseFunctionClass{ast.Truncate, 2, 2}},

	// time functions
	ast.AddDate:          &addDateFunctionClass{BaseFunctionClass{ast.AddDate, 3, 3}},
	ast.DateAdd:          &addDateFunctionClass{BaseFunctionClass{ast.DateAdd, 3, 3}},
	ast.SubDate:          &subDateFunctionClass{BaseFunctionClass{ast.SubDate, 3, 3}},
	ast.DateSub:          &subDateFunctionClass{BaseFunctionClass{ast.DateSub, 3, 3}},
	ast.AddTime:          &addTimeFunctionClass{BaseFunctionClass{ast.AddTime, 2, 2}},
	ast.ConvertTz:        &convertTzFunctionClass{BaseFunctionClass{ast.ConvertTz, 3, 3}},
	ast.Curdate:          &currentDateFunctionClass{BaseFunctionClass{ast.Curdate, 0, 0}},
	ast.CurrentDate:      &currentDateFunctionClass{BaseFunctionClass{ast.CurrentDate, 0, 0}},
	ast.CurrentTime:      &currentTimeFunctionClass{BaseFunctionClass{ast.CurrentTime, 0, 1}},
	ast.CurrentTimestamp: &nowFunctionClass{BaseFunctionClass{ast.CurrentTimestamp, 0, 1}},
	ast.Curtime:          &currentTimeFunctionClass{BaseFunctionClass{ast.Curtime, 0, 1}},
	ast.Date:             &dateFunctionClass{BaseFunctionClass{ast.Date, 1, 1}},
	ast.DateLiteral:      &dateLiteralFunctionClass{BaseFunctionClass{ast.DateLiteral, 1, 1}},
	ast.DateFormat:       &dateFormatFunctionClass{BaseFunctionClass{ast.DateFormat, 2, 2}},
	ast.DateDiff:         &dateDiffFunctionClass{BaseFunctionClass{ast.DateDiff, 2, 2}},
	ast.Day:              &dayOfMonthFunctionClass{BaseFunctionClass{ast.Day, 1, 1}},
	ast.DayName:          &dayNameFunctionClass{BaseFunctionClass{ast.DayName, 1, 1}},
	ast.DayOfMonth:       &dayOfMonthFunctionClass{BaseFunctionClass{ast.DayOfMonth, 1, 1}},
	ast.DayOfWeek:        &dayOfWeekFunctionClass{BaseFunctionClass{ast.DayOfWeek, 1, 1}},
	ast.DayOfYear:        &dayOfYearFunctionClass{BaseFunctionClass{ast.DayOfYear, 1, 1}},
	ast.Extract:          &extractFunctionClass{BaseFunctionClass{ast.Extract, 2, 2}},
	ast.FromDays:         &fromDaysFunctionClass{BaseFunctionClass{ast.FromDays, 1, 1}},
	ast.FromUnixTime:     &fromUnixTimeFunctionClass{BaseFunctionClass{ast.FromUnixTime, 1, 2}},
	ast.GetFormat:        &getFormatFunctionClass{BaseFunctionClass{ast.GetFormat, 2, 2}},
	ast.Hour:             &hourFunctionClass{BaseFunctionClass{ast.Hour, 1, 1}},
	ast.LocalTime:        &nowFunctionClass{BaseFunctionClass{ast.LocalTime, 0, 1}},
	ast.LocalTimestamp:   &nowFunctionClass{BaseFunctionClass{ast.LocalTimestamp, 0, 1}},
	ast.MakeDate:         &makeDateFunctionClass{BaseFunctionClass{ast.MakeDate, 2, 2}},
	ast.MakeTime:         &makeTimeFunctionClass{BaseFunctionClass{ast.MakeTime, 3, 3}},
	ast.MicroSecond:      &microSecondFunctionClass{BaseFunctionClass{ast.MicroSecond, 1, 1}},
	ast.Minute:           &minuteFunctionClass{BaseFunctionClass{ast.Minute, 1, 1}},
	ast.Month:            &monthFunctionClass{BaseFunctionClass{ast.Month, 1, 1}},
	ast.MonthName:        &monthNameFunctionClass{BaseFunctionClass{ast.MonthName, 1, 1}},
	ast.Now:              &nowFunctionClass{BaseFunctionClass{ast.Now, 0, 1}},
	ast.PeriodAdd:        &periodAddFunctionClass{BaseFunctionClass{ast.PeriodAdd, 2, 2}},
	ast.PeriodDiff:       &periodDiffFunctionClass{BaseFunctionClass{ast.PeriodDiff, 2, 2}},
	ast.Quarter:          &quarterFunctionClass{BaseFunctionClass{ast.Quarter, 1, 1}},
	ast.SecToTime:        &secToTimeFunctionClass{BaseFunctionClass{ast.SecToTime, 1, 1}},
	ast.Second:           &secondFunctionClass{BaseFunctionClass{ast.Second, 1, 1}},
	ast.StrToDate:        &strToDateFunctionClass{BaseFunctionClass{ast.StrToDate, 2, 2}},
	ast.SubTime:          &subTimeFunctionClass{BaseFunctionClass{ast.SubTime, 2, 2}},
	ast.Sysdate:          &sysDateFunctionClass{BaseFunctionClass{ast.Sysdate, 0, 1}},
	ast.Time:             &timeFunctionClass{BaseFunctionClass{ast.Time, 1, 1}},
	ast.TimeLiteral:      &timeLiteralFunctionClass{BaseFunctionClass{ast.TimeLiteral, 1, 1}},
	ast.TimeFormat:       &timeFormatFunctionClass{BaseFunctionClass{ast.TimeFormat, 2, 2}},
	ast.TimeToSec:        &timeToSecFunctionClass{BaseFunctionClass{ast.TimeToSec, 1, 1}},
	ast.TimeDiff:         &timeDiffFunctionClass{BaseFunctionClass{ast.TimeDiff, 2, 2}},
	ast.Timestamp:        &timestampFunctionClass{BaseFunctionClass{ast.Timestamp, 1, 2}},
	ast.TimestampLiteral: &timestampLiteralFunctionClass{BaseFunctionClass{ast.TimestampLiteral, 1, 2}},
	ast.TimestampAdd:     &timestampAddFunctionClass{BaseFunctionClass{ast.TimestampAdd, 3, 3}},
	ast.TimestampDiff:    &timestampDiffFunctionClass{BaseFunctionClass{ast.TimestampDiff, 3, 3}},
	ast.ToDays:           &toDaysFunctionClass{BaseFunctionClass{ast.ToDays, 1, 1}},
	ast.ToSeconds:        &toSecondsFunctionClass{BaseFunctionClass{ast.ToSeconds, 1, 1}},
	ast.UnixTimestamp:    &unixTimestampFunctionClass{BaseFunctionClass{ast.UnixTimestamp, 0, 1}},
	ast.UTCDate:          &utcDateFunctionClass{BaseFunctionClass{ast.UTCDate, 0, 0}},
	ast.UTCTime:          &utcTimeFunctionClass{BaseFunctionClass{ast.UTCTime, 0, 1}},
	ast.UTCTimestamp:     &utcTimestampFunctionClass{BaseFunctionClass{ast.UTCTimestamp, 0, 1}},
	ast.Week:             &weekFunctionClass{BaseFunctionClass{ast.Week, 1, 2}},
	ast.Weekday:          &weekDayFunctionClass{BaseFunctionClass{ast.Weekday, 1, 1}},
	ast.WeekOfYear:       &weekOfYearFunctionClass{BaseFunctionClass{ast.WeekOfYear, 1, 1}},
	ast.Year:             &yearFunctionClass{BaseFunctionClass{ast.Year, 1, 1}},
	ast.YearWeek:         &yearWeekFunctionClass{BaseFunctionClass{ast.YearWeek, 1, 2}},
	ast.LastDay:          &lastDayFunctionClass{BaseFunctionClass{ast.LastDay, 1, 1}},

	// string functions
	ast.ASCII:           &asciiFunctionClass{BaseFunctionClass{ast.ASCII, 1, 1}},
	ast.Bin:             &binFunctionClass{BaseFunctionClass{ast.Bin, 1, 1}},
	ast.Concat:          &concatFunctionClass{BaseFunctionClass{ast.Concat, 1, -1}},
	ast.ConcatWS:        &concatWSFunctionClass{BaseFunctionClass{ast.ConcatWS, 2, -1}},
	ast.Convert:         &convertFunctionClass{BaseFunctionClass{ast.Convert, 2, 2}},
	ast.Elt:             &eltFunctionClass{BaseFunctionClass{ast.Elt, 2, -1}},
	ast.ExportSet:       &exportSetFunctionClass{BaseFunctionClass{ast.ExportSet, 3, 5}},
	ast.Field:           &fieldFunctionClass{BaseFunctionClass{ast.Field, 2, -1}},
	ast.Format:          &formatFunctionClass{BaseFunctionClass{ast.Format, 2, 3}},
	ast.FromBase64:      &fromBase64FunctionClass{BaseFunctionClass{ast.FromBase64, 1, 1}},
	ast.InsertFunc:      &insertFunctionClass{BaseFunctionClass{ast.InsertFunc, 4, 4}},
	ast.Instr:           &instrFunctionClass{BaseFunctionClass{ast.Instr, 2, 2}},
	ast.Lcase:           &lowerFunctionClass{BaseFunctionClass{ast.Lcase, 1, 1}},
	ast.Left:            &leftFunctionClass{BaseFunctionClass{ast.Left, 2, 2}},
	ast.Right:           &rightFunctionClass{BaseFunctionClass{ast.Right, 2, 2}},
	ast.Length:          &lengthFunctionClass{BaseFunctionClass{ast.Length, 1, 1}},
	ast.LoadFile:        &loadFileFunctionClass{BaseFunctionClass{ast.LoadFile, 1, 1}},
	ast.Locate:          &locateFunctionClass{BaseFunctionClass{ast.Locate, 2, 3}},
	ast.Lower:           &lowerFunctionClass{BaseFunctionClass{ast.Lower, 1, 1}},
	ast.Lpad:            &lpadFunctionClass{BaseFunctionClass{ast.Lpad, 3, 3}},
	ast.LTrim:           &lTrimFunctionClass{BaseFunctionClass{ast.LTrim, 1, 1}},
	ast.Mid:             &substringFunctionClass{BaseFunctionClass{ast.Mid, 3, 3}},
	ast.MakeSet:         &makeSetFunctionClass{BaseFunctionClass{ast.MakeSet, 2, -1}},
	ast.Oct:             &octFunctionClass{BaseFunctionClass{ast.Oct, 1, 1}},
	ast.OctetLength:     &lengthFunctionClass{BaseFunctionClass{ast.OctetLength, 1, 1}},
	ast.Ord:             &ordFunctionClass{BaseFunctionClass{ast.Ord, 1, 1}},
	ast.Position:        &locateFunctionClass{BaseFunctionClass{ast.Position, 2, 2}},
	ast.Quote:           &quoteFunctionClass{BaseFunctionClass{ast.Quote, 1, 1}},
	ast.Repeat:          &repeatFunctionClass{BaseFunctionClass{ast.Repeat, 2, 2}},
	ast.Replace:         &replaceFunctionClass{BaseFunctionClass{ast.Replace, 3, 3}},
	ast.Reverse:         &reverseFunctionClass{BaseFunctionClass{ast.Reverse, 1, 1}},
	ast.RTrim:           &rTrimFunctionClass{BaseFunctionClass{ast.RTrim, 1, 1}},
	ast.Space:           &spaceFunctionClass{BaseFunctionClass{ast.Space, 1, 1}},
	ast.Strcmp:          &strcmpFunctionClass{BaseFunctionClass{ast.Strcmp, 2, 2}},
	ast.Substring:       &substringFunctionClass{BaseFunctionClass{ast.Substring, 2, 3}},
	ast.Substr:          &substringFunctionClass{BaseFunctionClass{ast.Substr, 2, 3}},
	ast.SubstringIndex:  &substringIndexFunctionClass{BaseFunctionClass{ast.SubstringIndex, 3, 3}},
	ast.ToBase64:        &toBase64FunctionClass{BaseFunctionClass{ast.ToBase64, 1, 1}},
	ast.Trim:            &trimFunctionClass{BaseFunctionClass{ast.Trim, 1, 3}},
	ast.Upper:           &upperFunctionClass{BaseFunctionClass{ast.Upper, 1, 1}},
	ast.Ucase:           &upperFunctionClass{BaseFunctionClass{ast.Ucase, 1, 1}},
	ast.Hex:             &hexFunctionClass{BaseFunctionClass{ast.Hex, 1, 1}},
	ast.Unhex:           &unhexFunctionClass{BaseFunctionClass{ast.Unhex, 1, 1}},
	ast.Rpad:            &rpadFunctionClass{BaseFunctionClass{ast.Rpad, 3, 3}},
	ast.BitLength:       &bitLengthFunctionClass{BaseFunctionClass{ast.BitLength, 1, 1}},
	ast.CharFunc:        &charFunctionClass{BaseFunctionClass{ast.CharFunc, 2, -1}},
	ast.CharLength:      &charLengthFunctionClass{BaseFunctionClass{ast.CharLength, 1, 1}},
	ast.CharacterLength: &charLengthFunctionClass{BaseFunctionClass{ast.CharacterLength, 1, 1}},
	ast.FindInSet:       &findInSetFunctionClass{BaseFunctionClass{ast.FindInSet, 2, 2}},

	// information functions
	ast.ConnectionID: &connectionIDFunctionClass{BaseFunctionClass{ast.ConnectionID, 0, 0}},
	ast.CurrentUser:  &currentUserFunctionClass{BaseFunctionClass{ast.CurrentUser, 0, 0}},
	ast.CurrentRole:  &currentRoleFunctionClass{BaseFunctionClass{ast.CurrentRole, 0, 0}},
	ast.Database:     &databaseFunctionClass{BaseFunctionClass{ast.Database, 0, 0}},
	// This function is a synonym for DATABASE().
	// See http://dev.mysql.com/doc/refman/5.7/en/information-functions.html#function_schema
	ast.Schema:       &databaseFunctionClass{BaseFunctionClass{ast.Schema, 0, 0}},
	ast.FoundRows:    &foundRowsFunctionClass{BaseFunctionClass{ast.FoundRows, 0, 0}},
	ast.LastInsertId: &lastInsertIDFunctionClass{BaseFunctionClass{ast.LastInsertId, 0, 1}},
	ast.User:         &userFunctionClass{BaseFunctionClass{ast.User, 0, 0}},
	ast.Version:      &versionFunctionClass{BaseFunctionClass{ast.Version, 0, 0}},
	ast.Benchmark:    &benchmarkFunctionClass{BaseFunctionClass{ast.Benchmark, 2, 2}},
	ast.Charset:      &charsetFunctionClass{BaseFunctionClass{ast.Charset, 1, 1}},
	ast.Coercibility: &coercibilityFunctionClass{BaseFunctionClass{ast.Coercibility, 1, 1}},
	ast.Collation:    &collationFunctionClass{BaseFunctionClass{ast.Collation, 1, 1}},
	ast.RowCount:     &rowCountFunctionClass{BaseFunctionClass{ast.RowCount, 0, 0}},
	ast.SessionUser:  &userFunctionClass{BaseFunctionClass{ast.SessionUser, 0, 0}},
	ast.SystemUser:   &userFunctionClass{BaseFunctionClass{ast.SystemUser, 0, 0}},

	// control functions
	ast.If:     &ifFunctionClass{BaseFunctionClass{ast.If, 3, 3}},
	ast.Ifnull: &ifNullFunctionClass{BaseFunctionClass{ast.Ifnull, 2, 2}},

	// miscellaneous functions
	ast.Sleep:           &sleepFunctionClass{BaseFunctionClass{ast.Sleep, 1, 1}},
	ast.AnyValue:        &anyValueFunctionClass{BaseFunctionClass{ast.AnyValue, 1, 1}},
	ast.DefaultFunc:     &defaultFunctionClass{BaseFunctionClass{ast.DefaultFunc, 1, 1}},
	ast.InetAton:        &inetAtonFunctionClass{BaseFunctionClass{ast.InetAton, 1, 1}},
	ast.InetNtoa:        &inetNtoaFunctionClass{BaseFunctionClass{ast.InetNtoa, 1, 1}},
	ast.Inet6Aton:       &inet6AtonFunctionClass{BaseFunctionClass{ast.Inet6Aton, 1, 1}},
	ast.Inet6Ntoa:       &inet6NtoaFunctionClass{BaseFunctionClass{ast.Inet6Ntoa, 1, 1}},
	ast.IsFreeLock:      &isFreeLockFunctionClass{BaseFunctionClass{ast.IsFreeLock, 1, 1}},
	ast.IsIPv4:          &isIPv4FunctionClass{BaseFunctionClass{ast.IsIPv4, 1, 1}},
	ast.IsIPv4Compat:    &isIPv4CompatFunctionClass{BaseFunctionClass{ast.IsIPv4Compat, 1, 1}},
	ast.IsIPv4Mapped:    &isIPv4MappedFunctionClass{BaseFunctionClass{ast.IsIPv4Mapped, 1, 1}},
	ast.IsIPv6:          &isIPv6FunctionClass{BaseFunctionClass{ast.IsIPv6, 1, 1}},
	ast.IsUsedLock:      &isUsedLockFunctionClass{BaseFunctionClass{ast.IsUsedLock, 1, 1}},
	ast.MasterPosWait:   &masterPosWaitFunctionClass{BaseFunctionClass{ast.MasterPosWait, 2, 4}},
	ast.NameConst:       &nameConstFunctionClass{BaseFunctionClass{ast.NameConst, 2, 2}},
	ast.ReleaseAllLocks: &releaseAllLocksFunctionClass{BaseFunctionClass{ast.ReleaseAllLocks, 0, 0}},
	ast.UUID:            &uuidFunctionClass{BaseFunctionClass{ast.UUID, 0, 0}},
	ast.UUIDShort:       &uuidShortFunctionClass{BaseFunctionClass{ast.UUIDShort, 0, 0}},

	// get_lock() and release_lock() are parsed but do nothing.
	// It is used for preventing error in Ruby's activerecord migrations.
	ast.GetLock:     &lockFunctionClass{BaseFunctionClass{ast.GetLock, 2, 2}},
	ast.ReleaseLock: &releaseLockFunctionClass{BaseFunctionClass{ast.ReleaseLock, 1, 1}},

	ast.LogicAnd:   &logicAndFunctionClass{BaseFunctionClass{ast.LogicAnd, 2, 2}},
	ast.LogicOr:    &logicOrFunctionClass{BaseFunctionClass{ast.LogicOr, 2, 2}},
	ast.LogicXor:   &logicXorFunctionClass{BaseFunctionClass{ast.LogicXor, 2, 2}},
	ast.GE:         &compareFunctionClass{BaseFunctionClass{ast.GE, 2, 2}, opcode.GE},
	ast.LE:         &compareFunctionClass{BaseFunctionClass{ast.LE, 2, 2}, opcode.LE},
	ast.EQ:         &compareFunctionClass{BaseFunctionClass{ast.EQ, 2, 2}, opcode.EQ},
	ast.NE:         &compareFunctionClass{BaseFunctionClass{ast.NE, 2, 2}, opcode.NE},
	ast.LT:         &compareFunctionClass{BaseFunctionClass{ast.LT, 2, 2}, opcode.LT},
	ast.GT:         &compareFunctionClass{BaseFunctionClass{ast.GT, 2, 2}, opcode.GT},
	ast.NullEQ:     &compareFunctionClass{BaseFunctionClass{ast.NullEQ, 2, 2}, opcode.NullEQ},
	ast.Plus:       &arithmeticPlusFunctionClass{BaseFunctionClass{ast.Plus, 2, 2}},
	ast.Minus:      &arithmeticMinusFunctionClass{BaseFunctionClass{ast.Minus, 2, 2}},
	ast.Mod:        &arithmeticModFunctionClass{BaseFunctionClass{ast.Mod, 2, 2}},
	ast.Div:        &arithmeticDivideFunctionClass{BaseFunctionClass{ast.Div, 2, 2}},
	ast.Mul:        &arithmeticMultiplyFunctionClass{BaseFunctionClass{ast.Mul, 2, 2}},
	ast.IntDiv:     &arithmeticIntDivideFunctionClass{BaseFunctionClass{ast.IntDiv, 2, 2}},
	ast.BitNeg:     &bitNegFunctionClass{BaseFunctionClass{ast.BitNeg, 1, 1}},
	ast.And:        &bitAndFunctionClass{BaseFunctionClass{ast.And, 2, 2}},
	ast.LeftShift:  &leftShiftFunctionClass{BaseFunctionClass{ast.LeftShift, 2, 2}},
	ast.RightShift: &rightShiftFunctionClass{BaseFunctionClass{ast.RightShift, 2, 2}},
	ast.UnaryNot:   &unaryNotFunctionClass{BaseFunctionClass{ast.UnaryNot, 1, 1}},
	ast.Or:         &bitOrFunctionClass{BaseFunctionClass{ast.Or, 2, 2}},
	ast.Xor:        &bitXorFunctionClass{BaseFunctionClass{ast.Xor, 2, 2}},
	ast.UnaryMinus: &unaryMinusFunctionClass{BaseFunctionClass{ast.UnaryMinus, 1, 1}},
	ast.In:         &inFunctionClass{BaseFunctionClass{ast.In, 2, -1}},
	ast.IsTruth:    &isTrueOrFalseFunctionClass{BaseFunctionClass{ast.IsTruth, 1, 1}, opcode.IsTruth, false},
	ast.IsFalsity:  &isTrueOrFalseFunctionClass{BaseFunctionClass{ast.IsFalsity, 1, 1}, opcode.IsFalsity, false},
	ast.Like:       &likeFunctionClass{BaseFunctionClass{ast.Like, 3, 3}},
	ast.Regexp:     &regexpFunctionClass{BaseFunctionClass{ast.Regexp, 2, 2}},
	ast.Case:       &caseWhenFunctionClass{BaseFunctionClass{ast.Case, 1, -1}},
	ast.RowFunc:    &rowFunctionClass{BaseFunctionClass{ast.RowFunc, 2, -1}},
	ast.SetVar:     &setVarFunctionClass{BaseFunctionClass{ast.SetVar, 2, 2}},
	ast.GetVar:     &getVarFunctionClass{BaseFunctionClass{ast.GetVar, 1, 1}},
	ast.BitCount:   &bitCountFunctionClass{BaseFunctionClass{ast.BitCount, 1, 1}},
	ast.GetParam:   &getParamFunctionClass{BaseFunctionClass{ast.GetParam, 1, 1}},

	// encryption and compression functions
	ast.AesDecrypt:               &aesDecryptFunctionClass{BaseFunctionClass{ast.AesDecrypt, 2, 3}},
	ast.AesEncrypt:               &aesEncryptFunctionClass{BaseFunctionClass{ast.AesEncrypt, 2, 3}},
	ast.Compress:                 &compressFunctionClass{BaseFunctionClass{ast.Compress, 1, 1}},
	ast.Decode:                   &decodeFunctionClass{BaseFunctionClass{ast.Decode, 2, 2}},
	ast.DesDecrypt:               &desDecryptFunctionClass{BaseFunctionClass{ast.DesDecrypt, 1, 2}},
	ast.DesEncrypt:               &desEncryptFunctionClass{BaseFunctionClass{ast.DesEncrypt, 1, 2}},
	ast.Encode:                   &encodeFunctionClass{BaseFunctionClass{ast.Encode, 2, 2}},
	ast.Encrypt:                  &encryptFunctionClass{BaseFunctionClass{ast.Encrypt, 1, 2}},
	ast.MD5:                      &md5FunctionClass{BaseFunctionClass{ast.MD5, 1, 1}},
	ast.OldPassword:              &oldPasswordFunctionClass{BaseFunctionClass{ast.OldPassword, 1, 1}},
	ast.PasswordFunc:             &passwordFunctionClass{BaseFunctionClass{ast.PasswordFunc, 1, 1}},
	ast.RandomBytes:              &randomBytesFunctionClass{BaseFunctionClass{ast.RandomBytes, 1, 1}},
	ast.SHA1:                     &sha1FunctionClass{BaseFunctionClass{ast.SHA1, 1, 1}},
	ast.SHA:                      &sha1FunctionClass{BaseFunctionClass{ast.SHA, 1, 1}},
	ast.SHA2:                     &sha2FunctionClass{BaseFunctionClass{ast.SHA2, 2, 2}},
	ast.Uncompress:               &uncompressFunctionClass{BaseFunctionClass{ast.Uncompress, 1, 1}},
	ast.UncompressedLength:       &uncompressedLengthFunctionClass{BaseFunctionClass{ast.UncompressedLength, 1, 1}},
	ast.ValidatePasswordStrength: &validatePasswordStrengthFunctionClass{BaseFunctionClass{ast.ValidatePasswordStrength, 1, 1}},

	// json functions
	ast.JSONType:          &jsonTypeFunctionClass{BaseFunctionClass{ast.JSONType, 1, 1}},
	ast.JSONExtract:       &jsonExtractFunctionClass{BaseFunctionClass{ast.JSONExtract, 2, -1}},
	ast.JSONUnquote:       &jsonUnquoteFunctionClass{BaseFunctionClass{ast.JSONUnquote, 1, 1}},
	ast.JSONSet:           &jsonSetFunctionClass{BaseFunctionClass{ast.JSONSet, 3, -1}},
	ast.JSONInsert:        &jsonInsertFunctionClass{BaseFunctionClass{ast.JSONInsert, 3, -1}},
	ast.JSONReplace:       &jsonReplaceFunctionClass{BaseFunctionClass{ast.JSONReplace, 3, -1}},
	ast.JSONRemove:        &jsonRemoveFunctionClass{BaseFunctionClass{ast.JSONRemove, 2, -1}},
	ast.JSONMerge:         &jsonMergeFunctionClass{BaseFunctionClass{ast.JSONMerge, 2, -1}},
	ast.JSONObject:        &jsonObjectFunctionClass{BaseFunctionClass{ast.JSONObject, 0, -1}},
	ast.JSONArray:         &jsonArrayFunctionClass{BaseFunctionClass{ast.JSONArray, 0, -1}},
	ast.JSONContains:      &jsonContainsFunctionClass{BaseFunctionClass{ast.JSONContains, 2, 3}},
	ast.JSONContainsPath:  &jsonContainsPathFunctionClass{BaseFunctionClass{ast.JSONContainsPath, 3, -1}},
	ast.JSONValid:         &jsonValidFunctionClass{BaseFunctionClass{ast.JSONValid, 1, 1}},
	ast.JSONArrayAppend:   &jsonArrayAppendFunctionClass{BaseFunctionClass{ast.JSONArrayAppend, 3, -1}},
	ast.JSONArrayInsert:   &jsonArrayInsertFunctionClass{BaseFunctionClass{ast.JSONArrayInsert, 3, -1}},
	ast.JSONMergePatch:    &jsonMergePatchFunctionClass{BaseFunctionClass{ast.JSONMergePatch, 2, -1}},
	ast.JSONMergePreserve: &jsonMergePreserveFunctionClass{BaseFunctionClass{ast.JSONMergePreserve, 2, -1}},
	ast.JSONPretty:        &jsonPrettyFunctionClass{BaseFunctionClass{ast.JSONPretty, 1, 1}},
	ast.JSONQuote:         &jsonQuoteFunctionClass{BaseFunctionClass{ast.JSONQuote, 1, 1}},
	ast.JSONSearch:        &jsonSearchFunctionClass{BaseFunctionClass{ast.JSONSearch, 3, -1}},
	ast.JSONStorageSize:   &jsonStorageSizeFunctionClass{BaseFunctionClass{ast.JSONStorageSize, 1, 1}},
	ast.JSONDepth:         &jsonDepthFunctionClass{BaseFunctionClass{ast.JSONDepth, 1, 1}},
	ast.JSONKeys:          &jsonKeysFunctionClass{BaseFunctionClass{ast.JSONKeys, 1, 2}},
	ast.JSONLength:        &jsonLengthFunctionClass{BaseFunctionClass{ast.JSONLength, 1, 2}},

	// TiDB internal function.
	ast.TiDBDecodeKey: &tidbDecodeKeyFunctionClass{BaseFunctionClass{ast.TiDBDecodeKey, 1, 1}},
	// This function is used to show tidb-server version info.
	ast.TiDBVersion:    &tidbVersionFunctionClass{BaseFunctionClass{ast.TiDBVersion, 0, 0}},
	ast.TiDBIsDDLOwner: &tidbIsDDLOwnerFunctionClass{BaseFunctionClass{ast.TiDBIsDDLOwner, 0, 0}},
	ast.TiDBParseTso:   &tidbParseTsoFunctionClass{BaseFunctionClass{ast.TiDBParseTso, 1, 1}},
	ast.TiDBDecodePlan: &tidbDecodePlanFunctionClass{BaseFunctionClass{ast.TiDBDecodePlan, 1, 1}},
}

// IsFunctionSupported check if given function name is a builtin sql function.
func IsFunctionSupported(name string) bool {
	_, ok := funcs[name]
	return ok
}

func AddUserDefinedFunction(name string, class FunctionClass) {
	funcs[name] = class
}
