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
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/cznic/mathutil"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tipb/go-tipb"
	"go.uber.org/zap"
)

const ( // GET_FORMAT first argument.
	dateFormat      = "DATE"
	datetimeFormat  = "DATETIME"
	timestampFormat = "TIMESTAMP"
	timeFormat      = "TIME"
)

const ( // GET_FORMAT location.
	usaLocation      = "USA"
	jisLocation      = "JIS"
	isoLocation      = "ISO"
	eurLocation      = "EUR"
	internalLocation = "INTERNAL"
)

var (
	// durationPattern checks whether a string matchs the format of duration.
	durationPattern = regexp.MustCompile(`^\s*[-]?(((\d{1,2}\s+)?0*\d{0,3}(:0*\d{1,2}){0,2})|(\d{1,7}))?(\.\d*)?\s*$`)

	// timestampPattern checks whether a string matchs the format of timestamp.
	timestampPattern = regexp.MustCompile(`^\s*0*\d{1,4}([^\d]0*\d{1,2}){2}\s+(0*\d{0,2}([^\d]0*\d{1,2}){2})?(\.\d*)?\s*$`)

	// datePattern determine whether to match the format of date.
	datePattern = regexp.MustCompile(`^\s*((0*\d{1,4}([^\d]0*\d{1,2}){2})|(\d{2,4}(\d{2}){2}))\s*$`)
)

var (
	_ FunctionClass = &dateFunctionClass{}
	_ FunctionClass = &dateLiteralFunctionClass{}
	_ FunctionClass = &dateDiffFunctionClass{}
	_ FunctionClass = &timeDiffFunctionClass{}
	_ FunctionClass = &dateFormatFunctionClass{}
	_ FunctionClass = &hourFunctionClass{}
	_ FunctionClass = &minuteFunctionClass{}
	_ FunctionClass = &secondFunctionClass{}
	_ FunctionClass = &microSecondFunctionClass{}
	_ FunctionClass = &monthFunctionClass{}
	_ FunctionClass = &monthNameFunctionClass{}
	_ FunctionClass = &nowFunctionClass{}
	_ FunctionClass = &dayNameFunctionClass{}
	_ FunctionClass = &dayOfMonthFunctionClass{}
	_ FunctionClass = &dayOfWeekFunctionClass{}
	_ FunctionClass = &dayOfYearFunctionClass{}
	_ FunctionClass = &weekFunctionClass{}
	_ FunctionClass = &weekDayFunctionClass{}
	_ FunctionClass = &weekOfYearFunctionClass{}
	_ FunctionClass = &yearFunctionClass{}
	_ FunctionClass = &yearWeekFunctionClass{}
	_ FunctionClass = &fromUnixTimeFunctionClass{}
	_ FunctionClass = &getFormatFunctionClass{}
	_ FunctionClass = &strToDateFunctionClass{}
	_ FunctionClass = &sysDateFunctionClass{}
	_ FunctionClass = &currentDateFunctionClass{}
	_ FunctionClass = &currentTimeFunctionClass{}
	_ FunctionClass = &timeFunctionClass{}
	_ FunctionClass = &timeLiteralFunctionClass{}
	_ FunctionClass = &utcDateFunctionClass{}
	_ FunctionClass = &utcTimestampFunctionClass{}
	_ FunctionClass = &extractFunctionClass{}
	_ FunctionClass = &unixTimestampFunctionClass{}
	_ FunctionClass = &addTimeFunctionClass{}
	_ FunctionClass = &convertTzFunctionClass{}
	_ FunctionClass = &makeDateFunctionClass{}
	_ FunctionClass = &makeTimeFunctionClass{}
	_ FunctionClass = &periodAddFunctionClass{}
	_ FunctionClass = &periodDiffFunctionClass{}
	_ FunctionClass = &quarterFunctionClass{}
	_ FunctionClass = &secToTimeFunctionClass{}
	_ FunctionClass = &subTimeFunctionClass{}
	_ FunctionClass = &timeFormatFunctionClass{}
	_ FunctionClass = &timeToSecFunctionClass{}
	_ FunctionClass = &timestampAddFunctionClass{}
	_ FunctionClass = &toDaysFunctionClass{}
	_ FunctionClass = &toSecondsFunctionClass{}
	_ FunctionClass = &utcTimeFunctionClass{}
	_ FunctionClass = &timestampFunctionClass{}
	_ FunctionClass = &timestampLiteralFunctionClass{}
	_ FunctionClass = &lastDayFunctionClass{}
	_ FunctionClass = &addDateFunctionClass{}
	_ FunctionClass = &subDateFunctionClass{}
)

var (
	_ BuiltinFunc = &builtinDateSig{}
	_ BuiltinFunc = &builtinDateLiteralSig{}
	_ BuiltinFunc = &builtinDateDiffSig{}
	_ BuiltinFunc = &builtinNullTimeDiffSig{}
	_ BuiltinFunc = &builtinTimeStringTimeDiffSig{}
	_ BuiltinFunc = &builtinDurationStringTimeDiffSig{}
	_ BuiltinFunc = &builtinDurationDurationTimeDiffSig{}
	_ BuiltinFunc = &builtinStringTimeTimeDiffSig{}
	_ BuiltinFunc = &builtinStringDurationTimeDiffSig{}
	_ BuiltinFunc = &builtinStringStringTimeDiffSig{}
	_ BuiltinFunc = &builtinTimeTimeTimeDiffSig{}
	_ BuiltinFunc = &builtinDateFormatSig{}
	_ BuiltinFunc = &builtinHourSig{}
	_ BuiltinFunc = &builtinMinuteSig{}
	_ BuiltinFunc = &builtinSecondSig{}
	_ BuiltinFunc = &builtinMicroSecondSig{}
	_ BuiltinFunc = &builtinMonthSig{}
	_ BuiltinFunc = &builtinMonthNameSig{}
	_ BuiltinFunc = &builtinNowWithArgSig{}
	_ BuiltinFunc = &builtinNowWithoutArgSig{}
	_ BuiltinFunc = &builtinDayNameSig{}
	_ BuiltinFunc = &builtinDayOfMonthSig{}
	_ BuiltinFunc = &builtinDayOfWeekSig{}
	_ BuiltinFunc = &builtinDayOfYearSig{}
	_ BuiltinFunc = &builtinWeekWithModeSig{}
	_ BuiltinFunc = &builtinWeekWithoutModeSig{}
	_ BuiltinFunc = &builtinWeekDaySig{}
	_ BuiltinFunc = &builtinWeekOfYearSig{}
	_ BuiltinFunc = &builtinYearSig{}
	_ BuiltinFunc = &builtinYearWeekWithModeSig{}
	_ BuiltinFunc = &builtinYearWeekWithoutModeSig{}
	_ BuiltinFunc = &builtinGetFormatSig{}
	_ BuiltinFunc = &builtinSysDateWithFspSig{}
	_ BuiltinFunc = &builtinSysDateWithoutFspSig{}
	_ BuiltinFunc = &builtinCurrentDateSig{}
	_ BuiltinFunc = &builtinCurrentTime0ArgSig{}
	_ BuiltinFunc = &builtinCurrentTime1ArgSig{}
	_ BuiltinFunc = &builtinTimeSig{}
	_ BuiltinFunc = &builtinTimeLiteralSig{}
	_ BuiltinFunc = &builtinUTCDateSig{}
	_ BuiltinFunc = &builtinUTCTimestampWithArgSig{}
	_ BuiltinFunc = &builtinUTCTimestampWithoutArgSig{}
	_ BuiltinFunc = &builtinAddDatetimeAndDurationSig{}
	_ BuiltinFunc = &builtinAddDatetimeAndStringSig{}
	_ BuiltinFunc = &builtinAddTimeDateTimeNullSig{}
	_ BuiltinFunc = &builtinAddStringAndDurationSig{}
	_ BuiltinFunc = &builtinAddStringAndStringSig{}
	_ BuiltinFunc = &builtinAddTimeStringNullSig{}
	_ BuiltinFunc = &builtinAddDurationAndDurationSig{}
	_ BuiltinFunc = &builtinAddDurationAndStringSig{}
	_ BuiltinFunc = &builtinAddTimeDurationNullSig{}
	_ BuiltinFunc = &builtinAddDateAndDurationSig{}
	_ BuiltinFunc = &builtinAddDateAndStringSig{}
	_ BuiltinFunc = &builtinSubDatetimeAndDurationSig{}
	_ BuiltinFunc = &builtinSubDatetimeAndStringSig{}
	_ BuiltinFunc = &builtinSubTimeDateTimeNullSig{}
	_ BuiltinFunc = &builtinSubStringAndDurationSig{}
	_ BuiltinFunc = &builtinSubStringAndStringSig{}
	_ BuiltinFunc = &builtinSubTimeStringNullSig{}
	_ BuiltinFunc = &builtinSubDurationAndDurationSig{}
	_ BuiltinFunc = &builtinSubDurationAndStringSig{}
	_ BuiltinFunc = &builtinSubTimeDurationNullSig{}
	_ BuiltinFunc = &builtinSubDateAndDurationSig{}
	_ BuiltinFunc = &builtinSubDateAndStringSig{}
	_ BuiltinFunc = &builtinUnixTimestampCurrentSig{}
	_ BuiltinFunc = &builtinUnixTimestampIntSig{}
	_ BuiltinFunc = &builtinUnixTimestampDecSig{}
	_ BuiltinFunc = &builtinConvertTzSig{}
	_ BuiltinFunc = &builtinMakeDateSig{}
	_ BuiltinFunc = &builtinMakeTimeSig{}
	_ BuiltinFunc = &builtinPeriodAddSig{}
	_ BuiltinFunc = &builtinPeriodDiffSig{}
	_ BuiltinFunc = &builtinQuarterSig{}
	_ BuiltinFunc = &builtinSecToTimeSig{}
	_ BuiltinFunc = &builtinTimeToSecSig{}
	_ BuiltinFunc = &builtinTimestampAddSig{}
	_ BuiltinFunc = &builtinToDaysSig{}
	_ BuiltinFunc = &builtinToSecondsSig{}
	_ BuiltinFunc = &builtinUTCTimeWithArgSig{}
	_ BuiltinFunc = &builtinUTCTimeWithoutArgSig{}
	_ BuiltinFunc = &builtinTimestamp1ArgSig{}
	_ BuiltinFunc = &builtinTimestamp2ArgsSig{}
	_ BuiltinFunc = &builtinTimestampLiteralSig{}
	_ BuiltinFunc = &builtinLastDaySig{}
	_ BuiltinFunc = &builtinStrToDateDateSig{}
	_ BuiltinFunc = &builtinStrToDateDatetimeSig{}
	_ BuiltinFunc = &builtinStrToDateDurationSig{}
	_ BuiltinFunc = &builtinFromUnixTime1ArgSig{}
	_ BuiltinFunc = &builtinFromUnixTime2ArgSig{}
	_ BuiltinFunc = &builtinExtractDatetimeSig{}
	_ BuiltinFunc = &builtinExtractDurationSig{}
	_ BuiltinFunc = &builtinAddDateStringStringSig{}
	_ BuiltinFunc = &builtinAddDateStringIntSig{}
	_ BuiltinFunc = &builtinAddDateStringRealSig{}
	_ BuiltinFunc = &builtinAddDateStringDecimalSig{}
	_ BuiltinFunc = &builtinAddDateIntStringSig{}
	_ BuiltinFunc = &builtinAddDateIntIntSig{}
	_ BuiltinFunc = &builtinAddDateIntRealSig{}
	_ BuiltinFunc = &builtinAddDateIntDecimalSig{}
	_ BuiltinFunc = &builtinAddDateDatetimeStringSig{}
	_ BuiltinFunc = &builtinAddDateDatetimeIntSig{}
	_ BuiltinFunc = &builtinAddDateDatetimeRealSig{}
	_ BuiltinFunc = &builtinAddDateDatetimeDecimalSig{}
	_ BuiltinFunc = &builtinSubDateStringStringSig{}
	_ BuiltinFunc = &builtinSubDateStringIntSig{}
	_ BuiltinFunc = &builtinSubDateStringRealSig{}
	_ BuiltinFunc = &builtinSubDateStringDecimalSig{}
	_ BuiltinFunc = &builtinSubDateIntStringSig{}
	_ BuiltinFunc = &builtinSubDateIntIntSig{}
	_ BuiltinFunc = &builtinSubDateIntRealSig{}
	_ BuiltinFunc = &builtinSubDateIntDecimalSig{}
	_ BuiltinFunc = &builtinSubDateDatetimeStringSig{}
	_ BuiltinFunc = &builtinSubDateDatetimeIntSig{}
	_ BuiltinFunc = &builtinSubDateDatetimeRealSig{}
	_ BuiltinFunc = &builtinSubDateDatetimeDecimalSig{}
)

func convertTimeToMysqlTime(t time.Time, fsp int8, roundMode types.RoundMode) (types.Time, error) {
	var tr time.Time
	var err error
	if roundMode == types.ModeTruncate {
		tr, err = types.TruncateFrac(t, fsp)
	} else {
		tr, err = types.RoundFrac(t, fsp)
	}
	if err != nil {
		return types.Time{}, err
	}

	return types.Time{
		Time: types.FromGoTime(tr),
		Type: mysql.TypeDatetime,
		Fsp:  fsp,
	}, nil
}

type dateFunctionClass struct {
	BaseFunctionClass
}

func (c *dateFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETDatetime, types.ETDatetime)
	bf.Tp.Tp, bf.Tp.Flen, bf.Tp.Decimal = mysql.TypeDate, 10, 0
	sig := &builtinDateSig{bf}
	return sig, nil
}

type builtinDateSig struct {
	BaseBuiltinFunc
}

func (b *builtinDateSig) Clone() BuiltinFunc {
	newSig := &builtinDateSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalTime evals DATE(expr).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_date
func (b *builtinDateSig) evalTime(row chunk.Row) (types.Time, bool, error) {
	expr, isNull, err := b.Args[0].EvalTime(b.Ctx, row)
	if isNull || err != nil {
		return types.Time{}, true, handleInvalidTimeError(b.Ctx, err)
	}

	if expr.IsZero() {
		return types.Time{}, true, handleInvalidTimeError(b.Ctx, types.ErrIncorrectDatetimeValue.GenWithStackByArgs(expr.String()))
	}

	expr.Time = types.FromDate(expr.Time.Year(), expr.Time.Month(), expr.Time.Day(), 0, 0, 0, 0)
	expr.Type = mysql.TypeDate
	return expr, false, nil
}

type dateLiteralFunctionClass struct {
	BaseFunctionClass
}

func (c *dateLiteralFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	con, ok := args[0].(*Constant)
	if !ok {
		panic("Unexpected parameter for date literal")
	}
	dt, err := con.Eval(chunk.Row{})
	if err != nil {
		return nil, err
	}
	str := dt.GetString()
	if !datePattern.MatchString(str) {
		return nil, types.ErrIncorrectDatetimeValue.GenWithStackByArgs(str)
	}
	tm, err := types.ParseDate(ctx.GetSessionVars().StmtCtx, str)
	if err != nil {
		return nil, err
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, []Expression{}, types.ETDatetime)
	bf.Tp.Tp, bf.Tp.Flen, bf.Tp.Decimal = mysql.TypeDate, 10, 0
	sig := &builtinDateLiteralSig{bf, tm}
	return sig, nil
}

type builtinDateLiteralSig struct {
	BaseBuiltinFunc
	literal types.Time
}

func (b *builtinDateLiteralSig) Clone() BuiltinFunc {
	newSig := &builtinDateLiteralSig{literal: b.literal}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalTime evals DATE 'stringLit'.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-literals.html
func (b *builtinDateLiteralSig) evalTime(row chunk.Row) (types.Time, bool, error) {
	mode := b.Ctx.GetSessionVars().SQLMode
	if mode.HasNoZeroDateMode() && b.literal.IsZero() {
		return b.literal, true, types.ErrIncorrectDatetimeValue.GenWithStackByArgs(b.literal.String())
	}
	if mode.HasNoZeroInDateMode() && (b.literal.InvalidZero() && !b.literal.IsZero()) {
		return b.literal, true, types.ErrIncorrectDatetimeValue.GenWithStackByArgs(b.literal.String())
	}
	return b.literal, false, nil
}

type dateDiffFunctionClass struct {
	BaseFunctionClass
}

func (c *dateDiffFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETDatetime, types.ETDatetime)
	sig := &builtinDateDiffSig{bf}
	return sig, nil
}

type builtinDateDiffSig struct {
	BaseBuiltinFunc
}

func (b *builtinDateDiffSig) Clone() BuiltinFunc {
	newSig := &builtinDateDiffSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalInt evals a builtinDateDiffSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_datediff
func (b *builtinDateDiffSig) evalInt(row chunk.Row) (int64, bool, error) {
	lhs, isNull, err := b.Args[0].EvalTime(b.Ctx, row)
	if isNull || err != nil {
		return 0, true, handleInvalidTimeError(b.Ctx, err)
	}
	rhs, isNull, err := b.Args[1].EvalTime(b.Ctx, row)
	if isNull || err != nil {
		return 0, true, handleInvalidTimeError(b.Ctx, err)
	}
	if invalidLHS, invalidRHS := lhs.InvalidZero(), rhs.InvalidZero(); invalidLHS || invalidRHS {
		if invalidLHS {
			err = handleInvalidTimeError(b.Ctx, types.ErrIncorrectDatetimeValue.GenWithStackByArgs(lhs.String()))
		}
		if invalidRHS {
			err = handleInvalidTimeError(b.Ctx, types.ErrIncorrectDatetimeValue.GenWithStackByArgs(rhs.String()))
		}
		return 0, true, err
	}
	return int64(types.DateDiff(lhs.Time, rhs.Time)), false, nil
}

type timeDiffFunctionClass struct {
	BaseFunctionClass
}

func (c *timeDiffFunctionClass) getArgEvalTp(fieldTp *types.FieldType) types.EvalType {
	argTp := types.ETString
	switch tp := fieldTp.EvalType(); tp {
	case types.ETDuration, types.ETDatetime, types.ETTimestamp:
		argTp = tp
	}
	return argTp
}

func (c *timeDiffFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}

	arg0FieldTp, arg1FieldTp := args[0].GetType(), args[1].GetType()
	arg0Tp, arg1Tp := c.getArgEvalTp(arg0FieldTp), c.getArgEvalTp(arg1FieldTp)
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETDuration, arg0Tp, arg1Tp)

	arg0Dec, err := getExpressionFsp(ctx, args[0])
	if err != nil {
		return nil, err
	}
	arg1Dec, err := getExpressionFsp(ctx, args[1])
	if err != nil {
		return nil, err
	}
	bf.Tp.Decimal = mathutil.Max(arg0Dec, arg1Dec)

	var sig BuiltinFunc
	// arg0 and arg1 must be the same time type(compatible), or timediff will return NULL.
	// TODO: we don't really need Duration type, actually in MySQL, it use Time class to represent
	// all the time type, and use filed type to distinguish datetime, date, timestamp or time(duration).
	// With the duration type, we are hard to port all the MySQL behavior.
	switch arg0Tp {
	case types.ETDuration:
		switch arg1Tp {
		case types.ETDuration:
			sig = &builtinDurationDurationTimeDiffSig{bf}
		case types.ETDatetime, types.ETTimestamp:
			sig = &builtinNullTimeDiffSig{bf}
		default:
			sig = &builtinDurationStringTimeDiffSig{bf}
		}
	case types.ETDatetime, types.ETTimestamp:
		switch arg1Tp {
		case types.ETDuration:
			sig = &builtinNullTimeDiffSig{bf}
		case types.ETDatetime, types.ETTimestamp:
			sig = &builtinTimeTimeTimeDiffSig{bf}
		default:
			sig = &builtinTimeStringTimeDiffSig{bf}
		}
	default:
		switch arg1Tp {
		case types.ETDuration:
			sig = &builtinStringDurationTimeDiffSig{bf}
		case types.ETDatetime, types.ETTimestamp:
			sig = &builtinStringTimeTimeDiffSig{bf}
		default:
			sig = &builtinStringStringTimeDiffSig{bf}
		}
	}
	return sig, nil
}

type builtinDurationDurationTimeDiffSig struct {
	BaseBuiltinFunc
}

func (b *builtinDurationDurationTimeDiffSig) Clone() BuiltinFunc {
	newSig := &builtinDurationDurationTimeDiffSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalDuration evals a builtinDurationDurationTimeDiffSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_timediff
func (b *builtinDurationDurationTimeDiffSig) evalDuration(row chunk.Row) (d types.Duration, isNull bool, err error) {
	lhs, isNull, err := b.Args[0].EvalDuration(b.Ctx, row)
	if isNull || err != nil {
		return d, isNull, err
	}

	rhs, isNull, err := b.Args[1].EvalDuration(b.Ctx, row)
	if isNull || err != nil {
		return d, isNull, err
	}

	d, isNull, err = calculateDurationTimeDiff(b.Ctx, lhs, rhs)
	return d, isNull, err
}

type builtinTimeTimeTimeDiffSig struct {
	BaseBuiltinFunc
}

func (b *builtinTimeTimeTimeDiffSig) Clone() BuiltinFunc {
	newSig := &builtinTimeTimeTimeDiffSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalDuration evals a builtinTimeTimeTimeDiffSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_timediff
func (b *builtinTimeTimeTimeDiffSig) evalDuration(row chunk.Row) (d types.Duration, isNull bool, err error) {
	lhs, isNull, err := b.Args[0].EvalTime(b.Ctx, row)
	if isNull || err != nil {
		return d, isNull, err
	}

	rhs, isNull, err := b.Args[1].EvalTime(b.Ctx, row)
	if isNull || err != nil {
		return d, isNull, err
	}

	sc := b.Ctx.GetSessionVars().StmtCtx
	d, isNull, err = calculateTimeDiff(sc, lhs, rhs)
	return d, isNull, err
}

type builtinDurationStringTimeDiffSig struct {
	BaseBuiltinFunc
}

func (b *builtinDurationStringTimeDiffSig) Clone() BuiltinFunc {
	newSig := &builtinDurationStringTimeDiffSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalDuration evals a builtinDurationStringTimeDiffSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_timediff
func (b *builtinDurationStringTimeDiffSig) evalDuration(row chunk.Row) (d types.Duration, isNull bool, err error) {
	lhs, isNull, err := b.Args[0].EvalDuration(b.Ctx, row)
	if isNull || err != nil {
		return d, isNull, err
	}

	rhsStr, isNull, err := b.Args[1].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return d, isNull, err
	}

	sc := b.Ctx.GetSessionVars().StmtCtx
	rhs, _, isDuration, err := convertStringToDuration(sc, rhsStr, int8(b.Tp.Decimal))
	if err != nil || !isDuration {
		return d, true, err
	}

	d, isNull, err = calculateDurationTimeDiff(b.Ctx, lhs, rhs)
	return d, isNull, err
}

type builtinStringDurationTimeDiffSig struct {
	BaseBuiltinFunc
}

func (b *builtinStringDurationTimeDiffSig) Clone() BuiltinFunc {
	newSig := &builtinStringDurationTimeDiffSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalDuration evals a builtinStringDurationTimeDiffSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_timediff
func (b *builtinStringDurationTimeDiffSig) evalDuration(row chunk.Row) (d types.Duration, isNull bool, err error) {
	lhsStr, isNull, err := b.Args[0].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return d, isNull, err
	}

	rhs, isNull, err := b.Args[1].EvalDuration(b.Ctx, row)
	if isNull || err != nil {
		return d, isNull, err
	}

	sc := b.Ctx.GetSessionVars().StmtCtx
	lhs, _, isDuration, err := convertStringToDuration(sc, lhsStr, int8(b.Tp.Decimal))
	if err != nil || !isDuration {
		return d, true, err
	}

	d, isNull, err = calculateDurationTimeDiff(b.Ctx, lhs, rhs)
	return d, isNull, err
}

// calculateTimeDiff calculates interval difference of two types.Time.
func calculateTimeDiff(sc *stmtctx.StatementContext, lhs, rhs types.Time) (d types.Duration, isNull bool, err error) {
	d = lhs.Sub(sc, &rhs)
	d.Duration, err = types.TruncateOverflowMySQLTime(d.Duration)
	if types.ErrTruncatedWrongVal.Equal(err) {
		err = sc.HandleTruncate(err)
	}
	return d, err != nil, err
}

// calculateDurationTimeDiff calculates interval difference of two types.Duration.
func calculateDurationTimeDiff(ctx sessionctx.Context, lhs, rhs types.Duration) (d types.Duration, isNull bool, err error) {
	d, err = lhs.Sub(rhs)
	if err != nil {
		return d, true, err
	}

	d.Duration, err = types.TruncateOverflowMySQLTime(d.Duration)
	if types.ErrTruncatedWrongVal.Equal(err) {
		sc := ctx.GetSessionVars().StmtCtx
		err = sc.HandleTruncate(err)
	}
	return d, err != nil, err
}

type builtinTimeStringTimeDiffSig struct {
	BaseBuiltinFunc
}

func (b *builtinTimeStringTimeDiffSig) Clone() BuiltinFunc {
	newSig := &builtinTimeStringTimeDiffSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalDuration evals a builtinTimeStringTimeDiffSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_timediff
func (b *builtinTimeStringTimeDiffSig) evalDuration(row chunk.Row) (d types.Duration, isNull bool, err error) {
	lhs, isNull, err := b.Args[0].EvalTime(b.Ctx, row)
	if isNull || err != nil {
		return d, isNull, err
	}

	rhsStr, isNull, err := b.Args[1].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return d, isNull, err
	}

	sc := b.Ctx.GetSessionVars().StmtCtx
	_, rhs, isDuration, err := convertStringToDuration(sc, rhsStr, int8(b.Tp.Decimal))
	if err != nil || isDuration {
		return d, true, err
	}

	d, isNull, err = calculateTimeDiff(sc, lhs, rhs)
	return d, isNull, err
}

type builtinStringTimeTimeDiffSig struct {
	BaseBuiltinFunc
}

func (b *builtinStringTimeTimeDiffSig) Clone() BuiltinFunc {
	newSig := &builtinStringTimeTimeDiffSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalDuration evals a builtinStringTimeTimeDiffSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_timediff
func (b *builtinStringTimeTimeDiffSig) evalDuration(row chunk.Row) (d types.Duration, isNull bool, err error) {
	lhsStr, isNull, err := b.Args[0].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return d, isNull, err
	}

	rhs, isNull, err := b.Args[1].EvalTime(b.Ctx, row)
	if isNull || err != nil {
		return d, isNull, err
	}

	sc := b.Ctx.GetSessionVars().StmtCtx
	_, lhs, isDuration, err := convertStringToDuration(sc, lhsStr, int8(b.Tp.Decimal))
	if err != nil || isDuration {
		return d, true, err
	}

	d, isNull, err = calculateTimeDiff(sc, lhs, rhs)
	return d, isNull, err
}

type builtinStringStringTimeDiffSig struct {
	BaseBuiltinFunc
}

func (b *builtinStringStringTimeDiffSig) Clone() BuiltinFunc {
	newSig := &builtinStringStringTimeDiffSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalDuration evals a builtinStringStringTimeDiffSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_timediff
func (b *builtinStringStringTimeDiffSig) evalDuration(row chunk.Row) (d types.Duration, isNull bool, err error) {
	lhs, isNull, err := b.Args[0].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return d, isNull, err
	}

	rhs, isNull, err := b.Args[1].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return d, isNull, err
	}

	sc := b.Ctx.GetSessionVars().StmtCtx
	fsp := int8(b.Tp.Decimal)
	lhsDur, lhsTime, lhsIsDuration, err := convertStringToDuration(sc, lhs, fsp)
	if err != nil {
		return d, true, err
	}

	rhsDur, rhsTime, rhsIsDuration, err := convertStringToDuration(sc, rhs, fsp)
	if err != nil {
		return d, true, err
	}

	if lhsIsDuration != rhsIsDuration {
		return d, true, nil
	}

	if lhsIsDuration {
		d, isNull, err = calculateDurationTimeDiff(b.Ctx, lhsDur, rhsDur)
	} else {
		d, isNull, err = calculateTimeDiff(sc, lhsTime, rhsTime)
	}

	return d, isNull, err
}

type builtinNullTimeDiffSig struct {
	BaseBuiltinFunc
}

func (b *builtinNullTimeDiffSig) Clone() BuiltinFunc {
	newSig := &builtinNullTimeDiffSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalDuration evals a builtinNullTimeDiffSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_timediff
func (b *builtinNullTimeDiffSig) evalDuration(row chunk.Row) (d types.Duration, isNull bool, err error) {
	return d, true, nil
}

// convertStringToDuration converts string to duration, it return types.Time because in some case
// it will converts string to datetime.
func convertStringToDuration(sc *stmtctx.StatementContext, str string, fsp int8) (d types.Duration, t types.Time,
	isDuration bool, err error) {
	if n := strings.IndexByte(str, '.'); n >= 0 {
		lenStrFsp := len(str[n+1:])
		if lenStrFsp <= int(types.MaxFsp) {
			fsp = mathutil.MaxInt8(int8(lenStrFsp), fsp)
		}
	}
	return types.StrToDuration(sc, str, fsp)
}

type dateFormatFunctionClass struct {
	BaseFunctionClass
}

func (c *dateFormatFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETString, types.ETDatetime, types.ETString)
	// worst case: formatMask=%r%r%r...%r, each %r takes 11 characters
	bf.Tp.Flen = (args[1].GetType().Flen + 1) / 2 * 11
	sig := &builtinDateFormatSig{bf}
	sig.SetPbCode(tipb.ScalarFuncSig_DateFormatSig)
	return sig, nil
}

type builtinDateFormatSig struct {
	BaseBuiltinFunc
}

func (b *builtinDateFormatSig) Clone() BuiltinFunc {
	newSig := &builtinDateFormatSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalString evals a builtinDateFormatSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_date-format
func (b *builtinDateFormatSig) EvalString(row chunk.Row) (string, bool, error) {
	t, isNull, err := b.Args[0].EvalTime(b.Ctx, row)
	if isNull || err != nil {
		return "", isNull, handleInvalidTimeError(b.Ctx, err)
	}
	if t.InvalidZero() {
		return "", true, handleInvalidTimeError(b.Ctx, types.ErrIncorrectDatetimeValue.GenWithStackByArgs(t.String()))
	}
	formatMask, isNull, err := b.Args[1].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}

	res, err := t.DateFormat(formatMask)
	return res, isNull, err
}

type fromDaysFunctionClass struct {
	BaseFunctionClass
}

func (c *fromDaysFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETDatetime, types.ETInt)
	bf.Tp.Flen, bf.Tp.Decimal = 10, 0
	sig := &builtinFromDaysSig{bf}
	return sig, nil
}

type builtinFromDaysSig struct {
	BaseBuiltinFunc
}

func (b *builtinFromDaysSig) Clone() BuiltinFunc {
	newSig := &builtinFromDaysSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalTime evals FROM_DAYS(N).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_from-days
func (b *builtinFromDaysSig) evalTime(row chunk.Row) (types.Time, bool, error) {
	n, isNull, err := b.Args[0].EvalInt(b.Ctx, row)
	if isNull || err != nil {
		return types.Time{}, true, err
	}

	return types.TimeFromDays(n), false, nil
}

type hourFunctionClass struct {
	BaseFunctionClass
}

func (c *hourFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETDuration)
	bf.Tp.Flen, bf.Tp.Decimal = 3, 0
	sig := &builtinHourSig{bf}
	return sig, nil
}

type builtinHourSig struct {
	BaseBuiltinFunc
}

func (b *builtinHourSig) Clone() BuiltinFunc {
	newSig := &builtinHourSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalInt evals HOUR(time).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_hour
func (b *builtinHourSig) evalInt(row chunk.Row) (int64, bool, error) {
	dur, isNull, err := b.Args[0].EvalDuration(b.Ctx, row)
	// ignore error and return NULL
	if isNull || err != nil {
		return 0, true, nil
	}
	return int64(dur.Hour()), false, nil
}

type minuteFunctionClass struct {
	BaseFunctionClass
}

func (c *minuteFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETDuration)
	bf.Tp.Flen, bf.Tp.Decimal = 2, 0
	sig := &builtinMinuteSig{bf}
	return sig, nil
}

type builtinMinuteSig struct {
	BaseBuiltinFunc
}

func (b *builtinMinuteSig) Clone() BuiltinFunc {
	newSig := &builtinMinuteSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalInt evals MINUTE(time).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_minute
func (b *builtinMinuteSig) evalInt(row chunk.Row) (int64, bool, error) {
	dur, isNull, err := b.Args[0].EvalDuration(b.Ctx, row)
	// ignore error and return NULL
	if isNull || err != nil {
		return 0, true, nil
	}
	return int64(dur.Minute()), false, nil
}

type secondFunctionClass struct {
	BaseFunctionClass
}

func (c *secondFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETDuration)
	bf.Tp.Flen, bf.Tp.Decimal = 2, 0
	sig := &builtinSecondSig{bf}
	return sig, nil
}

type builtinSecondSig struct {
	BaseBuiltinFunc
}

func (b *builtinSecondSig) Clone() BuiltinFunc {
	newSig := &builtinSecondSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalInt evals SECOND(time).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_second
func (b *builtinSecondSig) evalInt(row chunk.Row) (int64, bool, error) {
	dur, isNull, err := b.Args[0].EvalDuration(b.Ctx, row)
	// ignore error and return NULL
	if isNull || err != nil {
		return 0, true, nil
	}
	return int64(dur.Second()), false, nil
}

type microSecondFunctionClass struct {
	BaseFunctionClass
}

func (c *microSecondFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETDuration)
	bf.Tp.Flen, bf.Tp.Decimal = 6, 0
	sig := &builtinMicroSecondSig{bf}
	return sig, nil
}

type builtinMicroSecondSig struct {
	BaseBuiltinFunc
}

func (b *builtinMicroSecondSig) Clone() BuiltinFunc {
	newSig := &builtinMicroSecondSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalInt evals MICROSECOND(expr).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_microsecond
func (b *builtinMicroSecondSig) evalInt(row chunk.Row) (int64, bool, error) {
	dur, isNull, err := b.Args[0].EvalDuration(b.Ctx, row)
	// ignore error and return NULL
	if isNull || err != nil {
		return 0, true, nil
	}
	return int64(dur.MicroSecond()), false, nil
}

type monthFunctionClass struct {
	BaseFunctionClass
}

func (c *monthFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETDatetime)
	bf.Tp.Flen, bf.Tp.Decimal = 2, 0
	sig := &builtinMonthSig{bf}
	return sig, nil
}

type builtinMonthSig struct {
	BaseBuiltinFunc
}

func (b *builtinMonthSig) Clone() BuiltinFunc {
	newSig := &builtinMonthSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalInt evals MONTH(date).
// see: https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_month
func (b *builtinMonthSig) evalInt(row chunk.Row) (int64, bool, error) {
	date, isNull, err := b.Args[0].EvalTime(b.Ctx, row)

	if isNull || err != nil {
		return 0, true, handleInvalidTimeError(b.Ctx, err)
	}

	if date.IsZero() {
		if b.Ctx.GetSessionVars().SQLMode.HasNoZeroDateMode() {
			return 0, true, handleInvalidTimeError(b.Ctx, types.ErrIncorrectDatetimeValue.GenWithStackByArgs(date.String()))
		}
		return 0, false, nil
	}

	return int64(date.Time.Month()), false, nil
}

// monthNameFunctionClass see https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_monthname
type monthNameFunctionClass struct {
	BaseFunctionClass
}

func (c *monthNameFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETString, types.ETDatetime)
	bf.Tp.Flen = 10
	sig := &builtinMonthNameSig{bf}
	return sig, nil
}

type builtinMonthNameSig struct {
	BaseBuiltinFunc
}

func (b *builtinMonthNameSig) Clone() BuiltinFunc {
	newSig := &builtinMonthNameSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinMonthNameSig) EvalString(row chunk.Row) (string, bool, error) {
	arg, isNull, err := b.Args[0].EvalTime(b.Ctx, row)
	if isNull || err != nil {
		return "", true, handleInvalidTimeError(b.Ctx, err)
	}
	mon := arg.Time.Month()
	if (arg.IsZero() && b.Ctx.GetSessionVars().SQLMode.HasNoZeroDateMode()) || mon < 0 || mon > len(types.MonthNames) {
		return "", true, handleInvalidTimeError(b.Ctx, types.ErrIncorrectDatetimeValue.GenWithStackByArgs(arg.String()))
	} else if mon == 0 || arg.IsZero() {
		return "", true, nil
	}
	return types.MonthNames[mon-1], false, nil
}

type dayNameFunctionClass struct {
	BaseFunctionClass
}

func (c *dayNameFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETString, types.ETDatetime)
	bf.Tp.Flen = 10
	sig := &builtinDayNameSig{bf}
	return sig, nil
}

type builtinDayNameSig struct {
	BaseBuiltinFunc
}

func (b *builtinDayNameSig) Clone() BuiltinFunc {
	newSig := &builtinDayNameSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinDayNameSig) evalIndex(row chunk.Row) (int64, bool, error) {
	arg, isNull, err := b.Args[0].EvalTime(b.Ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	if arg.InvalidZero() {
		return 0, true, handleInvalidTimeError(b.Ctx, types.ErrIncorrectDatetimeValue.GenWithStackByArgs(arg.String()))
	}
	// Monday is 0, ... Sunday = 6 in MySQL
	// but in go, Sunday is 0, ... Saturday is 6
	// w will do a conversion.
	res := (int64(arg.Time.Weekday()) + 6) % 7
	return res, false, nil
}

// evalString evals a builtinDayNameSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_dayname
func (b *builtinDayNameSig) EvalString(row chunk.Row) (string, bool, error) {
	idx, isNull, err := b.evalIndex(row)
	if isNull || err != nil {
		return "", isNull, err
	}
	return types.WeekdayNames[idx], false, nil
}

func (b *builtinDayNameSig) evalReal(row chunk.Row) (float64, bool, error) {
	idx, isNull, err := b.evalIndex(row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	return float64(idx), false, nil
}

func (b *builtinDayNameSig) evalInt(row chunk.Row) (int64, bool, error) {
	idx, isNull, err := b.evalIndex(row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	return idx, false, nil
}

type dayOfMonthFunctionClass struct {
	BaseFunctionClass
}

func (c *dayOfMonthFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETDatetime)
	bf.Tp.Flen = 2
	sig := &builtinDayOfMonthSig{bf}
	return sig, nil
}

type builtinDayOfMonthSig struct {
	BaseBuiltinFunc
}

func (b *builtinDayOfMonthSig) Clone() BuiltinFunc {
	newSig := &builtinDayOfMonthSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalInt evals a builtinDayOfMonthSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_dayofmonth
func (b *builtinDayOfMonthSig) evalInt(row chunk.Row) (int64, bool, error) {
	arg, isNull, err := b.Args[0].EvalTime(b.Ctx, row)
	if isNull || err != nil {
		return 0, true, handleInvalidTimeError(b.Ctx, err)
	}
	if arg.IsZero() {
		if b.Ctx.GetSessionVars().SQLMode.HasNoZeroDateMode() {
			return 0, true, handleInvalidTimeError(b.Ctx, types.ErrIncorrectDatetimeValue.GenWithStackByArgs(arg.String()))
		}
		return 0, false, nil
	}
	return int64(arg.Time.Day()), false, nil
}

type dayOfWeekFunctionClass struct {
	BaseFunctionClass
}

func (c *dayOfWeekFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETDatetime)
	bf.Tp.Flen = 1
	sig := &builtinDayOfWeekSig{bf}
	return sig, nil
}

type builtinDayOfWeekSig struct {
	BaseBuiltinFunc
}

func (b *builtinDayOfWeekSig) Clone() BuiltinFunc {
	newSig := &builtinDayOfWeekSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalInt evals a builtinDayOfWeekSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_dayofweek
func (b *builtinDayOfWeekSig) evalInt(row chunk.Row) (int64, bool, error) {
	arg, isNull, err := b.Args[0].EvalTime(b.Ctx, row)
	if isNull || err != nil {
		return 0, true, handleInvalidTimeError(b.Ctx, err)
	}
	if arg.InvalidZero() {
		return 0, true, handleInvalidTimeError(b.Ctx, types.ErrIncorrectDatetimeValue.GenWithStackByArgs(arg.String()))
	}
	// 1 is Sunday, 2 is Monday, .... 7 is Saturday
	return int64(arg.Time.Weekday() + 1), false, nil
}

type dayOfYearFunctionClass struct {
	BaseFunctionClass
}

func (c *dayOfYearFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETDatetime)
	bf.Tp.Flen = 3
	sig := &builtinDayOfYearSig{bf}
	return sig, nil
}

type builtinDayOfYearSig struct {
	BaseBuiltinFunc
}

func (b *builtinDayOfYearSig) Clone() BuiltinFunc {
	newSig := &builtinDayOfYearSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalInt evals a builtinDayOfYearSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_dayofyear
func (b *builtinDayOfYearSig) evalInt(row chunk.Row) (int64, bool, error) {
	arg, isNull, err := b.Args[0].EvalTime(b.Ctx, row)
	if isNull || err != nil {
		return 0, isNull, handleInvalidTimeError(b.Ctx, err)
	}
	if arg.InvalidZero() {
		return 0, true, handleInvalidTimeError(b.Ctx, types.ErrIncorrectDatetimeValue.GenWithStackByArgs(arg.String()))
	}

	return int64(arg.Time.YearDay()), false, nil
}

type weekFunctionClass struct {
	BaseFunctionClass
}

func (c *weekFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}

	argTps := []types.EvalType{types.ETDatetime}
	if len(args) == 2 {
		argTps = append(argTps, types.ETInt)
	}

	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETInt, argTps...)

	bf.Tp.Flen, bf.Tp.Decimal = 2, 0

	var sig BuiltinFunc
	if len(args) == 2 {
		sig = &builtinWeekWithModeSig{bf}
	} else {
		sig = &builtinWeekWithoutModeSig{bf}
	}
	return sig, nil
}

type builtinWeekWithModeSig struct {
	BaseBuiltinFunc
}

func (b *builtinWeekWithModeSig) Clone() BuiltinFunc {
	newSig := &builtinWeekWithModeSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalInt evals WEEK(date, mode).
// see: https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_week
func (b *builtinWeekWithModeSig) evalInt(row chunk.Row) (int64, bool, error) {
	date, isNull, err := b.Args[0].EvalTime(b.Ctx, row)

	if isNull || err != nil {
		return 0, true, handleInvalidTimeError(b.Ctx, err)
	}

	if date.IsZero() {
		return 0, true, handleInvalidTimeError(b.Ctx, types.ErrIncorrectDatetimeValue.GenWithStackByArgs(date.String()))
	}

	mode, isNull, err := b.Args[1].EvalInt(b.Ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}

	week := date.Time.Week(int(mode))
	return int64(week), false, nil
}

type builtinWeekWithoutModeSig struct {
	BaseBuiltinFunc
}

func (b *builtinWeekWithoutModeSig) Clone() BuiltinFunc {
	newSig := &builtinWeekWithoutModeSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalInt evals WEEK(date).
// see: https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_week
func (b *builtinWeekWithoutModeSig) evalInt(row chunk.Row) (int64, bool, error) {
	date, isNull, err := b.Args[0].EvalTime(b.Ctx, row)

	if isNull || err != nil {
		return 0, true, handleInvalidTimeError(b.Ctx, err)
	}

	if date.IsZero() {
		return 0, true, handleInvalidTimeError(b.Ctx, types.ErrIncorrectDatetimeValue.GenWithStackByArgs(date.String()))
	}

	mode := 0
	modeStr, ok := b.Ctx.GetSessionVars().GetSystemVar(variable.DefaultWeekFormat)
	if ok && modeStr != "" {
		mode, err = strconv.Atoi(modeStr)
		if err != nil {
			return 0, true, handleInvalidTimeError(b.Ctx, types.ErrInvalidWeekModeFormat.GenWithStackByArgs(modeStr))
		}
	}

	week := date.Time.Week(mode)
	return int64(week), false, nil
}

type weekDayFunctionClass struct {
	BaseFunctionClass
}

func (c *weekDayFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}

	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETDatetime)
	bf.Tp.Flen = 1

	sig := &builtinWeekDaySig{bf}
	return sig, nil
}

type builtinWeekDaySig struct {
	BaseBuiltinFunc
}

func (b *builtinWeekDaySig) Clone() BuiltinFunc {
	newSig := &builtinWeekDaySig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalInt evals WEEKDAY(date).
func (b *builtinWeekDaySig) evalInt(row chunk.Row) (int64, bool, error) {
	date, isNull, err := b.Args[0].EvalTime(b.Ctx, row)
	if isNull || err != nil {
		return 0, true, handleInvalidTimeError(b.Ctx, err)
	}

	if date.IsZero() {
		return 0, true, handleInvalidTimeError(b.Ctx, types.ErrIncorrectDatetimeValue.GenWithStackByArgs(date.String()))
	}

	return int64(date.Time.Weekday()+6) % 7, false, nil
}

type weekOfYearFunctionClass struct {
	BaseFunctionClass
}

func (c *weekOfYearFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETDatetime)
	bf.Tp.Flen, bf.Tp.Decimal = 2, 0
	sig := &builtinWeekOfYearSig{bf}
	return sig, nil
}

type builtinWeekOfYearSig struct {
	BaseBuiltinFunc
}

func (b *builtinWeekOfYearSig) Clone() BuiltinFunc {
	newSig := &builtinWeekOfYearSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalInt evals WEEKOFYEAR(date).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_weekofyear
func (b *builtinWeekOfYearSig) evalInt(row chunk.Row) (int64, bool, error) {
	date, isNull, err := b.Args[0].EvalTime(b.Ctx, row)

	if isNull || err != nil {
		return 0, true, handleInvalidTimeError(b.Ctx, err)
	}

	if date.IsZero() {
		return 0, true, handleInvalidTimeError(b.Ctx, types.ErrIncorrectDatetimeValue.GenWithStackByArgs(date.String()))
	}

	week := date.Time.Week(3)
	return int64(week), false, nil
}

type yearFunctionClass struct {
	BaseFunctionClass
}

func (c *yearFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETDatetime)
	bf.Tp.Flen, bf.Tp.Decimal = 4, 0
	sig := &builtinYearSig{bf}
	return sig, nil
}

type builtinYearSig struct {
	BaseBuiltinFunc
}

func (b *builtinYearSig) Clone() BuiltinFunc {
	newSig := &builtinYearSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalInt evals YEAR(date).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_year
func (b *builtinYearSig) evalInt(row chunk.Row) (int64, bool, error) {
	date, isNull, err := b.Args[0].EvalTime(b.Ctx, row)

	if isNull || err != nil {
		return 0, true, handleInvalidTimeError(b.Ctx, err)
	}

	if date.IsZero() {
		if b.Ctx.GetSessionVars().SQLMode.HasNoZeroDateMode() {
			return 0, true, handleInvalidTimeError(b.Ctx, types.ErrIncorrectDatetimeValue.GenWithStackByArgs(date.String()))
		}
		return 0, false, nil
	}
	return int64(date.Time.Year()), false, nil
}

type yearWeekFunctionClass struct {
	BaseFunctionClass
}

func (c *yearWeekFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	argTps := []types.EvalType{types.ETDatetime}
	if len(args) == 2 {
		argTps = append(argTps, types.ETInt)
	}

	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETInt, argTps...)

	bf.Tp.Flen, bf.Tp.Decimal = 6, 0

	var sig BuiltinFunc
	if len(args) == 2 {
		sig = &builtinYearWeekWithModeSig{bf}
	} else {
		sig = &builtinYearWeekWithoutModeSig{bf}
	}
	return sig, nil
}

type builtinYearWeekWithModeSig struct {
	BaseBuiltinFunc
}

func (b *builtinYearWeekWithModeSig) Clone() BuiltinFunc {
	newSig := &builtinYearWeekWithModeSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalInt evals YEARWEEK(date,mode).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_yearweek
func (b *builtinYearWeekWithModeSig) evalInt(row chunk.Row) (int64, bool, error) {
	date, isNull, err := b.Args[0].EvalTime(b.Ctx, row)
	if isNull || err != nil {
		return 0, isNull, handleInvalidTimeError(b.Ctx, err)
	}
	if date.IsZero() {
		return 0, true, handleInvalidTimeError(b.Ctx, types.ErrIncorrectDatetimeValue.GenWithStackByArgs(date.String()))
	}

	mode, isNull, err := b.Args[1].EvalInt(b.Ctx, row)
	if err != nil {
		return 0, true, err
	}
	if isNull {
		mode = 0
	}

	year, week := date.Time.YearWeek(int(mode))
	result := int64(week + year*100)
	if result < 0 {
		return int64(math.MaxUint32), false, nil
	}
	return result, false, nil
}

type builtinYearWeekWithoutModeSig struct {
	BaseBuiltinFunc
}

func (b *builtinYearWeekWithoutModeSig) Clone() BuiltinFunc {
	newSig := &builtinYearWeekWithoutModeSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalInt evals YEARWEEK(date).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_yearweek
func (b *builtinYearWeekWithoutModeSig) evalInt(row chunk.Row) (int64, bool, error) {
	date, isNull, err := b.Args[0].EvalTime(b.Ctx, row)
	if isNull || err != nil {
		return 0, true, handleInvalidTimeError(b.Ctx, err)
	}

	if date.InvalidZero() {
		return 0, true, handleInvalidTimeError(b.Ctx, types.ErrIncorrectDatetimeValue.GenWithStackByArgs(date.String()))
	}

	year, week := date.Time.YearWeek(0)
	result := int64(week + year*100)
	if result < 0 {
		return int64(math.MaxUint32), false, nil
	}
	return result, false, nil
}

type fromUnixTimeFunctionClass struct {
	BaseFunctionClass
}

func (c *fromUnixTimeFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (sig BuiltinFunc, err error) {
	if err = c.VerifyArgs(args); err != nil {
		return nil, err
	}

	retTp, argTps := types.ETDatetime, make([]types.EvalType, 0, len(args))
	argTps = append(argTps, types.ETDecimal)
	if len(args) == 2 {
		retTp = types.ETString
		argTps = append(argTps, types.ETString)
	}

	_, isArg0Con := args[0].(*Constant)
	isArg0Str := args[0].GetType().EvalType() == types.ETString
	bf := NewBaseBuiltinFuncWithTp(ctx, args, retTp, argTps...)

	if len(args) > 1 {
		bf.Tp.Flen = args[1].GetType().Flen
		return &builtinFromUnixTime2ArgSig{bf}, nil
	}

	// Calculate the time fsp.
	switch {
	case isArg0Str:
		bf.Tp.Decimal = int(types.MaxFsp)
	case isArg0Con:
		arg0, arg0IsNull, err0 := args[0].EvalDecimal(ctx, chunk.Row{})
		if err0 != nil {
			return nil, err0
		}

		bf.Tp.Decimal = int(types.MaxFsp)
		if !arg0IsNull {
			fsp := int(arg0.GetDigitsFrac())
			bf.Tp.Decimal = mathutil.Min(fsp, int(types.MaxFsp))
		}
	}

	return &builtinFromUnixTime1ArgSig{bf}, nil
}

func evalFromUnixTime(ctx sessionctx.Context, fsp int8, row chunk.Row, arg Expression) (res types.Time, isNull bool, err error) {
	unixTimeStamp, isNull, err := arg.EvalDecimal(ctx, row)
	if err != nil || isNull {
		return res, isNull, err
	}
	// 0 <= unixTimeStamp <= INT32_MAX
	if unixTimeStamp.IsNegative() {
		return res, true, nil
	}
	integralPart, err := unixTimeStamp.ToInt()
	if err != nil && !terror.ErrorEqual(err, types.ErrTruncated) {
		return res, true, err
	}
	if integralPart > int64(math.MaxInt32) {
		return res, true, nil
	}
	// Split the integral part and fractional part of a decimal timestamp.
	// e.g. for timestamp 12345.678,
	// first get the integral part 12345,
	// then (12345.678 - 12345) * (10^9) to get the decimal part and convert it to nanosecond precision.
	integerDecimalTp := new(types.MyDecimal).FromInt(integralPart)
	fracDecimalTp := new(types.MyDecimal)
	err = types.DecimalSub(unixTimeStamp, integerDecimalTp, fracDecimalTp)
	if err != nil {
		return res, true, err
	}
	nano := new(types.MyDecimal).FromInt(int64(time.Second))
	x := new(types.MyDecimal)
	err = types.DecimalMul(fracDecimalTp, nano, x)
	if err != nil {
		return res, true, err
	}
	fractionalPart, err := x.ToInt() // here fractionalPart is result multiplying the original fractional part by 10^9.
	if err != nil && !terror.ErrorEqual(err, types.ErrTruncated) {
		return res, true, err
	}
	fracDigitsNumber := unixTimeStamp.GetDigitsFrac()
	if fsp < 0 {
		fsp = types.MaxFsp
	}
	fsp = mathutil.MaxInt8(fracDigitsNumber, fsp)
	if fsp > types.MaxFsp {
		fsp = types.MaxFsp
	}

	sc := ctx.GetSessionVars().StmtCtx
	tmp := time.Unix(integralPart, fractionalPart).In(sc.TimeZone)
	t, err := convertTimeToMysqlTime(tmp, fsp, types.ModeHalfEven)
	if err != nil {
		return res, true, err
	}
	return t, false, nil
}

type builtinFromUnixTime1ArgSig struct {
	BaseBuiltinFunc
}

func (b *builtinFromUnixTime1ArgSig) Clone() BuiltinFunc {
	newSig := &builtinFromUnixTime1ArgSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalTime evals a builtinFromUnixTime1ArgSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_from-unixtime
func (b *builtinFromUnixTime1ArgSig) evalTime(row chunk.Row) (res types.Time, isNull bool, err error) {
	return evalFromUnixTime(b.Ctx, int8(b.Tp.Decimal), row, b.Args[0])
}

type builtinFromUnixTime2ArgSig struct {
	BaseBuiltinFunc
}

func (b *builtinFromUnixTime2ArgSig) Clone() BuiltinFunc {
	newSig := &builtinFromUnixTime2ArgSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalString evals a builtinFromUnixTime2ArgSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_from-unixtime
func (b *builtinFromUnixTime2ArgSig) EvalString(row chunk.Row) (res string, isNull bool, err error) {
	format, isNull, err := b.Args[1].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	t, isNull, err := evalFromUnixTime(b.Ctx, int8(b.Tp.Decimal), row, b.Args[0])
	if isNull || err != nil {
		return "", isNull, err
	}
	res, err = t.DateFormat(format)
	return res, err != nil, err
}

type getFormatFunctionClass struct {
	BaseFunctionClass
}

func (c *getFormatFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETString, types.ETString, types.ETString)
	bf.Tp.Flen = 17
	sig := &builtinGetFormatSig{bf}
	return sig, nil
}

type builtinGetFormatSig struct {
	BaseBuiltinFunc
}

func (b *builtinGetFormatSig) Clone() BuiltinFunc {
	newSig := &builtinGetFormatSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalString evals a builtinGetFormatSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_get-format
func (b *builtinGetFormatSig) EvalString(row chunk.Row) (string, bool, error) {
	t, isNull, err := b.Args[0].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	l, isNull, err := b.Args[1].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}

	var res string
	switch t {
	case dateFormat:
		switch l {
		case usaLocation:
			res = "%m.%d.%Y"
		case jisLocation:
			res = "%Y-%m-%d"
		case isoLocation:
			res = "%Y-%m-%d"
		case eurLocation:
			res = "%d.%m.%Y"
		case internalLocation:
			res = "%Y%m%d"
		}
	case datetimeFormat, timestampFormat:
		switch l {
		case usaLocation:
			res = "%Y-%m-%d %H.%i.%s"
		case jisLocation:
			res = "%Y-%m-%d %H:%i:%s"
		case isoLocation:
			res = "%Y-%m-%d %H:%i:%s"
		case eurLocation:
			res = "%Y-%m-%d %H.%i.%s"
		case internalLocation:
			res = "%Y%m%d%H%i%s"
		}
	case timeFormat:
		switch l {
		case usaLocation:
			res = "%h:%i:%s %p"
		case jisLocation:
			res = "%H:%i:%s"
		case isoLocation:
			res = "%H:%i:%s"
		case eurLocation:
			res = "%H.%i.%s"
		case internalLocation:
			res = "%H%i%s"
		}
	}

	return res, false, nil
}

type strToDateFunctionClass struct {
	BaseFunctionClass
}

func (c *strToDateFunctionClass) getRetTp(ctx sessionctx.Context, arg Expression) (tp byte, fsp int8) {
	tp = mysql.TypeDatetime
	if _, ok := arg.(*Constant); !ok {
		return tp, types.MaxFsp
	}
	strArg := WrapWithCastAsString(ctx, arg)
	format, isNull, err := strArg.EvalString(ctx, chunk.Row{})
	if err != nil || isNull {
		return
	}

	isDuration, isDate := types.GetFormatType(format)
	if isDuration && !isDate {
		tp = mysql.TypeDuration
	} else if !isDuration && isDate {
		tp = mysql.TypeDate
	}
	if strings.Contains(format, "%f") {
		fsp = types.MaxFsp
	}
	return
}

// getFunction see https://dev.mysql.com/doc/refman/5.5/en/date-and-time-functions.html#function_str-to-date
func (c *strToDateFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (sig BuiltinFunc, err error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	retTp, fsp := c.getRetTp(ctx, args[1])
	switch retTp {
	case mysql.TypeDate:
		bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETDatetime, types.ETString, types.ETString)
		bf.Tp.Tp, bf.Tp.Flen, bf.Tp.Decimal = mysql.TypeDate, mysql.MaxDateWidth, int(types.MinFsp)
		sig = &builtinStrToDateDateSig{bf}
	case mysql.TypeDatetime:
		bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETDatetime, types.ETString, types.ETString)
		if fsp == types.MinFsp {
			bf.Tp.Flen, bf.Tp.Decimal = mysql.MaxDatetimeWidthNoFsp, int(types.MinFsp)
		} else {
			bf.Tp.Flen, bf.Tp.Decimal = mysql.MaxDatetimeWidthWithFsp, int(types.MaxFsp)
		}
		sig = &builtinStrToDateDatetimeSig{bf}
	case mysql.TypeDuration:
		bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETDuration, types.ETString, types.ETString)
		if fsp == types.MinFsp {
			bf.Tp.Flen, bf.Tp.Decimal = mysql.MaxDurationWidthNoFsp, int(types.MinFsp)
		} else {
			bf.Tp.Flen, bf.Tp.Decimal = mysql.MaxDurationWidthWithFsp, int(types.MaxFsp)
		}
		sig = &builtinStrToDateDurationSig{bf}
	}
	return sig, nil
}

type builtinStrToDateDateSig struct {
	BaseBuiltinFunc
}

func (b *builtinStrToDateDateSig) Clone() BuiltinFunc {
	newSig := &builtinStrToDateDateSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinStrToDateDateSig) evalTime(row chunk.Row) (types.Time, bool, error) {
	date, isNull, err := b.Args[0].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return types.Time{}, isNull, err
	}
	format, isNull, err := b.Args[1].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return types.Time{}, isNull, err
	}
	var t types.Time
	sc := b.Ctx.GetSessionVars().StmtCtx
	succ := t.StrToDate(sc, date, format)
	if !succ {
		return types.Time{}, true, handleInvalidTimeError(b.Ctx, types.ErrIncorrectDatetimeValue.GenWithStackByArgs(t.String()))
	}
	if b.Ctx.GetSessionVars().SQLMode.HasNoZeroDateMode() && (t.Time.Year() == 0 || t.Time.Month() == 0 || t.Time.Day() == 0) {
		return types.Time{}, true, handleInvalidTimeError(b.Ctx, types.ErrIncorrectDatetimeValue.GenWithStackByArgs(t.String()))
	}
	t.Type, t.Fsp = mysql.TypeDate, types.MinFsp
	return t, false, nil
}

type builtinStrToDateDatetimeSig struct {
	BaseBuiltinFunc
}

func (b *builtinStrToDateDatetimeSig) Clone() BuiltinFunc {
	newSig := &builtinStrToDateDatetimeSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinStrToDateDatetimeSig) evalTime(row chunk.Row) (types.Time, bool, error) {
	date, isNull, err := b.Args[0].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return types.Time{}, isNull, err
	}
	format, isNull, err := b.Args[1].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return types.Time{}, isNull, err
	}
	var t types.Time
	sc := b.Ctx.GetSessionVars().StmtCtx
	succ := t.StrToDate(sc, date, format)
	if !succ {
		return types.Time{}, true, handleInvalidTimeError(b.Ctx, types.ErrIncorrectDatetimeValue.GenWithStackByArgs(t.String()))
	}
	if b.Ctx.GetSessionVars().SQLMode.HasNoZeroDateMode() && (t.Time.Year() == 0 || t.Time.Month() == 0 || t.Time.Day() == 0) {
		return types.Time{}, true, handleInvalidTimeError(b.Ctx, types.ErrIncorrectDatetimeValue.GenWithStackByArgs(t.String()))
	}
	t.Type, t.Fsp = mysql.TypeDatetime, int8(b.Tp.Decimal)
	return t, false, nil
}

type builtinStrToDateDurationSig struct {
	BaseBuiltinFunc
}

func (b *builtinStrToDateDurationSig) Clone() BuiltinFunc {
	newSig := &builtinStrToDateDurationSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalDuration
// TODO: If the NO_ZERO_DATE or NO_ZERO_IN_DATE SQL mode is enabled, zero dates or part of dates are disallowed.
// In that case, STR_TO_DATE() returns NULL and generates a warning.
func (b *builtinStrToDateDurationSig) evalDuration(row chunk.Row) (types.Duration, bool, error) {
	date, isNull, err := b.Args[0].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return types.Duration{}, isNull, err
	}
	format, isNull, err := b.Args[1].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return types.Duration{}, isNull, err
	}
	var t types.Time
	sc := b.Ctx.GetSessionVars().StmtCtx
	succ := t.StrToDate(sc, date, format)
	if !succ {
		return types.Duration{}, true, handleInvalidTimeError(b.Ctx, types.ErrIncorrectDatetimeValue.GenWithStackByArgs(t.String()))
	}
	if b.Ctx.GetSessionVars().SQLMode.HasNoZeroDateMode() && (t.Time.Year() == 0 || t.Time.Month() == 0 || t.Time.Day() == 0) {
		return types.Duration{}, true, handleInvalidTimeError(b.Ctx, types.ErrIncorrectDatetimeValue.GenWithStackByArgs(t.String()))
	}
	t.Fsp = int8(b.Tp.Decimal)
	dur, err := t.ConvertToDuration()
	return dur, err != nil, err
}

type sysDateFunctionClass struct {
	BaseFunctionClass
}

func (c *sysDateFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	var argTps = make([]types.EvalType, 0)
	if len(args) == 1 {
		argTps = append(argTps, types.ETInt)
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETDatetime, argTps...)
	bf.Tp.Flen, bf.Tp.Decimal = 19, 0

	var sig BuiltinFunc
	if len(args) == 1 {
		sig = &builtinSysDateWithFspSig{bf}
	} else {
		sig = &builtinSysDateWithoutFspSig{bf}
	}
	return sig, nil
}

type builtinSysDateWithFspSig struct {
	BaseBuiltinFunc
}

func (b *builtinSysDateWithFspSig) Clone() BuiltinFunc {
	newSig := &builtinSysDateWithFspSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalTime evals SYSDATE(fsp).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_sysdate
func (b *builtinSysDateWithFspSig) evalTime(row chunk.Row) (d types.Time, isNull bool, err error) {
	fsp, isNull, err := b.Args[0].EvalInt(b.Ctx, row)
	if isNull || err != nil {
		return types.Time{}, isNull, err
	}

	loc := b.Ctx.GetSessionVars().Location()
	now := time.Now().In(loc)
	result, err := convertTimeToMysqlTime(now, int8(fsp), types.ModeHalfEven)
	if err != nil {
		return types.Time{}, true, err
	}
	return result, false, nil
}

type builtinSysDateWithoutFspSig struct {
	BaseBuiltinFunc
}

func (b *builtinSysDateWithoutFspSig) Clone() BuiltinFunc {
	newSig := &builtinSysDateWithoutFspSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalTime evals SYSDATE().
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_sysdate
func (b *builtinSysDateWithoutFspSig) evalTime(row chunk.Row) (d types.Time, isNull bool, err error) {
	tz := b.Ctx.GetSessionVars().Location()
	now := time.Now().In(tz)
	result, err := convertTimeToMysqlTime(now, 0, types.ModeHalfEven)
	if err != nil {
		return types.Time{}, true, err
	}
	return result, false, nil
}

type currentDateFunctionClass struct {
	BaseFunctionClass
}

func (c *currentDateFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETDatetime)
	bf.Tp.Flen, bf.Tp.Decimal = 10, 0
	sig := &builtinCurrentDateSig{bf}
	return sig, nil
}

type builtinCurrentDateSig struct {
	BaseBuiltinFunc
}

func (b *builtinCurrentDateSig) Clone() BuiltinFunc {
	newSig := &builtinCurrentDateSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalTime evals CURDATE().
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_curdate
func (b *builtinCurrentDateSig) evalTime(row chunk.Row) (d types.Time, isNull bool, err error) {
	tz := b.Ctx.GetSessionVars().Location()
	nowTs, err := getStmtTimestamp(b.Ctx)
	if err != nil {
		return types.Time{}, true, err
	}
	year, month, day := nowTs.In(tz).Date()
	result := types.Time{
		Time: types.FromDate(year, int(month), day, 0, 0, 0, 0),
		Type: mysql.TypeDate,
		Fsp:  0}
	return result, false, nil
}

type currentTimeFunctionClass struct {
	BaseFunctionClass
}

func (c *currentTimeFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (sig BuiltinFunc, err error) {
	if err = c.VerifyArgs(args); err != nil {
		return nil, err
	}

	if len(args) == 0 {
		bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETDuration)
		bf.Tp.Flen, bf.Tp.Decimal = mysql.MaxDurationWidthNoFsp, int(types.MinFsp)
		sig = &builtinCurrentTime0ArgSig{bf}
		return sig, nil
	}
	// args[0] must be a constant which should not be null.
	_, ok := args[0].(*Constant)
	fsp := int64(types.MaxFsp)
	if ok {
		fsp, _, err = args[0].EvalInt(ctx, chunk.Row{})
		if err != nil {
			return nil, err
		}
		if fsp > int64(types.MaxFsp) {
			return nil, errors.Errorf("Too-big precision %v specified for 'curtime'. Maximum is %v.", fsp, types.MaxFsp)
		} else if fsp < int64(types.MinFsp) {
			return nil, errors.Errorf("Invalid negative %d specified, must in [0, 6].", fsp)
		}
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETDuration, types.ETInt)
	bf.Tp.Flen, bf.Tp.Decimal = mysql.MaxDurationWidthWithFsp, int(fsp)
	sig = &builtinCurrentTime1ArgSig{bf}
	return sig, nil
}

type builtinCurrentTime0ArgSig struct {
	BaseBuiltinFunc
}

func (b *builtinCurrentTime0ArgSig) Clone() BuiltinFunc {
	newSig := &builtinCurrentTime0ArgSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinCurrentTime0ArgSig) evalDuration(row chunk.Row) (types.Duration, bool, error) {
	tz := b.Ctx.GetSessionVars().Location()
	nowTs, err := getStmtTimestamp(b.Ctx)
	if err != nil {
		return types.Duration{}, true, err
	}
	dur := nowTs.In(tz).Format(types.TimeFormat)
	res, err := types.ParseDuration(b.Ctx.GetSessionVars().StmtCtx, dur, types.MinFsp)
	if err != nil {
		return types.Duration{}, true, err
	}
	return res, false, nil
}

type builtinCurrentTime1ArgSig struct {
	BaseBuiltinFunc
}

func (b *builtinCurrentTime1ArgSig) Clone() BuiltinFunc {
	newSig := &builtinCurrentTime1ArgSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinCurrentTime1ArgSig) evalDuration(row chunk.Row) (types.Duration, bool, error) {
	fsp, _, err := b.Args[0].EvalInt(b.Ctx, row)
	if err != nil {
		return types.Duration{}, true, err
	}
	tz := b.Ctx.GetSessionVars().Location()
	nowTs, err := getStmtTimestamp(b.Ctx)
	if err != nil {
		return types.Duration{}, true, err
	}
	dur := nowTs.In(tz).Format(types.TimeFSPFormat)
	res, err := types.ParseDuration(b.Ctx.GetSessionVars().StmtCtx, dur, int8(fsp))
	if err != nil {
		return types.Duration{}, true, err
	}
	return res, false, nil
}

type timeFunctionClass struct {
	BaseFunctionClass
}

func (c *timeFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	err := c.VerifyArgs(args)
	if err != nil {
		return nil, err
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETDuration, types.ETString)
	bf.Tp.Decimal, err = getExpressionFsp(ctx, args[0])
	if err != nil {
		return nil, err
	}
	sig := &builtinTimeSig{bf}
	return sig, nil
}

type builtinTimeSig struct {
	BaseBuiltinFunc
}

func (b *builtinTimeSig) Clone() BuiltinFunc {
	newSig := &builtinTimeSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalDuration evals a builtinTimeSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_time.
func (b *builtinTimeSig) evalDuration(row chunk.Row) (res types.Duration, isNull bool, err error) {
	expr, isNull, err := b.Args[0].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}

	fsp := 0
	if idx := strings.Index(expr, "."); idx != -1 {
		fsp = len(expr) - idx - 1
	}

	var tmpFsp int8
	if tmpFsp, err = types.CheckFsp(fsp); err != nil {
		return res, isNull, err
	}
	fsp = int(tmpFsp)

	sc := b.Ctx.GetSessionVars().StmtCtx
	res, err = types.ParseDuration(sc, expr, int8(fsp))
	if types.ErrTruncatedWrongVal.Equal(err) {
		err = sc.HandleTruncate(err)
	}
	return res, isNull, err
}

type timeLiteralFunctionClass struct {
	BaseFunctionClass
}

func (c *timeLiteralFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	con, ok := args[0].(*Constant)
	if !ok {
		panic("Unexpected parameter for time literal")
	}
	dt, err := con.Eval(chunk.Row{})
	if err != nil {
		return nil, err
	}
	str := dt.GetString()
	if !isDuration(str) {
		return nil, types.ErrIncorrectDatetimeValue.GenWithStackByArgs(str)
	}
	duration, err := types.ParseDuration(ctx.GetSessionVars().StmtCtx, str, types.GetFsp(str))
	if err != nil {
		return nil, err
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, []Expression{}, types.ETDuration)
	bf.Tp.Flen, bf.Tp.Decimal = 10, int(duration.Fsp)
	if int(duration.Fsp) > 0 {
		bf.Tp.Flen += 1 + int(duration.Fsp)
	}
	sig := &builtinTimeLiteralSig{bf, duration}
	return sig, nil
}

type builtinTimeLiteralSig struct {
	BaseBuiltinFunc
	duration types.Duration
}

func (b *builtinTimeLiteralSig) Clone() BuiltinFunc {
	newSig := &builtinTimeLiteralSig{duration: b.duration}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalDuration evals TIME 'stringLit'.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-literals.html
func (b *builtinTimeLiteralSig) evalDuration(row chunk.Row) (types.Duration, bool, error) {
	return b.duration, false, nil
}

type utcDateFunctionClass struct {
	BaseFunctionClass
}

func (c *utcDateFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETDatetime)
	bf.Tp.Flen, bf.Tp.Decimal = 10, 0
	sig := &builtinUTCDateSig{bf}
	return sig, nil
}

type builtinUTCDateSig struct {
	BaseBuiltinFunc
}

func (b *builtinUTCDateSig) Clone() BuiltinFunc {
	newSig := &builtinUTCDateSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalTime evals UTC_DATE, UTC_DATE().
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_utc-date
func (b *builtinUTCDateSig) evalTime(row chunk.Row) (types.Time, bool, error) {
	nowTs, err := getStmtTimestamp(b.Ctx)
	if err != nil {
		return types.Time{}, true, err
	}
	year, month, day := nowTs.UTC().Date()
	result := types.Time{
		Time: types.FromGoTime(time.Date(year, month, day, 0, 0, 0, 0, time.UTC)),
		Type: mysql.TypeDate,
		Fsp:  types.UnspecifiedFsp}
	return result, false, nil
}

type utcTimestampFunctionClass struct {
	BaseFunctionClass
}

func getFlenAndDecimal4UTCTimestampAndNow(ctx sessionctx.Context, arg Expression) (flen, decimal int) {
	if constant, ok := arg.(*Constant); ok {
		fsp, isNull, err := constant.EvalInt(ctx, chunk.Row{})
		if isNull || err != nil || fsp > int64(types.MaxFsp) {
			decimal = int(types.MaxFsp)
		} else if fsp < int64(types.MinFsp) {
			decimal = int(types.MinFsp)
		} else {
			decimal = int(fsp)
		}
	}
	if decimal > 0 {
		flen = 19 + 1 + decimal
	} else {
		flen = 19
	}
	return flen, decimal
}

func (c *utcTimestampFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	argTps := make([]types.EvalType, 0, 1)
	if len(args) == 1 {
		argTps = append(argTps, types.ETInt)
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETDatetime, argTps...)

	if len(args) == 1 {
		bf.Tp.Flen, bf.Tp.Decimal = getFlenAndDecimal4UTCTimestampAndNow(bf.Ctx, args[0])
	} else {
		bf.Tp.Flen, bf.Tp.Decimal = 19, 0
	}

	var sig BuiltinFunc
	if len(args) == 1 {
		sig = &builtinUTCTimestampWithArgSig{bf}
	} else {
		sig = &builtinUTCTimestampWithoutArgSig{bf}
	}
	return sig, nil
}

func evalUTCTimestampWithFsp(ctx sessionctx.Context, fsp int8) (types.Time, bool, error) {
	nowTs, err := getStmtTimestamp(ctx)
	if err != nil {
		return types.Time{}, true, err
	}
	result, err := convertTimeToMysqlTime(nowTs.UTC(), fsp, types.ModeHalfEven)
	if err != nil {
		return types.Time{}, true, err
	}
	return result, false, nil
}

type builtinUTCTimestampWithArgSig struct {
	BaseBuiltinFunc
}

func (b *builtinUTCTimestampWithArgSig) Clone() BuiltinFunc {
	newSig := &builtinUTCTimestampWithArgSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalTime evals UTC_TIMESTAMP(fsp).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_utc-timestamp
func (b *builtinUTCTimestampWithArgSig) evalTime(row chunk.Row) (types.Time, bool, error) {
	num, isNull, err := b.Args[0].EvalInt(b.Ctx, row)
	if err != nil {
		return types.Time{}, true, err
	}

	if !isNull && num > int64(types.MaxFsp) {
		return types.Time{}, true, errors.Errorf("Too-big precision %v specified for 'utc_timestamp'. Maximum is %v.", num, types.MaxFsp)
	}
	if !isNull && num < int64(types.MinFsp) {
		return types.Time{}, true, errors.Errorf("Invalid negative %d specified, must in [0, 6].", num)
	}

	result, isNull, err := evalUTCTimestampWithFsp(b.Ctx, int8(num))
	return result, isNull, err
}

type builtinUTCTimestampWithoutArgSig struct {
	BaseBuiltinFunc
}

func (b *builtinUTCTimestampWithoutArgSig) Clone() BuiltinFunc {
	newSig := &builtinUTCTimestampWithoutArgSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalTime evals UTC_TIMESTAMP().
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_utc-timestamp
func (b *builtinUTCTimestampWithoutArgSig) evalTime(row chunk.Row) (types.Time, bool, error) {
	result, isNull, err := evalUTCTimestampWithFsp(b.Ctx, int8(0))
	return result, isNull, err
}

type nowFunctionClass struct {
	BaseFunctionClass
}

func (c *nowFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	argTps := make([]types.EvalType, 0, 1)
	if len(args) == 1 {
		argTps = append(argTps, types.ETInt)
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETDatetime, argTps...)

	if len(args) == 1 {
		bf.Tp.Flen, bf.Tp.Decimal = getFlenAndDecimal4UTCTimestampAndNow(bf.Ctx, args[0])
	} else {
		bf.Tp.Flen, bf.Tp.Decimal = 19, 0
	}

	var sig BuiltinFunc
	if len(args) == 1 {
		sig = &builtinNowWithArgSig{bf}
	} else {
		sig = &builtinNowWithoutArgSig{bf}
	}
	return sig, nil
}

func evalNowWithFsp(ctx sessionctx.Context, fsp int8) (types.Time, bool, error) {
	nowTs, err := getStmtTimestamp(ctx)
	if err != nil {
		return types.Time{}, true, err
	}

	// In MySQL's implementation, now() will truncate the result instead of rounding it.
	// Results below are from MySQL 5.7, which can prove it.
	// mysql> select now(6), now(3), now();
	//	+----------------------------+-------------------------+---------------------+
	//	| now(6)                     | now(3)                  | now()               |
	//	+----------------------------+-------------------------+---------------------+
	//	| 2019-03-25 15:57:56.612966 | 2019-03-25 15:57:56.612 | 2019-03-25 15:57:56 |
	//	+----------------------------+-------------------------+---------------------+
	result, err := convertTimeToMysqlTime(nowTs, fsp, types.ModeTruncate)
	if err != nil {
		return types.Time{}, true, err
	}

	err = result.ConvertTimeZone(time.Local, ctx.GetSessionVars().Location())
	if err != nil {
		return types.Time{}, true, err
	}

	return result, false, nil
}

type builtinNowWithArgSig struct {
	BaseBuiltinFunc
}

func (b *builtinNowWithArgSig) Clone() BuiltinFunc {
	newSig := &builtinNowWithArgSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalTime evals NOW(fsp)
// see: https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_now
func (b *builtinNowWithArgSig) evalTime(row chunk.Row) (types.Time, bool, error) {
	fsp, isNull, err := b.Args[0].EvalInt(b.Ctx, row)

	if err != nil {
		return types.Time{}, true, err
	}

	if isNull {
		fsp = 0
	} else if fsp > int64(types.MaxFsp) {
		return types.Time{}, true, errors.Errorf("Too-big precision %v specified for 'now'. Maximum is %v.", fsp, types.MaxFsp)
	} else if fsp < int64(types.MinFsp) {
		return types.Time{}, true, errors.Errorf("Invalid negative %d specified, must in [0, 6].", fsp)
	}

	result, isNull, err := evalNowWithFsp(b.Ctx, int8(fsp))
	return result, isNull, err
}

type builtinNowWithoutArgSig struct {
	BaseBuiltinFunc
}

func (b *builtinNowWithoutArgSig) Clone() BuiltinFunc {
	newSig := &builtinNowWithoutArgSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalTime evals NOW()
// see: https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_now
func (b *builtinNowWithoutArgSig) evalTime(row chunk.Row) (types.Time, bool, error) {
	result, isNull, err := evalNowWithFsp(b.Ctx, int8(0))
	return result, isNull, err
}

type extractFunctionClass struct {
	BaseFunctionClass
}

func (c *extractFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (sig BuiltinFunc, err error) {
	if err = c.VerifyArgs(args); err != nil {
		return nil, err
	}

	datetimeUnits := map[string]struct{}{
		"DAY":             {},
		"WEEK":            {},
		"MONTH":           {},
		"QUARTER":         {},
		"YEAR":            {},
		"DAY_MICROSECOND": {},
		"DAY_SECOND":      {},
		"DAY_MINUTE":      {},
		"DAY_HOUR":        {},
		"YEAR_MONTH":      {},
	}
	isDatetimeUnit := true
	args[0] = WrapWithCastAsString(ctx, args[0])
	if _, isCon := args[0].(*Constant); isCon {
		unit, _, err1 := args[0].EvalString(ctx, chunk.Row{})
		if err1 != nil {
			return nil, err1
		}
		_, isDatetimeUnit = datetimeUnits[unit]
	}
	var bf BaseBuiltinFunc
	if isDatetimeUnit {
		bf = NewBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETString, types.ETDatetime)
		sig = &builtinExtractDatetimeSig{bf}
	} else {
		bf = NewBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETString, types.ETDuration)
		sig = &builtinExtractDurationSig{bf}
	}
	return sig, nil
}

type builtinExtractDatetimeSig struct {
	BaseBuiltinFunc
}

func (b *builtinExtractDatetimeSig) Clone() BuiltinFunc {
	newSig := &builtinExtractDatetimeSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalInt evals a builtinExtractDatetimeSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_extract
func (b *builtinExtractDatetimeSig) evalInt(row chunk.Row) (int64, bool, error) {
	unit, isNull, err := b.Args[0].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	dt, isNull, err := b.Args[1].EvalTime(b.Ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	res, err := types.ExtractDatetimeNum(&dt, unit)
	return res, err != nil, err
}

type builtinExtractDurationSig struct {
	BaseBuiltinFunc
}

func (b *builtinExtractDurationSig) Clone() BuiltinFunc {
	newSig := &builtinExtractDurationSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalInt evals a builtinExtractDurationSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_extract
func (b *builtinExtractDurationSig) evalInt(row chunk.Row) (int64, bool, error) {
	unit, isNull, err := b.Args[0].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	dur, isNull, err := b.Args[1].EvalDuration(b.Ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	res, err := types.ExtractDurationNum(&dur, unit)
	return res, err != nil, err
}

// baseDateArithmitical is the base class for all "builtinAddDateXXXSig" and "builtinSubDateXXXSig",
// which provides parameter getter and date arithmetical calculate functions.
type baseDateArithmitical struct {
	// intervalRegexp is "*Regexp" used to extract string interval for "DAY" unit.
	intervalRegexp *regexp.Regexp
}

func newDateArighmeticalUtil() baseDateArithmitical {
	return baseDateArithmitical{
		intervalRegexp: regexp.MustCompile(`[\d]+`),
	}
}

func (du *baseDateArithmitical) getDateFromString(ctx sessionctx.Context, args []Expression, row chunk.Row, unit string) (types.Time, bool, error) {
	dateStr, isNull, err := args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return types.Time{}, true, err
	}

	dateTp := mysql.TypeDate
	if !types.IsDateFormat(dateStr) || types.IsClockUnit(unit) {
		dateTp = mysql.TypeDatetime
	}

	sc := ctx.GetSessionVars().StmtCtx
	date, err := types.ParseTime(sc, dateStr, dateTp, types.MaxFsp)
	return date, err != nil, handleInvalidTimeError(ctx, err)
}

func (du *baseDateArithmitical) getDateFromInt(ctx sessionctx.Context, args []Expression, row chunk.Row, unit string) (types.Time, bool, error) {
	dateInt, isNull, err := args[0].EvalInt(ctx, row)
	if isNull || err != nil {
		return types.Time{}, true, err
	}

	sc := ctx.GetSessionVars().StmtCtx
	date, err := types.ParseTimeFromInt64(sc, dateInt)
	if err != nil {
		return types.Time{}, true, handleInvalidTimeError(ctx, err)
	}

	dateTp := mysql.TypeDate
	if date.Type == mysql.TypeDatetime || date.Type == mysql.TypeTimestamp || types.IsClockUnit(unit) {
		dateTp = mysql.TypeDatetime
	}
	date.Type = dateTp
	return date, false, nil
}

func (du *baseDateArithmitical) getDateFromDatetime(ctx sessionctx.Context, args []Expression, row chunk.Row, unit string) (types.Time, bool, error) {
	date, isNull, err := args[0].EvalTime(ctx, row)
	if isNull || err != nil {
		return types.Time{}, true, err
	}

	dateTp := mysql.TypeDate
	if date.Type == mysql.TypeDatetime || date.Type == mysql.TypeTimestamp || types.IsClockUnit(unit) {
		dateTp = mysql.TypeDatetime
	}
	date.Type = dateTp
	return date, false, nil
}

func (du *baseDateArithmitical) getIntervalFromString(ctx sessionctx.Context, args []Expression, row chunk.Row, unit string) (string, bool, error) {
	interval, isNull, err := args[1].EvalString(ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	// unit "DAY" and "HOUR" has to be specially handled.
	if toLower := strings.ToLower(unit); toLower == "day" || toLower == "hour" {
		if strings.ToLower(interval) == "true" {
			interval = "1"
		} else if strings.ToLower(interval) == "false" {
			interval = "0"
		} else {
			interval = du.intervalRegexp.FindString(interval)
		}
	}
	return interval, false, nil
}

func (du *baseDateArithmitical) getIntervalFromDecimal(ctx sessionctx.Context, args []Expression, row chunk.Row, unit string) (string, bool, error) {
	decimal, isNull, err := args[1].EvalDecimal(ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	interval := decimal.String()

	switch strings.ToUpper(unit) {
	case "HOUR_MINUTE", "MINUTE_SECOND", "YEAR_MONTH", "DAY_HOUR", "DAY_MINUTE",
		"DAY_SECOND", "DAY_MICROSECOND", "HOUR_MICROSECOND", "HOUR_SECOND", "MINUTE_MICROSECOND", "SECOND_MICROSECOND":
		neg := false
		if interval != "" && interval[0] == '-' {
			neg = true
			interval = interval[1:]
		}
		switch strings.ToUpper(unit) {
		case "HOUR_MINUTE", "MINUTE_SECOND":
			interval = strings.Replace(interval, ".", ":", -1)
		case "YEAR_MONTH":
			interval = strings.Replace(interval, ".", "-", -1)
		case "DAY_HOUR":
			interval = strings.Replace(interval, ".", " ", -1)
		case "DAY_MINUTE":
			interval = "0 " + strings.Replace(interval, ".", ":", -1)
		case "DAY_SECOND":
			interval = "0 00:" + strings.Replace(interval, ".", ":", -1)
		case "DAY_MICROSECOND":
			interval = "0 00:00:" + interval
		case "HOUR_MICROSECOND":
			interval = "00:00:" + interval
		case "HOUR_SECOND":
			interval = "00:" + strings.Replace(interval, ".", ":", -1)
		case "MINUTE_MICROSECOND":
			interval = "00:" + interval
		case "SECOND_MICROSECOND":
			/* keep interval as original decimal */
		}
		if neg {
			interval = "-" + interval
		}
	case "SECOND":
		// Decimal's EvalString is like %f format.
		interval, isNull, err = args[1].EvalString(ctx, row)
		if isNull || err != nil {
			return "", true, err
		}
	default:
		// YEAR, QUARTER, MONTH, WEEK, DAY, HOUR, MINUTE, MICROSECOND
		args[1] = WrapWithCastAsInt(ctx, args[1])
		interval, isNull, err = args[1].EvalString(ctx, row)
		if isNull || err != nil {
			return "", true, err
		}
	}

	return interval, false, nil
}

func (du *baseDateArithmitical) getIntervalFromInt(ctx sessionctx.Context, args []Expression, row chunk.Row, unit string) (string, bool, error) {
	interval, isNull, err := args[1].EvalInt(ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	return strconv.FormatInt(interval, 10), false, nil
}

func (du *baseDateArithmitical) getIntervalFromReal(ctx sessionctx.Context, args []Expression, row chunk.Row, unit string) (string, bool, error) {
	interval, isNull, err := args[1].EvalReal(ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	return strconv.FormatFloat(interval, 'f', args[1].GetType().Decimal, 64), false, nil
}

func (du *baseDateArithmitical) add(ctx sessionctx.Context, date types.Time, interval string, unit string) (types.Time, bool, error) {
	year, month, day, nano, err := types.ParseDurationValue(unit, interval)
	if err := handleInvalidTimeError(ctx, err); err != nil {
		return types.Time{}, true, err
	}

	goTime, err := date.Time.GoTime(time.Local)
	if err := handleInvalidTimeError(ctx, err); err != nil {
		return types.Time{}, true, err
	}

	goTime = goTime.Add(time.Duration(nano))
	goTime = types.AddDate(year, month, day, goTime)

	if goTime.Nanosecond() == 0 {
		date.Fsp = 0
	} else {
		date.Fsp = 6
	}

	if goTime.Year() < 0 || goTime.Year() > (1<<16-1) {
		return types.Time{}, true, handleInvalidTimeError(ctx, types.ErrDatetimeFunctionOverflow.GenWithStackByArgs("datetime"))
	}

	date.Time = types.FromGoTime(goTime)
	overflow, err := types.DateTimeIsOverflow(ctx.GetSessionVars().StmtCtx, date)
	if err := handleInvalidTimeError(ctx, err); err != nil {
		return types.Time{}, true, err
	}
	if overflow {
		return types.Time{}, true, handleInvalidTimeError(ctx, types.ErrDatetimeFunctionOverflow.GenWithStackByArgs("datetime"))
	}
	return date, false, nil
}

func (du *baseDateArithmitical) addDuration(ctx sessionctx.Context, d types.Duration, interval string, unit string) (types.Duration, bool, error) {
	dur, err := types.ExtractDurationValue(unit, interval)
	if err != nil {
		return types.ZeroDuration, true, handleInvalidTimeError(ctx, err)
	}
	retDur, err := d.Add(dur)
	if err != nil {
		return types.ZeroDuration, true, err
	}
	return retDur, false, nil
}

func (du *baseDateArithmitical) subDuration(ctx sessionctx.Context, d types.Duration, interval string, unit string) (types.Duration, bool, error) {
	dur, err := types.ExtractDurationValue(unit, interval)
	if err != nil {
		return types.ZeroDuration, true, handleInvalidTimeError(ctx, err)
	}
	retDur, err := d.Sub(dur)
	if err != nil {
		return types.ZeroDuration, true, err
	}
	return retDur, false, nil
}

func (du *baseDateArithmitical) sub(ctx sessionctx.Context, date types.Time, interval string, unit string) (types.Time, bool, error) {
	year, month, day, nano, err := types.ParseDurationValue(unit, interval)
	if err := handleInvalidTimeError(ctx, err); err != nil {
		return types.Time{}, true, err
	}
	year, month, day, nano = -year, -month, -day, -nano

	goTime, err := date.Time.GoTime(time.Local)
	if err := handleInvalidTimeError(ctx, err); err != nil {
		return types.Time{}, true, err
	}

	duration := time.Duration(nano)
	goTime = goTime.Add(duration)
	goTime = types.AddDate(year, month, day, goTime)

	if goTime.Nanosecond() == 0 {
		date.Fsp = 0
	} else {
		date.Fsp = 6
	}

	if goTime.Year() < 0 || goTime.Year() > (1<<16-1) {
		return types.Time{}, true, handleInvalidTimeError(ctx, types.ErrDatetimeFunctionOverflow.GenWithStackByArgs("datetime"))
	}

	date.Time = types.FromGoTime(goTime)
	overflow, err := types.DateTimeIsOverflow(ctx.GetSessionVars().StmtCtx, date)
	if err := handleInvalidTimeError(ctx, err); err != nil {
		return types.Time{}, true, err
	}
	if overflow {
		return types.Time{}, true, handleInvalidTimeError(ctx, types.ErrDatetimeFunctionOverflow.GenWithStackByArgs("datetime"))
	}
	return date, false, nil
}

type addDateFunctionClass struct {
	BaseFunctionClass
}

func (c *addDateFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (sig BuiltinFunc, err error) {
	if err = c.VerifyArgs(args); err != nil {
		return nil, err
	}

	dateEvalTp := args[0].GetType().EvalType()
	if dateEvalTp != types.ETString && dateEvalTp != types.ETInt && dateEvalTp != types.ETDuration {
		dateEvalTp = types.ETDatetime
	}

	intervalEvalTp := args[1].GetType().EvalType()
	if intervalEvalTp != types.ETString && intervalEvalTp != types.ETDecimal && intervalEvalTp != types.ETReal {
		intervalEvalTp = types.ETInt
	}

	argTps := []types.EvalType{dateEvalTp, intervalEvalTp, types.ETString}
	var bf BaseBuiltinFunc
	if dateEvalTp == types.ETDuration {
		unit, _, err := args[2].EvalString(ctx, chunk.Row{})
		if err != nil {
			return nil, err
		}
		internalFsp := 0
		switch unit {
		// If the unit has micro second, then the fsp must be the MaxFsp.
		case "MICROSECOND", "SECOND_MICROSECOND", "MINUTE_MICROSECOND", "HOUR_MICROSECOND", "DAY_MICROSECOND":
			internalFsp = int(types.MaxFsp)
		// If the unit is second, the fsp is related with the arg[1]'s.
		case "SECOND":
			internalFsp = int(types.MaxFsp)
			if intervalEvalTp != types.ETString {
				internalFsp = mathutil.Min(args[1].GetType().Decimal, int(types.MaxFsp))
			}
			// Otherwise, the fsp should be 0.
		}
		bf = NewBaseBuiltinFuncWithTp(ctx, args, types.ETDuration, argTps...)
		arg0Dec, err := getExpressionFsp(ctx, args[0])
		if err != nil {
			return nil, err
		}
		bf.Tp.Flen, bf.Tp.Decimal = mysql.MaxDurationWidthWithFsp, mathutil.Max(arg0Dec, internalFsp)
	} else {
		bf = NewBaseBuiltinFuncWithTp(ctx, args, types.ETDatetime, argTps...)
		bf.Tp.Flen, bf.Tp.Decimal = mysql.MaxDatetimeFullWidth, types.UnspecifiedLength
	}

	switch {
	case dateEvalTp == types.ETString && intervalEvalTp == types.ETString:
		sig = &builtinAddDateStringStringSig{
			BaseBuiltinFunc:      bf,
			baseDateArithmitical: newDateArighmeticalUtil(),
		}
	case dateEvalTp == types.ETString && intervalEvalTp == types.ETInt:
		sig = &builtinAddDateStringIntSig{
			BaseBuiltinFunc:      bf,
			baseDateArithmitical: newDateArighmeticalUtil(),
		}
	case dateEvalTp == types.ETString && intervalEvalTp == types.ETReal:
		sig = &builtinAddDateStringRealSig{
			BaseBuiltinFunc:      bf,
			baseDateArithmitical: newDateArighmeticalUtil(),
		}
	case dateEvalTp == types.ETString && intervalEvalTp == types.ETDecimal:
		sig = &builtinAddDateStringDecimalSig{
			BaseBuiltinFunc:      bf,
			baseDateArithmitical: newDateArighmeticalUtil(),
		}
	case dateEvalTp == types.ETInt && intervalEvalTp == types.ETString:
		sig = &builtinAddDateIntStringSig{
			BaseBuiltinFunc:      bf,
			baseDateArithmitical: newDateArighmeticalUtil(),
		}
	case dateEvalTp == types.ETInt && intervalEvalTp == types.ETInt:
		sig = &builtinAddDateIntIntSig{
			BaseBuiltinFunc:      bf,
			baseDateArithmitical: newDateArighmeticalUtil(),
		}
	case dateEvalTp == types.ETInt && intervalEvalTp == types.ETReal:
		sig = &builtinAddDateIntRealSig{
			BaseBuiltinFunc:      bf,
			baseDateArithmitical: newDateArighmeticalUtil(),
		}
	case dateEvalTp == types.ETInt && intervalEvalTp == types.ETDecimal:
		sig = &builtinAddDateIntDecimalSig{
			BaseBuiltinFunc:      bf,
			baseDateArithmitical: newDateArighmeticalUtil(),
		}
	case dateEvalTp == types.ETDatetime && intervalEvalTp == types.ETString:
		sig = &builtinAddDateDatetimeStringSig{
			BaseBuiltinFunc:      bf,
			baseDateArithmitical: newDateArighmeticalUtil(),
		}
	case dateEvalTp == types.ETDatetime && intervalEvalTp == types.ETInt:
		sig = &builtinAddDateDatetimeIntSig{
			BaseBuiltinFunc:      bf,
			baseDateArithmitical: newDateArighmeticalUtil(),
		}
	case dateEvalTp == types.ETDatetime && intervalEvalTp == types.ETReal:
		sig = &builtinAddDateDatetimeRealSig{
			BaseBuiltinFunc:      bf,
			baseDateArithmitical: newDateArighmeticalUtil(),
		}
	case dateEvalTp == types.ETDatetime && intervalEvalTp == types.ETDecimal:
		sig = &builtinAddDateDatetimeDecimalSig{
			BaseBuiltinFunc:      bf,
			baseDateArithmitical: newDateArighmeticalUtil(),
		}
	case dateEvalTp == types.ETDuration && intervalEvalTp == types.ETString:
		sig = &builtinAddDateDurationStringSig{
			BaseBuiltinFunc:      bf,
			baseDateArithmitical: newDateArighmeticalUtil(),
		}
	case dateEvalTp == types.ETDuration && intervalEvalTp == types.ETInt:
		sig = &builtinAddDateDurationIntSig{
			BaseBuiltinFunc:      bf,
			baseDateArithmitical: newDateArighmeticalUtil(),
		}
	case dateEvalTp == types.ETDuration && intervalEvalTp == types.ETReal:
		sig = &builtinAddDateDurationRealSig{
			BaseBuiltinFunc:      bf,
			baseDateArithmitical: newDateArighmeticalUtil(),
		}
	case dateEvalTp == types.ETDuration && intervalEvalTp == types.ETDecimal:
		sig = &builtinAddDateDurationDecimalSig{
			BaseBuiltinFunc:      bf,
			baseDateArithmitical: newDateArighmeticalUtil(),
		}
	}
	return sig, nil
}

type builtinAddDateStringStringSig struct {
	BaseBuiltinFunc
	baseDateArithmitical
}

func (b *builtinAddDateStringStringSig) Clone() BuiltinFunc {
	newSig := &builtinAddDateStringStringSig{baseDateArithmitical: b.baseDateArithmitical}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalTime evals ADDDATE(date,INTERVAL expr unit).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_adddate
func (b *builtinAddDateStringStringSig) evalTime(row chunk.Row) (types.Time, bool, error) {
	unit, isNull, err := b.Args[2].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return types.Time{}, true, err
	}

	date, isNull, err := b.getDateFromString(b.Ctx, b.Args, row, unit)
	if isNull || err != nil {
		return types.Time{}, true, err
	}

	interval, isNull, err := b.getIntervalFromString(b.Ctx, b.Args, row, unit)
	if isNull || err != nil {
		return types.Time{}, true, err
	}

	result, isNull, err := b.add(b.Ctx, date, interval, unit)
	return result, isNull || err != nil, err
}

type builtinAddDateStringIntSig struct {
	BaseBuiltinFunc
	baseDateArithmitical
}

func (b *builtinAddDateStringIntSig) Clone() BuiltinFunc {
	newSig := &builtinAddDateStringIntSig{baseDateArithmitical: b.baseDateArithmitical}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalTime evals ADDDATE(date,INTERVAL expr unit).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_adddate
func (b *builtinAddDateStringIntSig) evalTime(row chunk.Row) (types.Time, bool, error) {
	unit, isNull, err := b.Args[2].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return types.Time{}, true, err
	}

	date, isNull, err := b.getDateFromString(b.Ctx, b.Args, row, unit)
	if isNull || err != nil {
		return types.Time{}, true, err
	}

	interval, isNull, err := b.getIntervalFromInt(b.Ctx, b.Args, row, unit)
	if isNull || err != nil {
		return types.Time{}, true, err
	}

	result, isNull, err := b.add(b.Ctx, date, interval, unit)
	return result, isNull || err != nil, err
}

type builtinAddDateStringRealSig struct {
	BaseBuiltinFunc
	baseDateArithmitical
}

func (b *builtinAddDateStringRealSig) Clone() BuiltinFunc {
	newSig := &builtinAddDateStringRealSig{baseDateArithmitical: b.baseDateArithmitical}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalTime evals ADDDATE(date,INTERVAL expr unit).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_adddate
func (b *builtinAddDateStringRealSig) evalTime(row chunk.Row) (types.Time, bool, error) {
	unit, isNull, err := b.Args[2].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return types.Time{}, true, err
	}

	date, isNull, err := b.getDateFromString(b.Ctx, b.Args, row, unit)
	if isNull || err != nil {
		return types.Time{}, true, err
	}

	interval, isNull, err := b.getIntervalFromReal(b.Ctx, b.Args, row, unit)
	if isNull || err != nil {
		return types.Time{}, true, err
	}

	result, isNull, err := b.add(b.Ctx, date, interval, unit)
	return result, isNull || err != nil, err
}

type builtinAddDateStringDecimalSig struct {
	BaseBuiltinFunc
	baseDateArithmitical
}

func (b *builtinAddDateStringDecimalSig) Clone() BuiltinFunc {
	newSig := &builtinAddDateStringDecimalSig{baseDateArithmitical: b.baseDateArithmitical}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalTime evals ADDDATE(date,INTERVAL expr unit).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_adddate
func (b *builtinAddDateStringDecimalSig) evalTime(row chunk.Row) (types.Time, bool, error) {
	unit, isNull, err := b.Args[2].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return types.Time{}, true, err
	}

	date, isNull, err := b.getDateFromString(b.Ctx, b.Args, row, unit)
	if isNull || err != nil {
		return types.Time{}, true, err
	}

	interval, isNull, err := b.getIntervalFromDecimal(b.Ctx, b.Args, row, unit)
	if isNull || err != nil {
		return types.Time{}, true, err
	}

	result, isNull, err := b.add(b.Ctx, date, interval, unit)
	return result, isNull || err != nil, err
}

type builtinAddDateIntStringSig struct {
	BaseBuiltinFunc
	baseDateArithmitical
}

func (b *builtinAddDateIntStringSig) Clone() BuiltinFunc {
	newSig := &builtinAddDateIntStringSig{baseDateArithmitical: b.baseDateArithmitical}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalTime evals ADDDATE(date,INTERVAL expr unit).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_adddate
func (b *builtinAddDateIntStringSig) evalTime(row chunk.Row) (types.Time, bool, error) {
	unit, isNull, err := b.Args[2].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return types.Time{}, true, err
	}

	date, isNull, err := b.getDateFromInt(b.Ctx, b.Args, row, unit)
	if isNull || err != nil {
		return types.Time{}, true, err
	}

	interval, isNull, err := b.getIntervalFromString(b.Ctx, b.Args, row, unit)
	if isNull || err != nil {
		return types.Time{}, true, err
	}

	result, isNull, err := b.add(b.Ctx, date, interval, unit)
	return result, isNull || err != nil, err
}

type builtinAddDateIntIntSig struct {
	BaseBuiltinFunc
	baseDateArithmitical
}

func (b *builtinAddDateIntIntSig) Clone() BuiltinFunc {
	newSig := &builtinAddDateIntIntSig{baseDateArithmitical: b.baseDateArithmitical}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalTime evals ADDDATE(date,INTERVAL expr unit).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_adddate
func (b *builtinAddDateIntIntSig) evalTime(row chunk.Row) (types.Time, bool, error) {
	unit, isNull, err := b.Args[2].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return types.Time{}, true, err
	}

	date, isNull, err := b.getDateFromInt(b.Ctx, b.Args, row, unit)
	if isNull || err != nil {
		return types.Time{}, true, err
	}

	interval, isNull, err := b.getIntervalFromInt(b.Ctx, b.Args, row, unit)
	if isNull || err != nil {
		return types.Time{}, true, err
	}

	result, isNull, err := b.add(b.Ctx, date, interval, unit)
	return result, isNull || err != nil, err
}

type builtinAddDateIntRealSig struct {
	BaseBuiltinFunc
	baseDateArithmitical
}

func (b *builtinAddDateIntRealSig) Clone() BuiltinFunc {
	newSig := &builtinAddDateIntRealSig{baseDateArithmitical: b.baseDateArithmitical}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalTime evals ADDDATE(date,INTERVAL expr unit).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_adddate
func (b *builtinAddDateIntRealSig) evalTime(row chunk.Row) (types.Time, bool, error) {
	unit, isNull, err := b.Args[2].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return types.Time{}, true, err
	}

	date, isNull, err := b.getDateFromInt(b.Ctx, b.Args, row, unit)
	if isNull || err != nil {
		return types.Time{}, true, err
	}

	interval, isNull, err := b.getIntervalFromReal(b.Ctx, b.Args, row, unit)
	if isNull || err != nil {
		return types.Time{}, true, err
	}

	result, isNull, err := b.add(b.Ctx, date, interval, unit)
	return result, isNull || err != nil, err
}

type builtinAddDateIntDecimalSig struct {
	BaseBuiltinFunc
	baseDateArithmitical
}

func (b *builtinAddDateIntDecimalSig) Clone() BuiltinFunc {
	newSig := &builtinAddDateIntDecimalSig{baseDateArithmitical: b.baseDateArithmitical}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalTime evals ADDDATE(date,INTERVAL expr unit).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_adddate
func (b *builtinAddDateIntDecimalSig) evalTime(row chunk.Row) (types.Time, bool, error) {
	unit, isNull, err := b.Args[2].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return types.Time{}, true, err
	}

	date, isNull, err := b.getDateFromInt(b.Ctx, b.Args, row, unit)
	if isNull || err != nil {
		return types.Time{}, true, err
	}

	interval, isNull, err := b.getIntervalFromDecimal(b.Ctx, b.Args, row, unit)
	if isNull || err != nil {
		return types.Time{}, true, err
	}

	result, isNull, err := b.add(b.Ctx, date, interval, unit)
	return result, isNull || err != nil, err
}

type builtinAddDateDatetimeStringSig struct {
	BaseBuiltinFunc
	baseDateArithmitical
}

func (b *builtinAddDateDatetimeStringSig) Clone() BuiltinFunc {
	newSig := &builtinAddDateDatetimeStringSig{baseDateArithmitical: b.baseDateArithmitical}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalTime evals ADDDATE(date,INTERVAL expr unit).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_adddate
func (b *builtinAddDateDatetimeStringSig) evalTime(row chunk.Row) (types.Time, bool, error) {
	unit, isNull, err := b.Args[2].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return types.Time{}, true, err
	}

	date, isNull, err := b.getDateFromDatetime(b.Ctx, b.Args, row, unit)
	if isNull || err != nil {
		return types.Time{}, true, err
	}

	interval, isNull, err := b.getIntervalFromString(b.Ctx, b.Args, row, unit)
	if isNull || err != nil {
		return types.Time{}, true, err
	}

	result, isNull, err := b.add(b.Ctx, date, interval, unit)
	return result, isNull || err != nil, err
}

type builtinAddDateDatetimeIntSig struct {
	BaseBuiltinFunc
	baseDateArithmitical
}

func (b *builtinAddDateDatetimeIntSig) Clone() BuiltinFunc {
	newSig := &builtinAddDateDatetimeIntSig{baseDateArithmitical: b.baseDateArithmitical}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalTime evals ADDDATE(date,INTERVAL expr unit).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_adddate
func (b *builtinAddDateDatetimeIntSig) evalTime(row chunk.Row) (types.Time, bool, error) {
	unit, isNull, err := b.Args[2].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return types.Time{}, true, err
	}

	date, isNull, err := b.getDateFromDatetime(b.Ctx, b.Args, row, unit)
	if isNull || err != nil {
		return types.Time{}, true, err
	}

	interval, isNull, err := b.getIntervalFromInt(b.Ctx, b.Args, row, unit)
	if isNull || err != nil {
		return types.Time{}, true, err
	}

	result, isNull, err := b.add(b.Ctx, date, interval, unit)
	return result, isNull || err != nil, err
}

type builtinAddDateDatetimeRealSig struct {
	BaseBuiltinFunc
	baseDateArithmitical
}

func (b *builtinAddDateDatetimeRealSig) Clone() BuiltinFunc {
	newSig := &builtinAddDateDatetimeRealSig{baseDateArithmitical: b.baseDateArithmitical}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalTime evals ADDDATE(date,INTERVAL expr unit).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_adddate
func (b *builtinAddDateDatetimeRealSig) evalTime(row chunk.Row) (types.Time, bool, error) {
	unit, isNull, err := b.Args[2].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return types.Time{}, true, err
	}

	date, isNull, err := b.getDateFromDatetime(b.Ctx, b.Args, row, unit)
	if isNull || err != nil {
		return types.Time{}, true, err
	}

	interval, isNull, err := b.getIntervalFromReal(b.Ctx, b.Args, row, unit)
	if isNull || err != nil {
		return types.Time{}, true, err
	}

	result, isNull, err := b.add(b.Ctx, date, interval, unit)
	return result, isNull || err != nil, err
}

type builtinAddDateDatetimeDecimalSig struct {
	BaseBuiltinFunc
	baseDateArithmitical
}

func (b *builtinAddDateDatetimeDecimalSig) Clone() BuiltinFunc {
	newSig := &builtinAddDateDatetimeDecimalSig{baseDateArithmitical: b.baseDateArithmitical}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalTime evals ADDDATE(date,INTERVAL expr unit).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_adddate
func (b *builtinAddDateDatetimeDecimalSig) evalTime(row chunk.Row) (types.Time, bool, error) {
	unit, isNull, err := b.Args[2].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return types.Time{}, true, err
	}

	date, isNull, err := b.getDateFromDatetime(b.Ctx, b.Args, row, unit)
	if isNull || err != nil {
		return types.Time{}, true, err
	}

	interval, isNull, err := b.getIntervalFromDecimal(b.Ctx, b.Args, row, unit)
	if isNull || err != nil {
		return types.Time{}, true, err
	}

	result, isNull, err := b.add(b.Ctx, date, interval, unit)
	return result, isNull || err != nil, err
}

type builtinAddDateDurationStringSig struct {
	BaseBuiltinFunc
	baseDateArithmitical
}

func (b *builtinAddDateDurationStringSig) Clone() BuiltinFunc {
	newSig := &builtinAddDateDurationStringSig{baseDateArithmitical: b.baseDateArithmitical}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinAddDateDurationStringSig) evalDuration(row chunk.Row) (types.Duration, bool, error) {
	unit, isNull, err := b.Args[2].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return types.ZeroDuration, true, err
	}

	dur, isNull, err := b.Args[0].EvalDuration(b.Ctx, row)
	if isNull || err != nil {
		return types.ZeroDuration, true, err
	}

	interval, isNull, err := b.getIntervalFromString(b.Ctx, b.Args, row, unit)
	if isNull || err != nil {
		return types.ZeroDuration, true, err
	}

	result, isNull, err := b.addDuration(b.Ctx, dur, interval, unit)
	return result, isNull || err != nil, err
}

type builtinAddDateDurationIntSig struct {
	BaseBuiltinFunc
	baseDateArithmitical
}

func (b *builtinAddDateDurationIntSig) Clone() BuiltinFunc {
	newSig := &builtinAddDateDurationIntSig{baseDateArithmitical: b.baseDateArithmitical}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinAddDateDurationIntSig) evalDuration(row chunk.Row) (types.Duration, bool, error) {
	unit, isNull, err := b.Args[2].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return types.ZeroDuration, true, err
	}

	dur, isNull, err := b.Args[0].EvalDuration(b.Ctx, row)
	if isNull || err != nil {
		return types.ZeroDuration, true, err
	}
	interval, isNull, err := b.getIntervalFromInt(b.Ctx, b.Args, row, unit)
	if isNull || err != nil {
		return types.ZeroDuration, true, err
	}

	result, isNull, err := b.addDuration(b.Ctx, dur, interval, unit)
	return result, isNull || err != nil, err
}

type builtinAddDateDurationDecimalSig struct {
	BaseBuiltinFunc
	baseDateArithmitical
}

func (b *builtinAddDateDurationDecimalSig) Clone() BuiltinFunc {
	newSig := &builtinAddDateDurationDecimalSig{baseDateArithmitical: b.baseDateArithmitical}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinAddDateDurationDecimalSig) evalDuration(row chunk.Row) (types.Duration, bool, error) {
	unit, isNull, err := b.Args[2].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return types.ZeroDuration, true, err
	}

	dur, isNull, err := b.Args[0].EvalDuration(b.Ctx, row)
	if isNull || err != nil {
		return types.ZeroDuration, true, err
	}
	interval, isNull, err := b.getIntervalFromDecimal(b.Ctx, b.Args, row, unit)
	if isNull || err != nil {
		return types.ZeroDuration, true, err
	}

	result, isNull, err := b.addDuration(b.Ctx, dur, interval, unit)
	return result, isNull || err != nil, err
}

type builtinAddDateDurationRealSig struct {
	BaseBuiltinFunc
	baseDateArithmitical
}

func (b *builtinAddDateDurationRealSig) Clone() BuiltinFunc {
	newSig := &builtinAddDateDurationRealSig{baseDateArithmitical: b.baseDateArithmitical}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinAddDateDurationRealSig) evalDuration(row chunk.Row) (types.Duration, bool, error) {
	unit, isNull, err := b.Args[2].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return types.ZeroDuration, true, err
	}

	dur, isNull, err := b.Args[0].EvalDuration(b.Ctx, row)
	if isNull || err != nil {
		return types.ZeroDuration, true, err
	}
	interval, isNull, err := b.getIntervalFromReal(b.Ctx, b.Args, row, unit)
	if isNull || err != nil {
		return types.ZeroDuration, true, err
	}

	result, isNull, err := b.addDuration(b.Ctx, dur, interval, unit)
	return result, isNull || err != nil, err
}

type subDateFunctionClass struct {
	BaseFunctionClass
}

func (c *subDateFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (sig BuiltinFunc, err error) {
	if err = c.VerifyArgs(args); err != nil {
		return nil, err
	}

	dateEvalTp := args[0].GetType().EvalType()
	if dateEvalTp != types.ETString && dateEvalTp != types.ETInt && dateEvalTp != types.ETDuration {
		dateEvalTp = types.ETDatetime
	}

	intervalEvalTp := args[1].GetType().EvalType()
	if intervalEvalTp != types.ETString && intervalEvalTp != types.ETDecimal && intervalEvalTp != types.ETReal {
		intervalEvalTp = types.ETInt
	}

	argTps := []types.EvalType{dateEvalTp, intervalEvalTp, types.ETString}
	var bf BaseBuiltinFunc
	if dateEvalTp == types.ETDuration {
		unit, _, err := args[2].EvalString(ctx, chunk.Row{})
		if err != nil {
			return nil, err
		}
		internalFsp := 0
		switch unit {
		// If the unit has micro second, then the fsp must be the MaxFsp.
		case "MICROSECOND", "SECOND_MICROSECOND", "MINUTE_MICROSECOND", "HOUR_MICROSECOND", "DAY_MICROSECOND":
			internalFsp = int(types.MaxFsp)
		// If the unit is second, the fsp is related with the arg[1]'s.
		case "SECOND":
			internalFsp = int(types.MaxFsp)
			if intervalEvalTp != types.ETString {
				internalFsp = mathutil.Min(args[1].GetType().Decimal, int(types.MaxFsp))
			}
			// Otherwise, the fsp should be 0.
		}
		bf = NewBaseBuiltinFuncWithTp(ctx, args, types.ETDuration, argTps...)
		arg0Dec, err := getExpressionFsp(ctx, args[0])
		if err != nil {
			return nil, err
		}
		bf.Tp.Flen, bf.Tp.Decimal = mysql.MaxDurationWidthWithFsp, mathutil.Max(arg0Dec, internalFsp)
	} else {
		bf = NewBaseBuiltinFuncWithTp(ctx, args, types.ETDatetime, argTps...)
		bf.Tp.Flen, bf.Tp.Decimal = mysql.MaxDatetimeFullWidth, types.UnspecifiedLength
	}

	switch {
	case dateEvalTp == types.ETString && intervalEvalTp == types.ETString:
		sig = &builtinSubDateStringStringSig{
			BaseBuiltinFunc:      bf,
			baseDateArithmitical: newDateArighmeticalUtil(),
		}
	case dateEvalTp == types.ETString && intervalEvalTp == types.ETInt:
		sig = &builtinSubDateStringIntSig{
			BaseBuiltinFunc:      bf,
			baseDateArithmitical: newDateArighmeticalUtil(),
		}
	case dateEvalTp == types.ETString && intervalEvalTp == types.ETReal:
		sig = &builtinSubDateStringRealSig{
			BaseBuiltinFunc:      bf,
			baseDateArithmitical: newDateArighmeticalUtil(),
		}
	case dateEvalTp == types.ETString && intervalEvalTp == types.ETDecimal:
		sig = &builtinSubDateStringDecimalSig{
			BaseBuiltinFunc:      bf,
			baseDateArithmitical: newDateArighmeticalUtil(),
		}
	case dateEvalTp == types.ETInt && intervalEvalTp == types.ETString:
		sig = &builtinSubDateIntStringSig{
			BaseBuiltinFunc:      bf,
			baseDateArithmitical: newDateArighmeticalUtil(),
		}
	case dateEvalTp == types.ETInt && intervalEvalTp == types.ETInt:
		sig = &builtinSubDateIntIntSig{
			BaseBuiltinFunc:      bf,
			baseDateArithmitical: newDateArighmeticalUtil(),
		}
	case dateEvalTp == types.ETInt && intervalEvalTp == types.ETReal:
		sig = &builtinSubDateIntRealSig{
			BaseBuiltinFunc:      bf,
			baseDateArithmitical: newDateArighmeticalUtil(),
		}
	case dateEvalTp == types.ETInt && intervalEvalTp == types.ETDecimal:
		sig = &builtinSubDateIntDecimalSig{
			BaseBuiltinFunc:      bf,
			baseDateArithmitical: newDateArighmeticalUtil(),
		}
	case dateEvalTp == types.ETDatetime && intervalEvalTp == types.ETString:
		sig = &builtinSubDateDatetimeStringSig{
			BaseBuiltinFunc:      bf,
			baseDateArithmitical: newDateArighmeticalUtil(),
		}
	case dateEvalTp == types.ETDatetime && intervalEvalTp == types.ETInt:
		sig = &builtinSubDateDatetimeIntSig{
			BaseBuiltinFunc:      bf,
			baseDateArithmitical: newDateArighmeticalUtil(),
		}
	case dateEvalTp == types.ETDatetime && intervalEvalTp == types.ETReal:
		sig = &builtinSubDateDatetimeRealSig{
			BaseBuiltinFunc:      bf,
			baseDateArithmitical: newDateArighmeticalUtil(),
		}
	case dateEvalTp == types.ETDatetime && intervalEvalTp == types.ETDecimal:
		sig = &builtinSubDateDatetimeDecimalSig{
			BaseBuiltinFunc:      bf,
			baseDateArithmitical: newDateArighmeticalUtil(),
		}
	case dateEvalTp == types.ETDuration && intervalEvalTp == types.ETString:
		sig = &builtinSubDateDurationStringSig{
			BaseBuiltinFunc:      bf,
			baseDateArithmitical: newDateArighmeticalUtil(),
		}
	case dateEvalTp == types.ETDuration && intervalEvalTp == types.ETInt:
		sig = &builtinSubDateDurationIntSig{
			BaseBuiltinFunc:      bf,
			baseDateArithmitical: newDateArighmeticalUtil(),
		}
	case dateEvalTp == types.ETDuration && intervalEvalTp == types.ETReal:
		sig = &builtinSubDateDurationRealSig{
			BaseBuiltinFunc:      bf,
			baseDateArithmitical: newDateArighmeticalUtil(),
		}
	case dateEvalTp == types.ETDuration && intervalEvalTp == types.ETDecimal:
		sig = &builtinSubDateDurationDecimalSig{
			BaseBuiltinFunc:      bf,
			baseDateArithmitical: newDateArighmeticalUtil(),
		}
	}
	return sig, nil
}

type builtinSubDateStringStringSig struct {
	BaseBuiltinFunc
	baseDateArithmitical
}

func (b *builtinSubDateStringStringSig) Clone() BuiltinFunc {
	newSig := &builtinSubDateStringStringSig{baseDateArithmitical: b.baseDateArithmitical}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalTime evals SUBDATE(date,INTERVAL expr unit).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_subdate
func (b *builtinSubDateStringStringSig) evalTime(row chunk.Row) (types.Time, bool, error) {
	unit, isNull, err := b.Args[2].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return types.Time{}, true, err
	}

	date, isNull, err := b.getDateFromString(b.Ctx, b.Args, row, unit)
	if isNull || err != nil {
		return types.Time{}, true, err
	}

	interval, isNull, err := b.getIntervalFromString(b.Ctx, b.Args, row, unit)
	if isNull || err != nil {
		return types.Time{}, true, err
	}

	result, isNull, err := b.sub(b.Ctx, date, interval, unit)
	return result, isNull || err != nil, err
}

type builtinSubDateStringIntSig struct {
	BaseBuiltinFunc
	baseDateArithmitical
}

func (b *builtinSubDateStringIntSig) Clone() BuiltinFunc {
	newSig := &builtinSubDateStringIntSig{baseDateArithmitical: b.baseDateArithmitical}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalTime evals SUBDATE(date,INTERVAL expr unit).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_subdate
func (b *builtinSubDateStringIntSig) evalTime(row chunk.Row) (types.Time, bool, error) {
	unit, isNull, err := b.Args[2].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return types.Time{}, true, err
	}

	date, isNull, err := b.getDateFromString(b.Ctx, b.Args, row, unit)
	if isNull || err != nil {
		return types.Time{}, true, err
	}

	interval, isNull, err := b.getIntervalFromInt(b.Ctx, b.Args, row, unit)
	if isNull || err != nil {
		return types.Time{}, true, err
	}

	result, isNull, err := b.sub(b.Ctx, date, interval, unit)
	return result, isNull || err != nil, err
}

type builtinSubDateStringRealSig struct {
	BaseBuiltinFunc
	baseDateArithmitical
}

func (b *builtinSubDateStringRealSig) Clone() BuiltinFunc {
	newSig := &builtinSubDateStringRealSig{baseDateArithmitical: b.baseDateArithmitical}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalTime evals SUBDATE(date,INTERVAL expr unit).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_subdate
func (b *builtinSubDateStringRealSig) evalTime(row chunk.Row) (types.Time, bool, error) {
	unit, isNull, err := b.Args[2].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return types.Time{}, true, err
	}

	date, isNull, err := b.getDateFromString(b.Ctx, b.Args, row, unit)
	if isNull || err != nil {
		return types.Time{}, true, err
	}

	interval, isNull, err := b.getIntervalFromReal(b.Ctx, b.Args, row, unit)
	if isNull || err != nil {
		return types.Time{}, true, err
	}

	result, isNull, err := b.sub(b.Ctx, date, interval, unit)
	return result, isNull || err != nil, err
}

type builtinSubDateStringDecimalSig struct {
	BaseBuiltinFunc
	baseDateArithmitical
}

func (b *builtinSubDateStringDecimalSig) Clone() BuiltinFunc {
	newSig := &builtinSubDateStringDecimalSig{baseDateArithmitical: b.baseDateArithmitical}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinSubDateStringDecimalSig) evalTime(row chunk.Row) (types.Time, bool, error) {
	unit, isNull, err := b.Args[2].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return types.Time{}, true, err
	}

	date, isNull, err := b.getDateFromString(b.Ctx, b.Args, row, unit)
	if isNull || err != nil {
		return types.Time{}, true, err
	}

	interval, isNull, err := b.getIntervalFromDecimal(b.Ctx, b.Args, row, unit)
	if isNull || err != nil {
		return types.Time{}, true, err
	}

	result, isNull, err := b.sub(b.Ctx, date, interval, unit)
	return result, isNull || err != nil, err
}

type builtinSubDateIntStringSig struct {
	BaseBuiltinFunc
	baseDateArithmitical
}

func (b *builtinSubDateIntStringSig) Clone() BuiltinFunc {
	newSig := &builtinSubDateIntStringSig{baseDateArithmitical: b.baseDateArithmitical}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalTime evals SUBDATE(date,INTERVAL expr unit).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_subdate
func (b *builtinSubDateIntStringSig) evalTime(row chunk.Row) (types.Time, bool, error) {
	unit, isNull, err := b.Args[2].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return types.Time{}, true, err
	}

	date, isNull, err := b.getDateFromInt(b.Ctx, b.Args, row, unit)
	if isNull || err != nil {
		return types.Time{}, true, err
	}

	interval, isNull, err := b.getIntervalFromString(b.Ctx, b.Args, row, unit)
	if isNull || err != nil {
		return types.Time{}, true, err
	}

	result, isNull, err := b.sub(b.Ctx, date, interval, unit)
	return result, isNull || err != nil, err
}

type builtinSubDateIntIntSig struct {
	BaseBuiltinFunc
	baseDateArithmitical
}

func (b *builtinSubDateIntIntSig) Clone() BuiltinFunc {
	newSig := &builtinSubDateIntIntSig{baseDateArithmitical: b.baseDateArithmitical}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalTime evals SUBDATE(date,INTERVAL expr unit).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_subdate
func (b *builtinSubDateIntIntSig) evalTime(row chunk.Row) (types.Time, bool, error) {
	unit, isNull, err := b.Args[2].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return types.Time{}, true, err
	}

	date, isNull, err := b.getDateFromInt(b.Ctx, b.Args, row, unit)
	if isNull || err != nil {
		return types.Time{}, true, err
	}

	interval, isNull, err := b.getIntervalFromInt(b.Ctx, b.Args, row, unit)
	if isNull || err != nil {
		return types.Time{}, true, err
	}

	result, isNull, err := b.sub(b.Ctx, date, interval, unit)
	return result, isNull || err != nil, err
}

type builtinSubDateIntRealSig struct {
	BaseBuiltinFunc
	baseDateArithmitical
}

func (b *builtinSubDateIntRealSig) Clone() BuiltinFunc {
	newSig := &builtinSubDateIntRealSig{baseDateArithmitical: b.baseDateArithmitical}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalTime evals SUBDATE(date,INTERVAL expr unit).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_subdate
func (b *builtinSubDateIntRealSig) evalTime(row chunk.Row) (types.Time, bool, error) {
	unit, isNull, err := b.Args[2].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return types.Time{}, true, err
	}

	date, isNull, err := b.getDateFromInt(b.Ctx, b.Args, row, unit)
	if isNull || err != nil {
		return types.Time{}, true, err
	}

	interval, isNull, err := b.getIntervalFromReal(b.Ctx, b.Args, row, unit)
	if isNull || err != nil {
		return types.Time{}, true, err
	}

	result, isNull, err := b.sub(b.Ctx, date, interval, unit)
	return result, isNull || err != nil, err
}

type builtinSubDateDatetimeStringSig struct {
	BaseBuiltinFunc
	baseDateArithmitical
}

type builtinSubDateIntDecimalSig struct {
	BaseBuiltinFunc
	baseDateArithmitical
}

func (b *builtinSubDateIntDecimalSig) Clone() BuiltinFunc {
	newSig := &builtinSubDateIntDecimalSig{baseDateArithmitical: b.baseDateArithmitical}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalTime evals SUBDATE(date,INTERVAL expr unit).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_subdate
func (b *builtinSubDateIntDecimalSig) evalTime(row chunk.Row) (types.Time, bool, error) {
	unit, isNull, err := b.Args[2].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return types.Time{}, true, err
	}

	date, isNull, err := b.getDateFromInt(b.Ctx, b.Args, row, unit)
	if isNull || err != nil {
		return types.Time{}, true, err
	}

	interval, isNull, err := b.getIntervalFromDecimal(b.Ctx, b.Args, row, unit)
	if isNull || err != nil {
		return types.Time{}, true, err
	}

	result, isNull, err := b.sub(b.Ctx, date, interval, unit)
	return result, isNull || err != nil, err
}

func (b *builtinSubDateDatetimeStringSig) Clone() BuiltinFunc {
	newSig := &builtinSubDateDatetimeStringSig{baseDateArithmitical: b.baseDateArithmitical}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalTime evals SUBDATE(date,INTERVAL expr unit).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_subdate
func (b *builtinSubDateDatetimeStringSig) evalTime(row chunk.Row) (types.Time, bool, error) {
	unit, isNull, err := b.Args[2].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return types.Time{}, true, err
	}

	date, isNull, err := b.getDateFromDatetime(b.Ctx, b.Args, row, unit)
	if isNull || err != nil {
		return types.Time{}, true, err
	}

	interval, isNull, err := b.getIntervalFromString(b.Ctx, b.Args, row, unit)
	if isNull || err != nil {
		return types.Time{}, true, err
	}

	result, isNull, err := b.sub(b.Ctx, date, interval, unit)
	return result, isNull || err != nil, err
}

type builtinSubDateDatetimeIntSig struct {
	BaseBuiltinFunc
	baseDateArithmitical
}

func (b *builtinSubDateDatetimeIntSig) Clone() BuiltinFunc {
	newSig := &builtinSubDateDatetimeIntSig{baseDateArithmitical: b.baseDateArithmitical}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalTime evals SUBDATE(date,INTERVAL expr unit).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_subdate
func (b *builtinSubDateDatetimeIntSig) evalTime(row chunk.Row) (types.Time, bool, error) {
	unit, isNull, err := b.Args[2].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return types.Time{}, true, err
	}

	date, isNull, err := b.getDateFromDatetime(b.Ctx, b.Args, row, unit)
	if isNull || err != nil {
		return types.Time{}, true, err
	}

	interval, isNull, err := b.getIntervalFromInt(b.Ctx, b.Args, row, unit)
	if isNull || err != nil {
		return types.Time{}, true, err
	}

	result, isNull, err := b.sub(b.Ctx, date, interval, unit)
	return result, isNull || err != nil, err
}

type builtinSubDateDatetimeRealSig struct {
	BaseBuiltinFunc
	baseDateArithmitical
}

func (b *builtinSubDateDatetimeRealSig) Clone() BuiltinFunc {
	newSig := &builtinSubDateDatetimeRealSig{baseDateArithmitical: b.baseDateArithmitical}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalTime evals SUBDATE(date,INTERVAL expr unit).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_subdate
func (b *builtinSubDateDatetimeRealSig) evalTime(row chunk.Row) (types.Time, bool, error) {
	unit, isNull, err := b.Args[2].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return types.Time{}, true, err
	}

	date, isNull, err := b.getDateFromDatetime(b.Ctx, b.Args, row, unit)
	if isNull || err != nil {
		return types.Time{}, true, err
	}

	interval, isNull, err := b.getIntervalFromReal(b.Ctx, b.Args, row, unit)
	if isNull || err != nil {
		return types.Time{}, true, err
	}

	result, isNull, err := b.sub(b.Ctx, date, interval, unit)
	return result, isNull || err != nil, err
}

type builtinSubDateDatetimeDecimalSig struct {
	BaseBuiltinFunc
	baseDateArithmitical
}

func (b *builtinSubDateDatetimeDecimalSig) Clone() BuiltinFunc {
	newSig := &builtinSubDateDatetimeDecimalSig{baseDateArithmitical: b.baseDateArithmitical}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalTime evals SUBDATE(date,INTERVAL expr unit).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_subdate
func (b *builtinSubDateDatetimeDecimalSig) evalTime(row chunk.Row) (types.Time, bool, error) {
	unit, isNull, err := b.Args[2].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return types.Time{}, true, err
	}

	date, isNull, err := b.getDateFromDatetime(b.Ctx, b.Args, row, unit)
	if isNull || err != nil {
		return types.Time{}, true, err
	}

	interval, isNull, err := b.getIntervalFromDecimal(b.Ctx, b.Args, row, unit)
	if isNull || err != nil {
		return types.Time{}, true, err
	}

	result, isNull, err := b.sub(b.Ctx, date, interval, unit)
	return result, isNull || err != nil, err
}

type builtinSubDateDurationStringSig struct {
	BaseBuiltinFunc
	baseDateArithmitical
}

func (b *builtinSubDateDurationStringSig) Clone() BuiltinFunc {
	newSig := &builtinSubDateDurationStringSig{baseDateArithmitical: b.baseDateArithmitical}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinSubDateDurationStringSig) evalDuration(row chunk.Row) (types.Duration, bool, error) {
	unit, isNull, err := b.Args[2].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return types.ZeroDuration, true, err
	}

	dur, isNull, err := b.Args[0].EvalDuration(b.Ctx, row)
	if isNull || err != nil {
		return types.ZeroDuration, true, err
	}

	interval, isNull, err := b.getIntervalFromString(b.Ctx, b.Args, row, unit)
	if isNull || err != nil {
		return types.ZeroDuration, true, err
	}

	result, isNull, err := b.subDuration(b.Ctx, dur, interval, unit)
	return result, isNull || err != nil, err
}

type builtinSubDateDurationIntSig struct {
	BaseBuiltinFunc
	baseDateArithmitical
}

func (b *builtinSubDateDurationIntSig) Clone() BuiltinFunc {
	newSig := &builtinSubDateDurationIntSig{baseDateArithmitical: b.baseDateArithmitical}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinSubDateDurationIntSig) evalDuration(row chunk.Row) (types.Duration, bool, error) {
	unit, isNull, err := b.Args[2].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return types.ZeroDuration, true, err
	}

	dur, isNull, err := b.Args[0].EvalDuration(b.Ctx, row)
	if isNull || err != nil {
		return types.ZeroDuration, true, err
	}

	interval, isNull, err := b.getIntervalFromInt(b.Ctx, b.Args, row, unit)
	if isNull || err != nil {
		return types.ZeroDuration, true, err
	}

	result, isNull, err := b.subDuration(b.Ctx, dur, interval, unit)
	return result, isNull || err != nil, err
}

type builtinSubDateDurationDecimalSig struct {
	BaseBuiltinFunc
	baseDateArithmitical
}

func (b *builtinSubDateDurationDecimalSig) Clone() BuiltinFunc {
	newSig := &builtinSubDateDurationDecimalSig{baseDateArithmitical: b.baseDateArithmitical}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinSubDateDurationDecimalSig) evalDuration(row chunk.Row) (types.Duration, bool, error) {
	unit, isNull, err := b.Args[2].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return types.ZeroDuration, true, err
	}

	dur, isNull, err := b.Args[0].EvalDuration(b.Ctx, row)
	if isNull || err != nil {
		return types.ZeroDuration, true, err
	}

	interval, isNull, err := b.getIntervalFromDecimal(b.Ctx, b.Args, row, unit)
	if isNull || err != nil {
		return types.ZeroDuration, true, err
	}

	result, isNull, err := b.subDuration(b.Ctx, dur, interval, unit)
	return result, isNull || err != nil, err
}

type builtinSubDateDurationRealSig struct {
	BaseBuiltinFunc
	baseDateArithmitical
}

func (b *builtinSubDateDurationRealSig) Clone() BuiltinFunc {
	newSig := &builtinSubDateDurationRealSig{baseDateArithmitical: b.baseDateArithmitical}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinSubDateDurationRealSig) evalDuration(row chunk.Row) (types.Duration, bool, error) {
	unit, isNull, err := b.Args[2].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return types.ZeroDuration, true, err
	}

	dur, isNull, err := b.Args[0].EvalDuration(b.Ctx, row)
	if isNull || err != nil {
		return types.ZeroDuration, true, err
	}
	interval, isNull, err := b.getIntervalFromReal(b.Ctx, b.Args, row, unit)
	if isNull || err != nil {
		return types.ZeroDuration, true, err
	}

	result, isNull, err := b.subDuration(b.Ctx, dur, interval, unit)
	return result, isNull || err != nil, err
}

type timestampDiffFunctionClass struct {
	BaseFunctionClass
}

func (c *timestampDiffFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETString, types.ETDatetime, types.ETDatetime)
	sig := &builtinTimestampDiffSig{bf}
	return sig, nil
}

type builtinTimestampDiffSig struct {
	BaseBuiltinFunc
}

func (b *builtinTimestampDiffSig) Clone() BuiltinFunc {
	newSig := &builtinTimestampDiffSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalInt evals a builtinTimestampDiffSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_timestampdiff
func (b *builtinTimestampDiffSig) evalInt(row chunk.Row) (int64, bool, error) {
	unit, isNull, err := b.Args[0].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	lhs, isNull, err := b.Args[1].EvalTime(b.Ctx, row)
	if isNull || err != nil {
		return 0, isNull, handleInvalidTimeError(b.Ctx, err)
	}
	rhs, isNull, err := b.Args[2].EvalTime(b.Ctx, row)
	if isNull || err != nil {
		return 0, isNull, handleInvalidTimeError(b.Ctx, err)
	}
	if invalidLHS, invalidRHS := lhs.InvalidZero(), rhs.InvalidZero(); invalidLHS || invalidRHS {
		if invalidLHS {
			err = handleInvalidTimeError(b.Ctx, types.ErrIncorrectDatetimeValue.GenWithStackByArgs(lhs.String()))
		}
		if invalidRHS {
			err = handleInvalidTimeError(b.Ctx, types.ErrIncorrectDatetimeValue.GenWithStackByArgs(rhs.String()))
		}
		return 0, true, err
	}
	return types.TimestampDiff(unit, lhs, rhs), false, nil
}

type unixTimestampFunctionClass struct {
	BaseFunctionClass
}

func (c *unixTimestampFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	var (
		argTps              []types.EvalType
		retTp               types.EvalType
		retFLen, retDecimal int
	)

	if len(args) == 0 {
		retTp, retDecimal = types.ETInt, 0
	} else {
		argTps = []types.EvalType{types.ETDatetime}
		argType := args[0].GetType()
		argEvaltp := argType.EvalType()
		if argEvaltp == types.ETString {
			// Treat types.ETString as unspecified decimal.
			retDecimal = types.UnspecifiedLength
			if cnst, ok := args[0].(*Constant); ok {
				tmpStr, _, err := cnst.EvalString(ctx, chunk.Row{})
				if err != nil {
					return nil, err
				}
				retDecimal = 0
				if dotIdx := strings.LastIndex(tmpStr, "."); dotIdx >= 0 {
					retDecimal = len(tmpStr) - dotIdx - 1
				}
			}
		} else {
			retDecimal = argType.Decimal
		}
		if retDecimal > 6 || retDecimal == types.UnspecifiedLength {
			retDecimal = 6
		}
		if retDecimal == 0 {
			retTp = types.ETInt
		} else {
			retTp = types.ETDecimal
		}
	}
	if retTp == types.ETInt {
		retFLen = 11
	} else if retTp == types.ETDecimal {
		retFLen = 12 + retDecimal
	} else {
		panic("Unexpected retTp")
	}

	bf := NewBaseBuiltinFuncWithTp(ctx, args, retTp, argTps...)
	bf.Tp.Flen = retFLen
	bf.Tp.Decimal = retDecimal

	var sig BuiltinFunc
	if len(args) == 0 {
		sig = &builtinUnixTimestampCurrentSig{bf}
	} else if retTp == types.ETInt {
		sig = &builtinUnixTimestampIntSig{bf}
	} else if retTp == types.ETDecimal {
		sig = &builtinUnixTimestampDecSig{bf}
	}
	return sig, nil
}

// goTimeToMysqlUnixTimestamp converts go time into MySQL's Unix timestamp.
// MySQL's Unix timestamp ranges in int32. Values out of range should be rewritten to 0.
func goTimeToMysqlUnixTimestamp(t time.Time, decimal int) (*types.MyDecimal, error) {
	nanoSeconds := t.UnixNano()
	if nanoSeconds < 0 || (nanoSeconds/1e3) >= (math.MaxInt32+1)*1e6 {
		return new(types.MyDecimal), nil
	}
	dec := new(types.MyDecimal)
	// Here we don't use float to prevent precision lose.
	dec.FromInt(nanoSeconds)
	err := dec.Shift(-9)
	if err != nil {
		return nil, err
	}

	// In MySQL's implementation, unix_timestamp() will truncate the result instead of rounding it.
	// Results below are from MySQL 5.7, which can prove it.
	//	mysql> select unix_timestamp(), unix_timestamp(now(0)), now(0), unix_timestamp(now(3)), now(3), now(6);
	//	+------------------+------------------------+---------------------+------------------------+-------------------------+----------------------------+
	//	| unix_timestamp() | unix_timestamp(now(0)) | now(0)              | unix_timestamp(now(3)) | now(3)                  | now(6)                     |
	//	+------------------+------------------------+---------------------+------------------------+-------------------------+----------------------------+
	//	|       1553503194 |             1553503194 | 2019-03-25 16:39:54 |         1553503194.992 | 2019-03-25 16:39:54.992 | 2019-03-25 16:39:54.992969 |
	//	+------------------+------------------------+---------------------+------------------------+-------------------------+----------------------------+
	err = dec.Round(dec, decimal, types.ModeTruncate)
	return dec, err
}

type builtinUnixTimestampCurrentSig struct {
	BaseBuiltinFunc
}

func (b *builtinUnixTimestampCurrentSig) Clone() BuiltinFunc {
	newSig := &builtinUnixTimestampCurrentSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalInt evals a UNIX_TIMESTAMP().
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_unix-timestamp
func (b *builtinUnixTimestampCurrentSig) evalInt(row chunk.Row) (int64, bool, error) {
	nowTs, err := getStmtTimestamp(b.Ctx)
	if err != nil {
		return 0, true, err
	}
	dec, err := goTimeToMysqlUnixTimestamp(nowTs, 1)
	if err != nil {
		return 0, true, err
	}
	intVal, err := dec.ToInt()
	terror.Log(err)
	return intVal, false, nil
}

type builtinUnixTimestampIntSig struct {
	BaseBuiltinFunc
}

func (b *builtinUnixTimestampIntSig) Clone() BuiltinFunc {
	newSig := &builtinUnixTimestampIntSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalInt evals a UNIX_TIMESTAMP(time).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_unix-timestamp
func (b *builtinUnixTimestampIntSig) evalInt(row chunk.Row) (int64, bool, error) {
	val, isNull, err := b.Args[0].EvalTime(b.Ctx, row)
	if err != nil && terror.ErrorEqual(types.ErrInvalidTimeFormat.GenWithStackByArgs(val), err) {
		// Return 0 for invalid date time.
		return 0, false, nil
	}
	if isNull {
		return 0, true, nil
	}
	t, err := val.Time.GoTime(getTimeZone(b.Ctx))
	if err != nil {
		return 0, false, nil
	}
	dec, err := goTimeToMysqlUnixTimestamp(t, 1)
	if err != nil {
		return 0, true, err
	}
	intVal, err := dec.ToInt()
	terror.Log(err)
	return intVal, false, nil
}

type builtinUnixTimestampDecSig struct {
	BaseBuiltinFunc
}

func (b *builtinUnixTimestampDecSig) Clone() BuiltinFunc {
	newSig := &builtinUnixTimestampDecSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalDecimal evals a UNIX_TIMESTAMP(time).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_unix-timestamp
func (b *builtinUnixTimestampDecSig) evalDecimal(row chunk.Row) (*types.MyDecimal, bool, error) {
	val, isNull, err := b.Args[0].EvalTime(b.Ctx, row)
	if isNull || err != nil {
		// Return 0 for invalid date time.
		return new(types.MyDecimal), isNull, nil
	}
	t, err := val.Time.GoTime(getTimeZone(b.Ctx))
	if err != nil {
		return new(types.MyDecimal), false, nil
	}
	result, err := goTimeToMysqlUnixTimestamp(t, b.Tp.Decimal)
	return result, err != nil, err
}

type timestampFunctionClass struct {
	BaseFunctionClass
}

func (c *timestampFunctionClass) getDefaultFsp(tp *types.FieldType) int8 {
	if tp.Tp == mysql.TypeDatetime || tp.Tp == mysql.TypeDate || tp.Tp == mysql.TypeDuration ||
		tp.Tp == mysql.TypeTimestamp {
		return int8(tp.Decimal)
	}
	switch cls := tp.EvalType(); cls {
	case types.ETInt:
		return types.MinFsp
	case types.ETReal, types.ETDatetime, types.ETTimestamp, types.ETDuration, types.ETJson, types.ETString:
		return types.MaxFsp
	case types.ETDecimal:
		if tp.Decimal < int(types.MaxFsp) {
			return int8(tp.Decimal)
		}
		return types.MaxFsp
	}
	return types.MaxFsp
}

func (c *timestampFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	evalTps, argLen := []types.EvalType{types.ETString}, len(args)
	if argLen == 2 {
		evalTps = append(evalTps, types.ETString)
	}
	fsp := c.getDefaultFsp(args[0].GetType())
	if argLen == 2 {
		fsp = mathutil.MaxInt8(fsp, c.getDefaultFsp(args[1].GetType()))
	}
	isFloat := false
	switch args[0].GetType().Tp {
	case mysql.TypeFloat, mysql.TypeDouble, mysql.TypeDecimal:
		isFloat = true
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETDatetime, evalTps...)
	bf.Tp.Decimal, bf.Tp.Flen = -1, 19
	if fsp != 0 {
		bf.Tp.Flen += 1 + int(fsp)
	}
	var sig BuiltinFunc
	if argLen == 2 {
		sig = &builtinTimestamp2ArgsSig{bf, isFloat}
	} else {
		sig = &builtinTimestamp1ArgSig{bf, isFloat}
	}
	return sig, nil
}

type builtinTimestamp1ArgSig struct {
	BaseBuiltinFunc

	isFloat bool
}

func (b *builtinTimestamp1ArgSig) Clone() BuiltinFunc {
	newSig := &builtinTimestamp1ArgSig{isFloat: b.isFloat}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalTime evals a builtinTimestamp1ArgSig.
// See https://dev.mysql.com/doc/refman/5.5/en/date-and-time-functions.html#function_timestamp
func (b *builtinTimestamp1ArgSig) evalTime(row chunk.Row) (types.Time, bool, error) {
	s, isNull, err := b.Args[0].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return types.Time{}, isNull, err
	}
	var tm types.Time
	sc := b.Ctx.GetSessionVars().StmtCtx
	if b.isFloat {
		tm, err = types.ParseTimeFromFloatString(sc, s, mysql.TypeDatetime, types.GetFsp(s))
	} else {
		tm, err = types.ParseTime(sc, s, mysql.TypeDatetime, types.GetFsp(s))
	}
	if err != nil {
		return types.Time{}, true, handleInvalidTimeError(b.Ctx, err)
	}
	return tm, false, nil
}

type builtinTimestamp2ArgsSig struct {
	BaseBuiltinFunc

	isFloat bool
}

func (b *builtinTimestamp2ArgsSig) Clone() BuiltinFunc {
	newSig := &builtinTimestamp2ArgsSig{isFloat: b.isFloat}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalTime evals a builtinTimestamp2ArgsSig.
// See https://dev.mysql.com/doc/refman/5.5/en/date-and-time-functions.html#function_timestamp
func (b *builtinTimestamp2ArgsSig) evalTime(row chunk.Row) (types.Time, bool, error) {
	arg0, isNull, err := b.Args[0].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return types.Time{}, isNull, err
	}
	var tm types.Time
	sc := b.Ctx.GetSessionVars().StmtCtx
	if b.isFloat {
		tm, err = types.ParseTimeFromFloatString(sc, arg0, mysql.TypeDatetime, types.GetFsp(arg0))
	} else {
		tm, err = types.ParseTime(sc, arg0, mysql.TypeDatetime, types.GetFsp(arg0))
	}
	if err != nil {
		return types.Time{}, true, handleInvalidTimeError(b.Ctx, err)
	}
	arg1, isNull, err := b.Args[1].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return types.Time{}, isNull, err
	}
	if !isDuration(arg1) {
		return types.Time{}, true, nil
	}
	duration, err := types.ParseDuration(sc, arg1, types.GetFsp(arg1))
	if err != nil {
		return types.Time{}, true, handleInvalidTimeError(b.Ctx, err)
	}
	tmp, err := tm.Add(sc, duration)
	if err != nil {
		return types.Time{}, true, err
	}
	return tmp, false, nil
}

type timestampLiteralFunctionClass struct {
	BaseFunctionClass
}

func (c *timestampLiteralFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	con, ok := args[0].(*Constant)
	if !ok {
		panic("Unexpected parameter for timestamp literal")
	}
	dt, err := con.Eval(chunk.Row{})
	if err != nil {
		return nil, err
	}
	str, err := dt.ToString()
	if err != nil {
		return nil, err
	}
	if !timestampPattern.MatchString(str) {
		return nil, types.ErrIncorrectDatetimeValue.GenWithStackByArgs(str)
	}
	tm, err := types.ParseTime(ctx.GetSessionVars().StmtCtx, str, mysql.TypeTimestamp, types.GetFsp(str))
	if err != nil {
		return nil, err
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, []Expression{}, types.ETDatetime)
	bf.Tp.Flen, bf.Tp.Decimal = mysql.MaxDatetimeWidthNoFsp, int(tm.Fsp)
	if tm.Fsp > 0 {
		bf.Tp.Flen += int(tm.Fsp) + 1
	}
	sig := &builtinTimestampLiteralSig{bf, tm}
	return sig, nil
}

type builtinTimestampLiteralSig struct {
	BaseBuiltinFunc
	tm types.Time
}

func (b *builtinTimestampLiteralSig) Clone() BuiltinFunc {
	newSig := &builtinTimestampLiteralSig{tm: b.tm}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalTime evals TIMESTAMP 'stringLit'.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-literals.html
func (b *builtinTimestampLiteralSig) evalTime(row chunk.Row) (types.Time, bool, error) {
	return b.tm, false, nil
}

// getFsp4TimeAddSub is used to in function 'ADDTIME' and 'SUBTIME' to evaluate `fsp` for the
// second parameter. It's used only if the second parameter is of string type. It's different
// from getFsp in that the result of getFsp4TimeAddSub is either 6 or 0.
func getFsp4TimeAddSub(s string) int8 {
	if len(s)-strings.Index(s, ".")-1 == len(s) {
		return types.MinFsp
	}
	for _, c := range s[strings.Index(s, ".")+1:] {
		if c != '0' {
			return types.MaxFsp
		}
	}
	return types.MinFsp
}

// getBf4TimeAddSub parses input types, generates baseBuiltinFunc and set related attributes for
// builtin function 'ADDTIME' and 'SUBTIME'
func getBf4TimeAddSub(ctx sessionctx.Context, args []Expression) (tp1, tp2 *types.FieldType, bf BaseBuiltinFunc, err error) {
	tp1, tp2 = args[0].GetType(), args[1].GetType()
	var argTp1, argTp2, retTp types.EvalType
	switch tp1.Tp {
	case mysql.TypeDatetime, mysql.TypeTimestamp:
		argTp1, retTp = types.ETDatetime, types.ETDatetime
	case mysql.TypeDuration:
		argTp1, retTp = types.ETDuration, types.ETDuration
	case mysql.TypeDate:
		argTp1, retTp = types.ETDuration, types.ETString
	default:
		argTp1, retTp = types.ETString, types.ETString
	}
	switch tp2.Tp {
	case mysql.TypeDatetime, mysql.TypeDuration:
		argTp2 = types.ETDuration
	default:
		argTp2 = types.ETString
	}
	arg0Dec, err := getExpressionFsp(ctx, args[0])
	if err != nil {
		return
	}
	arg1Dec, err := getExpressionFsp(ctx, args[1])
	if err != nil {
		return
	}

	bf = NewBaseBuiltinFuncWithTp(ctx, args, retTp, argTp1, argTp2)
	bf.Tp.Decimal = mathutil.Min(mathutil.Max(arg0Dec, arg1Dec), int(types.MaxFsp))
	if retTp == types.ETString {
		bf.Tp.Tp, bf.Tp.Flen, bf.Tp.Decimal = mysql.TypeString, mysql.MaxDatetimeWidthWithFsp, types.UnspecifiedLength
	}
	return
}

func getTimeZone(ctx sessionctx.Context) *time.Location {
	ret := ctx.GetSessionVars().TimeZone
	if ret == nil {
		ret = time.Local
	}
	return ret
}

// isDuration returns a boolean indicating whether the str matches the format of duration.
// See https://dev.mysql.com/doc/refman/5.7/en/time.html
func isDuration(str string) bool {
	return durationPattern.MatchString(str)
}

// strDatetimeAddDuration adds duration to datetime string, returns a string value.
func strDatetimeAddDuration(sc *stmtctx.StatementContext, d string, arg1 types.Duration) (string, error) {
	arg0, err := types.ParseTime(sc, d, mysql.TypeDatetime, types.MaxFsp)
	if err != nil {
		return "", err
	}
	ret, err := arg0.Add(sc, arg1)
	if err != nil {
		return "", err
	}
	fsp := types.MaxFsp
	if ret.Time.Microsecond() == 0 {
		fsp = types.MinFsp
	}
	ret.Fsp = fsp
	return ret.String(), nil
}

// strDurationAddDuration adds duration to duration string, returns a string value.
func strDurationAddDuration(sc *stmtctx.StatementContext, d string, arg1 types.Duration) (string, error) {
	arg0, err := types.ParseDuration(sc, d, types.MaxFsp)
	if err != nil {
		return "", err
	}
	tmpDuration, err := arg0.Add(arg1)
	if err != nil {
		return "", err
	}
	tmpDuration.Fsp = types.MaxFsp
	if tmpDuration.MicroSecond() == 0 {
		tmpDuration.Fsp = types.MinFsp
	}
	return tmpDuration.String(), nil
}

// strDatetimeSubDuration subtracts duration from datetime string, returns a string value.
func strDatetimeSubDuration(sc *stmtctx.StatementContext, d string, arg1 types.Duration) (string, error) {
	arg0, err := types.ParseTime(sc, d, mysql.TypeDatetime, types.MaxFsp)
	if err != nil {
		return "", err
	}
	arg1time, err := arg1.ConvertToTime(sc, uint8(types.GetFsp(arg1.String())))
	if err != nil {
		return "", err
	}
	tmpDuration := arg0.Sub(sc, &arg1time)
	fsp := types.MaxFsp
	if tmpDuration.MicroSecond() == 0 {
		fsp = types.MinFsp
	}
	resultDuration, err := tmpDuration.ConvertToTime(sc, mysql.TypeDatetime)
	if err != nil {
		return "", err
	}
	resultDuration.Fsp = fsp
	return resultDuration.String(), nil
}

// strDurationSubDuration subtracts duration from duration string, returns a string value.
func strDurationSubDuration(sc *stmtctx.StatementContext, d string, arg1 types.Duration) (string, error) {
	arg0, err := types.ParseDuration(sc, d, types.MaxFsp)
	if err != nil {
		return "", err
	}
	tmpDuration, err := arg0.Sub(arg1)
	if err != nil {
		return "", err
	}
	tmpDuration.Fsp = types.MaxFsp
	if tmpDuration.MicroSecond() == 0 {
		tmpDuration.Fsp = types.MinFsp
	}
	return tmpDuration.String(), nil
}

type addTimeFunctionClass struct {
	BaseFunctionClass
}

func (c *addTimeFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (sig BuiltinFunc, err error) {
	if err = c.VerifyArgs(args); err != nil {
		return nil, err
	}
	tp1, tp2, bf, err := getBf4TimeAddSub(ctx, args)
	if err != nil {
		return nil, err
	}
	switch tp1.Tp {
	case mysql.TypeDatetime, mysql.TypeTimestamp:
		switch tp2.Tp {
		case mysql.TypeDuration:
			sig = &builtinAddDatetimeAndDurationSig{bf}
		case mysql.TypeDatetime, mysql.TypeTimestamp:
			sig = &builtinAddTimeDateTimeNullSig{bf}
		default:
			sig = &builtinAddDatetimeAndStringSig{bf}
		}
	case mysql.TypeDate:
		switch tp2.Tp {
		case mysql.TypeDuration:
			sig = &builtinAddDateAndDurationSig{bf}
		case mysql.TypeDatetime, mysql.TypeTimestamp:
			sig = &builtinAddTimeStringNullSig{bf}
		default:
			sig = &builtinAddDateAndStringSig{bf}
		}
	case mysql.TypeDuration:
		switch tp2.Tp {
		case mysql.TypeDuration:
			sig = &builtinAddDurationAndDurationSig{bf}
		case mysql.TypeDatetime, mysql.TypeTimestamp:
			sig = &builtinAddTimeDurationNullSig{bf}
		default:
			sig = &builtinAddDurationAndStringSig{bf}
		}
	default:
		switch tp2.Tp {
		case mysql.TypeDuration:
			sig = &builtinAddStringAndDurationSig{bf}
		case mysql.TypeDatetime, mysql.TypeTimestamp:
			sig = &builtinAddTimeStringNullSig{bf}
		default:
			sig = &builtinAddStringAndStringSig{bf}
		}
	}
	return sig, nil
}

type builtinAddTimeDateTimeNullSig struct {
	BaseBuiltinFunc
}

func (b *builtinAddTimeDateTimeNullSig) Clone() BuiltinFunc {
	newSig := &builtinAddTimeDateTimeNullSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalTime evals a builtinAddTimeDateTimeNullSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_addtime
func (b *builtinAddTimeDateTimeNullSig) evalTime(row chunk.Row) (types.Time, bool, error) {
	return types.ZeroDatetime, true, nil
}

type builtinAddDatetimeAndDurationSig struct {
	BaseBuiltinFunc
}

func (b *builtinAddDatetimeAndDurationSig) Clone() BuiltinFunc {
	newSig := &builtinAddDatetimeAndDurationSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalTime evals a builtinAddDatetimeAndDurationSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_addtime
func (b *builtinAddDatetimeAndDurationSig) evalTime(row chunk.Row) (types.Time, bool, error) {
	arg0, isNull, err := b.Args[0].EvalTime(b.Ctx, row)
	if isNull || err != nil {
		return types.ZeroDatetime, isNull, err
	}
	arg1, isNull, err := b.Args[1].EvalDuration(b.Ctx, row)
	if isNull || err != nil {
		return types.ZeroDatetime, isNull, err
	}
	result, err := arg0.Add(b.Ctx.GetSessionVars().StmtCtx, arg1)
	return result, err != nil, err
}

type builtinAddDatetimeAndStringSig struct {
	BaseBuiltinFunc
}

func (b *builtinAddDatetimeAndStringSig) Clone() BuiltinFunc {
	newSig := &builtinAddDatetimeAndStringSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalTime evals a builtinAddDatetimeAndStringSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_addtime
func (b *builtinAddDatetimeAndStringSig) evalTime(row chunk.Row) (types.Time, bool, error) {
	arg0, isNull, err := b.Args[0].EvalTime(b.Ctx, row)
	if isNull || err != nil {
		return types.ZeroDatetime, isNull, err
	}
	s, isNull, err := b.Args[1].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return types.ZeroDatetime, isNull, err
	}
	if !isDuration(s) {
		return types.ZeroDatetime, true, nil
	}
	sc := b.Ctx.GetSessionVars().StmtCtx
	arg1, err := types.ParseDuration(sc, s, types.GetFsp(s))
	if err != nil {
		if terror.ErrorEqual(err, types.ErrTruncatedWrongVal) {
			sc.AppendWarning(err)
			return types.ZeroDatetime, true, nil
		}
		return types.ZeroDatetime, true, err
	}
	result, err := arg0.Add(sc, arg1)
	return result, err != nil, err
}

type builtinAddTimeDurationNullSig struct {
	BaseBuiltinFunc
}

func (b *builtinAddTimeDurationNullSig) Clone() BuiltinFunc {
	newSig := &builtinAddTimeDurationNullSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalDuration evals a builtinAddTimeDurationNullSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_addtime
func (b *builtinAddTimeDurationNullSig) evalDuration(row chunk.Row) (types.Duration, bool, error) {
	return types.ZeroDuration, true, nil
}

type builtinAddDurationAndDurationSig struct {
	BaseBuiltinFunc
}

func (b *builtinAddDurationAndDurationSig) Clone() BuiltinFunc {
	newSig := &builtinAddDurationAndDurationSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalDuration evals a builtinAddDurationAndDurationSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_addtime
func (b *builtinAddDurationAndDurationSig) evalDuration(row chunk.Row) (types.Duration, bool, error) {
	arg0, isNull, err := b.Args[0].EvalDuration(b.Ctx, row)
	if isNull || err != nil {
		return types.ZeroDuration, isNull, err
	}
	arg1, isNull, err := b.Args[1].EvalDuration(b.Ctx, row)
	if isNull || err != nil {
		return types.ZeroDuration, isNull, err
	}
	result, err := arg0.Add(arg1)
	if err != nil {
		return types.ZeroDuration, true, err
	}
	return result, false, nil
}

type builtinAddDurationAndStringSig struct {
	BaseBuiltinFunc
}

func (b *builtinAddDurationAndStringSig) Clone() BuiltinFunc {
	newSig := &builtinAddDurationAndStringSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalDuration evals a builtinAddDurationAndStringSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_addtime
func (b *builtinAddDurationAndStringSig) evalDuration(row chunk.Row) (types.Duration, bool, error) {
	arg0, isNull, err := b.Args[0].EvalDuration(b.Ctx, row)
	if isNull || err != nil {
		return types.ZeroDuration, isNull, err
	}
	s, isNull, err := b.Args[1].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return types.ZeroDuration, isNull, err
	}
	if !isDuration(s) {
		return types.ZeroDuration, true, nil
	}
	sc := b.Ctx.GetSessionVars().StmtCtx
	arg1, err := types.ParseDuration(sc, s, types.GetFsp(s))
	if err != nil {
		if terror.ErrorEqual(err, types.ErrTruncatedWrongVal) {
			sc.AppendWarning(err)
			return types.ZeroDuration, true, nil
		}
		return types.ZeroDuration, true, err
	}
	result, err := arg0.Add(arg1)
	if err != nil {
		return types.ZeroDuration, true, err
	}
	return result, false, nil
}

type builtinAddTimeStringNullSig struct {
	BaseBuiltinFunc
}

func (b *builtinAddTimeStringNullSig) Clone() BuiltinFunc {
	newSig := &builtinAddTimeStringNullSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalString evals a builtinAddDurationAndDurationSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_addtime
func (b *builtinAddTimeStringNullSig) EvalString(row chunk.Row) (string, bool, error) {
	return "", true, nil
}

type builtinAddStringAndDurationSig struct {
	BaseBuiltinFunc
}

func (b *builtinAddStringAndDurationSig) Clone() BuiltinFunc {
	newSig := &builtinAddStringAndDurationSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalString evals a builtinAddStringAndDurationSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_addtime
func (b *builtinAddStringAndDurationSig) EvalString(row chunk.Row) (result string, isNull bool, err error) {
	var (
		arg0 string
		arg1 types.Duration
	)
	arg0, isNull, err = b.Args[0].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	arg1, isNull, err = b.Args[1].EvalDuration(b.Ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	sc := b.Ctx.GetSessionVars().StmtCtx
	if isDuration(arg0) {
		result, err = strDurationAddDuration(sc, arg0, arg1)
		if err != nil {
			if terror.ErrorEqual(err, types.ErrTruncatedWrongVal) {
				sc.AppendWarning(err)
				return "", true, nil
			}
			return "", true, err
		}
		return result, false, nil
	}
	result, err = strDatetimeAddDuration(sc, arg0, arg1)
	return result, err != nil, err
}

type builtinAddStringAndStringSig struct {
	BaseBuiltinFunc
}

func (b *builtinAddStringAndStringSig) Clone() BuiltinFunc {
	newSig := &builtinAddStringAndStringSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalString evals a builtinAddStringAndStringSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_addtime
func (b *builtinAddStringAndStringSig) EvalString(row chunk.Row) (result string, isNull bool, err error) {
	var (
		arg0, arg1Str string
		arg1          types.Duration
	)
	arg0, isNull, err = b.Args[0].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	arg1Type := b.Args[1].GetType()
	if mysql.HasBinaryFlag(arg1Type.Flag) {
		return "", true, nil
	}
	arg1Str, isNull, err = b.Args[1].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	sc := b.Ctx.GetSessionVars().StmtCtx
	arg1, err = types.ParseDuration(sc, arg1Str, getFsp4TimeAddSub(arg1Str))
	if err != nil {
		if terror.ErrorEqual(err, types.ErrTruncatedWrongVal) {
			sc.AppendWarning(err)
			return "", true, nil
		}
		return "", true, err
	}
	if isDuration(arg0) {
		result, err = strDurationAddDuration(sc, arg0, arg1)
		if err != nil {
			if terror.ErrorEqual(err, types.ErrTruncatedWrongVal) {
				sc.AppendWarning(err)
				return "", true, nil
			}
			return "", true, err
		}
		return result, false, nil
	}
	result, err = strDatetimeAddDuration(sc, arg0, arg1)
	return result, err != nil, err
}

type builtinAddDateAndDurationSig struct {
	BaseBuiltinFunc
}

func (b *builtinAddDateAndDurationSig) Clone() BuiltinFunc {
	newSig := &builtinAddDateAndDurationSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalString evals a builtinAddDurationAndDurationSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_addtime
func (b *builtinAddDateAndDurationSig) EvalString(row chunk.Row) (string, bool, error) {
	arg0, isNull, err := b.Args[0].EvalDuration(b.Ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	arg1, isNull, err := b.Args[1].EvalDuration(b.Ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	result, err := arg0.Add(arg1)
	return result.String(), err != nil, err
}

type builtinAddDateAndStringSig struct {
	BaseBuiltinFunc
}

func (b *builtinAddDateAndStringSig) Clone() BuiltinFunc {
	newSig := &builtinAddDateAndStringSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalString evals a builtinAddDateAndStringSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_addtime
func (b *builtinAddDateAndStringSig) EvalString(row chunk.Row) (string, bool, error) {
	arg0, isNull, err := b.Args[0].EvalDuration(b.Ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	s, isNull, err := b.Args[1].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	if !isDuration(s) {
		return "", true, nil
	}
	sc := b.Ctx.GetSessionVars().StmtCtx
	arg1, err := types.ParseDuration(sc, s, getFsp4TimeAddSub(s))
	if err != nil {
		if terror.ErrorEqual(err, types.ErrTruncatedWrongVal) {
			sc.AppendWarning(err)
			return "", true, nil
		}
		return "", true, err
	}
	result, err := arg0.Add(arg1)
	return result.String(), err != nil, err
}

type convertTzFunctionClass struct {
	BaseFunctionClass
}

func (c *convertTzFunctionClass) getDecimal(ctx sessionctx.Context, arg Expression) int {
	decimal := int(types.MaxFsp)
	if dt, isConstant := arg.(*Constant); isConstant {
		switch arg.GetType().EvalType() {
		case types.ETInt:
			decimal = 0
		case types.ETReal, types.ETDecimal:
			decimal = arg.GetType().Decimal
		case types.ETString:
			str, isNull, err := dt.EvalString(ctx, chunk.Row{})
			if err == nil && !isNull {
				decimal = types.DateFSP(str)
			}
		}
	}
	if decimal > int(types.MaxFsp) {
		return int(types.MaxFsp)
	}
	if decimal < int(types.MinFsp) {
		return int(types.MinFsp)
	}
	return decimal
}

func (c *convertTzFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	// tzRegex holds the regex to check whether a string is a time zone.
	tzRegex, err := regexp.Compile(`(^(\+|-)(0?[0-9]|1[0-2]):[0-5]?\d$)|(^\+13:00$)`)
	if err != nil {
		return nil, err
	}

	decimal := c.getDecimal(ctx, args[0])
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETDatetime, types.ETDatetime, types.ETString, types.ETString)
	bf.Tp.Decimal = decimal
	sig := &builtinConvertTzSig{
		BaseBuiltinFunc: bf,
		timezoneRegex:   tzRegex,
	}
	return sig, nil
}

type builtinConvertTzSig struct {
	BaseBuiltinFunc
	timezoneRegex *regexp.Regexp
}

func (b *builtinConvertTzSig) Clone() BuiltinFunc {
	newSig := &builtinConvertTzSig{timezoneRegex: b.timezoneRegex}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalTime evals CONVERT_TZ(dt,from_tz,to_tz).
// `CONVERT_TZ` function returns NULL if the arguments are invalid.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_convert-tz
func (b *builtinConvertTzSig) evalTime(row chunk.Row) (types.Time, bool, error) {
	dt, isNull, err := b.Args[0].EvalTime(b.Ctx, row)
	if isNull || err != nil {
		return types.Time{}, true, nil
	}

	fromTzStr, isNull, err := b.Args[1].EvalString(b.Ctx, row)
	if isNull || err != nil || fromTzStr == "" {
		return types.Time{}, true, nil
	}

	toTzStr, isNull, err := b.Args[2].EvalString(b.Ctx, row)
	if isNull || err != nil || toTzStr == "" {
		return types.Time{}, true, nil
	}

	fromTzMatched := b.timezoneRegex.MatchString(fromTzStr)
	toTzMatched := b.timezoneRegex.MatchString(toTzStr)

	if !fromTzMatched && !toTzMatched {
		fromTz, err := time.LoadLocation(fromTzStr)
		if err != nil {
			return types.Time{}, true, nil
		}

		toTz, err := time.LoadLocation(toTzStr)
		if err != nil {
			return types.Time{}, true, nil
		}

		t, err := dt.Time.GoTime(fromTz)
		if err != nil {
			return types.Time{}, true, nil
		}

		return types.Time{
			Time: types.FromGoTime(t.In(toTz)),
			Type: mysql.TypeDatetime,
			Fsp:  int8(b.Tp.Decimal),
		}, false, nil
	}
	if fromTzMatched && toTzMatched {
		t, err := dt.Time.GoTime(time.Local)
		if err != nil {
			return types.Time{}, true, nil
		}

		return types.Time{
			Time: types.FromGoTime(t.Add(timeZone2Duration(toTzStr) - timeZone2Duration(fromTzStr))),
			Type: mysql.TypeDatetime,
			Fsp:  int8(b.Tp.Decimal),
		}, false, nil
	}
	return types.Time{}, true, nil
}

type makeDateFunctionClass struct {
	BaseFunctionClass
}

func (c *makeDateFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETDatetime, types.ETInt, types.ETInt)
	tp := bf.Tp
	tp.Tp, tp.Flen, tp.Decimal = mysql.TypeDate, mysql.MaxDateWidth, 0
	sig := &builtinMakeDateSig{bf}
	return sig, nil
}

type builtinMakeDateSig struct {
	BaseBuiltinFunc
}

func (b *builtinMakeDateSig) Clone() BuiltinFunc {
	newSig := &builtinMakeDateSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalTime evaluates a builtinMakeDateSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_makedate
func (b *builtinMakeDateSig) evalTime(row chunk.Row) (d types.Time, isNull bool, err error) {
	args := b.getArgs()
	var year, dayOfYear int64
	year, isNull, err = args[0].EvalInt(b.Ctx, row)
	if isNull || err != nil {
		return d, true, err
	}
	dayOfYear, isNull, err = args[1].EvalInt(b.Ctx, row)
	if isNull || err != nil {
		return d, true, err
	}
	if dayOfYear <= 0 || year < 0 || year > 9999 {
		return d, true, nil
	}
	if year < 70 {
		year += 2000
	} else if year < 100 {
		year += 1900
	}
	startTime := types.Time{
		Time: types.FromDate(int(year), 1, 1, 0, 0, 0, 0),
		Type: mysql.TypeDate,
		Fsp:  0,
	}
	retTimestamp := types.TimestampDiff("DAY", types.ZeroDate, startTime)
	if retTimestamp == 0 {
		return d, true, handleInvalidTimeError(b.Ctx, types.ErrIncorrectDatetimeValue.GenWithStackByArgs(startTime.String()))
	}
	ret := types.TimeFromDays(retTimestamp + dayOfYear - 1)
	if ret.IsZero() || ret.Time.Year() > 9999 {
		return d, true, nil
	}
	return ret, false, nil
}

type makeTimeFunctionClass struct {
	BaseFunctionClass
}

func (c *makeTimeFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	tp, flen, decimal := args[2].GetType().EvalType(), 10, 0
	switch tp {
	case types.ETInt:
	case types.ETReal, types.ETDecimal:
		decimal = args[2].GetType().Decimal
		if decimal > 6 || decimal == types.UnspecifiedLength {
			decimal = 6
		}
		if decimal > 0 {
			flen += 1 + decimal
		}
	default:
		flen, decimal = 17, 6
	}
	arg0Type, arg1Type := args[0].GetType().EvalType(), args[1].GetType().EvalType()
	// For ETString type, arg must be evaluated rounding down to int64
	// For other types, arg is evaluated rounding to int64
	if arg0Type == types.ETString {
		arg0Type = types.ETReal
	} else {
		arg0Type = types.ETInt
	}
	if arg1Type == types.ETString {
		arg1Type = types.ETReal
	} else {
		arg1Type = types.ETInt
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETDuration, arg0Type, arg1Type, types.ETReal)
	bf.Tp.Flen, bf.Tp.Decimal = flen, decimal
	sig := &builtinMakeTimeSig{bf}
	return sig, nil
}

type builtinMakeTimeSig struct {
	BaseBuiltinFunc
}

func (b *builtinMakeTimeSig) Clone() BuiltinFunc {
	newSig := &builtinMakeTimeSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinMakeTimeSig) getIntParam(arg Expression, row chunk.Row) (int64, bool, error) {
	if arg.GetType().EvalType() == types.ETReal {
		fRes, isNull, err := arg.EvalReal(b.Ctx, row)
		return int64(fRes), isNull, handleInvalidTimeError(b.Ctx, err)
	}
	iRes, isNull, err := arg.EvalInt(b.Ctx, row)
	return iRes, isNull, handleInvalidTimeError(b.Ctx, err)
}

// evalDuration evals a builtinMakeTimeIntSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_maketime
func (b *builtinMakeTimeSig) evalDuration(row chunk.Row) (types.Duration, bool, error) {
	dur := types.ZeroDuration
	dur.Fsp = types.MaxFsp
	hour, isNull, err := b.getIntParam(b.Args[0], row)
	if isNull || err != nil {
		return dur, isNull, err
	}
	minute, isNull, err := b.getIntParam(b.Args[1], row)
	if isNull || err != nil {
		return dur, isNull, err
	}
	if minute < 0 || minute >= 60 {
		return dur, true, nil
	}
	second, isNull, err := b.Args[2].EvalReal(b.Ctx, row)
	if isNull || err != nil {
		return dur, isNull, err
	}
	if second < 0 || second >= 60 {
		return dur, true, nil
	}
	var overflow bool
	// MySQL TIME datatype: https://dev.mysql.com/doc/refman/5.7/en/time.html
	// ranges from '-838:59:59.000000' to '838:59:59.000000'
	if hour < 0 && mysql.HasUnsignedFlag(b.Args[0].GetType().Flag) {
		hour = 838
		overflow = true
	}
	if hour < -838 {
		hour = -838
		overflow = true
	} else if hour > 838 {
		hour = 838
		overflow = true
	}
	if hour == -838 || hour == 838 {
		if second > 59 {
			second = 59
		}
	}
	if overflow {
		minute = 59
		second = 59
	}
	fsp := b.Tp.Decimal
	dur, err = types.ParseDuration(b.Ctx.GetSessionVars().StmtCtx, fmt.Sprintf("%02d:%02d:%v", hour, minute, second), int8(fsp))
	if err != nil {
		return dur, true, err
	}
	return dur, false, nil
}

type periodAddFunctionClass struct {
	BaseFunctionClass
}

func (c *periodAddFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}

	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETInt, types.ETInt)
	bf.Tp.Flen = 6
	sig := &builtinPeriodAddSig{bf}
	return sig, nil
}

// validPeriod checks if this period is valid, it comes from MySQL 8.0+.
func validPeriod(p int64) bool {
	return !(p < 0 || p%100 == 0 || p%100 > 12)
}

// period2Month converts a period to months, in which period is represented in the format of YYMM or YYYYMM.
// Note that the period argument is not a date value.
func period2Month(period uint64) uint64 {
	if period == 0 {
		return 0
	}

	year, month := period/100, period%100
	if year < 70 {
		year += 2000
	} else if year < 100 {
		year += 1900
	}

	return year*12 + month - 1
}

// month2Period converts a month to a period.
func month2Period(month uint64) uint64 {
	if month == 0 {
		return 0
	}

	year := month / 12
	if year < 70 {
		year += 2000
	} else if year < 100 {
		year += 1900
	}

	return year*100 + month%12 + 1
}

type builtinPeriodAddSig struct {
	BaseBuiltinFunc
}

func (b *builtinPeriodAddSig) Clone() BuiltinFunc {
	newSig := &builtinPeriodAddSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalInt evals PERIOD_ADD(P,N).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_period-add
func (b *builtinPeriodAddSig) evalInt(row chunk.Row) (int64, bool, error) {
	p, isNull, err := b.Args[0].EvalInt(b.Ctx, row)
	if isNull || err != nil {
		return 0, true, err
	}

	n, isNull, err := b.Args[1].EvalInt(b.Ctx, row)
	if isNull || err != nil {
		return 0, true, err
	}

	// in MySQL, if p is invalid but n is NULL, the result is NULL, so we have to check if n is NULL first.
	if !validPeriod(p) {
		return 0, false, errIncorrectArgs.GenWithStackByArgs("period_add")
	}

	sumMonth := int64(period2Month(uint64(p))) + n
	return int64(month2Period(uint64(sumMonth))), false, nil
}

type periodDiffFunctionClass struct {
	BaseFunctionClass
}

func (c *periodDiffFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETInt, types.ETInt)
	bf.Tp.Flen = 6
	sig := &builtinPeriodDiffSig{bf}
	return sig, nil
}

type builtinPeriodDiffSig struct {
	BaseBuiltinFunc
}

func (b *builtinPeriodDiffSig) Clone() BuiltinFunc {
	newSig := &builtinPeriodDiffSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalInt evals PERIOD_DIFF(P1,P2).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_period-diff
func (b *builtinPeriodDiffSig) evalInt(row chunk.Row) (int64, bool, error) {
	p1, isNull, err := b.Args[0].EvalInt(b.Ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}

	p2, isNull, err := b.Args[1].EvalInt(b.Ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}

	if !validPeriod(p1) {
		return 0, false, errIncorrectArgs.GenWithStackByArgs("period_diff")
	}

	if !validPeriod(p2) {
		return 0, false, errIncorrectArgs.GenWithStackByArgs("period_diff")
	}

	return int64(period2Month(uint64(p1)) - period2Month(uint64(p2))), false, nil
}

type quarterFunctionClass struct {
	BaseFunctionClass
}

func (c *quarterFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}

	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETDatetime)
	bf.Tp.Flen = 1

	sig := &builtinQuarterSig{bf}
	return sig, nil
}

type builtinQuarterSig struct {
	BaseBuiltinFunc
}

func (b *builtinQuarterSig) Clone() BuiltinFunc {
	newSig := &builtinQuarterSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalInt evals QUARTER(date).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_quarter
func (b *builtinQuarterSig) evalInt(row chunk.Row) (int64, bool, error) {
	date, isNull, err := b.Args[0].EvalTime(b.Ctx, row)
	if isNull || err != nil {
		return 0, true, handleInvalidTimeError(b.Ctx, err)
	}

	if date.IsZero() {
		return 0, true, handleInvalidTimeError(b.Ctx, types.ErrIncorrectDatetimeValue.GenWithStackByArgs(date.String()))
	}

	return int64((date.Time.Month() + 2) / 3), false, nil
}

type secToTimeFunctionClass struct {
	BaseFunctionClass
}

func (c *secToTimeFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	var retFlen, retFsp int
	argType := args[0].GetType()
	argEvalTp := argType.EvalType()
	if argEvalTp == types.ETString {
		retFsp = types.UnspecifiedLength
	} else {
		retFsp = argType.Decimal
	}
	if retFsp > int(types.MaxFsp) || retFsp == int(types.UnspecifiedFsp) {
		retFsp = int(types.MaxFsp)
	} else if retFsp < int(types.MinFsp) {
		retFsp = int(types.MinFsp)
	}
	retFlen = 10
	if retFsp > 0 {
		retFlen += 1 + retFsp
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETDuration, types.ETReal)
	bf.Tp.Flen, bf.Tp.Decimal = retFlen, retFsp
	sig := &builtinSecToTimeSig{bf}
	return sig, nil
}

type builtinSecToTimeSig struct {
	BaseBuiltinFunc
}

func (b *builtinSecToTimeSig) Clone() BuiltinFunc {
	newSig := &builtinSecToTimeSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalDuration evals SEC_TO_TIME(seconds).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_sec-to-time
func (b *builtinSecToTimeSig) evalDuration(row chunk.Row) (types.Duration, bool, error) {
	secondsFloat, isNull, err := b.Args[0].EvalReal(b.Ctx, row)
	if isNull || err != nil {
		return types.Duration{}, isNull, err
	}
	var (
		hour          int64
		minute        int64
		second        int64
		demical       float64
		secondDemical float64
		negative      string
	)

	if secondsFloat < 0 {
		negative = "-"
		secondsFloat = math.Abs(secondsFloat)
	}
	seconds := int64(secondsFloat)
	demical = secondsFloat - float64(seconds)

	hour = seconds / 3600
	if hour > 838 {
		hour = 838
		minute = 59
		second = 59
	} else {
		minute = seconds % 3600 / 60
		second = seconds % 60
	}
	secondDemical = float64(second) + demical

	var dur types.Duration
	dur, err = types.ParseDuration(b.Ctx.GetSessionVars().StmtCtx, fmt.Sprintf("%s%02d:%02d:%v", negative, hour, minute, secondDemical), int8(b.Tp.Decimal))
	if err != nil {
		return types.Duration{}, err != nil, err
	}
	return dur, false, nil
}

type subTimeFunctionClass struct {
	BaseFunctionClass
}

func (c *subTimeFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (sig BuiltinFunc, err error) {
	if err = c.VerifyArgs(args); err != nil {
		return nil, err
	}
	tp1, tp2, bf, err := getBf4TimeAddSub(ctx, args)
	if err != nil {
		return nil, err
	}
	switch tp1.Tp {
	case mysql.TypeDatetime, mysql.TypeTimestamp:
		switch tp2.Tp {
		case mysql.TypeDuration:
			sig = &builtinSubDatetimeAndDurationSig{bf}
		case mysql.TypeDatetime, mysql.TypeTimestamp:
			sig = &builtinSubTimeDateTimeNullSig{bf}
		default:
			sig = &builtinSubDatetimeAndStringSig{bf}
		}
	case mysql.TypeDate:
		switch tp2.Tp {
		case mysql.TypeDuration:
			sig = &builtinSubDateAndDurationSig{bf}
		case mysql.TypeDatetime, mysql.TypeTimestamp:
			sig = &builtinSubTimeStringNullSig{bf}
		default:
			sig = &builtinSubDateAndStringSig{bf}
		}
	case mysql.TypeDuration:
		switch tp2.Tp {
		case mysql.TypeDuration:
			sig = &builtinSubDurationAndDurationSig{bf}
		case mysql.TypeDatetime, mysql.TypeTimestamp:
			sig = &builtinSubTimeDurationNullSig{bf}
		default:
			sig = &builtinSubDurationAndStringSig{bf}
		}
	default:
		switch tp2.Tp {
		case mysql.TypeDuration:
			sig = &builtinSubStringAndDurationSig{bf}
		case mysql.TypeDatetime, mysql.TypeTimestamp:
			sig = &builtinSubTimeStringNullSig{bf}
		default:
			sig = &builtinSubStringAndStringSig{bf}
		}
	}
	return sig, nil
}

type builtinSubDatetimeAndDurationSig struct {
	BaseBuiltinFunc
}

func (b *builtinSubDatetimeAndDurationSig) Clone() BuiltinFunc {
	newSig := &builtinSubDatetimeAndDurationSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalTime evals a builtinSubDatetimeAndDurationSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_subtime
func (b *builtinSubDatetimeAndDurationSig) evalTime(row chunk.Row) (types.Time, bool, error) {
	arg0, isNull, err := b.Args[0].EvalTime(b.Ctx, row)
	if isNull || err != nil {
		return types.ZeroDatetime, isNull, err
	}
	arg1, isNull, err := b.Args[1].EvalDuration(b.Ctx, row)
	if isNull || err != nil {
		return types.ZeroDatetime, isNull, err
	}
	sc := b.Ctx.GetSessionVars().StmtCtx
	arg1time, err := arg1.ConvertToTime(sc, mysql.TypeDatetime)
	if err != nil {
		return arg1time, true, err
	}
	tmpDuration := arg0.Sub(sc, &arg1time)
	result, err := tmpDuration.ConvertToTime(sc, arg0.Type)
	return result, err != nil, err
}

type builtinSubDatetimeAndStringSig struct {
	BaseBuiltinFunc
}

func (b *builtinSubDatetimeAndStringSig) Clone() BuiltinFunc {
	newSig := &builtinSubDatetimeAndStringSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalTime evals a builtinSubDatetimeAndStringSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_subtime
func (b *builtinSubDatetimeAndStringSig) evalTime(row chunk.Row) (types.Time, bool, error) {
	arg0, isNull, err := b.Args[0].EvalTime(b.Ctx, row)
	if isNull || err != nil {
		return types.ZeroDatetime, isNull, err
	}
	s, isNull, err := b.Args[1].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return types.ZeroDatetime, isNull, err
	}
	if err != nil {
		return types.ZeroDatetime, true, err
	}
	if !isDuration(s) {
		return types.ZeroDatetime, true, nil
	}
	sc := b.Ctx.GetSessionVars().StmtCtx
	arg1, err := types.ParseDuration(sc, s, types.GetFsp(s))
	if err != nil {
		if terror.ErrorEqual(err, types.ErrTruncatedWrongVal) {
			sc.AppendWarning(err)
			return types.ZeroDatetime, true, nil
		}
		return types.ZeroDatetime, true, err
	}
	arg1time, err := arg1.ConvertToTime(sc, mysql.TypeDatetime)
	if err != nil {
		return types.ZeroDatetime, true, err
	}
	tmpDuration := arg0.Sub(sc, &arg1time)
	result, err := tmpDuration.ConvertToTime(sc, mysql.TypeDatetime)
	return result, err != nil, err
}

type builtinSubTimeDateTimeNullSig struct {
	BaseBuiltinFunc
}

func (b *builtinSubTimeDateTimeNullSig) Clone() BuiltinFunc {
	newSig := &builtinSubTimeDateTimeNullSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalTime evals a builtinSubTimeDateTimeNullSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_subtime
func (b *builtinSubTimeDateTimeNullSig) evalTime(row chunk.Row) (types.Time, bool, error) {
	return types.ZeroDatetime, true, nil
}

type builtinSubStringAndDurationSig struct {
	BaseBuiltinFunc
}

func (b *builtinSubStringAndDurationSig) Clone() BuiltinFunc {
	newSig := &builtinSubStringAndDurationSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalString evals a builtinSubStringAndDurationSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_subtime
func (b *builtinSubStringAndDurationSig) EvalString(row chunk.Row) (result string, isNull bool, err error) {
	var (
		arg0 string
		arg1 types.Duration
	)
	arg0, isNull, err = b.Args[0].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	arg1, isNull, err = b.Args[1].EvalDuration(b.Ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	sc := b.Ctx.GetSessionVars().StmtCtx
	if isDuration(arg0) {
		result, err = strDurationSubDuration(sc, arg0, arg1)
		if err != nil {
			if terror.ErrorEqual(err, types.ErrTruncatedWrongVal) {
				sc.AppendWarning(err)
				return "", true, nil
			}
			return "", true, err
		}
		return result, false, nil
	}
	result, err = strDatetimeSubDuration(sc, arg0, arg1)
	return result, err != nil, err
}

type builtinSubStringAndStringSig struct {
	BaseBuiltinFunc
}

func (b *builtinSubStringAndStringSig) Clone() BuiltinFunc {
	newSig := &builtinSubStringAndStringSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalString evals a builtinSubStringAndStringSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_subtime
func (b *builtinSubStringAndStringSig) EvalString(row chunk.Row) (result string, isNull bool, err error) {
	var (
		s, arg0 string
		arg1    types.Duration
	)
	arg0, isNull, err = b.Args[0].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	arg1Type := b.Args[1].GetType()
	if mysql.HasBinaryFlag(arg1Type.Flag) {
		return "", true, nil
	}
	s, isNull, err = b.Args[1].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	sc := b.Ctx.GetSessionVars().StmtCtx
	arg1, err = types.ParseDuration(sc, s, getFsp4TimeAddSub(s))
	if err != nil {
		if terror.ErrorEqual(err, types.ErrTruncatedWrongVal) {
			sc.AppendWarning(err)
			return "", true, nil
		}
		return "", true, err
	}
	if isDuration(arg0) {
		result, err = strDurationSubDuration(sc, arg0, arg1)
		if err != nil {
			if terror.ErrorEqual(err, types.ErrTruncatedWrongVal) {
				sc.AppendWarning(err)
				return "", true, nil
			}
			return "", true, err
		}
		return result, false, nil
	}
	result, err = strDatetimeSubDuration(sc, arg0, arg1)
	return result, err != nil, err
}

type builtinSubTimeStringNullSig struct {
	BaseBuiltinFunc
}

func (b *builtinSubTimeStringNullSig) Clone() BuiltinFunc {
	newSig := &builtinSubTimeStringNullSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalString evals a builtinSubTimeStringNullSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_subtime
func (b *builtinSubTimeStringNullSig) EvalString(row chunk.Row) (string, bool, error) {
	return "", true, nil
}

type builtinSubDurationAndDurationSig struct {
	BaseBuiltinFunc
}

func (b *builtinSubDurationAndDurationSig) Clone() BuiltinFunc {
	newSig := &builtinSubDurationAndDurationSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalDuration evals a builtinSubDurationAndDurationSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_subtime
func (b *builtinSubDurationAndDurationSig) evalDuration(row chunk.Row) (types.Duration, bool, error) {
	arg0, isNull, err := b.Args[0].EvalDuration(b.Ctx, row)
	if isNull || err != nil {
		return types.ZeroDuration, isNull, err
	}
	arg1, isNull, err := b.Args[1].EvalDuration(b.Ctx, row)
	if isNull || err != nil {
		return types.ZeroDuration, isNull, err
	}
	result, err := arg0.Sub(arg1)
	if err != nil {
		return types.ZeroDuration, true, err
	}
	return result, false, nil
}

type builtinSubDurationAndStringSig struct {
	BaseBuiltinFunc
}

func (b *builtinSubDurationAndStringSig) Clone() BuiltinFunc {
	newSig := &builtinSubDurationAndStringSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalDuration evals a builtinSubDurationAndStringSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_subtime
func (b *builtinSubDurationAndStringSig) evalDuration(row chunk.Row) (types.Duration, bool, error) {
	arg0, isNull, err := b.Args[0].EvalDuration(b.Ctx, row)
	if isNull || err != nil {
		return types.ZeroDuration, isNull, err
	}
	s, isNull, err := b.Args[1].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return types.ZeroDuration, isNull, err
	}
	if !isDuration(s) {
		return types.ZeroDuration, true, nil
	}
	sc := b.Ctx.GetSessionVars().StmtCtx
	arg1, err := types.ParseDuration(sc, s, types.GetFsp(s))
	if err != nil {
		if terror.ErrorEqual(err, types.ErrTruncatedWrongVal) {
			sc.AppendWarning(err)
			return types.ZeroDuration, true, nil
		}
		return types.ZeroDuration, true, err
	}
	result, err := arg0.Sub(arg1)
	return result, err != nil, err
}

type builtinSubTimeDurationNullSig struct {
	BaseBuiltinFunc
}

func (b *builtinSubTimeDurationNullSig) Clone() BuiltinFunc {
	newSig := &builtinSubTimeDurationNullSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalDuration evals a builtinSubTimeDurationNullSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_subtime
func (b *builtinSubTimeDurationNullSig) evalDuration(row chunk.Row) (types.Duration, bool, error) {
	return types.ZeroDuration, true, nil
}

type builtinSubDateAndDurationSig struct {
	BaseBuiltinFunc
}

func (b *builtinSubDateAndDurationSig) Clone() BuiltinFunc {
	newSig := &builtinSubDateAndDurationSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalString evals a builtinSubDateAndDurationSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_subtime
func (b *builtinSubDateAndDurationSig) EvalString(row chunk.Row) (string, bool, error) {
	arg0, isNull, err := b.Args[0].EvalDuration(b.Ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	arg1, isNull, err := b.Args[1].EvalDuration(b.Ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	result, err := arg0.Sub(arg1)
	return result.String(), err != nil, err
}

type builtinSubDateAndStringSig struct {
	BaseBuiltinFunc
}

func (b *builtinSubDateAndStringSig) Clone() BuiltinFunc {
	newSig := &builtinSubDateAndStringSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalString evals a builtinSubDateAndStringSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_subtime
func (b *builtinSubDateAndStringSig) EvalString(row chunk.Row) (string, bool, error) {
	arg0, isNull, err := b.Args[0].EvalDuration(b.Ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	s, isNull, err := b.Args[1].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	if !isDuration(s) {
		return "", true, nil
	}
	sc := b.Ctx.GetSessionVars().StmtCtx
	arg1, err := types.ParseDuration(sc, s, getFsp4TimeAddSub(s))
	if err != nil {
		if terror.ErrorEqual(err, types.ErrTruncatedWrongVal) {
			sc.AppendWarning(err)
			return "", true, nil
		}
		return "", true, err
	}
	result, err := arg0.Sub(arg1)
	if err != nil {
		return "", true, err
	}
	return result.String(), false, nil
}

type timeFormatFunctionClass struct {
	BaseFunctionClass
}

func (c *timeFormatFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETString, types.ETDuration, types.ETString)
	// worst case: formatMask=%r%r%r...%r, each %r takes 11 characters
	bf.Tp.Flen = (args[1].GetType().Flen + 1) / 2 * 11
	sig := &builtinTimeFormatSig{bf}
	return sig, nil
}

type builtinTimeFormatSig struct {
	BaseBuiltinFunc
}

func (b *builtinTimeFormatSig) Clone() BuiltinFunc {
	newSig := &builtinTimeFormatSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalString evals a builtinTimeFormatSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_time-format
func (b *builtinTimeFormatSig) EvalString(row chunk.Row) (string, bool, error) {
	dur, isNull, err := b.Args[0].EvalDuration(b.Ctx, row)
	// if err != nil, then dur is ZeroDuration, outputs 00:00:00 in this case which follows the behavior of mysql.
	if err != nil {
		logutil.BgLogger().Warn("time_format.args[0].EvalDuration failed", zap.Error(err))
	}
	if isNull {
		return "", isNull, err
	}
	formatMask, isNull, err := b.Args[1].EvalString(b.Ctx, row)
	if err != nil || isNull {
		return "", isNull, err
	}
	res, err := b.formatTime(b.Ctx, dur, formatMask)
	return res, isNull, err
}

// formatTime see https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_time-format
func (b *builtinTimeFormatSig) formatTime(ctx sessionctx.Context, t types.Duration, formatMask string) (res string, err error) {
	t2 := types.Time{
		Time: types.FromDate(0, 0, 0, t.Hour(), t.Minute(), t.Second(), t.MicroSecond()),
		Type: mysql.TypeDate, Fsp: 0}

	str, err := t2.DateFormat(formatMask)
	return str, err
}

type timeToSecFunctionClass struct {
	BaseFunctionClass
}

func (c *timeToSecFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETDuration)
	bf.Tp.Flen = 10
	sig := &builtinTimeToSecSig{bf}
	return sig, nil
}

type builtinTimeToSecSig struct {
	BaseBuiltinFunc
}

func (b *builtinTimeToSecSig) Clone() BuiltinFunc {
	newSig := &builtinTimeToSecSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalInt evals TIME_TO_SEC(time).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_time-to-sec
func (b *builtinTimeToSecSig) evalInt(row chunk.Row) (int64, bool, error) {
	duration, isNull, err := b.Args[0].EvalDuration(b.Ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	var sign int
	if duration.Duration >= 0 {
		sign = 1
	} else {
		sign = -1
	}
	return int64(sign * (duration.Hour()*3600 + duration.Minute()*60 + duration.Second())), false, nil
}

type timestampAddFunctionClass struct {
	BaseFunctionClass
}

func (c *timestampAddFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETString, types.ETString, types.ETInt, types.ETDatetime)
	bf.Tp = &types.FieldType{Tp: mysql.TypeString, Flen: mysql.MaxDatetimeWidthNoFsp, Decimal: types.UnspecifiedLength}
	sig := &builtinTimestampAddSig{bf}
	return sig, nil

}

type builtinTimestampAddSig struct {
	BaseBuiltinFunc
}

func (b *builtinTimestampAddSig) Clone() BuiltinFunc {
	newSig := &builtinTimestampAddSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalString evals a builtinTimestampAddSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_timestampadd
func (b *builtinTimestampAddSig) EvalString(row chunk.Row) (string, bool, error) {
	unit, isNull, err := b.Args[0].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	v, isNull, err := b.Args[1].EvalInt(b.Ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	arg, isNull, err := b.Args[2].EvalTime(b.Ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	tm1, err := arg.Time.GoTime(time.Local)
	if err != nil {
		return "", isNull, err
	}
	var tb time.Time
	fsp := types.DefaultFsp
	switch unit {
	case "MICROSECOND":
		tb = tm1.Add(time.Duration(v) * time.Microsecond)
		fsp = types.MaxFsp
	case "SECOND":
		tb = tm1.Add(time.Duration(v) * time.Second)
	case "MINUTE":
		tb = tm1.Add(time.Duration(v) * time.Minute)
	case "HOUR":
		tb = tm1.Add(time.Duration(v) * time.Hour)
	case "DAY":
		tb = tm1.AddDate(0, 0, int(v))
	case "WEEK":
		tb = tm1.AddDate(0, 0, 7*int(v))
	case "MONTH":
		tb = tm1.AddDate(0, int(v), 0)
	case "QUARTER":
		tb = tm1.AddDate(0, 3*int(v), 0)
	case "YEAR":
		tb = tm1.AddDate(int(v), 0, 0)
	default:
		return "", true, types.ErrInvalidTimeFormat.GenWithStackByArgs(unit)
	}
	r := types.Time{Time: types.FromGoTime(tb), Type: b.resolveType(arg.Type, unit), Fsp: fsp}
	if err = r.Check(b.Ctx.GetSessionVars().StmtCtx); err != nil {
		return "", true, handleInvalidTimeError(b.Ctx, err)
	}
	return r.String(), false, nil
}

func (b *builtinTimestampAddSig) resolveType(typ uint8, unit string) uint8 {
	// The approach below is from MySQL.
	// The field type for the result of an Item_date function is defined as
	// follows:
	//
	//- If first arg is a MYSQL_TYPE_DATETIME result is MYSQL_TYPE_DATETIME
	//- If first arg is a MYSQL_TYPE_DATE and the interval type uses hours,
	//	minutes, seconds or microsecond then type is MYSQL_TYPE_DATETIME.
	//- Otherwise the result is MYSQL_TYPE_STRING
	//	(This is because you can't know if the string contains a DATE, MYSQL_TIME
	//	or DATETIME argument)
	if typ == mysql.TypeDate && (unit == "HOUR" || unit == "MINUTE" || unit == "SECOND" || unit == "MICROSECOND") {
		return mysql.TypeDatetime
	}
	return typ
}

type toDaysFunctionClass struct {
	BaseFunctionClass
}

func (c *toDaysFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETDatetime)
	sig := &builtinToDaysSig{bf}
	return sig, nil
}

type builtinToDaysSig struct {
	BaseBuiltinFunc
}

func (b *builtinToDaysSig) Clone() BuiltinFunc {
	newSig := &builtinToDaysSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalInt evals a builtinToDaysSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_to-days
func (b *builtinToDaysSig) evalInt(row chunk.Row) (int64, bool, error) {
	arg, isNull, err := b.Args[0].EvalTime(b.Ctx, row)

	if isNull || err != nil {
		return 0, true, handleInvalidTimeError(b.Ctx, err)
	}
	ret := types.TimestampDiff("DAY", types.ZeroDate, arg)
	if ret == 0 {
		return 0, true, handleInvalidTimeError(b.Ctx, types.ErrIncorrectDatetimeValue.GenWithStackByArgs(arg.String()))
	}
	return ret, false, nil
}

type toSecondsFunctionClass struct {
	BaseFunctionClass
}

func (c *toSecondsFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETDatetime)
	sig := &builtinToSecondsSig{bf}
	return sig, nil
}

type builtinToSecondsSig struct {
	BaseBuiltinFunc
}

func (b *builtinToSecondsSig) Clone() BuiltinFunc {
	newSig := &builtinToSecondsSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalInt evals a builtinToSecondsSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_to-seconds
func (b *builtinToSecondsSig) evalInt(row chunk.Row) (int64, bool, error) {
	arg, isNull, err := b.Args[0].EvalTime(b.Ctx, row)
	if isNull || err != nil {
		return 0, true, handleInvalidTimeError(b.Ctx, err)
	}
	ret := types.TimestampDiff("SECOND", types.ZeroDate, arg)
	if ret == 0 {
		return 0, true, handleInvalidTimeError(b.Ctx, types.ErrIncorrectDatetimeValue.GenWithStackByArgs(arg.String()))
	}
	return ret, false, nil
}

type utcTimeFunctionClass struct {
	BaseFunctionClass
}

func (c *utcTimeFunctionClass) getFlenAndDecimal4UTCTime(ctx sessionctx.Context, args []Expression) (flen, decimal int) {
	if len(args) == 0 {
		flen, decimal = 8, 0
		return
	}
	if constant, ok := args[0].(*Constant); ok {
		fsp, isNull, err := constant.EvalInt(ctx, chunk.Row{})
		if isNull || err != nil || fsp > int64(types.MaxFsp) {
			decimal = int(types.MaxFsp)
		} else if fsp < int64(types.MinFsp) {
			decimal = int(types.MinFsp)
		} else {
			decimal = int(fsp)
		}
	}
	if decimal > 0 {
		flen = 8 + 1 + decimal
	} else {
		flen = 8
	}
	return flen, decimal
}

func (c *utcTimeFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	argTps := make([]types.EvalType, 0, 1)
	if len(args) == 1 {
		argTps = append(argTps, types.ETInt)
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETDuration, argTps...)
	bf.Tp.Flen, bf.Tp.Decimal = c.getFlenAndDecimal4UTCTime(bf.Ctx, args)

	var sig BuiltinFunc
	if len(args) == 1 {
		sig = &builtinUTCTimeWithArgSig{bf}
	} else {
		sig = &builtinUTCTimeWithoutArgSig{bf}
	}
	return sig, nil
}

type builtinUTCTimeWithoutArgSig struct {
	BaseBuiltinFunc
}

func (b *builtinUTCTimeWithoutArgSig) Clone() BuiltinFunc {
	newSig := &builtinUTCTimeWithoutArgSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalDuration evals a builtinUTCTimeWithoutArgSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_utc-time
func (b *builtinUTCTimeWithoutArgSig) evalDuration(row chunk.Row) (types.Duration, bool, error) {
	nowTs, err := getStmtTimestamp(b.Ctx)
	if err != nil {
		return types.Duration{}, true, err
	}
	v, err := types.ParseDuration(b.Ctx.GetSessionVars().StmtCtx, nowTs.UTC().Format(types.TimeFormat), 0)
	return v, false, err
}

type builtinUTCTimeWithArgSig struct {
	BaseBuiltinFunc
}

func (b *builtinUTCTimeWithArgSig) Clone() BuiltinFunc {
	newSig := &builtinUTCTimeWithArgSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalDuration evals a builtinUTCTimeWithArgSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_utc-time
func (b *builtinUTCTimeWithArgSig) evalDuration(row chunk.Row) (types.Duration, bool, error) {
	fsp, isNull, err := b.Args[0].EvalInt(b.Ctx, row)
	if isNull || err != nil {
		return types.Duration{}, isNull, err
	}
	if fsp > int64(types.MaxFsp) {
		return types.Duration{}, true, errors.Errorf("Too-big precision %v specified for 'utc_time'. Maximum is %v.", fsp, types.MaxFsp)
	}
	if fsp < int64(types.MinFsp) {
		return types.Duration{}, true, errors.Errorf("Invalid negative %d specified, must in [0, 6].", fsp)
	}
	nowTs, err := getStmtTimestamp(b.Ctx)
	if err != nil {
		return types.Duration{}, true, err
	}
	v, err := types.ParseDuration(b.Ctx.GetSessionVars().StmtCtx, nowTs.UTC().Format(types.TimeFSPFormat), int8(fsp))
	return v, false, err
}

type lastDayFunctionClass struct {
	BaseFunctionClass
}

func (c *lastDayFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETDatetime, types.ETDatetime)
	bf.Tp.Tp, bf.Tp.Flen, bf.Tp.Decimal = mysql.TypeDate, mysql.MaxDateWidth, int(types.DefaultFsp)
	sig := &builtinLastDaySig{bf}
	return sig, nil
}

type builtinLastDaySig struct {
	BaseBuiltinFunc
}

func (b *builtinLastDaySig) Clone() BuiltinFunc {
	newSig := &builtinLastDaySig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalTime evals a builtinLastDaySig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_last-day
func (b *builtinLastDaySig) evalTime(row chunk.Row) (types.Time, bool, error) {
	arg, isNull, err := b.Args[0].EvalTime(b.Ctx, row)
	if isNull || err != nil {
		return types.Time{}, true, handleInvalidTimeError(b.Ctx, err)
	}
	tm := arg.Time
	year, month, day := tm.Year(), tm.Month(), tm.Day()
	if month == 0 || day == 0 {
		return types.Time{}, true, handleInvalidTimeError(b.Ctx, types.ErrIncorrectDatetimeValue.GenWithStackByArgs(arg.String()))
	}
	lastDay := types.GetLastDay(year, month)
	ret := types.Time{
		Time: types.FromDate(year, month, lastDay, 0, 0, 0, 0),
		Type: mysql.TypeDate,
		Fsp:  types.DefaultFsp,
	}
	return ret, false, nil
}

// getExpressionFsp calculates the fsp from given expression.
func getExpressionFsp(ctx sessionctx.Context, expression Expression) (int, error) {
	constExp, isConstant := expression.(*Constant)
	if isConstant && types.IsString(expression.GetType().Tp) && !isTemporalColumn(expression) {
		str, isNil, err := constExp.EvalString(ctx, chunk.Row{})
		if isNil || err != nil {
			return 0, err
		}
		return int(types.GetFsp(str)), nil
	}
	return mathutil.Min(expression.GetType().Decimal, int(types.MaxFsp)), nil
}

// tidbParseTsoFunctionClass extracts physical time from a tso
type tidbParseTsoFunctionClass struct {
	BaseFunctionClass
}

func (c *tidbParseTsoFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	argTp := args[0].GetType().EvalType()
	bf := NewBaseBuiltinFuncWithTp(ctx, args, argTp, types.ETInt)

	bf.Tp.Tp, bf.Tp.Flen, bf.Tp.Decimal = mysql.TypeDate, mysql.MaxDateWidth, int(types.DefaultFsp)
	sig := &builtinTidbParseTsoSig{bf}
	return sig, nil
}

type builtinTidbParseTsoSig struct {
	BaseBuiltinFunc
}

func (b *builtinTidbParseTsoSig) Clone() BuiltinFunc {
	newSig := &builtinTidbParseTsoSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalTime evals a builtinTidbParseTsoSig.
func (b *builtinTidbParseTsoSig) evalTime(row chunk.Row) (types.Time, bool, error) {
	arg, isNull, err := b.Args[0].EvalInt(b.Ctx, row)
	if isNull || err != nil || arg <= 0 {
		return types.Time{}, true, handleInvalidTimeError(b.Ctx, err)
	}

	t := oracle.GetTimeFromTS(uint64(arg))
	result := types.Time{
		Time: types.FromGoTime(t),
		Type: mysql.TypeDatetime,
		Fsp:  types.MaxFsp,
	}
	err = result.ConvertTimeZone(time.Local, b.Ctx.GetSessionVars().Location())
	if err != nil {
		return types.Time{}, true, err
	}
	return result, false, nil
}
