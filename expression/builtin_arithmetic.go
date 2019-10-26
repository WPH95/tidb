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
	"fmt"
	"math"

	"github.com/cznic/mathutil"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tipb/go-tipb"
)

var (
	_ FunctionClass = &arithmeticPlusFunctionClass{}
	_ FunctionClass = &arithmeticMinusFunctionClass{}
	_ FunctionClass = &arithmeticDivideFunctionClass{}
	_ FunctionClass = &arithmeticMultiplyFunctionClass{}
	_ FunctionClass = &arithmeticIntDivideFunctionClass{}
	_ FunctionClass = &arithmeticModFunctionClass{}
)

var (
	_ BuiltinFunc = &builtinArithmeticPlusRealSig{}
	_ BuiltinFunc = &builtinArithmeticPlusDecimalSig{}
	_ BuiltinFunc = &builtinArithmeticPlusIntSig{}
	_ BuiltinFunc = &builtinArithmeticMinusRealSig{}
	_ BuiltinFunc = &builtinArithmeticMinusDecimalSig{}
	_ BuiltinFunc = &builtinArithmeticMinusIntSig{}
	_ BuiltinFunc = &builtinArithmeticDivideRealSig{}
	_ BuiltinFunc = &builtinArithmeticDivideDecimalSig{}
	_ BuiltinFunc = &builtinArithmeticMultiplyRealSig{}
	_ BuiltinFunc = &builtinArithmeticMultiplyDecimalSig{}
	_ BuiltinFunc = &builtinArithmeticMultiplyIntUnsignedSig{}
	_ BuiltinFunc = &builtinArithmeticMultiplyIntSig{}
	_ BuiltinFunc = &builtinArithmeticIntDivideIntSig{}
	_ BuiltinFunc = &builtinArithmeticIntDivideDecimalSig{}
	_ BuiltinFunc = &builtinArithmeticModIntSig{}
	_ BuiltinFunc = &builtinArithmeticModRealSig{}
	_ BuiltinFunc = &builtinArithmeticModDecimalSig{}
)

// precIncrement indicates the number of digits by which to increase the scale of the result of division operations
// performed with the / operator.
const precIncrement = 4

// numericContextResultType returns types.EvalType for numeric function's parameters.
// the returned types.EvalType should be one of: types.ETInt, types.ETDecimal, types.ETReal
func numericContextResultType(ft *types.FieldType) types.EvalType {
	if types.IsTypeTemporal(ft.Tp) {
		if ft.Decimal > 0 {
			return types.ETDecimal
		}
		return types.ETInt
	}
	if types.IsBinaryStr(ft) {
		return types.ETInt
	}
	evalTp4Ft := types.ETReal
	if !ft.Hybrid() {
		evalTp4Ft = ft.EvalType()
		if evalTp4Ft != types.ETDecimal && evalTp4Ft != types.ETInt {
			evalTp4Ft = types.ETReal
		}
	}
	return evalTp4Ft
}

// setFlenDecimal4Int is called to set proper `Flen` and `Decimal` of return
// type according to the two input parameter's types.
func setFlenDecimal4Int(retTp, a, b *types.FieldType) {
	retTp.Decimal = 0
	retTp.Flen = mysql.MaxIntWidth
}

// setFlenDecimal4RealOrDecimal is called to set proper `Flen` and `Decimal` of return
// type according to the two input parameter's types.
func setFlenDecimal4RealOrDecimal(retTp, a, b *types.FieldType, isReal bool, isMultiply bool) {
	if a.Decimal != types.UnspecifiedLength && b.Decimal != types.UnspecifiedLength {
		retTp.Decimal = a.Decimal + b.Decimal
		if !isMultiply {
			retTp.Decimal = mathutil.Max(a.Decimal, b.Decimal)
		}
		if !isReal && retTp.Decimal > mysql.MaxDecimalScale {
			retTp.Decimal = mysql.MaxDecimalScale
		}
		if a.Flen == types.UnspecifiedLength || b.Flen == types.UnspecifiedLength {
			retTp.Flen = types.UnspecifiedLength
			return
		}
		digitsInt := mathutil.Max(a.Flen-a.Decimal, b.Flen-b.Decimal)
		if isMultiply {
			digitsInt = a.Flen - a.Decimal + b.Flen - b.Decimal
		}
		retTp.Flen = digitsInt + retTp.Decimal + 3
		if isReal {
			retTp.Flen = mathutil.Min(retTp.Flen, mysql.MaxRealWidth)
			return
		}
		retTp.Flen = mathutil.Min(retTp.Flen, mysql.MaxDecimalWidth)
		return
	}
	if isReal {
		retTp.Flen, retTp.Decimal = types.UnspecifiedLength, types.UnspecifiedLength
	} else {
		retTp.Flen, retTp.Decimal = mysql.MaxDecimalWidth, mysql.MaxDecimalScale
	}
}

func (c *arithmeticDivideFunctionClass) setType4DivDecimal(retTp, a, b *types.FieldType) {
	var deca, decb = a.Decimal, b.Decimal
	if deca == int(types.UnspecifiedFsp) {
		deca = 0
	}
	if decb == int(types.UnspecifiedFsp) {
		decb = 0
	}
	retTp.Decimal = deca + precIncrement
	if retTp.Decimal > mysql.MaxDecimalScale {
		retTp.Decimal = mysql.MaxDecimalScale
	}
	if a.Flen == types.UnspecifiedLength {
		retTp.Flen = mysql.MaxDecimalWidth
		return
	}
	retTp.Flen = a.Flen + decb + precIncrement
	if retTp.Flen > mysql.MaxDecimalWidth {
		retTp.Flen = mysql.MaxDecimalWidth
	}
}

func (c *arithmeticDivideFunctionClass) setType4DivReal(retTp *types.FieldType) {
	retTp.Decimal = mysql.NotFixedDec
	retTp.Flen = mysql.MaxRealWidth
}

type arithmeticPlusFunctionClass struct {
	BaseFunctionClass
}

func (c *arithmeticPlusFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	lhsTp, rhsTp := args[0].GetType(), args[1].GetType()
	lhsEvalTp, rhsEvalTp := numericContextResultType(lhsTp), numericContextResultType(rhsTp)
	if lhsEvalTp == types.ETReal || rhsEvalTp == types.ETReal {
		bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETReal, types.ETReal, types.ETReal)
		setFlenDecimal4RealOrDecimal(bf.Tp, args[0].GetType(), args[1].GetType(), true, false)
		sig := &builtinArithmeticPlusRealSig{bf}
		sig.SetPbCode(tipb.ScalarFuncSig_PlusReal)
		return sig, nil
	} else if lhsEvalTp == types.ETDecimal || rhsEvalTp == types.ETDecimal {
		bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETDecimal, types.ETDecimal, types.ETDecimal)
		setFlenDecimal4RealOrDecimal(bf.Tp, args[0].GetType(), args[1].GetType(), false, false)
		sig := &builtinArithmeticPlusDecimalSig{bf}
		sig.SetPbCode(tipb.ScalarFuncSig_PlusDecimal)
		return sig, nil
	} else {
		bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETInt, types.ETInt)
		if mysql.HasUnsignedFlag(args[0].GetType().Flag) || mysql.HasUnsignedFlag(args[1].GetType().Flag) {
			bf.Tp.Flag |= mysql.UnsignedFlag
		}
		setFlenDecimal4Int(bf.Tp, args[0].GetType(), args[1].GetType())
		sig := &builtinArithmeticPlusIntSig{bf}
		sig.SetPbCode(tipb.ScalarFuncSig_PlusInt)
		return sig, nil
	}
}

type builtinArithmeticPlusIntSig struct {
	BaseBuiltinFunc
}

func (s *builtinArithmeticPlusIntSig) Clone() BuiltinFunc {
	newSig := &builtinArithmeticPlusIntSig{}
	newSig.CloneFrom(&s.BaseBuiltinFunc)
	return newSig
}

func (s *builtinArithmeticPlusIntSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	a, isNull, err := s.Args[0].EvalInt(s.Ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}

	b, isNull, err := s.Args[1].EvalInt(s.Ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}

	isLHSUnsigned := mysql.HasUnsignedFlag(s.Args[0].GetType().Flag)
	isRHSUnsigned := mysql.HasUnsignedFlag(s.Args[1].GetType().Flag)

	switch {
	case isLHSUnsigned && isRHSUnsigned:
		if uint64(a) > math.MaxUint64-uint64(b) {
			return 0, true, types.ErrOverflow.GenWithStackByArgs("BIGINT UNSIGNED", fmt.Sprintf("(%s + %s)", s.Args[0].String(), s.Args[1].String()))
		}
	case isLHSUnsigned && !isRHSUnsigned:
		if b < 0 && uint64(-b) > uint64(a) {
			return 0, true, types.ErrOverflow.GenWithStackByArgs("BIGINT UNSIGNED", fmt.Sprintf("(%s + %s)", s.Args[0].String(), s.Args[1].String()))
		}
		if b > 0 && uint64(a) > math.MaxUint64-uint64(b) {
			return 0, true, types.ErrOverflow.GenWithStackByArgs("BIGINT UNSIGNED", fmt.Sprintf("(%s + %s)", s.Args[0].String(), s.Args[1].String()))
		}
	case !isLHSUnsigned && isRHSUnsigned:
		if a < 0 && uint64(-a) > uint64(b) {
			return 0, true, types.ErrOverflow.GenWithStackByArgs("BIGINT UNSIGNED", fmt.Sprintf("(%s + %s)", s.Args[0].String(), s.Args[1].String()))
		}
		if a > 0 && uint64(b) > math.MaxUint64-uint64(a) {
			return 0, true, types.ErrOverflow.GenWithStackByArgs("BIGINT UNSIGNED", fmt.Sprintf("(%s + %s)", s.Args[0].String(), s.Args[1].String()))
		}
	case !isLHSUnsigned && !isRHSUnsigned:
		if (a > 0 && b > math.MaxInt64-a) || (a < 0 && b < math.MinInt64-a) {
			return 0, true, types.ErrOverflow.GenWithStackByArgs("BIGINT", fmt.Sprintf("(%s + %s)", s.Args[0].String(), s.Args[1].String()))
		}
	}

	return a + b, false, nil
}

type builtinArithmeticPlusDecimalSig struct {
	BaseBuiltinFunc
}

func (s *builtinArithmeticPlusDecimalSig) Clone() BuiltinFunc {
	newSig := &builtinArithmeticPlusDecimalSig{}
	newSig.CloneFrom(&s.BaseBuiltinFunc)
	return newSig
}

func (s *builtinArithmeticPlusDecimalSig) evalDecimal(row chunk.Row) (*types.MyDecimal, bool, error) {
	a, isNull, err := s.Args[0].EvalDecimal(s.Ctx, row)
	if isNull || err != nil {
		return nil, isNull, err
	}
	b, isNull, err := s.Args[1].EvalDecimal(s.Ctx, row)
	if isNull || err != nil {
		return nil, isNull, err
	}
	c := &types.MyDecimal{}
	err = types.DecimalAdd(a, b, c)
	if err != nil {
		return nil, true, err
	}
	return c, false, nil
}

type builtinArithmeticPlusRealSig struct {
	BaseBuiltinFunc
}

func (s *builtinArithmeticPlusRealSig) Clone() BuiltinFunc {
	newSig := &builtinArithmeticPlusRealSig{}
	newSig.CloneFrom(&s.BaseBuiltinFunc)
	return newSig
}

func (s *builtinArithmeticPlusRealSig) evalReal(row chunk.Row) (float64, bool, error) {
	a, isNull, err := s.Args[0].EvalReal(s.Ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	b, isNull, err := s.Args[1].EvalReal(s.Ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	if (a > 0 && b > math.MaxFloat64-a) || (a < 0 && b < -math.MaxFloat64-a) {
		return 0, true, types.ErrOverflow.GenWithStackByArgs("DOUBLE", fmt.Sprintf("(%s + %s)", s.Args[0].String(), s.Args[1].String()))
	}
	return a + b, false, nil
}

type arithmeticMinusFunctionClass struct {
	BaseFunctionClass
}

func (c *arithmeticMinusFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	lhsTp, rhsTp := args[0].GetType(), args[1].GetType()
	lhsEvalTp, rhsEvalTp := numericContextResultType(lhsTp), numericContextResultType(rhsTp)
	if lhsEvalTp == types.ETReal || rhsEvalTp == types.ETReal {
		bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETReal, types.ETReal, types.ETReal)
		setFlenDecimal4RealOrDecimal(bf.Tp, args[0].GetType(), args[1].GetType(), true, false)
		sig := &builtinArithmeticMinusRealSig{bf}
		sig.SetPbCode(tipb.ScalarFuncSig_MinusReal)
		return sig, nil
	} else if lhsEvalTp == types.ETDecimal || rhsEvalTp == types.ETDecimal {
		bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETDecimal, types.ETDecimal, types.ETDecimal)
		setFlenDecimal4RealOrDecimal(bf.Tp, args[0].GetType(), args[1].GetType(), false, false)
		sig := &builtinArithmeticMinusDecimalSig{bf}
		sig.SetPbCode(tipb.ScalarFuncSig_MinusDecimal)
		return sig, nil
	} else {

		bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETInt, types.ETInt)
		setFlenDecimal4Int(bf.Tp, args[0].GetType(), args[1].GetType())
		if (mysql.HasUnsignedFlag(args[0].GetType().Flag) || mysql.HasUnsignedFlag(args[1].GetType().Flag)) && !ctx.GetSessionVars().SQLMode.HasNoUnsignedSubtractionMode() {
			bf.Tp.Flag |= mysql.UnsignedFlag
		}
		sig := &builtinArithmeticMinusIntSig{BaseBuiltinFunc: bf}
		sig.SetPbCode(tipb.ScalarFuncSig_MinusInt)
		return sig, nil
	}
}

type builtinArithmeticMinusRealSig struct {
	BaseBuiltinFunc
}

func (s *builtinArithmeticMinusRealSig) Clone() BuiltinFunc {
	newSig := &builtinArithmeticMinusRealSig{}
	newSig.CloneFrom(&s.BaseBuiltinFunc)
	return newSig
}

func (s *builtinArithmeticMinusRealSig) evalReal(row chunk.Row) (float64, bool, error) {
	a, isNull, err := s.Args[0].EvalReal(s.Ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	b, isNull, err := s.Args[1].EvalReal(s.Ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	if (a > 0 && -b > math.MaxFloat64-a) || (a < 0 && -b < -math.MaxFloat64-a) {
		return 0, true, types.ErrOverflow.GenWithStackByArgs("DOUBLE", fmt.Sprintf("(%s - %s)", s.Args[0].String(), s.Args[1].String()))
	}
	return a - b, false, nil
}

type builtinArithmeticMinusDecimalSig struct {
	BaseBuiltinFunc
}

func (s *builtinArithmeticMinusDecimalSig) Clone() BuiltinFunc {
	newSig := &builtinArithmeticMinusDecimalSig{}
	newSig.CloneFrom(&s.BaseBuiltinFunc)
	return newSig
}

func (s *builtinArithmeticMinusDecimalSig) evalDecimal(row chunk.Row) (*types.MyDecimal, bool, error) {
	a, isNull, err := s.Args[0].EvalDecimal(s.Ctx, row)
	if isNull || err != nil {
		return nil, isNull, err
	}
	b, isNull, err := s.Args[1].EvalDecimal(s.Ctx, row)
	if isNull || err != nil {
		return nil, isNull, err
	}
	c := &types.MyDecimal{}
	err = types.DecimalSub(a, b, c)
	if err != nil {
		return nil, true, err
	}
	return c, false, nil
}

type builtinArithmeticMinusIntSig struct {
	BaseBuiltinFunc
}

func (s *builtinArithmeticMinusIntSig) Clone() BuiltinFunc {
	newSig := &builtinArithmeticMinusIntSig{}
	newSig.CloneFrom(&s.BaseBuiltinFunc)
	return newSig
}

func (s *builtinArithmeticMinusIntSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	a, isNull, err := s.Args[0].EvalInt(s.Ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}

	b, isNull, err := s.Args[1].EvalInt(s.Ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	forceToSigned := s.Ctx.GetSessionVars().SQLMode.HasNoUnsignedSubtractionMode()
	isLHSUnsigned := !forceToSigned && mysql.HasUnsignedFlag(s.Args[0].GetType().Flag)
	isRHSUnsigned := !forceToSigned && mysql.HasUnsignedFlag(s.Args[1].GetType().Flag)

	if forceToSigned && mysql.HasUnsignedFlag(s.Args[0].GetType().Flag) {
		if a < 0 || (a > math.MaxInt64) {
			return 0, true, types.ErrOverflow.GenWithStackByArgs("BIGINT UNSIGNED", fmt.Sprintf("(%s - %s)", s.Args[0].String(), s.Args[1].String()))
		}
	}
	if forceToSigned && mysql.HasUnsignedFlag(s.Args[1].GetType().Flag) {
		if b < 0 || (b > math.MaxInt64) {
			return 0, true, types.ErrOverflow.GenWithStackByArgs("BIGINT UNSIGNED", fmt.Sprintf("(%s - %s)", s.Args[0].String(), s.Args[1].String()))
		}
	}

	switch {
	case isLHSUnsigned && isRHSUnsigned:
		if uint64(a) < uint64(b) {
			return 0, true, types.ErrOverflow.GenWithStackByArgs("BIGINT UNSIGNED", fmt.Sprintf("(%s - %s)", s.Args[0].String(), s.Args[1].String()))
		}
	case isLHSUnsigned && !isRHSUnsigned:
		if b >= 0 && uint64(a) < uint64(b) {
			return 0, true, types.ErrOverflow.GenWithStackByArgs("BIGINT UNSIGNED", fmt.Sprintf("(%s - %s)", s.Args[0].String(), s.Args[1].String()))
		}
		if b < 0 && uint64(a) > math.MaxUint64-uint64(-b) {
			return 0, true, types.ErrOverflow.GenWithStackByArgs("BIGINT UNSIGNED", fmt.Sprintf("(%s - %s)", s.Args[0].String(), s.Args[1].String()))
		}
	case !isLHSUnsigned && isRHSUnsigned:
		if uint64(a-math.MinInt64) < uint64(b) {
			return 0, true, types.ErrOverflow.GenWithStackByArgs("BIGINT UNSIGNED", fmt.Sprintf("(%s - %s)", s.Args[0].String(), s.Args[1].String()))
		}
	case !isLHSUnsigned && !isRHSUnsigned:
		if (a > 0 && -b > math.MaxInt64-a) || (a < 0 && -b < math.MinInt64-a) {
			return 0, true, types.ErrOverflow.GenWithStackByArgs("BIGINT", fmt.Sprintf("(%s - %s)", s.Args[0].String(), s.Args[1].String()))
		}
	}
	return a - b, false, nil
}

type arithmeticMultiplyFunctionClass struct {
	BaseFunctionClass
}

func (c *arithmeticMultiplyFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	lhsTp, rhsTp := args[0].GetType(), args[1].GetType()
	lhsEvalTp, rhsEvalTp := numericContextResultType(lhsTp), numericContextResultType(rhsTp)
	if lhsEvalTp == types.ETReal || rhsEvalTp == types.ETReal {
		bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETReal, types.ETReal, types.ETReal)
		setFlenDecimal4RealOrDecimal(bf.Tp, args[0].GetType(), args[1].GetType(), true, true)
		sig := &builtinArithmeticMultiplyRealSig{bf}
		sig.SetPbCode(tipb.ScalarFuncSig_MultiplyReal)
		return sig, nil
	} else if lhsEvalTp == types.ETDecimal || rhsEvalTp == types.ETDecimal {
		bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETDecimal, types.ETDecimal, types.ETDecimal)
		setFlenDecimal4RealOrDecimal(bf.Tp, args[0].GetType(), args[1].GetType(), false, true)
		sig := &builtinArithmeticMultiplyDecimalSig{bf}
		sig.SetPbCode(tipb.ScalarFuncSig_MultiplyDecimal)
		return sig, nil
	} else {
		bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETInt, types.ETInt)
		if mysql.HasUnsignedFlag(lhsTp.Flag) || mysql.HasUnsignedFlag(rhsTp.Flag) {
			bf.Tp.Flag |= mysql.UnsignedFlag
			setFlenDecimal4Int(bf.Tp, args[0].GetType(), args[1].GetType())
			sig := &builtinArithmeticMultiplyIntUnsignedSig{bf}
			sig.SetPbCode(tipb.ScalarFuncSig_MultiplyInt)
			return sig, nil
		}
		setFlenDecimal4Int(bf.Tp, args[0].GetType(), args[1].GetType())
		sig := &builtinArithmeticMultiplyIntSig{bf}
		sig.SetPbCode(tipb.ScalarFuncSig_MultiplyInt)
		return sig, nil
	}
}

type builtinArithmeticMultiplyRealSig struct{ BaseBuiltinFunc }

func (s *builtinArithmeticMultiplyRealSig) Clone() BuiltinFunc {
	newSig := &builtinArithmeticMultiplyRealSig{}
	newSig.CloneFrom(&s.BaseBuiltinFunc)
	return newSig
}

type builtinArithmeticMultiplyDecimalSig struct{ BaseBuiltinFunc }

func (s *builtinArithmeticMultiplyDecimalSig) Clone() BuiltinFunc {
	newSig := &builtinArithmeticMultiplyDecimalSig{}
	newSig.CloneFrom(&s.BaseBuiltinFunc)
	return newSig
}

type builtinArithmeticMultiplyIntUnsignedSig struct{ BaseBuiltinFunc }

func (s *builtinArithmeticMultiplyIntUnsignedSig) Clone() BuiltinFunc {
	newSig := &builtinArithmeticMultiplyIntUnsignedSig{}
	newSig.CloneFrom(&s.BaseBuiltinFunc)
	return newSig
}

type builtinArithmeticMultiplyIntSig struct{ BaseBuiltinFunc }

func (s *builtinArithmeticMultiplyIntSig) Clone() BuiltinFunc {
	newSig := &builtinArithmeticMultiplyIntSig{}
	newSig.CloneFrom(&s.BaseBuiltinFunc)
	return newSig
}

func (s *builtinArithmeticMultiplyRealSig) evalReal(row chunk.Row) (float64, bool, error) {
	a, isNull, err := s.Args[0].EvalReal(s.Ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	b, isNull, err := s.Args[1].EvalReal(s.Ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	result := a * b
	if math.IsInf(result, 0) {
		return 0, true, types.ErrOverflow.GenWithStackByArgs("DOUBLE", fmt.Sprintf("(%s * %s)", s.Args[0].String(), s.Args[1].String()))
	}
	return result, false, nil
}

func (s *builtinArithmeticMultiplyDecimalSig) evalDecimal(row chunk.Row) (*types.MyDecimal, bool, error) {
	a, isNull, err := s.Args[0].EvalDecimal(s.Ctx, row)
	if isNull || err != nil {
		return nil, isNull, err
	}
	b, isNull, err := s.Args[1].EvalDecimal(s.Ctx, row)
	if isNull || err != nil {
		return nil, isNull, err
	}
	c := &types.MyDecimal{}
	err = types.DecimalMul(a, b, c)
	if err != nil && !terror.ErrorEqual(err, types.ErrTruncated) {
		return nil, true, err
	}
	return c, false, nil
}

func (s *builtinArithmeticMultiplyIntUnsignedSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	a, isNull, err := s.Args[0].EvalInt(s.Ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	unsignedA := uint64(a)
	b, isNull, err := s.Args[1].EvalInt(s.Ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	unsignedB := uint64(b)
	result := unsignedA * unsignedB
	if unsignedA != 0 && result/unsignedA != unsignedB {
		return 0, true, types.ErrOverflow.GenWithStackByArgs("BIGINT UNSIGNED", fmt.Sprintf("(%s * %s)", s.Args[0].String(), s.Args[1].String()))
	}
	return int64(result), false, nil
}

func (s *builtinArithmeticMultiplyIntSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	a, isNull, err := s.Args[0].EvalInt(s.Ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	b, isNull, err := s.Args[1].EvalInt(s.Ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	result := a * b
	if a != 0 && result/a != b {
		return 0, true, types.ErrOverflow.GenWithStackByArgs("BIGINT", fmt.Sprintf("(%s * %s)", s.Args[0].String(), s.Args[1].String()))
	}
	return result, false, nil
}

type arithmeticDivideFunctionClass struct {
	BaseFunctionClass
}

func (c *arithmeticDivideFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	lhsTp, rhsTp := args[0].GetType(), args[1].GetType()
	lhsEvalTp, rhsEvalTp := numericContextResultType(lhsTp), numericContextResultType(rhsTp)
	if lhsEvalTp == types.ETReal || rhsEvalTp == types.ETReal {
		bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETReal, types.ETReal, types.ETReal)
		c.setType4DivReal(bf.Tp)
		sig := &builtinArithmeticDivideRealSig{bf}
		sig.SetPbCode(tipb.ScalarFuncSig_DivideReal)
		return sig, nil
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETDecimal, types.ETDecimal, types.ETDecimal)
	c.setType4DivDecimal(bf.Tp, lhsTp, rhsTp)
	sig := &builtinArithmeticDivideDecimalSig{bf}
	sig.SetPbCode(tipb.ScalarFuncSig_DivideDecimal)
	return sig, nil
}

type builtinArithmeticDivideRealSig struct{ BaseBuiltinFunc }

func (s *builtinArithmeticDivideRealSig) Clone() BuiltinFunc {
	newSig := &builtinArithmeticDivideRealSig{}
	newSig.CloneFrom(&s.BaseBuiltinFunc)
	return newSig
}

type builtinArithmeticDivideDecimalSig struct{ BaseBuiltinFunc }

func (s *builtinArithmeticDivideDecimalSig) Clone() BuiltinFunc {
	newSig := &builtinArithmeticDivideDecimalSig{}
	newSig.CloneFrom(&s.BaseBuiltinFunc)
	return newSig
}

func (s *builtinArithmeticDivideRealSig) evalReal(row chunk.Row) (float64, bool, error) {
	a, isNull, err := s.Args[0].EvalReal(s.Ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	b, isNull, err := s.Args[1].EvalReal(s.Ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	if b == 0 {
		return 0, true, handleDivisionByZeroError(s.Ctx)
	}
	result := a / b
	if math.IsInf(result, 0) {
		return 0, true, types.ErrOverflow.GenWithStackByArgs("DOUBLE", fmt.Sprintf("(%s / %s)", s.Args[0].String(), s.Args[1].String()))
	}
	return result, false, nil
}

func (s *builtinArithmeticDivideDecimalSig) evalDecimal(row chunk.Row) (*types.MyDecimal, bool, error) {
	a, isNull, err := s.Args[0].EvalDecimal(s.Ctx, row)
	if isNull || err != nil {
		return nil, isNull, err
	}

	b, isNull, err := s.Args[1].EvalDecimal(s.Ctx, row)
	if isNull || err != nil {
		return nil, isNull, err
	}

	c := &types.MyDecimal{}
	err = types.DecimalDiv(a, b, c, types.DivFracIncr)
	if err == types.ErrDivByZero {
		return c, true, handleDivisionByZeroError(s.Ctx)
	} else if err == nil {
		_, frac := c.PrecisionAndFrac()
		if frac < s.BaseBuiltinFunc.Tp.Decimal {
			err = c.Round(c, s.BaseBuiltinFunc.Tp.Decimal, types.ModeHalfEven)
		}
	}
	return c, false, err
}

type arithmeticIntDivideFunctionClass struct {
	BaseFunctionClass
}

func (c *arithmeticIntDivideFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}

	lhsTp, rhsTp := args[0].GetType(), args[1].GetType()
	lhsEvalTp, rhsEvalTp := numericContextResultType(lhsTp), numericContextResultType(rhsTp)
	if lhsEvalTp == types.ETInt && rhsEvalTp == types.ETInt {
		bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETInt, types.ETInt)
		if mysql.HasUnsignedFlag(lhsTp.Flag) || mysql.HasUnsignedFlag(rhsTp.Flag) {
			bf.Tp.Flag |= mysql.UnsignedFlag
		}
		sig := &builtinArithmeticIntDivideIntSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_IntDivideInt)
		return sig, nil
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETDecimal, types.ETDecimal)
	if mysql.HasUnsignedFlag(lhsTp.Flag) || mysql.HasUnsignedFlag(rhsTp.Flag) {
		bf.Tp.Flag |= mysql.UnsignedFlag
	}
	sig := &builtinArithmeticIntDivideDecimalSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_IntDivideDecimal)
	return sig, nil
}

type builtinArithmeticIntDivideIntSig struct{ BaseBuiltinFunc }

func (s *builtinArithmeticIntDivideIntSig) Clone() BuiltinFunc {
	newSig := &builtinArithmeticIntDivideIntSig{}
	newSig.CloneFrom(&s.BaseBuiltinFunc)
	return newSig
}

type builtinArithmeticIntDivideDecimalSig struct{ BaseBuiltinFunc }

func (s *builtinArithmeticIntDivideDecimalSig) Clone() BuiltinFunc {
	newSig := &builtinArithmeticIntDivideDecimalSig{}
	newSig.CloneFrom(&s.BaseBuiltinFunc)
	return newSig
}

func (s *builtinArithmeticIntDivideIntSig) evalInt(row chunk.Row) (int64, bool, error) {
	b, isNull, err := s.Args[1].EvalInt(s.Ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}

	if b == 0 {
		return 0, true, handleDivisionByZeroError(s.Ctx)
	}

	a, isNull, err := s.Args[0].EvalInt(s.Ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}

	var (
		ret int64
		val uint64
	)
	isLHSUnsigned := mysql.HasUnsignedFlag(s.Args[0].GetType().Flag)
	isRHSUnsigned := mysql.HasUnsignedFlag(s.Args[1].GetType().Flag)

	switch {
	case isLHSUnsigned && isRHSUnsigned:
		ret = int64(uint64(a) / uint64(b))
	case isLHSUnsigned && !isRHSUnsigned:
		val, err = types.DivUintWithInt(uint64(a), b)
		ret = int64(val)
	case !isLHSUnsigned && isRHSUnsigned:
		val, err = types.DivIntWithUint(a, uint64(b))
		ret = int64(val)
	case !isLHSUnsigned && !isRHSUnsigned:
		ret, err = types.DivInt64(a, b)
	}

	return ret, err != nil, err
}

func (s *builtinArithmeticIntDivideDecimalSig) evalInt(row chunk.Row) (ret int64, isNull bool, err error) {
	sc := s.Ctx.GetSessionVars().StmtCtx
	var num [2]*types.MyDecimal
	for i, arg := range s.Args {
		num[i], isNull, err = arg.EvalDecimal(s.Ctx, row)
		// Its behavior is consistent with MySQL.
		if terror.ErrorEqual(err, types.ErrTruncated) {
			err = nil
		}
		if terror.ErrorEqual(err, types.ErrOverflow) {
			newErr := errTruncatedWrongValue.GenWithStackByArgs("DECIMAL", arg)
			err = sc.HandleOverflow(newErr, newErr)
		}
		if isNull || err != nil {
			return 0, isNull, err
		}
	}

	c := &types.MyDecimal{}
	err = types.DecimalDiv(num[0], num[1], c, types.DivFracIncr)
	if err == types.ErrDivByZero {
		return 0, true, handleDivisionByZeroError(s.Ctx)
	}
	if err == types.ErrTruncated {
		err = sc.HandleTruncate(errTruncatedWrongValue.GenWithStackByArgs("DECIMAL", c))
	}
	if err == types.ErrOverflow {
		newErr := errTruncatedWrongValue.GenWithStackByArgs("DECIMAL", c)
		err = sc.HandleOverflow(newErr, newErr)
	}
	if err != nil {
		return 0, true, err
	}

	isLHSUnsigned := mysql.HasUnsignedFlag(s.Args[0].GetType().Flag)
	isRHSUnsigned := mysql.HasUnsignedFlag(s.Args[1].GetType().Flag)

	if isLHSUnsigned || isRHSUnsigned {
		val, err := c.ToUint()
		// err returned by ToUint may be ErrTruncated or ErrOverflow, only handle ErrOverflow, ignore ErrTruncated.
		if err == types.ErrOverflow {
			v, err := c.ToInt()
			// when the final result is at (-1, 0], it should be return 0 instead of the error
			if v == 0 && err == types.ErrTruncated {
				ret = int64(0)
				return ret, false, nil
			}
			return 0, true, types.ErrOverflow.GenWithStackByArgs("BIGINT UNSIGNED", fmt.Sprintf("(%s DIV %s)", s.Args[0].String(), s.Args[1].String()))
		}
		ret = int64(val)
	} else {
		ret, err = c.ToInt()
		// err returned by ToInt may be ErrTruncated or ErrOverflow, only handle ErrOverflow, ignore ErrTruncated.
		if err == types.ErrOverflow {
			return 0, true, types.ErrOverflow.GenWithStackByArgs("BIGINT", fmt.Sprintf("(%s DIV %s)", s.Args[0].String(), s.Args[1].String()))
		}
	}

	return ret, false, nil
}

type arithmeticModFunctionClass struct {
	BaseFunctionClass
}

func (c *arithmeticModFunctionClass) setType4ModRealOrDecimal(retTp, a, b *types.FieldType, isDecimal bool) {
	if a.Decimal == types.UnspecifiedLength || b.Decimal == types.UnspecifiedLength {
		retTp.Decimal = types.UnspecifiedLength
	} else {
		retTp.Decimal = mathutil.Max(a.Decimal, b.Decimal)
		if isDecimal && retTp.Decimal > mysql.MaxDecimalScale {
			retTp.Decimal = mysql.MaxDecimalScale
		}
	}

	if a.Flen == types.UnspecifiedLength || b.Flen == types.UnspecifiedLength {
		retTp.Flen = types.UnspecifiedLength
	} else {
		retTp.Flen = mathutil.Max(a.Flen, b.Flen)
		if isDecimal {
			retTp.Flen = mathutil.Min(retTp.Flen, mysql.MaxDecimalWidth)
			return
		}
		retTp.Flen = mathutil.Min(retTp.Flen, mysql.MaxRealWidth)
	}
}

func (c *arithmeticModFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	lhsTp, rhsTp := args[0].GetType(), args[1].GetType()
	lhsEvalTp, rhsEvalTp := numericContextResultType(lhsTp), numericContextResultType(rhsTp)
	if lhsEvalTp == types.ETReal || rhsEvalTp == types.ETReal {
		bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETReal, types.ETReal, types.ETReal)
		c.setType4ModRealOrDecimal(bf.Tp, lhsTp, rhsTp, false)
		if mysql.HasUnsignedFlag(lhsTp.Flag) {
			bf.Tp.Flag |= mysql.UnsignedFlag
		}
		sig := &builtinArithmeticModRealSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_ModReal)
		return sig, nil
	} else if lhsEvalTp == types.ETDecimal || rhsEvalTp == types.ETDecimal {
		bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETDecimal, types.ETDecimal, types.ETDecimal)
		c.setType4ModRealOrDecimal(bf.Tp, lhsTp, rhsTp, true)
		if mysql.HasUnsignedFlag(lhsTp.Flag) {
			bf.Tp.Flag |= mysql.UnsignedFlag
		}
		sig := &builtinArithmeticModDecimalSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_ModDecimal)
		return sig, nil
	} else {
		bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETInt, types.ETInt)
		if mysql.HasUnsignedFlag(lhsTp.Flag) {
			bf.Tp.Flag |= mysql.UnsignedFlag
		}
		sig := &builtinArithmeticModIntSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_ModInt)
		return sig, nil
	}
}

type builtinArithmeticModRealSig struct {
	BaseBuiltinFunc
}

func (s *builtinArithmeticModRealSig) Clone() BuiltinFunc {
	newSig := &builtinArithmeticModRealSig{}
	newSig.CloneFrom(&s.BaseBuiltinFunc)
	return newSig
}

func (s *builtinArithmeticModRealSig) evalReal(row chunk.Row) (float64, bool, error) {
	b, isNull, err := s.Args[1].EvalReal(s.Ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}

	if b == 0 {
		return 0, true, handleDivisionByZeroError(s.Ctx)
	}

	a, isNull, err := s.Args[0].EvalReal(s.Ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}

	return math.Mod(a, b), false, nil
}

type builtinArithmeticModDecimalSig struct {
	BaseBuiltinFunc
}

func (s *builtinArithmeticModDecimalSig) Clone() BuiltinFunc {
	newSig := &builtinArithmeticModDecimalSig{}
	newSig.CloneFrom(&s.BaseBuiltinFunc)
	return newSig
}

func (s *builtinArithmeticModDecimalSig) evalDecimal(row chunk.Row) (*types.MyDecimal, bool, error) {
	a, isNull, err := s.Args[0].EvalDecimal(s.Ctx, row)
	if isNull || err != nil {
		return nil, isNull, err
	}
	b, isNull, err := s.Args[1].EvalDecimal(s.Ctx, row)
	if isNull || err != nil {
		return nil, isNull, err
	}
	c := &types.MyDecimal{}
	err = types.DecimalMod(a, b, c)
	if err == types.ErrDivByZero {
		return c, true, handleDivisionByZeroError(s.Ctx)
	}
	return c, err != nil, err
}

type builtinArithmeticModIntSig struct {
	BaseBuiltinFunc
}

func (s *builtinArithmeticModIntSig) Clone() BuiltinFunc {
	newSig := &builtinArithmeticModIntSig{}
	newSig.CloneFrom(&s.BaseBuiltinFunc)
	return newSig
}

func (s *builtinArithmeticModIntSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	b, isNull, err := s.Args[1].EvalInt(s.Ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}

	if b == 0 {
		return 0, true, handleDivisionByZeroError(s.Ctx)
	}

	a, isNull, err := s.Args[0].EvalInt(s.Ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}

	var ret int64
	isLHSUnsigned := mysql.HasUnsignedFlag(s.Args[0].GetType().Flag)
	isRHSUnsigned := mysql.HasUnsignedFlag(s.Args[1].GetType().Flag)

	switch {
	case isLHSUnsigned && isRHSUnsigned:
		ret = int64(uint64(a) % uint64(b))
	case isLHSUnsigned && !isRHSUnsigned:
		if b < 0 {
			ret = int64(uint64(a) % uint64(-b))
		} else {
			ret = int64(uint64(a) % uint64(b))
		}
	case !isLHSUnsigned && isRHSUnsigned:
		if a < 0 {
			ret = -int64(uint64(-a) % uint64(b))
		} else {
			ret = int64(uint64(a) % uint64(b))
		}
	case !isLHSUnsigned && !isRHSUnsigned:
		ret = a % b
	}

	return ret, false, nil
}
