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
	"regexp"

	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/stringutil"
	"github.com/pingcap/tipb/go-tipb"
)

var (
	_ FunctionClass = &likeFunctionClass{}
	_ FunctionClass = &regexpFunctionClass{}
)

var (
	_ BuiltinFunc = &builtinLikeSig{}
	_ BuiltinFunc = &builtinRegexpBinarySig{}
	_ BuiltinFunc = &builtinRegexpSig{}
)

type likeFunctionClass struct {
	BaseFunctionClass
}

func (c *likeFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	argTp := []types.EvalType{types.ETString, types.ETString, types.ETInt}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETInt, argTp...)
	bf.Tp.Flen = 1
	sig := &builtinLikeSig{bf}
	sig.SetPbCode(tipb.ScalarFuncSig_LikeSig)
	return sig, nil
}

type builtinLikeSig struct {
	BaseBuiltinFunc
}

func (b *builtinLikeSig) Clone() BuiltinFunc {
	newSig := &builtinLikeSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalInt evals a builtinLikeSig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-comparison-functions.html#operator_like
// NOTE: Currently tikv's like function is case sensitive, so we keep its behavior here.
func (b *builtinLikeSig) evalInt(row chunk.Row) (int64, bool, error) {
	valStr, isNull, err := b.Args[0].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}

	// TODO: We don't need to compile pattern if it has been compiled or it is static.
	patternStr, isNull, err := b.Args[1].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	val, isNull, err := b.Args[2].EvalInt(b.Ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	escape := byte(val)
	patChars, patTypes := stringutil.CompilePattern(patternStr, escape)
	match := stringutil.DoMatch(valStr, patChars, patTypes)
	return boolToInt64(match), false, nil
}

type regexpFunctionClass struct {
	BaseFunctionClass
}

func (c *regexpFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETString, types.ETString)
	bf.Tp.Flen = 1
	var sig BuiltinFunc
	if types.IsBinaryStr(args[0].GetType()) || types.IsBinaryStr(args[1].GetType()) {
		sig = newBuiltinRegexpBinarySig(bf)
	} else {
		sig = newBuiltinRegexpSig(bf)
	}
	return sig, nil
}

type builtinRegexpSharedSig struct {
	BaseBuiltinFunc
	compile        func(string) (*regexp.Regexp, error)
	memoizedRegexp *regexp.Regexp
	memoizedErr    error
}

func (b *builtinRegexpSharedSig) clone(from *builtinRegexpSharedSig) {
	b.CloneFrom(&from.BaseBuiltinFunc)
	b.compile = from.compile
	if from.memoizedRegexp != nil {
		b.memoizedRegexp = from.memoizedRegexp.Copy()
	}
	b.memoizedErr = from.memoizedErr
}

// evalInt evals `expr REGEXP pat`, or `expr RLIKE pat`.
// See https://dev.mysql.com/doc/refman/5.7/en/regexp.html#operator_regexp
func (b *builtinRegexpSharedSig) evalInt(row chunk.Row) (int64, bool, error) {
	expr, isNull, err := b.Args[0].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return 0, true, err
	}

	pat, isNull, err := b.Args[1].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return 0, true, err
	}

	// TODO: We don't need to compile pattern if it has been compiled or it is static.
	re, err := b.compile(pat)
	if err != nil {
		return 0, true, ErrRegexp.GenWithStackByArgs(err.Error())
	}
	return boolToInt64(re.MatchString(expr)), false, nil
}

type builtinRegexpBinarySig struct {
	builtinRegexpSharedSig
}

func newBuiltinRegexpBinarySig(bf BaseBuiltinFunc) *builtinRegexpBinarySig {
	shared := builtinRegexpSharedSig{BaseBuiltinFunc: bf}
	shared.compile = regexp.Compile
	return &builtinRegexpBinarySig{builtinRegexpSharedSig: shared}
}

func (b *builtinRegexpBinarySig) Clone() BuiltinFunc {
	newSig := &builtinRegexpBinarySig{}
	newSig.clone(&b.builtinRegexpSharedSig)
	return newSig
}

type builtinRegexpSig struct {
	builtinRegexpSharedSig
}

func newBuiltinRegexpSig(bf BaseBuiltinFunc) *builtinRegexpSig {
	shared := builtinRegexpSharedSig{BaseBuiltinFunc: bf}
	shared.compile = func(pat string) (*regexp.Regexp, error) {
		return regexp.Compile("(?i)" + pat)
	}
	return &builtinRegexpSig{builtinRegexpSharedSig: shared}
}

func (b *builtinRegexpSig) Clone() BuiltinFunc {
	newSig := &builtinRegexpSig{}
	newSig.clone(&b.builtinRegexpSharedSig)
	return newSig
}
