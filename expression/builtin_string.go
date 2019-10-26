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
	"bytes"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"math"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/charset"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/hack"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
	"golang.org/x/text/transform"
)

var (
	_ FunctionClass = &lengthFunctionClass{}
	_ FunctionClass = &asciiFunctionClass{}
	_ FunctionClass = &concatFunctionClass{}
	_ FunctionClass = &concatWSFunctionClass{}
	_ FunctionClass = &leftFunctionClass{}
	_ FunctionClass = &repeatFunctionClass{}
	_ FunctionClass = &lowerFunctionClass{}
	_ FunctionClass = &reverseFunctionClass{}
	_ FunctionClass = &spaceFunctionClass{}
	_ FunctionClass = &upperFunctionClass{}
	_ FunctionClass = &strcmpFunctionClass{}
	_ FunctionClass = &replaceFunctionClass{}
	_ FunctionClass = &convertFunctionClass{}
	_ FunctionClass = &substringFunctionClass{}
	_ FunctionClass = &substringIndexFunctionClass{}
	_ FunctionClass = &locateFunctionClass{}
	_ FunctionClass = &hexFunctionClass{}
	_ FunctionClass = &unhexFunctionClass{}
	_ FunctionClass = &trimFunctionClass{}
	_ FunctionClass = &lTrimFunctionClass{}
	_ FunctionClass = &rTrimFunctionClass{}
	_ FunctionClass = &lpadFunctionClass{}
	_ FunctionClass = &rpadFunctionClass{}
	_ FunctionClass = &bitLengthFunctionClass{}
	_ FunctionClass = &charFunctionClass{}
	_ FunctionClass = &charLengthFunctionClass{}
	_ FunctionClass = &findInSetFunctionClass{}
	_ FunctionClass = &fieldFunctionClass{}
	_ FunctionClass = &makeSetFunctionClass{}
	_ FunctionClass = &octFunctionClass{}
	_ FunctionClass = &ordFunctionClass{}
	_ FunctionClass = &quoteFunctionClass{}
	_ FunctionClass = &binFunctionClass{}
	_ FunctionClass = &eltFunctionClass{}
	_ FunctionClass = &exportSetFunctionClass{}
	_ FunctionClass = &formatFunctionClass{}
	_ FunctionClass = &fromBase64FunctionClass{}
	_ FunctionClass = &toBase64FunctionClass{}
	_ FunctionClass = &insertFunctionClass{}
	_ FunctionClass = &instrFunctionClass{}
	_ FunctionClass = &loadFileFunctionClass{}
)

var (
	_ BuiltinFunc = &builtinLengthSig{}
	_ BuiltinFunc = &builtinASCIISig{}
	_ BuiltinFunc = &builtinConcatSig{}
	_ BuiltinFunc = &builtinConcatWSSig{}
	_ BuiltinFunc = &builtinLeftBinarySig{}
	_ BuiltinFunc = &builtinLeftSig{}
	_ BuiltinFunc = &builtinRightBinarySig{}
	_ BuiltinFunc = &builtinRightSig{}
	_ BuiltinFunc = &builtinRepeatSig{}
	_ BuiltinFunc = &builtinLowerSig{}
	_ BuiltinFunc = &builtinReverseSig{}
	_ BuiltinFunc = &builtinReverseBinarySig{}
	_ BuiltinFunc = &builtinSpaceSig{}
	_ BuiltinFunc = &builtinUpperSig{}
	_ BuiltinFunc = &builtinStrcmpSig{}
	_ BuiltinFunc = &builtinReplaceSig{}
	_ BuiltinFunc = &builtinConvertSig{}
	_ BuiltinFunc = &builtinSubstringBinary2ArgsSig{}
	_ BuiltinFunc = &builtinSubstringBinary3ArgsSig{}
	_ BuiltinFunc = &builtinSubstring2ArgsSig{}
	_ BuiltinFunc = &builtinSubstring3ArgsSig{}
	_ BuiltinFunc = &builtinSubstringIndexSig{}
	_ BuiltinFunc = &builtinLocate2ArgsSig{}
	_ BuiltinFunc = &builtinLocate3ArgsSig{}
	_ BuiltinFunc = &builtinLocateBinary2ArgsSig{}
	_ BuiltinFunc = &builtinLocateBinary3ArgsSig{}
	_ BuiltinFunc = &builtinHexStrArgSig{}
	_ BuiltinFunc = &builtinHexIntArgSig{}
	_ BuiltinFunc = &builtinUnHexSig{}
	_ BuiltinFunc = &builtinTrim1ArgSig{}
	_ BuiltinFunc = &builtinTrim2ArgsSig{}
	_ BuiltinFunc = &builtinTrim3ArgsSig{}
	_ BuiltinFunc = &builtinLTrimSig{}
	_ BuiltinFunc = &builtinRTrimSig{}
	_ BuiltinFunc = &builtinLpadSig{}
	_ BuiltinFunc = &builtinLpadBinarySig{}
	_ BuiltinFunc = &builtinRpadSig{}
	_ BuiltinFunc = &builtinRpadBinarySig{}
	_ BuiltinFunc = &builtinBitLengthSig{}
	_ BuiltinFunc = &builtinCharSig{}
	_ BuiltinFunc = &builtinCharLengthSig{}
	_ BuiltinFunc = &builtinFindInSetSig{}
	_ BuiltinFunc = &builtinMakeSetSig{}
	_ BuiltinFunc = &builtinOctIntSig{}
	_ BuiltinFunc = &builtinOctStringSig{}
	_ BuiltinFunc = &builtinOrdSig{}
	_ BuiltinFunc = &builtinQuoteSig{}
	_ BuiltinFunc = &builtinBinSig{}
	_ BuiltinFunc = &builtinEltSig{}
	_ BuiltinFunc = &builtinExportSet3ArgSig{}
	_ BuiltinFunc = &builtinExportSet4ArgSig{}
	_ BuiltinFunc = &builtinExportSet5ArgSig{}
	_ BuiltinFunc = &builtinFormatWithLocaleSig{}
	_ BuiltinFunc = &builtinFormatSig{}
	_ BuiltinFunc = &builtinFromBase64Sig{}
	_ BuiltinFunc = &builtinToBase64Sig{}
	_ BuiltinFunc = &builtinInsertBinarySig{}
	_ BuiltinFunc = &builtinInsertSig{}
	_ BuiltinFunc = &builtinInstrSig{}
	_ BuiltinFunc = &builtinInstrBinarySig{}
	_ BuiltinFunc = &builtinFieldRealSig{}
	_ BuiltinFunc = &builtinFieldIntSig{}
	_ BuiltinFunc = &builtinFieldStringSig{}
)

func reverseBytes(origin []byte) []byte {
	for i, length := 0, len(origin); i < length/2; i++ {
		origin[i], origin[length-i-1] = origin[length-i-1], origin[i]
	}
	return origin
}

func reverseRunes(origin []rune) []rune {
	for i, length := 0, len(origin); i < length/2; i++ {
		origin[i], origin[length-i-1] = origin[length-i-1], origin[i]
	}
	return origin
}

// SetBinFlagOrBinStr sets resTp to binary string if argTp is a binary string,
// if not, sets the binary flag of resTp to true if argTp has binary flag.
func SetBinFlagOrBinStr(argTp *types.FieldType, resTp *types.FieldType) {
	if types.IsBinaryStr(argTp) {
		types.SetBinChsClnFlag(resTp)
	} else if mysql.HasBinaryFlag(argTp.Flag) || !types.IsNonBinaryStr(argTp) {
		resTp.Flag |= mysql.BinaryFlag
	}
}

type lengthFunctionClass struct {
	BaseFunctionClass
}

func (c *lengthFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETString)
	bf.Tp.Flen = 10
	sig := &builtinLengthSig{bf}
	return sig, nil
}

type builtinLengthSig struct {
	BaseBuiltinFunc
}

func (b *builtinLengthSig) Clone() BuiltinFunc {
	newSig := &builtinLengthSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalInt evaluates a builtinLengthSig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html
func (b *builtinLengthSig) evalInt(row chunk.Row) (int64, bool, error) {
	val, isNull, err := b.Args[0].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	return int64(len([]byte(val))), false, nil
}

type asciiFunctionClass struct {
	BaseFunctionClass
}

func (c *asciiFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETString)
	bf.Tp.Flen = 3
	sig := &builtinASCIISig{bf}
	return sig, nil
}

type builtinASCIISig struct {
	BaseBuiltinFunc
}

func (b *builtinASCIISig) Clone() BuiltinFunc {
	newSig := &builtinASCIISig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalInt evals a builtinASCIISig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_ascii
func (b *builtinASCIISig) evalInt(row chunk.Row) (int64, bool, error) {
	val, isNull, err := b.Args[0].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	if len(val) == 0 {
		return 0, false, nil
	}
	return int64(val[0]), false, nil
}

type concatFunctionClass struct {
	BaseFunctionClass
}

func (c *concatFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	argTps := make([]types.EvalType, 0, len(args))
	for i := 0; i < len(args); i++ {
		argTps = append(argTps, types.ETString)
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETString, argTps...)
	for i := range args {
		argType := args[i].GetType()
		SetBinFlagOrBinStr(argType, bf.Tp)

		if argType.Flen < 0 {
			bf.Tp.Flen = mysql.MaxBlobWidth
			logutil.BgLogger().Warn("unexpected `Flen` value(-1) in CONCAT's args", zap.Int("arg's index", i))
		}
		bf.Tp.Flen += argType.Flen
	}
	if bf.Tp.Flen >= mysql.MaxBlobWidth {
		bf.Tp.Flen = mysql.MaxBlobWidth
	}

	valStr, _ := ctx.GetSessionVars().GetSystemVar(variable.MaxAllowedPacket)
	maxAllowedPacket, err := strconv.ParseUint(valStr, 10, 64)
	if err != nil {
		return nil, err
	}

	sig := &builtinConcatSig{bf, maxAllowedPacket}
	return sig, nil
}

type builtinConcatSig struct {
	BaseBuiltinFunc
	maxAllowedPacket uint64
}

func (b *builtinConcatSig) Clone() BuiltinFunc {
	newSig := &builtinConcatSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	newSig.maxAllowedPacket = b.maxAllowedPacket
	return newSig
}

// evalString evals a builtinConcatSig
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_concat
func (b *builtinConcatSig) EvalString(row chunk.Row) (d string, isNull bool, err error) {
	var s []byte
	for _, a := range b.getArgs() {
		d, isNull, err = a.EvalString(b.Ctx, row)
		if isNull || err != nil {
			return d, isNull, err
		}
		if uint64(len(s)+len(d)) > b.maxAllowedPacket {
			b.Ctx.GetSessionVars().StmtCtx.AppendWarning(errWarnAllowedPacketOverflowed.GenWithStackByArgs("concat", b.maxAllowedPacket))
			return "", true, nil
		}
		s = append(s, []byte(d)...)
	}
	return string(s), false, nil
}

type concatWSFunctionClass struct {
	BaseFunctionClass
}

func (c *concatWSFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	argTps := make([]types.EvalType, 0, len(args))
	for i := 0; i < len(args); i++ {
		argTps = append(argTps, types.ETString)
	}

	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETString, argTps...)

	for i := range args {
		argType := args[i].GetType()
		SetBinFlagOrBinStr(argType, bf.Tp)

		// skip separator param
		if i != 0 {
			if argType.Flen < 0 {
				bf.Tp.Flen = mysql.MaxBlobWidth
				logutil.BgLogger().Warn("unexpected `Flen` value(-1) in CONCAT_WS's args", zap.Int("arg's index", i))
			}
			bf.Tp.Flen += argType.Flen
		}
	}

	// add separator
	argsLen := len(args) - 1
	bf.Tp.Flen += argsLen - 1

	if bf.Tp.Flen >= mysql.MaxBlobWidth {
		bf.Tp.Flen = mysql.MaxBlobWidth
	}

	valStr, _ := ctx.GetSessionVars().GetSystemVar(variable.MaxAllowedPacket)
	maxAllowedPacket, err := strconv.ParseUint(valStr, 10, 64)
	if err != nil {
		return nil, err
	}

	sig := &builtinConcatWSSig{bf, maxAllowedPacket}
	return sig, nil
}

type builtinConcatWSSig struct {
	BaseBuiltinFunc
	maxAllowedPacket uint64
}

func (b *builtinConcatWSSig) Clone() BuiltinFunc {
	newSig := &builtinConcatWSSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	newSig.maxAllowedPacket = b.maxAllowedPacket
	return newSig
}

// evalString evals a builtinConcatWSSig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_concat-ws
func (b *builtinConcatWSSig) EvalString(row chunk.Row) (string, bool, error) {
	args := b.getArgs()
	strs := make([]string, 0, len(args))
	var sep string
	var targetLength int

	N := len(args)
	if N > 0 {
		val, isNull, err := args[0].EvalString(b.Ctx, row)
		if err != nil || isNull {
			// If the separator is NULL, the result is NULL.
			return val, isNull, err
		}
		sep = val
	}
	for i := 1; i < N; i++ {
		val, isNull, err := args[i].EvalString(b.Ctx, row)
		if err != nil {
			return val, isNull, err
		}
		if isNull {
			// CONCAT_WS() does not skip empty strings. However,
			// it does skip any NULL values after the separator argument.
			continue
		}

		targetLength += len(val)
		if i > 1 {
			targetLength += len(sep)
		}
		if uint64(targetLength) > b.maxAllowedPacket {
			b.Ctx.GetSessionVars().StmtCtx.AppendWarning(errWarnAllowedPacketOverflowed.GenWithStackByArgs("concat_ws", b.maxAllowedPacket))
			return "", true, nil
		}
		strs = append(strs, val)
	}

	// TODO: check whether the length of result is larger than Flen
	return strings.Join(strs, sep), false, nil
}

type leftFunctionClass struct {
	BaseFunctionClass
}

func (c *leftFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETString, types.ETString, types.ETInt)
	argType := args[0].GetType()
	bf.Tp.Flen = argType.Flen
	SetBinFlagOrBinStr(argType, bf.Tp)
	if types.IsBinaryStr(argType) {
		sig := &builtinLeftBinarySig{bf}
		return sig, nil
	}
	sig := &builtinLeftSig{bf}
	return sig, nil
}

type builtinLeftBinarySig struct {
	BaseBuiltinFunc
}

func (b *builtinLeftBinarySig) Clone() BuiltinFunc {
	newSig := &builtinLeftBinarySig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalString evals LEFT(str,len).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_left
func (b *builtinLeftBinarySig) EvalString(row chunk.Row) (string, bool, error) {
	str, isNull, err := b.Args[0].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	left, isNull, err := b.Args[1].EvalInt(b.Ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	leftLength := int(left)
	if strLength := len(str); leftLength > strLength {
		leftLength = strLength
	} else if leftLength < 0 {
		leftLength = 0
	}
	return str[:leftLength], false, nil
}

type builtinLeftSig struct {
	BaseBuiltinFunc
}

func (b *builtinLeftSig) Clone() BuiltinFunc {
	newSig := &builtinLeftSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalString evals LEFT(str,len).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_left
func (b *builtinLeftSig) EvalString(row chunk.Row) (string, bool, error) {
	str, isNull, err := b.Args[0].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	left, isNull, err := b.Args[1].EvalInt(b.Ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	runes, leftLength := []rune(str), int(left)
	if runeLength := len(runes); leftLength > runeLength {
		leftLength = runeLength
	} else if leftLength < 0 {
		leftLength = 0
	}
	return string(runes[:leftLength]), false, nil
}

type rightFunctionClass struct {
	BaseFunctionClass
}

func (c *rightFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETString, types.ETString, types.ETInt)
	argType := args[0].GetType()
	bf.Tp.Flen = argType.Flen
	SetBinFlagOrBinStr(argType, bf.Tp)
	if types.IsBinaryStr(argType) {
		sig := &builtinRightBinarySig{bf}
		return sig, nil
	}
	sig := &builtinRightSig{bf}
	return sig, nil
}

type builtinRightBinarySig struct {
	BaseBuiltinFunc
}

func (b *builtinRightBinarySig) Clone() BuiltinFunc {
	newSig := &builtinRightBinarySig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalString evals RIGHT(str,len).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_right
func (b *builtinRightBinarySig) EvalString(row chunk.Row) (string, bool, error) {
	str, isNull, err := b.Args[0].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	right, isNull, err := b.Args[1].EvalInt(b.Ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	strLength, rightLength := len(str), int(right)
	if rightLength > strLength {
		rightLength = strLength
	} else if rightLength < 0 {
		rightLength = 0
	}
	return str[strLength-rightLength:], false, nil
}

type builtinRightSig struct {
	BaseBuiltinFunc
}

func (b *builtinRightSig) Clone() BuiltinFunc {
	newSig := &builtinRightSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalString evals RIGHT(str,len).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_right
func (b *builtinRightSig) EvalString(row chunk.Row) (string, bool, error) {
	str, isNull, err := b.Args[0].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	right, isNull, err := b.Args[1].EvalInt(b.Ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	runes := []rune(str)
	strLength, rightLength := len(runes), int(right)
	if rightLength > strLength {
		rightLength = strLength
	} else if rightLength < 0 {
		rightLength = 0
	}
	return string(runes[strLength-rightLength:]), false, nil
}

type repeatFunctionClass struct {
	BaseFunctionClass
}

func (c *repeatFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETString, types.ETString, types.ETInt)
	bf.Tp.Flen = mysql.MaxBlobWidth
	SetBinFlagOrBinStr(args[0].GetType(), bf.Tp)
	valStr, _ := ctx.GetSessionVars().GetSystemVar(variable.MaxAllowedPacket)
	maxAllowedPacket, err := strconv.ParseUint(valStr, 10, 64)
	if err != nil {
		return nil, err
	}
	sig := &builtinRepeatSig{bf, maxAllowedPacket}
	return sig, nil
}

type builtinRepeatSig struct {
	BaseBuiltinFunc
	maxAllowedPacket uint64
}

func (b *builtinRepeatSig) Clone() BuiltinFunc {
	newSig := &builtinRepeatSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	newSig.maxAllowedPacket = b.maxAllowedPacket
	return newSig
}

// evalString evals a builtinRepeatSig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_repeat
func (b *builtinRepeatSig) EvalString(row chunk.Row) (d string, isNull bool, err error) {
	str, isNull, err := b.Args[0].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	byteLength := len(str)

	num, isNull, err := b.Args[1].EvalInt(b.Ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	if num < 1 {
		return "", false, nil
	}
	if num > math.MaxInt32 {
		num = math.MaxInt32
	}

	if uint64(byteLength)*uint64(num) > b.maxAllowedPacket {
		b.Ctx.GetSessionVars().StmtCtx.AppendWarning(errWarnAllowedPacketOverflowed.GenWithStackByArgs("repeat", b.maxAllowedPacket))
		return "", true, nil
	}

	if int64(byteLength) > int64(b.Tp.Flen)/num {
		return "", true, nil
	}
	return strings.Repeat(str, int(num)), false, nil
}

type lowerFunctionClass struct {
	BaseFunctionClass
}

func (c *lowerFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETString, types.ETString)
	argTp := args[0].GetType()
	bf.Tp.Flen = argTp.Flen
	SetBinFlagOrBinStr(argTp, bf.Tp)
	sig := &builtinLowerSig{bf}
	return sig, nil
}

type builtinLowerSig struct {
	BaseBuiltinFunc
}

func (b *builtinLowerSig) Clone() BuiltinFunc {
	newSig := &builtinLowerSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalString evals a builtinLowerSig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_lower
func (b *builtinLowerSig) EvalString(row chunk.Row) (d string, isNull bool, err error) {
	d, isNull, err = b.Args[0].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return d, isNull, err
	}

	if types.IsBinaryStr(b.Args[0].GetType()) {
		return d, false, nil
	}

	return strings.ToLower(d), false, nil
}

type reverseFunctionClass struct {
	BaseFunctionClass
}

func (c *reverseFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETString, types.ETString)
	retTp := *args[0].GetType()
	retTp.Tp = mysql.TypeVarString
	retTp.Decimal = types.UnspecifiedLength
	bf.Tp = &retTp
	var sig BuiltinFunc
	if types.IsBinaryStr(bf.Tp) {
		sig = &builtinReverseBinarySig{bf}
	} else {
		sig = &builtinReverseSig{bf}
	}
	return sig, nil
}

type builtinReverseBinarySig struct {
	BaseBuiltinFunc
}

func (b *builtinReverseBinarySig) Clone() BuiltinFunc {
	newSig := &builtinReverseBinarySig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalString evals a REVERSE(str).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_reverse
func (b *builtinReverseBinarySig) EvalString(row chunk.Row) (string, bool, error) {
	str, isNull, err := b.Args[0].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	reversed := reverseBytes([]byte(str))
	return string(reversed), false, nil
}

type builtinReverseSig struct {
	BaseBuiltinFunc
}

func (b *builtinReverseSig) Clone() BuiltinFunc {
	newSig := &builtinReverseSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalString evals a REVERSE(str).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_reverse
func (b *builtinReverseSig) EvalString(row chunk.Row) (string, bool, error) {
	str, isNull, err := b.Args[0].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	reversed := reverseRunes([]rune(str))
	return string(reversed), false, nil
}

type spaceFunctionClass struct {
	BaseFunctionClass
}

func (c *spaceFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETString, types.ETInt)
	bf.Tp.Flen = mysql.MaxBlobWidth
	valStr, _ := ctx.GetSessionVars().GetSystemVar(variable.MaxAllowedPacket)
	maxAllowedPacket, err := strconv.ParseUint(valStr, 10, 64)
	if err != nil {
		return nil, err
	}
	sig := &builtinSpaceSig{bf, maxAllowedPacket}
	return sig, nil
}

type builtinSpaceSig struct {
	BaseBuiltinFunc
	maxAllowedPacket uint64
}

func (b *builtinSpaceSig) Clone() BuiltinFunc {
	newSig := &builtinSpaceSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	newSig.maxAllowedPacket = b.maxAllowedPacket
	return newSig
}

// evalString evals a builtinSpaceSig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_space
func (b *builtinSpaceSig) EvalString(row chunk.Row) (d string, isNull bool, err error) {
	var x int64

	x, isNull, err = b.Args[0].EvalInt(b.Ctx, row)
	if isNull || err != nil {
		return d, isNull, err
	}
	if x < 0 {
		x = 0
	}
	if uint64(x) > b.maxAllowedPacket {
		b.Ctx.GetSessionVars().StmtCtx.AppendWarning(errWarnAllowedPacketOverflowed.GenWithStackByArgs("space", b.maxAllowedPacket))
		return d, true, nil
	}
	if x > mysql.MaxBlobWidth {
		return d, true, nil
	}
	return strings.Repeat(" ", int(x)), false, nil
}

type upperFunctionClass struct {
	BaseFunctionClass
}

func (c *upperFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETString, types.ETString)
	argTp := args[0].GetType()
	bf.Tp.Flen = argTp.Flen
	SetBinFlagOrBinStr(argTp, bf.Tp)
	sig := &builtinUpperSig{bf}
	return sig, nil
}

type builtinUpperSig struct {
	BaseBuiltinFunc
}

func (b *builtinUpperSig) Clone() BuiltinFunc {
	newSig := &builtinUpperSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalString evals a builtinUpperSig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_upper
func (b *builtinUpperSig) EvalString(row chunk.Row) (d string, isNull bool, err error) {
	d, isNull, err = b.Args[0].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return d, isNull, err
	}

	if types.IsBinaryStr(b.Args[0].GetType()) {
		return d, false, nil
	}

	return strings.ToUpper(d), false, nil
}

type strcmpFunctionClass struct {
	BaseFunctionClass
}

func (c *strcmpFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETString, types.ETString)
	bf.Tp.Flen = 2
	types.SetBinChsClnFlag(bf.Tp)
	sig := &builtinStrcmpSig{bf}
	return sig, nil
}

type builtinStrcmpSig struct {
	BaseBuiltinFunc
}

func (b *builtinStrcmpSig) Clone() BuiltinFunc {
	newSig := &builtinStrcmpSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalInt evals a builtinStrcmpSig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-comparison-functions.html
func (b *builtinStrcmpSig) evalInt(row chunk.Row) (int64, bool, error) {
	var (
		left, right string
		isNull      bool
		err         error
	)

	left, isNull, err = b.Args[0].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	right, isNull, err = b.Args[1].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	res := types.CompareString(left, right)
	return int64(res), false, nil
}

type replaceFunctionClass struct {
	BaseFunctionClass
}

func (c *replaceFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETString, types.ETString, types.ETString, types.ETString)
	bf.Tp.Flen = c.fixLength(args)
	for _, a := range args {
		SetBinFlagOrBinStr(a.GetType(), bf.Tp)
	}
	sig := &builtinReplaceSig{bf}
	return sig, nil
}

// fixLength calculate the Flen of the return type.
func (c *replaceFunctionClass) fixLength(args []Expression) int {
	charLen := args[0].GetType().Flen
	oldStrLen := args[1].GetType().Flen
	diff := args[2].GetType().Flen - oldStrLen
	if diff > 0 && oldStrLen > 0 {
		charLen += (charLen / oldStrLen) * diff
	}
	return charLen
}

type builtinReplaceSig struct {
	BaseBuiltinFunc
}

func (b *builtinReplaceSig) Clone() BuiltinFunc {
	newSig := &builtinReplaceSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalString evals a builtinReplaceSig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_replace
func (b *builtinReplaceSig) EvalString(row chunk.Row) (d string, isNull bool, err error) {
	var str, oldStr, newStr string

	str, isNull, err = b.Args[0].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return d, isNull, err
	}
	oldStr, isNull, err = b.Args[1].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return d, isNull, err
	}
	newStr, isNull, err = b.Args[2].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return d, isNull, err
	}
	if oldStr == "" {
		return str, false, nil
	}
	return strings.Replace(str, oldStr, newStr, -1), false, nil
}

type convertFunctionClass struct {
	BaseFunctionClass
}

func (c *convertFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETString, types.ETString, types.ETString)

	charsetArg, ok := args[1].(*Constant)
	if !ok {
		// `args[1]` is limited by parser to be a constant string,
		// should never go into here.
		return nil, errIncorrectArgs.GenWithStackByArgs("charset")
	}
	transcodingName := charsetArg.Value.GetString()
	bf.Tp.Charset = strings.ToLower(transcodingName)
	// Quoted about the behavior of syntax CONVERT(expr, type) to CHAR():
	// In all cases, the string has the default collation for the character set.
	// See https://dev.mysql.com/doc/refman/5.7/en/cast-functions.html#function_convert
	// Here in syntax CONVERT(expr USING transcoding_name), behavior is kept the same,
	// picking the default collation of target charset.
	var err error
	bf.Tp.Collate, err = charset.GetDefaultCollation(bf.Tp.Charset)
	if err != nil {
		return nil, errUnknownCharacterSet.GenWithStackByArgs(transcodingName)
	}
	// Result will be a binary string if converts charset to BINARY.
	// See https://dev.mysql.com/doc/refman/5.7/en/charset-binary-set.html
	if types.IsBinaryStr(bf.Tp) {
		types.SetBinChsClnFlag(bf.Tp)
	} else {
		bf.Tp.Flag &= ^mysql.BinaryFlag
	}

	bf.Tp.Flen = mysql.MaxBlobWidth
	sig := &builtinConvertSig{bf}
	return sig, nil
}

type builtinConvertSig struct {
	BaseBuiltinFunc
}

func (b *builtinConvertSig) Clone() BuiltinFunc {
	newSig := &builtinConvertSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalString evals CONVERT(expr USING transcoding_name).
// Syntax CONVERT(expr, type) is parsed as cast expr so not handled here.
// See https://dev.mysql.com/doc/refman/5.7/en/cast-functions.html#function_convert
func (b *builtinConvertSig) EvalString(row chunk.Row) (string, bool, error) {
	expr, isNull, err := b.Args[0].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return "", true, err
	}

	// Since charset is already validated and set from getFunction(), there's no
	// need to get charset from args again.
	encoding, _ := charset.Lookup(b.Tp.Charset)
	// However, if `b.tp.Charset` is abnormally set to a wrong charset, we still
	// return with error.
	if encoding == nil {
		return "", true, errUnknownCharacterSet.GenWithStackByArgs(b.Tp.Charset)
	}

	target, _, err := transform.String(encoding.NewDecoder(), expr)
	return target, err != nil, err
}

type substringFunctionClass struct {
	BaseFunctionClass
}

func (c *substringFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	argTps := []types.EvalType{types.ETString, types.ETInt}
	if len(args) == 3 {
		argTps = append(argTps, types.ETInt)
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETString, argTps...)

	argType := args[0].GetType()
	bf.Tp.Flen = argType.Flen
	SetBinFlagOrBinStr(argType, bf.Tp)

	var sig BuiltinFunc
	switch {
	case len(args) == 3 && types.IsBinaryStr(argType):
		sig = &builtinSubstringBinary3ArgsSig{bf}
	case len(args) == 3:
		sig = &builtinSubstring3ArgsSig{bf}
	case len(args) == 2 && types.IsBinaryStr(argType):
		sig = &builtinSubstringBinary2ArgsSig{bf}
	case len(args) == 2:
		sig = &builtinSubstring2ArgsSig{bf}
	default:
		// Should never happens.
		return nil, errors.Errorf("SUBSTR invalid arg length, expect 2 or 3 but got: %v", len(args))
	}
	return sig, nil
}

type builtinSubstringBinary2ArgsSig struct {
	BaseBuiltinFunc
}

func (b *builtinSubstringBinary2ArgsSig) Clone() BuiltinFunc {
	newSig := &builtinSubstringBinary2ArgsSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalString evals SUBSTR(str,pos), SUBSTR(str FROM pos), SUBSTR() is a synonym for SUBSTRING().
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_substr
func (b *builtinSubstringBinary2ArgsSig) EvalString(row chunk.Row) (string, bool, error) {
	str, isNull, err := b.Args[0].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	pos, isNull, err := b.Args[1].EvalInt(b.Ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	length := int64(len(str))
	if pos < 0 {
		pos += length
	} else {
		pos--
	}
	if pos > length || pos < 0 {
		pos = length
	}
	return str[pos:], false, nil
}

type builtinSubstring2ArgsSig struct {
	BaseBuiltinFunc
}

func (b *builtinSubstring2ArgsSig) Clone() BuiltinFunc {
	newSig := &builtinSubstring2ArgsSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalString evals SUBSTR(str,pos), SUBSTR(str FROM pos), SUBSTR() is a synonym for SUBSTRING().
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_substr
func (b *builtinSubstring2ArgsSig) EvalString(row chunk.Row) (string, bool, error) {
	str, isNull, err := b.Args[0].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	pos, isNull, err := b.Args[1].EvalInt(b.Ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	runes := []rune(str)
	length := int64(len(runes))
	if pos < 0 {
		pos += length
	} else {
		pos--
	}
	if pos > length || pos < 0 {
		pos = length
	}
	return string(runes[pos:]), false, nil
}

type builtinSubstringBinary3ArgsSig struct {
	BaseBuiltinFunc
}

func (b *builtinSubstringBinary3ArgsSig) Clone() BuiltinFunc {
	newSig := &builtinSubstringBinary3ArgsSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalString evals SUBSTR(str,pos,len), SUBSTR(str FROM pos FOR len), SUBSTR() is a synonym for SUBSTRING().
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_substr
func (b *builtinSubstringBinary3ArgsSig) EvalString(row chunk.Row) (string, bool, error) {
	str, isNull, err := b.Args[0].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	pos, isNull, err := b.Args[1].EvalInt(b.Ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	length, isNull, err := b.Args[2].EvalInt(b.Ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	byteLen := int64(len(str))
	if pos < 0 {
		pos += byteLen
	} else {
		pos--
	}
	if pos > byteLen || pos < 0 {
		pos = byteLen
	}
	end := pos + length
	if end < pos {
		return "", false, nil
	} else if end < byteLen {
		return str[pos:end], false, nil
	}
	return str[pos:], false, nil
}

type builtinSubstring3ArgsSig struct {
	BaseBuiltinFunc
}

func (b *builtinSubstring3ArgsSig) Clone() BuiltinFunc {
	newSig := &builtinSubstring3ArgsSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalString evals SUBSTR(str,pos,len), SUBSTR(str FROM pos FOR len), SUBSTR() is a synonym for SUBSTRING().
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_substr
func (b *builtinSubstring3ArgsSig) EvalString(row chunk.Row) (string, bool, error) {
	str, isNull, err := b.Args[0].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	pos, isNull, err := b.Args[1].EvalInt(b.Ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	length, isNull, err := b.Args[2].EvalInt(b.Ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	runes := []rune(str)
	numRunes := int64(len(runes))
	if pos < 0 {
		pos += numRunes
	} else {
		pos--
	}
	if pos > numRunes || pos < 0 {
		pos = numRunes
	}
	end := pos + length
	if end < pos {
		return "", false, nil
	} else if end < numRunes {
		return string(runes[pos:end]), false, nil
	}
	return string(runes[pos:]), false, nil
}

type substringIndexFunctionClass struct {
	BaseFunctionClass
}

func (c *substringIndexFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETString, types.ETString, types.ETString, types.ETInt)
	argType := args[0].GetType()
	bf.Tp.Flen = argType.Flen
	SetBinFlagOrBinStr(argType, bf.Tp)
	sig := &builtinSubstringIndexSig{bf}
	return sig, nil
}

type builtinSubstringIndexSig struct {
	BaseBuiltinFunc
}

func (b *builtinSubstringIndexSig) Clone() BuiltinFunc {
	newSig := &builtinSubstringIndexSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalString evals a builtinSubstringIndexSig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_substring-index
func (b *builtinSubstringIndexSig) EvalString(row chunk.Row) (d string, isNull bool, err error) {
	var (
		str, delim string
		count      int64
	)
	str, isNull, err = b.Args[0].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return d, isNull, err
	}
	delim, isNull, err = b.Args[1].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return d, isNull, err
	}
	count, isNull, err = b.Args[2].EvalInt(b.Ctx, row)
	if isNull || err != nil {
		return d, isNull, err
	}
	if len(delim) == 0 {
		return "", false, nil
	}

	strs := strings.Split(str, delim)
	start, end := int64(0), int64(len(strs))
	if count > 0 {
		// If count is positive, everything to the left of the final delimiter (counting from the left) is returned.
		if count < end {
			end = count
		}
	} else {
		// If count is negative, everything to the right of the final delimiter (counting from the right) is returned.
		count = -count
		if count < 0 {
			// -count overflows max int64, returns an empty string.
			return "", false, nil
		}

		if count < end {
			start = end - count
		}
	}
	substrs := strs[start:end]
	return strings.Join(substrs, delim), false, nil
}

type locateFunctionClass struct {
	BaseFunctionClass
}

func (c *locateFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	hasStartPos, argTps := len(args) == 3, []types.EvalType{types.ETString, types.ETString}
	if hasStartPos {
		argTps = append(argTps, types.ETInt)
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETInt, argTps...)
	var sig BuiltinFunc
	// Loacte is multibyte safe, and is case-sensitive only if at least one argument is a binary string.
	hasBianryInput := types.IsBinaryStr(args[0].GetType()) || types.IsBinaryStr(args[1].GetType())
	switch {
	case hasStartPos && hasBianryInput:
		sig = &builtinLocateBinary3ArgsSig{bf}
	case hasStartPos:
		sig = &builtinLocate3ArgsSig{bf}
	case hasBianryInput:
		sig = &builtinLocateBinary2ArgsSig{bf}
	default:
		sig = &builtinLocate2ArgsSig{bf}
	}
	return sig, nil
}

type builtinLocateBinary2ArgsSig struct {
	BaseBuiltinFunc
}

func (b *builtinLocateBinary2ArgsSig) Clone() BuiltinFunc {
	newSig := &builtinLocateBinary2ArgsSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalInt evals LOCATE(substr,str), case-sensitive.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_locate
func (b *builtinLocateBinary2ArgsSig) evalInt(row chunk.Row) (int64, bool, error) {
	subStr, isNull, err := b.Args[0].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	str, isNull, err := b.Args[1].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	subStrLen := len(subStr)
	if subStrLen == 0 {
		return 1, false, nil
	}
	ret, idx := 0, strings.Index(str, subStr)
	if idx != -1 {
		ret = idx + 1
	}
	return int64(ret), false, nil
}

type builtinLocate2ArgsSig struct {
	BaseBuiltinFunc
}

func (b *builtinLocate2ArgsSig) Clone() BuiltinFunc {
	newSig := &builtinLocate2ArgsSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalInt evals LOCATE(substr,str), non case-sensitive.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_locate
func (b *builtinLocate2ArgsSig) evalInt(row chunk.Row) (int64, bool, error) {
	subStr, isNull, err := b.Args[0].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	str, isNull, err := b.Args[1].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	if int64(len([]rune(subStr))) == 0 {
		return 1, false, nil
	}
	slice := string([]rune(strings.ToLower(str)))
	ret, idx := 0, strings.Index(slice, strings.ToLower(subStr))
	if idx != -1 {
		ret = utf8.RuneCountInString(slice[:idx]) + 1
	}
	return int64(ret), false, nil
}

type builtinLocateBinary3ArgsSig struct {
	BaseBuiltinFunc
}

func (b *builtinLocateBinary3ArgsSig) Clone() BuiltinFunc {
	newSig := &builtinLocateBinary3ArgsSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalInt evals LOCATE(substr,str,pos), case-sensitive.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_locate
func (b *builtinLocateBinary3ArgsSig) evalInt(row chunk.Row) (int64, bool, error) {
	subStr, isNull, err := b.Args[0].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	str, isNull, err := b.Args[1].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	pos, isNull, err := b.Args[2].EvalInt(b.Ctx, row)
	// Transfer the argument which starts from 1 to real index which starts from 0.
	pos--
	if isNull || err != nil {
		return 0, isNull, err
	}
	subStrLen := len(subStr)
	if pos < 0 || pos > int64(len(str)-subStrLen) {
		return 0, false, nil
	} else if subStrLen == 0 {
		return pos + 1, false, nil
	}
	slice := str[pos:]
	idx := strings.Index(slice, subStr)
	if idx != -1 {
		return pos + int64(idx) + 1, false, nil
	}
	return 0, false, nil
}

type builtinLocate3ArgsSig struct {
	BaseBuiltinFunc
}

func (b *builtinLocate3ArgsSig) Clone() BuiltinFunc {
	newSig := &builtinLocate3ArgsSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalInt evals LOCATE(substr,str,pos), non case-sensitive.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_locate
func (b *builtinLocate3ArgsSig) evalInt(row chunk.Row) (int64, bool, error) {
	subStr, isNull, err := b.Args[0].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	str, isNull, err := b.Args[1].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	pos, isNull, err := b.Args[2].EvalInt(b.Ctx, row)
	// Transfer the argument which starts from 1 to real index which starts from 0.
	pos--
	if isNull || err != nil {
		return 0, isNull, err
	}
	subStrLen := len([]rune(subStr))
	if pos < 0 || pos > int64(len([]rune(strings.ToLower(str)))-subStrLen) {
		return 0, false, nil
	} else if subStrLen == 0 {
		return pos + 1, false, nil
	}
	slice := string([]rune(strings.ToLower(str))[pos:])
	idx := strings.Index(slice, strings.ToLower(subStr))
	if idx != -1 {
		return pos + int64(utf8.RuneCountInString(slice[:idx])) + 1, false, nil
	}
	return 0, false, nil
}

type hexFunctionClass struct {
	BaseFunctionClass
}

func (c *hexFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}

	argTp := args[0].GetType().EvalType()
	switch argTp {
	case types.ETString, types.ETDatetime, types.ETTimestamp, types.ETDuration, types.ETJson:
		bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETString, types.ETString)
		// Use UTF-8 as default
		bf.Tp.Flen = args[0].GetType().Flen * 3 * 2
		sig := &builtinHexStrArgSig{bf}
		return sig, nil
	case types.ETInt, types.ETReal, types.ETDecimal:
		bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETString, types.ETInt)
		bf.Tp.Flen = args[0].GetType().Flen * 2
		sig := &builtinHexIntArgSig{bf}
		return sig, nil
	default:
		return nil, errors.Errorf("Hex invalid args, need int or string but get %T", args[0].GetType())
	}
}

type builtinHexStrArgSig struct {
	BaseBuiltinFunc
}

func (b *builtinHexStrArgSig) Clone() BuiltinFunc {
	newSig := &builtinHexStrArgSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalString evals a builtinHexStrArgSig, corresponding to hex(str)
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_hex
func (b *builtinHexStrArgSig) EvalString(row chunk.Row) (string, bool, error) {
	d, isNull, err := b.Args[0].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return d, isNull, err
	}
	return strings.ToUpper(hex.EncodeToString(hack.Slice(d))), false, nil
}

type builtinHexIntArgSig struct {
	BaseBuiltinFunc
}

func (b *builtinHexIntArgSig) Clone() BuiltinFunc {
	newSig := &builtinHexIntArgSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalString evals a builtinHexIntArgSig, corresponding to hex(N)
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_hex
func (b *builtinHexIntArgSig) EvalString(row chunk.Row) (string, bool, error) {
	x, isNull, err := b.Args[0].EvalInt(b.Ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	return strings.ToUpper(fmt.Sprintf("%x", uint64(x))), false, nil
}

type unhexFunctionClass struct {
	BaseFunctionClass
}

func (c *unhexFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	var retFlen int

	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	argType := args[0].GetType()
	argEvalTp := argType.EvalType()
	switch argEvalTp {
	case types.ETString, types.ETDatetime, types.ETTimestamp, types.ETDuration, types.ETJson:
		// Use UTF-8 as default charset, so there're (Flen * 3 + 1) / 2 byte-pairs
		retFlen = (argType.Flen*3 + 1) / 2
	case types.ETInt, types.ETReal, types.ETDecimal:
		// For number value, there're (Flen + 1) / 2 byte-pairs
		retFlen = (argType.Flen + 1) / 2
	default:
		return nil, errors.Errorf("Unhex invalid args, need int or string but get %s", argType)
	}

	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETString, types.ETString)
	bf.Tp.Flen = retFlen
	types.SetBinChsClnFlag(bf.Tp)
	sig := &builtinUnHexSig{bf}
	return sig, nil
}

type builtinUnHexSig struct {
	BaseBuiltinFunc
}

func (b *builtinUnHexSig) Clone() BuiltinFunc {
	newSig := &builtinUnHexSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalString evals a builtinUnHexSig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_unhex
func (b *builtinUnHexSig) EvalString(row chunk.Row) (string, bool, error) {
	var bs []byte

	d, isNull, err := b.Args[0].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return d, isNull, err
	}
	// Add a '0' to the front, if the length is not the multiple of 2
	if len(d)%2 != 0 {
		d = "0" + d
	}
	bs, err = hex.DecodeString(d)
	if err != nil {
		return "", true, nil
	}
	return string(bs), false, nil
}

const spaceChars = " "

type trimFunctionClass struct {
	BaseFunctionClass
}

// getFunction sets trim built-in function signature.
// The syntax of trim in mysql is 'TRIM([{BOTH | LEADING | TRAILING} [remstr] FROM] str), TRIM([remstr FROM] str)',
// but we wil convert it into trim(str), trim(str, remstr) and trim(str, remstr, direction) in AST.
func (c *trimFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}

	switch len(args) {
	case 1:
		bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETString, types.ETString)
		argType := args[0].GetType()
		bf.Tp.Flen = argType.Flen
		SetBinFlagOrBinStr(argType, bf.Tp)
		sig := &builtinTrim1ArgSig{bf}
		return sig, nil

	case 2:
		bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETString, types.ETString, types.ETString)
		argType := args[0].GetType()
		SetBinFlagOrBinStr(argType, bf.Tp)
		sig := &builtinTrim2ArgsSig{bf}
		return sig, nil

	case 3:
		bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETString, types.ETString, types.ETString, types.ETInt)
		argType := args[0].GetType()
		bf.Tp.Flen = argType.Flen
		SetBinFlagOrBinStr(argType, bf.Tp)
		sig := &builtinTrim3ArgsSig{bf}
		return sig, nil

	default:
		return nil, c.VerifyArgs(args)
	}
}

type builtinTrim1ArgSig struct {
	BaseBuiltinFunc
}

func (b *builtinTrim1ArgSig) Clone() BuiltinFunc {
	newSig := &builtinTrim1ArgSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalString evals a builtinTrim1ArgSig, corresponding to trim(str)
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_trim
func (b *builtinTrim1ArgSig) EvalString(row chunk.Row) (d string, isNull bool, err error) {
	d, isNull, err = b.Args[0].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return d, isNull, err
	}
	return strings.Trim(d, spaceChars), false, nil
}

type builtinTrim2ArgsSig struct {
	BaseBuiltinFunc
}

func (b *builtinTrim2ArgsSig) Clone() BuiltinFunc {
	newSig := &builtinTrim2ArgsSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalString evals a builtinTrim2ArgsSig, corresponding to trim(str, remstr)
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_trim
func (b *builtinTrim2ArgsSig) EvalString(row chunk.Row) (d string, isNull bool, err error) {
	var str, remstr string

	str, isNull, err = b.Args[0].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return d, isNull, err
	}
	remstr, isNull, err = b.Args[1].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return d, isNull, err
	}
	d = trimLeft(str, remstr)
	d = trimRight(d, remstr)
	return d, false, nil
}

type builtinTrim3ArgsSig struct {
	BaseBuiltinFunc
}

func (b *builtinTrim3ArgsSig) Clone() BuiltinFunc {
	newSig := &builtinTrim3ArgsSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalString evals a builtinTrim3ArgsSig, corresponding to trim(str, remstr, direction)
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_trim
func (b *builtinTrim3ArgsSig) EvalString(row chunk.Row) (d string, isNull bool, err error) {
	var (
		str, remstr  string
		x            int64
		direction    ast.TrimDirectionType
		isRemStrNull bool
	)
	str, isNull, err = b.Args[0].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return d, isNull, err
	}
	remstr, isRemStrNull, err = b.Args[1].EvalString(b.Ctx, row)
	if err != nil {
		return d, isNull, err
	}
	x, isNull, err = b.Args[2].EvalInt(b.Ctx, row)
	if isNull || err != nil {
		return d, isNull, err
	}
	direction = ast.TrimDirectionType(x)
	if direction == ast.TrimLeading {
		if isRemStrNull {
			d = strings.TrimLeft(str, spaceChars)
		} else {
			d = trimLeft(str, remstr)
		}
	} else if direction == ast.TrimTrailing {
		if isRemStrNull {
			d = strings.TrimRight(str, spaceChars)
		} else {
			d = trimRight(str, remstr)
		}
	} else {
		if isRemStrNull {
			d = strings.Trim(str, spaceChars)
		} else {
			d = trimLeft(str, remstr)
			d = trimRight(d, remstr)
		}
	}
	return d, false, nil
}

type lTrimFunctionClass struct {
	BaseFunctionClass
}

func (c *lTrimFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETString, types.ETString)
	argType := args[0].GetType()
	bf.Tp.Flen = argType.Flen
	SetBinFlagOrBinStr(argType, bf.Tp)
	sig := &builtinLTrimSig{bf}
	return sig, nil
}

type builtinLTrimSig struct {
	BaseBuiltinFunc
}

func (b *builtinLTrimSig) Clone() BuiltinFunc {
	newSig := &builtinLTrimSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalString evals a builtinLTrimSig
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_ltrim
func (b *builtinLTrimSig) EvalString(row chunk.Row) (d string, isNull bool, err error) {
	d, isNull, err = b.Args[0].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return d, isNull, err
	}
	return strings.TrimLeft(d, spaceChars), false, nil
}

type rTrimFunctionClass struct {
	BaseFunctionClass
}

func (c *rTrimFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETString, types.ETString)
	argType := args[0].GetType()
	bf.Tp.Flen = argType.Flen
	SetBinFlagOrBinStr(argType, bf.Tp)
	sig := &builtinRTrimSig{bf}
	return sig, nil
}

type builtinRTrimSig struct {
	BaseBuiltinFunc
}

func (b *builtinRTrimSig) Clone() BuiltinFunc {
	newSig := &builtinRTrimSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalString evals a builtinRTrimSig
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_rtrim
func (b *builtinRTrimSig) EvalString(row chunk.Row) (d string, isNull bool, err error) {
	d, isNull, err = b.Args[0].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return d, isNull, err
	}
	return strings.TrimRight(d, spaceChars), false, nil
}

func trimLeft(str, remstr string) string {
	for {
		x := strings.TrimPrefix(str, remstr)
		if len(x) == len(str) {
			return x
		}
		str = x
	}
}

func trimRight(str, remstr string) string {
	for {
		x := strings.TrimSuffix(str, remstr)
		if len(x) == len(str) {
			return x
		}
		str = x
	}
}

func getFlen4LpadAndRpad(ctx sessionctx.Context, arg Expression) int {
	if constant, ok := arg.(*Constant); ok {
		length, isNull, err := constant.EvalInt(ctx, chunk.Row{})
		if err != nil {
			logutil.BgLogger().Error("eval `Flen` for LPAD/RPAD", zap.Error(err))
		}
		if isNull || err != nil || length > mysql.MaxBlobWidth {
			return mysql.MaxBlobWidth
		}
		return int(length)
	}
	return mysql.MaxBlobWidth
}

type lpadFunctionClass struct {
	BaseFunctionClass
}

func (c *lpadFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETString, types.ETString, types.ETInt, types.ETString)
	bf.Tp.Flen = getFlen4LpadAndRpad(bf.Ctx, args[1])
	SetBinFlagOrBinStr(args[0].GetType(), bf.Tp)
	SetBinFlagOrBinStr(args[2].GetType(), bf.Tp)

	valStr, _ := ctx.GetSessionVars().GetSystemVar(variable.MaxAllowedPacket)
	maxAllowedPacket, err := strconv.ParseUint(valStr, 10, 64)
	if err != nil {
		return nil, err
	}

	if types.IsBinaryStr(args[0].GetType()) || types.IsBinaryStr(args[2].GetType()) {
		sig := &builtinLpadBinarySig{bf, maxAllowedPacket}
		return sig, nil
	}
	if bf.Tp.Flen *= 4; bf.Tp.Flen > mysql.MaxBlobWidth {
		bf.Tp.Flen = mysql.MaxBlobWidth
	}
	sig := &builtinLpadSig{bf, maxAllowedPacket}
	return sig, nil
}

type builtinLpadBinarySig struct {
	BaseBuiltinFunc
	maxAllowedPacket uint64
}

func (b *builtinLpadBinarySig) Clone() BuiltinFunc {
	newSig := &builtinLpadBinarySig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	newSig.maxAllowedPacket = b.maxAllowedPacket
	return newSig
}

// evalString evals LPAD(str,len,padstr).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_lpad
func (b *builtinLpadBinarySig) EvalString(row chunk.Row) (string, bool, error) {
	str, isNull, err := b.Args[0].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	byteLength := len(str)

	length, isNull, err := b.Args[1].EvalInt(b.Ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	targetLength := int(length)

	if uint64(targetLength) > b.maxAllowedPacket {
		b.Ctx.GetSessionVars().StmtCtx.AppendWarning(errWarnAllowedPacketOverflowed.GenWithStackByArgs("lpad", b.maxAllowedPacket))
		return "", true, nil
	}

	padStr, isNull, err := b.Args[2].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	padLength := len(padStr)

	if targetLength < 0 || targetLength > b.Tp.Flen || (byteLength < targetLength && padLength == 0) {
		return "", true, nil
	}

	if tailLen := targetLength - byteLength; tailLen > 0 {
		repeatCount := tailLen/padLength + 1
		str = strings.Repeat(padStr, repeatCount)[:tailLen] + str
	}
	return str[:targetLength], false, nil
}

type builtinLpadSig struct {
	BaseBuiltinFunc
	maxAllowedPacket uint64
}

func (b *builtinLpadSig) Clone() BuiltinFunc {
	newSig := &builtinLpadSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	newSig.maxAllowedPacket = b.maxAllowedPacket
	return newSig
}

// evalString evals LPAD(str,len,padstr).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_lpad
func (b *builtinLpadSig) EvalString(row chunk.Row) (string, bool, error) {
	str, isNull, err := b.Args[0].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	runeLength := len([]rune(str))

	length, isNull, err := b.Args[1].EvalInt(b.Ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	targetLength := int(length)

	if uint64(targetLength)*uint64(mysql.MaxBytesOfCharacter) > b.maxAllowedPacket {
		b.Ctx.GetSessionVars().StmtCtx.AppendWarning(errWarnAllowedPacketOverflowed.GenWithStackByArgs("lpad", b.maxAllowedPacket))
		return "", true, nil
	}

	padStr, isNull, err := b.Args[2].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	padLength := len([]rune(padStr))

	if targetLength < 0 || targetLength*4 > b.Tp.Flen || (runeLength < targetLength && padLength == 0) {
		return "", true, nil
	}

	if tailLen := targetLength - runeLength; tailLen > 0 {
		repeatCount := tailLen/padLength + 1
		str = string([]rune(strings.Repeat(padStr, repeatCount))[:tailLen]) + str
	}
	return string([]rune(str)[:targetLength]), false, nil
}

type rpadFunctionClass struct {
	BaseFunctionClass
}

func (c *rpadFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETString, types.ETString, types.ETInt, types.ETString)
	bf.Tp.Flen = getFlen4LpadAndRpad(bf.Ctx, args[1])
	SetBinFlagOrBinStr(args[0].GetType(), bf.Tp)
	SetBinFlagOrBinStr(args[2].GetType(), bf.Tp)

	valStr, _ := ctx.GetSessionVars().GetSystemVar(variable.MaxAllowedPacket)
	maxAllowedPacket, err := strconv.ParseUint(valStr, 10, 64)
	if err != nil {
		return nil, err
	}

	if types.IsBinaryStr(args[0].GetType()) || types.IsBinaryStr(args[2].GetType()) {
		sig := &builtinRpadBinarySig{bf, maxAllowedPacket}
		return sig, nil
	}
	if bf.Tp.Flen *= 4; bf.Tp.Flen > mysql.MaxBlobWidth {
		bf.Tp.Flen = mysql.MaxBlobWidth
	}
	sig := &builtinRpadSig{bf, maxAllowedPacket}
	return sig, nil
}

type builtinRpadBinarySig struct {
	BaseBuiltinFunc
	maxAllowedPacket uint64
}

func (b *builtinRpadBinarySig) Clone() BuiltinFunc {
	newSig := &builtinRpadBinarySig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	newSig.maxAllowedPacket = b.maxAllowedPacket
	return newSig
}

// evalString evals RPAD(str,len,padstr).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_rpad
func (b *builtinRpadBinarySig) EvalString(row chunk.Row) (string, bool, error) {
	str, isNull, err := b.Args[0].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	byteLength := len(str)

	length, isNull, err := b.Args[1].EvalInt(b.Ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	targetLength := int(length)
	if uint64(targetLength) > b.maxAllowedPacket {
		b.Ctx.GetSessionVars().StmtCtx.AppendWarning(errWarnAllowedPacketOverflowed.GenWithStackByArgs("rpad", b.maxAllowedPacket))
		return "", true, nil
	}

	padStr, isNull, err := b.Args[2].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	padLength := len(padStr)

	if targetLength < 0 || targetLength > b.Tp.Flen || (byteLength < targetLength && padLength == 0) {
		return "", true, nil
	}

	if tailLen := targetLength - byteLength; tailLen > 0 {
		repeatCount := tailLen/padLength + 1
		str = str + strings.Repeat(padStr, repeatCount)
	}
	return str[:targetLength], false, nil
}

type builtinRpadSig struct {
	BaseBuiltinFunc
	maxAllowedPacket uint64
}

func (b *builtinRpadSig) Clone() BuiltinFunc {
	newSig := &builtinRpadSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	newSig.maxAllowedPacket = b.maxAllowedPacket
	return newSig
}

// evalString evals RPAD(str,len,padstr).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_rpad
func (b *builtinRpadSig) EvalString(row chunk.Row) (string, bool, error) {
	str, isNull, err := b.Args[0].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	runeLength := len([]rune(str))

	length, isNull, err := b.Args[1].EvalInt(b.Ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	targetLength := int(length)

	if uint64(targetLength)*uint64(mysql.MaxBytesOfCharacter) > b.maxAllowedPacket {
		b.Ctx.GetSessionVars().StmtCtx.AppendWarning(errWarnAllowedPacketOverflowed.GenWithStackByArgs("rpad", b.maxAllowedPacket))
		return "", true, nil
	}

	padStr, isNull, err := b.Args[2].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	padLength := len([]rune(padStr))

	if targetLength < 0 || targetLength*4 > b.Tp.Flen || (runeLength < targetLength && padLength == 0) {
		return "", true, nil
	}

	if tailLen := targetLength - runeLength; tailLen > 0 {
		repeatCount := tailLen/padLength + 1
		str = str + strings.Repeat(padStr, repeatCount)
	}
	return string([]rune(str)[:targetLength]), false, nil
}

type bitLengthFunctionClass struct {
	BaseFunctionClass
}

func (c *bitLengthFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETString)
	bf.Tp.Flen = 10
	sig := &builtinBitLengthSig{bf}
	return sig, nil
}

type builtinBitLengthSig struct {
	BaseBuiltinFunc
}

func (b *builtinBitLengthSig) Clone() BuiltinFunc {
	newSig := &builtinBitLengthSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalInt evaluates a builtinBitLengthSig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_bit-length
func (b *builtinBitLengthSig) evalInt(row chunk.Row) (int64, bool, error) {
	val, isNull, err := b.Args[0].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}

	return int64(len(val) * 8), false, nil
}

type charFunctionClass struct {
	BaseFunctionClass
}

func (c *charFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	argTps := make([]types.EvalType, 0, len(args))
	for i := 0; i < len(args)-1; i++ {
		argTps = append(argTps, types.ETInt)
	}
	argTps = append(argTps, types.ETString)
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETString, argTps...)
	bf.Tp.Flen = 4 * (len(args) - 1)
	types.SetBinChsClnFlag(bf.Tp)

	sig := &builtinCharSig{bf}
	return sig, nil
}

type builtinCharSig struct {
	BaseBuiltinFunc
}

func (b *builtinCharSig) Clone() BuiltinFunc {
	newSig := &builtinCharSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinCharSig) convertToBytes(ints []int64) []byte {
	buffer := bytes.NewBuffer([]byte{})
	for i := len(ints) - 1; i >= 0; i-- {
		for count, val := 0, ints[i]; count < 4; count++ {
			buffer.WriteByte(byte(val & 0xff))
			if val >>= 8; val == 0 {
				break
			}
		}
	}
	return reverseBytes(buffer.Bytes())
}

// evalString evals CHAR(N,... [USING charset_name]).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_char.
func (b *builtinCharSig) EvalString(row chunk.Row) (string, bool, error) {
	bigints := make([]int64, 0, len(b.Args)-1)

	for i := 0; i < len(b.Args)-1; i++ {
		val, IsNull, err := b.Args[i].EvalInt(b.Ctx, row)
		if err != nil {
			return "", true, err
		}
		if IsNull {
			continue
		}
		bigints = append(bigints, val)
	}
	// The last argument represents the charset name after "using".
	// Use default charset utf8 if it is nil.
	argCharset, IsNull, err := b.Args[len(b.Args)-1].EvalString(b.Ctx, row)
	if err != nil {
		return "", true, err
	}

	result := string(b.convertToBytes(bigints))
	charsetLabel := strings.ToLower(argCharset)
	if IsNull || charsetLabel == "ascii" || strings.HasPrefix(charsetLabel, "utf8") {
		return result, false, nil
	}

	encoding, charsetName := charset.Lookup(charsetLabel)
	if encoding == nil {
		return "", true, errors.Errorf("unknown encoding: %s", argCharset)
	}

	oldStr := result
	result, _, err = transform.String(encoding.NewDecoder(), result)
	if err != nil {
		logutil.BgLogger().Warn("change charset of string",
			zap.String("string", oldStr),
			zap.String("charset", charsetName),
			zap.Error(err))
		return "", true, err
	}
	return result, false, nil
}

type charLengthFunctionClass struct {
	BaseFunctionClass
}

func (c *charLengthFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if argsErr := c.VerifyArgs(args); argsErr != nil {
		return nil, argsErr
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETString)
	if types.IsBinaryStr(args[0].GetType()) {
		sig := &builtinCharLengthBinarySig{bf}
		return sig, nil
	}
	sig := &builtinCharLengthSig{bf}
	return sig, nil
}

type builtinCharLengthBinarySig struct {
	BaseBuiltinFunc
}

func (b *builtinCharLengthBinarySig) Clone() BuiltinFunc {
	newSig := &builtinCharLengthBinarySig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalInt evals a builtinCharLengthSig for binary string type.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_char-length
func (b *builtinCharLengthBinarySig) evalInt(row chunk.Row) (int64, bool, error) {
	val, isNull, err := b.Args[0].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	return int64(len(val)), false, nil
}

type builtinCharLengthSig struct {
	BaseBuiltinFunc
}

func (b *builtinCharLengthSig) Clone() BuiltinFunc {
	newSig := &builtinCharLengthSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalInt evals a builtinCharLengthSig for non-binary string type.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_char-length
func (b *builtinCharLengthSig) evalInt(row chunk.Row) (int64, bool, error) {
	val, isNull, err := b.Args[0].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	return int64(len([]rune(val))), false, nil
}

type findInSetFunctionClass struct {
	BaseFunctionClass
}

func (c *findInSetFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETString, types.ETString)
	bf.Tp.Flen = 3
	sig := &builtinFindInSetSig{bf}
	return sig, nil
}

type builtinFindInSetSig struct {
	BaseBuiltinFunc
}

func (b *builtinFindInSetSig) Clone() BuiltinFunc {
	newSig := &builtinFindInSetSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalInt evals FIND_IN_SET(str,strlist).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_find-in-set
// TODO: This function can be optimized by using bit arithmetic when the first argument is
// a constant string and the second is a column of type SET.
func (b *builtinFindInSetSig) evalInt(row chunk.Row) (int64, bool, error) {
	str, isNull, err := b.Args[0].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}

	strlist, isNull, err := b.Args[1].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}

	if len(strlist) == 0 {
		return 0, false, nil
	}

	for i, strInSet := range strings.Split(strlist, ",") {
		if str == strInSet {
			return int64(i + 1), false, nil
		}
	}
	return 0, false, nil
}

type fieldFunctionClass struct {
	BaseFunctionClass
}

func (c *fieldFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}

	isAllString, isAllNumber := true, true
	for i, length := 0, len(args); i < length; i++ {
		argTp := args[i].GetType().EvalType()
		isAllString = isAllString && (argTp == types.ETString)
		isAllNumber = isAllNumber && (argTp == types.ETInt)
	}

	argTps := make([]types.EvalType, len(args))
	argTp := types.ETReal
	if isAllString {
		argTp = types.ETString
	} else if isAllNumber {
		argTp = types.ETInt
	}
	for i, length := 0, len(args); i < length; i++ {
		argTps[i] = argTp
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETInt, argTps...)
	var sig BuiltinFunc
	switch argTp {
	case types.ETReal:
		sig = &builtinFieldRealSig{bf}
	case types.ETInt:
		sig = &builtinFieldIntSig{bf}
	case types.ETString:
		sig = &builtinFieldStringSig{bf}
	}
	return sig, nil
}

type builtinFieldIntSig struct {
	BaseBuiltinFunc
}

func (b *builtinFieldIntSig) Clone() BuiltinFunc {
	newSig := &builtinFieldIntSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalInt evals FIELD(str,str1,str2,str3,...).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_field
func (b *builtinFieldIntSig) evalInt(row chunk.Row) (int64, bool, error) {
	str, isNull, err := b.Args[0].EvalInt(b.Ctx, row)
	if isNull || err != nil {
		return 0, err != nil, err
	}
	for i, length := 1, len(b.Args); i < length; i++ {
		stri, isNull, err := b.Args[i].EvalInt(b.Ctx, row)
		if err != nil {
			return 0, true, err
		}
		if !isNull && str == stri {
			return int64(i), false, nil
		}
	}
	return 0, false, nil
}

type builtinFieldRealSig struct {
	BaseBuiltinFunc
}

func (b *builtinFieldRealSig) Clone() BuiltinFunc {
	newSig := &builtinFieldRealSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalInt evals FIELD(str,str1,str2,str3,...).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_field
func (b *builtinFieldRealSig) evalInt(row chunk.Row) (int64, bool, error) {
	str, isNull, err := b.Args[0].EvalReal(b.Ctx, row)
	if isNull || err != nil {
		return 0, err != nil, err
	}
	for i, length := 1, len(b.Args); i < length; i++ {
		stri, isNull, err := b.Args[i].EvalReal(b.Ctx, row)
		if err != nil {
			return 0, true, err
		}
		if !isNull && str == stri {
			return int64(i), false, nil
		}
	}
	return 0, false, nil
}

type builtinFieldStringSig struct {
	BaseBuiltinFunc
}

func (b *builtinFieldStringSig) Clone() BuiltinFunc {
	newSig := &builtinFieldStringSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalInt evals FIELD(str,str1,str2,str3,...).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_field
func (b *builtinFieldStringSig) evalInt(row chunk.Row) (int64, bool, error) {
	str, isNull, err := b.Args[0].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return 0, err != nil, err
	}
	for i, length := 1, len(b.Args); i < length; i++ {
		stri, isNull, err := b.Args[i].EvalString(b.Ctx, row)
		if err != nil {
			return 0, true, err
		}
		if !isNull && str == stri {
			return int64(i), false, nil
		}
	}
	return 0, false, nil
}

type makeSetFunctionClass struct {
	BaseFunctionClass
}

func (c *makeSetFunctionClass) getFlen(ctx sessionctx.Context, args []Expression) int {
	flen, count := 0, 0
	if constant, ok := args[0].(*Constant); ok {
		bits, isNull, err := constant.EvalInt(ctx, chunk.Row{})
		if err == nil && !isNull {
			for i, length := 1, len(args); i < length; i++ {
				if (bits & (1 << uint(i-1))) != 0 {
					flen += args[i].GetType().Flen
					count++
				}
			}
			if count > 0 {
				flen += count - 1
			}
			return flen
		}
	}
	for i, length := 1, len(args); i < length; i++ {
		flen += args[i].GetType().Flen
	}
	return flen + len(args) - 1 - 1
}

func (c *makeSetFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	argTps := make([]types.EvalType, len(args))
	argTps[0] = types.ETInt
	for i, length := 1, len(args); i < length; i++ {
		argTps[i] = types.ETString
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETString, argTps...)
	for i, length := 0, len(args); i < length; i++ {
		SetBinFlagOrBinStr(args[i].GetType(), bf.Tp)
	}
	bf.Tp.Flen = c.getFlen(bf.Ctx, args)
	if bf.Tp.Flen > mysql.MaxBlobWidth {
		bf.Tp.Flen = mysql.MaxBlobWidth
	}
	sig := &builtinMakeSetSig{bf}
	return sig, nil
}

type builtinMakeSetSig struct {
	BaseBuiltinFunc
}

func (b *builtinMakeSetSig) Clone() BuiltinFunc {
	newSig := &builtinMakeSetSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalString evals MAKE_SET(bits,str1,str2,...).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_make-set
func (b *builtinMakeSetSig) EvalString(row chunk.Row) (string, bool, error) {
	bits, isNull, err := b.Args[0].EvalInt(b.Ctx, row)
	if isNull || err != nil {
		return "", true, err
	}

	sets := make([]string, 0, len(b.Args)-1)
	for i, length := 1, len(b.Args); i < length; i++ {
		if (bits & (1 << uint(i-1))) == 0 {
			continue
		}
		str, isNull, err := b.Args[i].EvalString(b.Ctx, row)
		if err != nil {
			return "", true, err
		}
		if !isNull {
			sets = append(sets, str)
		}
	}

	return strings.Join(sets, ","), false, nil
}

type octFunctionClass struct {
	BaseFunctionClass
}

func (c *octFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	var sig BuiltinFunc
	if IsBinaryLiteral(args[0]) || args[0].GetType().EvalType() == types.ETInt {
		bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETString, types.ETInt)
		bf.Tp.Flen, bf.Tp.Decimal = 64, types.UnspecifiedLength
		sig = &builtinOctIntSig{bf}
	} else {
		bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETString, types.ETString)
		bf.Tp.Flen, bf.Tp.Decimal = 64, types.UnspecifiedLength
		sig = &builtinOctStringSig{bf}
	}

	return sig, nil
}

type builtinOctIntSig struct {
	BaseBuiltinFunc
}

func (b *builtinOctIntSig) Clone() BuiltinFunc {
	newSig := &builtinOctIntSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalString evals OCT(N).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_oct
func (b *builtinOctIntSig) EvalString(row chunk.Row) (string, bool, error) {
	val, isNull, err := b.Args[0].EvalInt(b.Ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}

	return strconv.FormatUint(uint64(val), 8), false, nil
}

type builtinOctStringSig struct {
	BaseBuiltinFunc
}

func (b *builtinOctStringSig) Clone() BuiltinFunc {
	newSig := &builtinOctStringSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalString evals OCT(N).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_oct
func (b *builtinOctStringSig) EvalString(row chunk.Row) (string, bool, error) {
	val, isNull, err := b.Args[0].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}

	negative, overflow := false, false
	val = getValidPrefix(strings.TrimSpace(val), 10)
	if len(val) == 0 {
		return "0", false, nil
	}

	if val[0] == '-' {
		negative, val = true, val[1:]
	}
	numVal, err := strconv.ParseUint(val, 10, 64)
	if err != nil {
		numError, ok := err.(*strconv.NumError)
		if !ok || numError.Err != strconv.ErrRange {
			return "", true, err
		}
		overflow = true
	}
	if negative && !overflow {
		numVal = -numVal
	}
	return strconv.FormatUint(numVal, 8), false, nil
}

type ordFunctionClass struct {
	BaseFunctionClass
}

func (c *ordFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETString)
	bf.Tp.Flen = 10
	sig := &builtinOrdSig{bf}
	return sig, nil
}

type builtinOrdSig struct {
	BaseBuiltinFunc
}

func (b *builtinOrdSig) Clone() BuiltinFunc {
	newSig := &builtinOrdSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalInt evals a builtinOrdSig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_ord
func (b *builtinOrdSig) evalInt(row chunk.Row) (int64, bool, error) {
	str, isNull, err := b.Args[0].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	if len(str) == 0 {
		return 0, false, nil
	}

	_, size := utf8.DecodeRuneInString(str)
	leftMost := str[:size]
	var result int64
	var factor int64 = 1
	for i := len(leftMost) - 1; i >= 0; i-- {
		result += int64(leftMost[i]) * factor
		factor *= 256
	}

	return result, false, nil
}

type quoteFunctionClass struct {
	BaseFunctionClass
}

func (c *quoteFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETString, types.ETString)
	SetBinFlagOrBinStr(args[0].GetType(), bf.Tp)
	bf.Tp.Flen = 2*args[0].GetType().Flen + 2
	if bf.Tp.Flen > mysql.MaxBlobWidth {
		bf.Tp.Flen = mysql.MaxBlobWidth
	}
	sig := &builtinQuoteSig{bf}
	return sig, nil
}

type builtinQuoteSig struct {
	BaseBuiltinFunc
}

func (b *builtinQuoteSig) Clone() BuiltinFunc {
	newSig := &builtinQuoteSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalString evals QUOTE(str).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_quote
func (b *builtinQuoteSig) EvalString(row chunk.Row) (string, bool, error) {
	str, isNull, err := b.Args[0].EvalString(b.Ctx, row)
	if err != nil {
		return "", true, err
	} else if isNull {
		// If the argument is NULL, the return value is the word "NULL" without enclosing single quotation marks. see ref.
		return "NULL", false, err
	}

	runes := []rune(str)
	buffer := bytes.NewBufferString("")
	buffer.WriteRune('\'')
	for i, runeLength := 0, len(runes); i < runeLength; i++ {
		switch runes[i] {
		case '\\', '\'':
			buffer.WriteRune('\\')
			buffer.WriteRune(runes[i])
		case 0:
			buffer.WriteRune('\\')
			buffer.WriteRune('0')
		case '\032':
			buffer.WriteRune('\\')
			buffer.WriteRune('Z')
		default:
			buffer.WriteRune(runes[i])
		}
	}
	buffer.WriteRune('\'')

	return buffer.String(), false, nil
}

type binFunctionClass struct {
	BaseFunctionClass
}

func (c *binFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETString, types.ETInt)
	bf.Tp.Flen = 64
	sig := &builtinBinSig{bf}
	return sig, nil
}

type builtinBinSig struct {
	BaseBuiltinFunc
}

func (b *builtinBinSig) Clone() BuiltinFunc {
	newSig := &builtinBinSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalString evals BIN(N).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_bin
func (b *builtinBinSig) EvalString(row chunk.Row) (string, bool, error) {
	val, isNull, err := b.Args[0].EvalInt(b.Ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	return fmt.Sprintf("%b", uint64(val)), false, nil
}

type eltFunctionClass struct {
	BaseFunctionClass
}

func (c *eltFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if argsErr := c.VerifyArgs(args); argsErr != nil {
		return nil, argsErr
	}
	argTps := make([]types.EvalType, 0, len(args))
	argTps = append(argTps, types.ETInt)
	for i := 1; i < len(args); i++ {
		argTps = append(argTps, types.ETString)
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETString, argTps...)
	for _, arg := range args[1:] {
		argType := arg.GetType()
		if types.IsBinaryStr(argType) {
			types.SetBinChsClnFlag(bf.Tp)
		}
		if argType.Flen > bf.Tp.Flen {
			bf.Tp.Flen = argType.Flen
		}
	}
	sig := &builtinEltSig{bf}
	return sig, nil
}

type builtinEltSig struct {
	BaseBuiltinFunc
}

func (b *builtinEltSig) Clone() BuiltinFunc {
	newSig := &builtinEltSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalString evals a builtinEltSig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_elt
func (b *builtinEltSig) EvalString(row chunk.Row) (string, bool, error) {
	idx, isNull, err := b.Args[0].EvalInt(b.Ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	if idx < 1 || idx >= int64(len(b.Args)) {
		return "", true, nil
	}
	arg, isNull, err := b.Args[idx].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	return arg, false, nil
}

type exportSetFunctionClass struct {
	BaseFunctionClass
}

func (c *exportSetFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (sig BuiltinFunc, err error) {
	if err = c.VerifyArgs(args); err != nil {
		return nil, err
	}
	argTps := make([]types.EvalType, 0, 5)
	argTps = append(argTps, types.ETInt, types.ETString, types.ETString)
	if len(args) > 3 {
		argTps = append(argTps, types.ETString)
	}
	if len(args) > 4 {
		argTps = append(argTps, types.ETInt)
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETString, argTps...)
	bf.Tp.Flen = mysql.MaxBlobWidth
	switch len(args) {
	case 3:
		sig = &builtinExportSet3ArgSig{bf}
	case 4:
		sig = &builtinExportSet4ArgSig{bf}
	case 5:
		sig = &builtinExportSet5ArgSig{bf}
	}
	return sig, nil
}

// exportSet evals EXPORT_SET(bits,on,off,separator,number_of_bits).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_export-set
func exportSet(bits int64, on, off, separator string, numberOfBits int64) string {
	result := ""
	for i := uint64(0); i < uint64(numberOfBits); i++ {
		if (bits & (1 << i)) > 0 {
			result += on
		} else {
			result += off
		}
		if i < uint64(numberOfBits)-1 {
			result += separator
		}
	}
	return result
}

type builtinExportSet3ArgSig struct {
	BaseBuiltinFunc
}

func (b *builtinExportSet3ArgSig) Clone() BuiltinFunc {
	newSig := &builtinExportSet3ArgSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalString evals EXPORT_SET(bits,on,off).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_export-set
func (b *builtinExportSet3ArgSig) EvalString(row chunk.Row) (string, bool, error) {
	bits, isNull, err := b.Args[0].EvalInt(b.Ctx, row)
	if isNull || err != nil {
		return "", true, err
	}

	on, isNull, err := b.Args[1].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return "", true, err
	}

	off, isNull, err := b.Args[2].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return "", true, err
	}

	return exportSet(bits, on, off, ",", 64), false, nil
}

type builtinExportSet4ArgSig struct {
	BaseBuiltinFunc
}

func (b *builtinExportSet4ArgSig) Clone() BuiltinFunc {
	newSig := &builtinExportSet4ArgSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalString evals EXPORT_SET(bits,on,off,separator).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_export-set
func (b *builtinExportSet4ArgSig) EvalString(row chunk.Row) (string, bool, error) {
	bits, isNull, err := b.Args[0].EvalInt(b.Ctx, row)
	if isNull || err != nil {
		return "", true, err
	}

	on, isNull, err := b.Args[1].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return "", true, err
	}

	off, isNull, err := b.Args[2].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return "", true, err
	}

	separator, isNull, err := b.Args[3].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return "", true, err
	}

	return exportSet(bits, on, off, separator, 64), false, nil
}

type builtinExportSet5ArgSig struct {
	BaseBuiltinFunc
}

func (b *builtinExportSet5ArgSig) Clone() BuiltinFunc {
	newSig := &builtinExportSet5ArgSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalString evals EXPORT_SET(bits,on,off,separator,number_of_bits).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_export-set
func (b *builtinExportSet5ArgSig) EvalString(row chunk.Row) (string, bool, error) {
	bits, isNull, err := b.Args[0].EvalInt(b.Ctx, row)
	if isNull || err != nil {
		return "", true, err
	}

	on, isNull, err := b.Args[1].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return "", true, err
	}

	off, isNull, err := b.Args[2].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return "", true, err
	}

	separator, isNull, err := b.Args[3].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return "", true, err
	}

	numberOfBits, isNull, err := b.Args[4].EvalInt(b.Ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	if numberOfBits < 0 || numberOfBits > 64 {
		numberOfBits = 64
	}

	return exportSet(bits, on, off, separator, numberOfBits), false, nil
}

type formatFunctionClass struct {
	BaseFunctionClass
}

func (c *formatFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	argTps := make([]types.EvalType, 2, 3)
	argTps[1] = types.ETInt
	argTp := args[0].GetType().EvalType()
	if argTp == types.ETDecimal || argTp == types.ETInt {
		argTps[0] = types.ETDecimal
	} else {
		argTps[0] = types.ETReal
	}
	if len(args) == 3 {
		argTps = append(argTps, types.ETString)
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETString, argTps...)
	bf.Tp.Flen = mysql.MaxBlobWidth
	var sig BuiltinFunc
	if len(args) == 3 {
		sig = &builtinFormatWithLocaleSig{bf}
	} else {
		sig = &builtinFormatSig{bf}
	}
	return sig, nil
}

// formatMaxDecimals limits the maximum number of decimal digits for result of
// function `format`, this value is same as `FORMAT_MAX_DECIMALS` in MySQL source code.
const formatMaxDecimals int64 = 30

// evalNumDecArgsForFormat evaluates first 2 arguments, i.e, x and d, for function `format`.
func evalNumDecArgsForFormat(f BuiltinFunc, row chunk.Row) (string, string, bool, error) {
	var xStr string
	arg0, arg1 := f.getArgs()[0], f.getArgs()[1]
	ctx := f.getCtx()
	if arg0.GetType().EvalType() == types.ETDecimal {
		x, isNull, err := arg0.EvalDecimal(ctx, row)
		if isNull || err != nil {
			return "", "", isNull, err
		}
		xStr = x.String()
	} else {
		x, isNull, err := arg0.EvalReal(ctx, row)
		if isNull || err != nil {
			return "", "", isNull, err
		}
		xStr = strconv.FormatFloat(x, 'f', -1, 64)
	}
	d, isNull, err := arg1.EvalInt(ctx, row)
	if isNull || err != nil {
		return "", "", isNull, err
	}
	if d < 0 {
		d = 0
	} else if d > formatMaxDecimals {
		d = formatMaxDecimals
	}
	xStr = roundFormatArgs(xStr, int(d))
	dStr := strconv.FormatInt(d, 10)
	return xStr, dStr, false, nil
}

func roundFormatArgs(xStr string, maxNumDecimals int) string {
	if !strings.Contains(xStr, ".") {
		return xStr
	}

	sign := false
	// xStr cannot have '+' prefix now.
	// It is built in `evalNumDecArgsFormat` after evaluating `Evalxxx` method.
	if strings.HasPrefix(xStr, "-") {
		xStr = strings.Trim(xStr, "-")
		sign = true
	}

	xArr := strings.Split(xStr, ".")
	integerPart := xArr[0]
	decimalPart := xArr[1]

	if len(decimalPart) > maxNumDecimals {
		t := []byte(decimalPart)
		carry := false
		if t[maxNumDecimals] >= '5' {
			carry = true
		}
		for i := maxNumDecimals - 1; i >= 0 && carry; i-- {
			if t[i] == '9' {
				t[i] = '0'
			} else {
				t[i] = t[i] + 1
				carry = false
			}
		}
		decimalPart = string(t)
		t = []byte(integerPart)
		for i := len(integerPart) - 1; i >= 0 && carry; i-- {
			if t[i] == '9' {
				t[i] = '0'
			} else {
				t[i] = t[i] + 1
				carry = false
			}
		}
		if carry {
			integerPart = "1" + string(t)
		} else {
			integerPart = string(t)
		}
	}

	xStr = integerPart + "." + decimalPart
	if sign {
		xStr = "-" + xStr
	}
	return xStr
}

type builtinFormatWithLocaleSig struct {
	BaseBuiltinFunc
}

func (b *builtinFormatWithLocaleSig) Clone() BuiltinFunc {
	newSig := &builtinFormatWithLocaleSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalString evals FORMAT(X,D,locale).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_format
func (b *builtinFormatWithLocaleSig) EvalString(row chunk.Row) (string, bool, error) {
	x, d, isNull, err := evalNumDecArgsForFormat(b, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	locale, isNull, err := b.Args[2].EvalString(b.Ctx, row)
	if err != nil {
		return "", false, err
	}
	if isNull {
		b.Ctx.GetSessionVars().StmtCtx.AppendWarning(errUnknownLocale.GenWithStackByArgs("NULL"))
		locale = "en_US"
	}
	formatString, err := mysql.GetLocaleFormatFunction(locale)(x, d)
	return formatString, false, err
}

type builtinFormatSig struct {
	BaseBuiltinFunc
}

func (b *builtinFormatSig) Clone() BuiltinFunc {
	newSig := &builtinFormatSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalString evals FORMAT(X,D).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_format
func (b *builtinFormatSig) EvalString(row chunk.Row) (string, bool, error) {
	x, d, isNull, err := evalNumDecArgsForFormat(b, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	formatString, err := mysql.GetLocaleFormatFunction("en_US")(x, d)
	return formatString, false, err
}

type fromBase64FunctionClass struct {
	BaseFunctionClass
}

func (c *fromBase64FunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETString, types.ETString)
	bf.Tp.Flen = mysql.MaxBlobWidth

	valStr, _ := ctx.GetSessionVars().GetSystemVar(variable.MaxAllowedPacket)
	maxAllowedPacket, err := strconv.ParseUint(valStr, 10, 64)
	if err != nil {
		return nil, err
	}

	types.SetBinChsClnFlag(bf.Tp)
	sig := &builtinFromBase64Sig{bf, maxAllowedPacket}
	return sig, nil
}

// base64NeededDecodedLength return the base64 decoded string length.
func base64NeededDecodedLength(n int) int {
	// Returns -1 indicate the result will overflow.
	if strconv.IntSize == 64 && n > math.MaxInt64/3 {
		return -1
	}
	if strconv.IntSize == 32 && n > math.MaxInt32/3 {
		return -1
	}
	return n * 3 / 4
}

type builtinFromBase64Sig struct {
	BaseBuiltinFunc
	maxAllowedPacket uint64
}

func (b *builtinFromBase64Sig) Clone() BuiltinFunc {
	newSig := &builtinFromBase64Sig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	newSig.maxAllowedPacket = b.maxAllowedPacket
	return newSig
}

// evalString evals FROM_BASE64(str).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_from-base64
func (b *builtinFromBase64Sig) EvalString(row chunk.Row) (string, bool, error) {
	str, isNull, err := b.Args[0].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return "", true, err
	}

	needDecodeLen := base64NeededDecodedLength(len(str))
	if needDecodeLen == -1 {
		return "", true, nil
	}
	if needDecodeLen > int(b.maxAllowedPacket) {
		b.Ctx.GetSessionVars().StmtCtx.AppendWarning(errWarnAllowedPacketOverflowed.GenWithStackByArgs("from_base64", b.maxAllowedPacket))
		return "", true, nil
	}

	str = strings.Replace(str, "\t", "", -1)
	str = strings.Replace(str, " ", "", -1)
	result, err := base64.StdEncoding.DecodeString(str)
	if err != nil {
		// When error happens, take `from_base64("asc")` as an example, we should return NULL.
		return "", true, nil
	}
	return string(result), false, nil
}

type toBase64FunctionClass struct {
	BaseFunctionClass
}

func (c *toBase64FunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETString, types.ETString)
	bf.Tp.Flen = base64NeededEncodedLength(bf.Args[0].GetType().Flen)

	valStr, _ := ctx.GetSessionVars().GetSystemVar(variable.MaxAllowedPacket)
	maxAllowedPacket, err := strconv.ParseUint(valStr, 10, 64)
	if err != nil {
		return nil, err
	}

	sig := &builtinToBase64Sig{bf, maxAllowedPacket}
	return sig, nil
}

type builtinToBase64Sig struct {
	BaseBuiltinFunc
	maxAllowedPacket uint64
}

func (b *builtinToBase64Sig) Clone() BuiltinFunc {
	newSig := &builtinToBase64Sig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	newSig.maxAllowedPacket = b.maxAllowedPacket
	return newSig
}

// base64NeededEncodedLength return the base64 encoded string length.
func base64NeededEncodedLength(n int) int {
	// Returns -1 indicate the result will overflow.
	if strconv.IntSize == 64 {
		// len(arg)            -> len(to_base64(arg))
		// 6827690988321067803 -> 9223372036854775804
		// 6827690988321067804 -> -9223372036854775808
		if n > 6827690988321067803 {
			return -1
		}
	} else {
		// len(arg)   -> len(to_base64(arg))
		// 1589695686 -> 2147483645
		// 1589695687 -> -2147483646
		if n > 1589695686 {
			return -1
		}
	}

	length := (n + 2) / 3 * 4
	return length + (length-1)/76
}

// evalString evals a builtinToBase64Sig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_to-base64
func (b *builtinToBase64Sig) EvalString(row chunk.Row) (d string, isNull bool, err error) {
	str, isNull, err := b.Args[0].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	needEncodeLen := base64NeededEncodedLength(len(str))
	if needEncodeLen == -1 {
		return "", true, nil
	}
	if needEncodeLen > int(b.maxAllowedPacket) {
		b.Ctx.GetSessionVars().StmtCtx.AppendWarning(errWarnAllowedPacketOverflowed.GenWithStackByArgs("to_base64", b.maxAllowedPacket))
		return "", true, nil
	}
	if b.Tp.Flen == -1 || b.Tp.Flen > mysql.MaxBlobWidth {
		return "", true, nil
	}

	//encode
	strBytes := []byte(str)
	result := base64.StdEncoding.EncodeToString(strBytes)
	//A newline is added after each 76 characters of encoded output to divide long output into multiple lines.
	count := len(result)
	if count > 76 {
		resultArr := splitToSubN(result, 76)
		result = strings.Join(resultArr, "\n")
	}

	return result, false, nil
}

// splitToSubN splits a string every n runes into a string[]
func splitToSubN(s string, n int) []string {
	subs := make([]string, 0, len(s)/n+1)
	for len(s) > n {
		subs = append(subs, s[:n])
		s = s[n:]
	}
	subs = append(subs, s)
	return subs
}

type insertFunctionClass struct {
	BaseFunctionClass
}

func (c *insertFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (sig BuiltinFunc, err error) {
	if err = c.VerifyArgs(args); err != nil {
		return nil, err
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETString, types.ETString, types.ETInt, types.ETInt, types.ETString)
	bf.Tp.Flen = mysql.MaxBlobWidth
	SetBinFlagOrBinStr(args[0].GetType(), bf.Tp)
	SetBinFlagOrBinStr(args[3].GetType(), bf.Tp)

	valStr, _ := ctx.GetSessionVars().GetSystemVar(variable.MaxAllowedPacket)
	maxAllowedPacket, err := strconv.ParseUint(valStr, 10, 64)
	if err != nil {
		return nil, err
	}

	if types.IsBinaryStr(args[0].GetType()) {
		sig = &builtinInsertBinarySig{bf, maxAllowedPacket}
	} else {
		sig = &builtinInsertSig{bf, maxAllowedPacket}
	}
	return sig, nil
}

type builtinInsertBinarySig struct {
	BaseBuiltinFunc
	maxAllowedPacket uint64
}

func (b *builtinInsertBinarySig) Clone() BuiltinFunc {
	newSig := &builtinInsertBinarySig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	newSig.maxAllowedPacket = b.maxAllowedPacket
	return newSig
}

// evalString evals INSERT(str,pos,len,newstr).
// See https://dev.mysql.com/doc/refman/5.6/en/string-functions.html#function_insert
func (b *builtinInsertBinarySig) EvalString(row chunk.Row) (string, bool, error) {
	str, isNull, err := b.Args[0].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return "", true, err
	}

	pos, isNull, err := b.Args[1].EvalInt(b.Ctx, row)
	if isNull || err != nil {
		return "", true, err
	}

	length, isNull, err := b.Args[2].EvalInt(b.Ctx, row)
	if isNull || err != nil {
		return "", true, err
	}

	newstr, isNull, err := b.Args[3].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return "", true, err
	}

	strLength := int64(len(str))
	if pos < 1 || pos > strLength {
		return str, false, nil
	}
	if length > strLength-pos+1 || length < 0 {
		length = strLength - pos + 1
	}

	if uint64(strLength-length+int64(len(newstr))) > b.maxAllowedPacket {
		b.Ctx.GetSessionVars().StmtCtx.AppendWarning(errWarnAllowedPacketOverflowed.GenWithStackByArgs("insert", b.maxAllowedPacket))
		return "", true, nil
	}

	return str[0:pos-1] + newstr + str[pos+length-1:], false, nil
}

type builtinInsertSig struct {
	BaseBuiltinFunc
	maxAllowedPacket uint64
}

func (b *builtinInsertSig) Clone() BuiltinFunc {
	newSig := &builtinInsertSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	newSig.maxAllowedPacket = b.maxAllowedPacket
	return newSig
}

// evalString evals INSERT(str,pos,len,newstr).
// See https://dev.mysql.com/doc/refman/5.6/en/string-functions.html#function_insert
func (b *builtinInsertSig) EvalString(row chunk.Row) (string, bool, error) {
	str, isNull, err := b.Args[0].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return "", true, err
	}

	pos, isNull, err := b.Args[1].EvalInt(b.Ctx, row)
	if isNull || err != nil {
		return "", true, err
	}

	length, isNull, err := b.Args[2].EvalInt(b.Ctx, row)
	if isNull || err != nil {
		return "", true, err
	}

	newstr, isNull, err := b.Args[3].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return "", true, err
	}

	runes := []rune(str)
	runeLength := int64(len(runes))
	if pos < 1 || pos > runeLength {
		return str, false, nil
	}
	if length > runeLength-pos+1 || length < 0 {
		length = runeLength - pos + 1
	}

	strHead := string(runes[0 : pos-1])
	strTail := string(runes[pos+length-1:])
	if uint64(len(strHead)+len(newstr)+len(strTail)) > b.maxAllowedPacket {
		b.Ctx.GetSessionVars().StmtCtx.AppendWarning(errWarnAllowedPacketOverflowed.GenWithStackByArgs("insert", b.maxAllowedPacket))
		return "", true, nil
	}
	return strHead + newstr + strTail, false, nil
}

type instrFunctionClass struct {
	BaseFunctionClass
}

func (c *instrFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETString, types.ETString)
	bf.Tp.Flen = 11
	if types.IsBinaryStr(bf.Args[0].GetType()) || types.IsBinaryStr(bf.Args[1].GetType()) {
		sig := &builtinInstrBinarySig{bf}
		return sig, nil
	}
	sig := &builtinInstrSig{bf}
	return sig, nil
}

type builtinInstrSig struct{ BaseBuiltinFunc }

func (b *builtinInstrSig) Clone() BuiltinFunc {
	newSig := &builtinInstrSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

type builtinInstrBinarySig struct{ BaseBuiltinFunc }

func (b *builtinInstrBinarySig) Clone() BuiltinFunc {
	newSig := &builtinInstrBinarySig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalInt evals INSTR(str,substr), case insensitive
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_instr
func (b *builtinInstrSig) evalInt(row chunk.Row) (int64, bool, error) {
	str, IsNull, err := b.Args[0].EvalString(b.Ctx, row)
	if IsNull || err != nil {
		return 0, true, err
	}
	str = strings.ToLower(str)

	substr, IsNull, err := b.Args[1].EvalString(b.Ctx, row)
	if IsNull || err != nil {
		return 0, true, err
	}
	substr = strings.ToLower(substr)

	idx := strings.Index(str, substr)
	if idx == -1 {
		return 0, false, nil
	}
	return int64(utf8.RuneCountInString(str[:idx]) + 1), false, nil
}

// evalInt evals INSTR(str,substr), case sensitive
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_instr
func (b *builtinInstrBinarySig) evalInt(row chunk.Row) (int64, bool, error) {
	str, IsNull, err := b.Args[0].EvalString(b.Ctx, row)
	if IsNull || err != nil {
		return 0, true, err
	}

	substr, IsNull, err := b.Args[1].EvalString(b.Ctx, row)
	if IsNull || err != nil {
		return 0, true, err
	}

	idx := strings.Index(str, substr)
	if idx == -1 {
		return 0, false, nil
	}
	return int64(idx + 1), false, nil
}

type loadFileFunctionClass struct {
	BaseFunctionClass
}

func (c *loadFileFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	return nil, errFunctionNotExists.GenWithStackByArgs("FUNCTION", "load_file")
}
