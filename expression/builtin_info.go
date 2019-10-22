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
	"encoding/hex"
	"fmt"
	"sort"
	"strconv"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/plancodec"
	"github.com/pingcap/tidb/util/printer"
)

var (
	_ FunctionClass = &databaseFunctionClass{}
	_ FunctionClass = &foundRowsFunctionClass{}
	_ FunctionClass = &currentUserFunctionClass{}
	_ FunctionClass = &currentRoleFunctionClass{}
	_ FunctionClass = &userFunctionClass{}
	_ FunctionClass = &connectionIDFunctionClass{}
	_ FunctionClass = &lastInsertIDFunctionClass{}
	_ FunctionClass = &versionFunctionClass{}
	_ FunctionClass = &benchmarkFunctionClass{}
	_ FunctionClass = &charsetFunctionClass{}
	_ FunctionClass = &coercibilityFunctionClass{}
	_ FunctionClass = &collationFunctionClass{}
	_ FunctionClass = &rowCountFunctionClass{}
	_ FunctionClass = &tidbVersionFunctionClass{}
	_ FunctionClass = &tidbIsDDLOwnerFunctionClass{}
	_ FunctionClass = &tidbDecodePlanFunctionClass{}
	_ FunctionClass = &tidbDecodeKeyFunctionClass{}
)

var (
	_ BuiltinFunc = &builtinDatabaseSig{}
	_ BuiltinFunc = &builtinFoundRowsSig{}
	_ BuiltinFunc = &builtinCurrentUserSig{}
	_ BuiltinFunc = &builtinUserSig{}
	_ BuiltinFunc = &builtinConnectionIDSig{}
	_ BuiltinFunc = &builtinLastInsertIDSig{}
	_ BuiltinFunc = &builtinLastInsertIDWithIDSig{}
	_ BuiltinFunc = &builtinVersionSig{}
	_ BuiltinFunc = &builtinTiDBVersionSig{}
	_ BuiltinFunc = &builtinRowCountSig{}
	_ BuiltinFunc = &builtinTiDBDecodeKeySig{}
)

type databaseFunctionClass struct {
	BaseFunctionClass
}

func (c *databaseFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETString)
	bf.Tp.Flen = 64
	sig := &builtinDatabaseSig{bf}
	return sig, nil
}

type builtinDatabaseSig struct {
	BaseBuiltinFunc
}

func (b *builtinDatabaseSig) Clone() BuiltinFunc {
	newSig := &builtinDatabaseSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalString evals a builtinDatabaseSig.
// See https://dev.mysql.com/doc/refman/5.7/en/information-functions.html
func (b *builtinDatabaseSig) EvalString(row chunk.Row) (string, bool, error) {
	currentDB := b.Ctx.GetSessionVars().CurrentDB
	return currentDB, currentDB == "", nil
}

type foundRowsFunctionClass struct {
	BaseFunctionClass
}

func (c *foundRowsFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETInt)
	bf.Tp.Flag |= mysql.UnsignedFlag
	sig := &builtinFoundRowsSig{bf}
	return sig, nil
}

type builtinFoundRowsSig struct {
	BaseBuiltinFunc
}

func (b *builtinFoundRowsSig) Clone() BuiltinFunc {
	newSig := &builtinFoundRowsSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalInt evals a builtinFoundRowsSig.
// See https://dev.mysql.com/doc/refman/5.7/en/information-functions.html#function_found-rows
// TODO: SQL_CALC_FOUND_ROWS and LIMIT not support for now, We will finish in another PR.
func (b *builtinFoundRowsSig) evalInt(row chunk.Row) (int64, bool, error) {
	data := b.Ctx.GetSessionVars()
	if data == nil {
		return 0, true, errors.Errorf("Missing session variable when eval builtin")
	}
	return int64(data.LastFoundRows), false, nil
}

type currentUserFunctionClass struct {
	BaseFunctionClass
}

func (c *currentUserFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETString)
	bf.Tp.Flen = 64
	sig := &builtinCurrentUserSig{bf}
	return sig, nil
}

type builtinCurrentUserSig struct {
	BaseBuiltinFunc
}

func (b *builtinCurrentUserSig) Clone() BuiltinFunc {
	newSig := &builtinCurrentUserSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalString evals a builtinCurrentUserSig.
// See https://dev.mysql.com/doc/refman/5.7/en/information-functions.html#function_current-user
func (b *builtinCurrentUserSig) EvalString(row chunk.Row) (string, bool, error) {
	data := b.Ctx.GetSessionVars()
	if data == nil || data.User == nil {
		return "", true, errors.Errorf("Missing session variable when eval builtin")
	}
	return data.User.AuthIdentityString(), false, nil
}

type currentRoleFunctionClass struct {
	BaseFunctionClass
}

func (c *currentRoleFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETString)
	bf.Tp.Flen = 64
	sig := &builtinCurrentRoleSig{bf}
	return sig, nil
}

type builtinCurrentRoleSig struct {
	BaseBuiltinFunc
}

func (b *builtinCurrentRoleSig) Clone() BuiltinFunc {
	newSig := &builtinCurrentRoleSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalString evals a builtinCurrentUserSig.
// See https://dev.mysql.com/doc/refman/5.7/en/information-functions.html#function_current-user
func (b *builtinCurrentRoleSig) EvalString(row chunk.Row) (string, bool, error) {
	data := b.Ctx.GetSessionVars()
	if data == nil || data.ActiveRoles == nil {
		return "", true, errors.Errorf("Missing session variable when eval builtin")
	}
	if len(data.ActiveRoles) == 0 {
		return "", false, nil
	}
	res := ""
	sortedRes := make([]string, 0, 10)
	for _, r := range data.ActiveRoles {
		sortedRes = append(sortedRes, r.String())
	}
	sort.Strings(sortedRes)
	for i, r := range sortedRes {
		res += r
		if i != len(data.ActiveRoles)-1 {
			res += ","
		}
	}
	return res, false, nil
}

type userFunctionClass struct {
	BaseFunctionClass
}

func (c *userFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETString)
	bf.Tp.Flen = 64
	sig := &builtinUserSig{bf}
	return sig, nil
}

type builtinUserSig struct {
	BaseBuiltinFunc
}

func (b *builtinUserSig) Clone() BuiltinFunc {
	newSig := &builtinUserSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalString evals a builtinUserSig.
// See https://dev.mysql.com/doc/refman/5.7/en/information-functions.html#function_user
func (b *builtinUserSig) EvalString(row chunk.Row) (string, bool, error) {
	data := b.Ctx.GetSessionVars()
	if data == nil || data.User == nil {
		return "", true, errors.Errorf("Missing session variable when eval builtin")
	}

	return data.User.String(), false, nil
}

type connectionIDFunctionClass struct {
	BaseFunctionClass
}

func (c *connectionIDFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETInt)
	bf.Tp.Flag |= mysql.UnsignedFlag
	sig := &builtinConnectionIDSig{bf}
	return sig, nil
}

type builtinConnectionIDSig struct {
	BaseBuiltinFunc
}

func (b *builtinConnectionIDSig) Clone() BuiltinFunc {
	newSig := &builtinConnectionIDSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinConnectionIDSig) evalInt(_ chunk.Row) (int64, bool, error) {
	data := b.Ctx.GetSessionVars()
	if data == nil {
		return 0, true, errors.Errorf("Missing session variable when evalue builtin")
	}
	return int64(data.ConnectionID), false, nil
}

type lastInsertIDFunctionClass struct {
	BaseFunctionClass
}

func (c *lastInsertIDFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (sig BuiltinFunc, err error) {
	if err = c.VerifyArgs(args); err != nil {
		return nil, err
	}

	var argsTp []types.EvalType
	if len(args) == 1 {
		argsTp = append(argsTp, types.ETInt)
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETInt, argsTp...)
	bf.Tp.Flag |= mysql.UnsignedFlag

	if len(args) == 1 {
		sig = &builtinLastInsertIDWithIDSig{bf}
	} else {
		sig = &builtinLastInsertIDSig{bf}
	}
	return sig, err
}

type builtinLastInsertIDSig struct {
	BaseBuiltinFunc
}

func (b *builtinLastInsertIDSig) Clone() BuiltinFunc {
	newSig := &builtinLastInsertIDSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalInt evals LAST_INSERT_ID().
// See https://dev.mysql.com/doc/refman/5.7/en/information-functions.html#function_last-insert-id.
func (b *builtinLastInsertIDSig) evalInt(row chunk.Row) (res int64, isNull bool, err error) {
	res = int64(b.Ctx.GetSessionVars().StmtCtx.PrevLastInsertID)
	return res, false, nil
}

type builtinLastInsertIDWithIDSig struct {
	BaseBuiltinFunc
}

func (b *builtinLastInsertIDWithIDSig) Clone() BuiltinFunc {
	newSig := &builtinLastInsertIDWithIDSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalInt evals LAST_INSERT_ID(expr).
// See https://dev.mysql.com/doc/refman/5.7/en/information-functions.html#function_last-insert-id.
func (b *builtinLastInsertIDWithIDSig) evalInt(row chunk.Row) (res int64, isNull bool, err error) {
	res, isNull, err = b.Args[0].EvalInt(b.Ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}

	b.Ctx.GetSessionVars().SetLastInsertID(uint64(res))
	return res, false, nil
}

type versionFunctionClass struct {
	BaseFunctionClass
}

func (c *versionFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETString)
	bf.Tp.Flen = 64
	sig := &builtinVersionSig{bf}
	return sig, nil
}

type builtinVersionSig struct {
	BaseBuiltinFunc
}

func (b *builtinVersionSig) Clone() BuiltinFunc {
	newSig := &builtinVersionSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalString evals a builtinVersionSig.
// See https://dev.mysql.com/doc/refman/5.7/en/information-functions.html#function_version
func (b *builtinVersionSig) EvalString(row chunk.Row) (string, bool, error) {
	return mysql.ServerVersion, false, nil
}

type tidbVersionFunctionClass struct {
	BaseFunctionClass
}

func (c *tidbVersionFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETString)
	bf.Tp.Flen = len(printer.GetTiDBInfo())
	sig := &builtinTiDBVersionSig{bf}
	return sig, nil
}

type builtinTiDBVersionSig struct {
	BaseBuiltinFunc
}

func (b *builtinTiDBVersionSig) Clone() BuiltinFunc {
	newSig := &builtinTiDBVersionSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalString evals a builtinTiDBVersionSig.
// This will show git hash and build time for tidb-server.
func (b *builtinTiDBVersionSig) EvalString(_ chunk.Row) (string, bool, error) {
	return printer.GetTiDBInfo(), false, nil
}

type tidbIsDDLOwnerFunctionClass struct {
	BaseFunctionClass
}

func (c *tidbIsDDLOwnerFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETInt)
	sig := &builtinTiDBIsDDLOwnerSig{bf}
	return sig, nil
}

type builtinTiDBIsDDLOwnerSig struct {
	BaseBuiltinFunc
}

func (b *builtinTiDBIsDDLOwnerSig) Clone() BuiltinFunc {
	newSig := &builtinTiDBIsDDLOwnerSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalInt evals a builtinTiDBIsDDLOwnerSig.
func (b *builtinTiDBIsDDLOwnerSig) evalInt(_ chunk.Row) (res int64, isNull bool, err error) {
	ddlOwnerChecker := b.Ctx.DDLOwnerChecker()
	if ddlOwnerChecker.IsOwner() {
		res = 1
	}

	return res, false, nil
}

type benchmarkFunctionClass struct {
	BaseFunctionClass
}

func (c *benchmarkFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}

	// Syntax: BENCHMARK(loop_count, expression)
	// Define with same eval type of input arg to avoid unnecessary cast function.
	sameEvalType := args[1].GetType().EvalType()
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETInt, sameEvalType)
	sig := &builtinBenchmarkSig{bf}
	return sig, nil
}

type builtinBenchmarkSig struct {
	BaseBuiltinFunc
}

func (b *builtinBenchmarkSig) Clone() BuiltinFunc {
	newSig := &builtinBenchmarkSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalInt evals a builtinBenchmarkSig. It will execute expression repeatedly count times.
// See https://dev.mysql.com/doc/refman/5.7/en/information-functions.html#function_benchmark
func (b *builtinBenchmarkSig) evalInt(row chunk.Row) (int64, bool, error) {
	// Get loop count.
	loopCount, isNull, err := b.Args[0].EvalInt(b.Ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}

	// BENCHMARK() will return NULL if loop count < 0,
	// behavior observed on MySQL 5.7.24.
	if loopCount < 0 {
		return 0, true, nil
	}

	// Eval loop count times based on arg type.
	// BENCHMARK() will pass-through the eval error,
	// behavior observed on MySQL 5.7.24.
	var i int64
	arg, ctx := b.Args[1], b.Ctx
	switch evalType := arg.GetType().EvalType(); evalType {
	case types.ETInt:
		for ; i < loopCount; i++ {
			_, isNull, err = arg.EvalInt(ctx, row)
			if err != nil {
				return 0, isNull, err
			}
		}
	case types.ETReal:
		for ; i < loopCount; i++ {
			_, isNull, err = arg.EvalReal(ctx, row)
			if err != nil {
				return 0, isNull, err
			}
		}
	case types.ETDecimal:
		for ; i < loopCount; i++ {
			_, isNull, err = arg.EvalDecimal(ctx, row)
			if err != nil {
				return 0, isNull, err
			}
		}
	case types.ETString:
		for ; i < loopCount; i++ {
			_, isNull, err = arg.EvalString(ctx, row)
			if err != nil {
				return 0, isNull, err
			}
		}
	case types.ETDatetime, types.ETTimestamp:
		for ; i < loopCount; i++ {
			_, isNull, err = arg.EvalTime(ctx, row)
			if err != nil {
				return 0, isNull, err
			}
		}
	case types.ETDuration:
		for ; i < loopCount; i++ {
			_, isNull, err = arg.EvalDuration(ctx, row)
			if err != nil {
				return 0, isNull, err
			}
		}
	case types.ETJson:
		for ; i < loopCount; i++ {
			_, isNull, err = arg.EvalJSON(ctx, row)
			if err != nil {
				return 0, isNull, err
			}
		}
	default: // Should never go into here.
		return 0, true, errors.Errorf("EvalType %v not implemented for builtin BENCHMARK()", evalType)
	}

	// Return value of BENCHMARK() is always 0.
	return 0, false, nil
}

type charsetFunctionClass struct {
	BaseFunctionClass
}

func (c *charsetFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	return nil, errFunctionNotExists.GenWithStackByArgs("FUNCTION", "CHARSET")
}

type coercibilityFunctionClass struct {
	BaseFunctionClass
}

func (c *coercibilityFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	return nil, errFunctionNotExists.GenWithStackByArgs("FUNCTION", "COERCIBILITY")
}

type collationFunctionClass struct {
	BaseFunctionClass
}

func (c *collationFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	return nil, errFunctionNotExists.GenWithStackByArgs("FUNCTION", "COLLATION")
}

type rowCountFunctionClass struct {
	BaseFunctionClass
}

func (c *rowCountFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (sig BuiltinFunc, err error) {
	if err = c.VerifyArgs(args); err != nil {
		return nil, err
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETInt)
	sig = &builtinRowCountSig{bf}
	return sig, nil
}

type builtinRowCountSig struct {
	BaseBuiltinFunc
}

func (b *builtinRowCountSig) Clone() BuiltinFunc {
	newSig := &builtinRowCountSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalInt evals ROW_COUNT().
// See https://dev.mysql.com/doc/refman/5.7/en/information-functions.html#function_row-count.
func (b *builtinRowCountSig) evalInt(_ chunk.Row) (res int64, isNull bool, err error) {
	res = int64(b.Ctx.GetSessionVars().StmtCtx.PrevAffectedRows)
	return res, false, nil
}

type tidbDecodeKeyFunctionClass struct {
	BaseFunctionClass
}

func (c *tidbDecodeKeyFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETString, types.ETString)
	sig := &builtinTiDBDecodeKeySig{bf}
	return sig, nil
}

type builtinTiDBDecodeKeySig struct {
	BaseBuiltinFunc
}

func (b *builtinTiDBDecodeKeySig) Clone() BuiltinFunc {
	newSig := &builtinTiDBDecodeKeySig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalInt evals a builtinTiDBIsDDLOwnerSig.
func (b *builtinTiDBDecodeKeySig) EvalString(row chunk.Row) (string, bool, error) {
	s, isNull, err := b.Args[0].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	return decodeKey(b.Ctx, s), false, nil
}

func decodeKey(ctx sessionctx.Context, s string) string {
	key, err := hex.DecodeString(s)
	if err != nil {
		ctx.GetSessionVars().StmtCtx.AppendWarning(errors.Errorf("invalid record/index key: %X", key))
		return s
	}
	// Auto decode byte if needed.
	_, bs, err := codec.DecodeBytes([]byte(key), nil)
	if err == nil {
		key = bs
	}
	// Try to decode it as a record key.
	tableID, handle, err := tablecodec.DecodeRecordKey(key)
	if err == nil {
		return "tableID=" + strconv.FormatInt(tableID, 10) + ", _tidb_rowid=" + strconv.FormatInt(handle, 10)
	}
	// Try decode as table index key.
	tableID, indexID, indexValues, err := tablecodec.DecodeIndexKeyPrefix(key)
	if err == nil {
		idxValueStr := fmt.Sprintf("%X", indexValues)
		return "tableID=" + strconv.FormatInt(tableID, 10) + ", indexID=" + strconv.FormatInt(indexID, 10) + ", indexValues=" + idxValueStr
	}

	// TODO: try to decode other type key.
	ctx.GetSessionVars().StmtCtx.AppendWarning(errors.Errorf("invalid record/index key: %X", key))
	return s
}

type tidbDecodePlanFunctionClass struct {
	BaseFunctionClass
}

func (c *tidbDecodePlanFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETString, types.ETString)
	sig := &builtinTiDBDecodePlanSig{bf}
	return sig, nil
}

type builtinTiDBDecodePlanSig struct {
	BaseBuiltinFunc
}

func (b *builtinTiDBDecodePlanSig) Clone() BuiltinFunc {
	newSig := &builtinTiDBDecodePlanSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinTiDBDecodePlanSig) EvalString(row chunk.Row) (string, bool, error) {
	planString, isNull, err := b.Args[0].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	planTree, err := plancodec.DecodePlan(planString)
	return planTree, false, err
}
