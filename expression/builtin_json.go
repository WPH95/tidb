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
	json2 "encoding/json"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/types/json"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/hack"
	"github.com/pingcap/tidb/util/stringutil"
	"github.com/pingcap/tipb/go-tipb"
)

var (
	_ FunctionClass = &jsonTypeFunctionClass{}
	_ FunctionClass = &jsonExtractFunctionClass{}
	_ FunctionClass = &jsonUnquoteFunctionClass{}
	_ FunctionClass = &jsonQuoteFunctionClass{}
	_ FunctionClass = &jsonSetFunctionClass{}
	_ FunctionClass = &jsonInsertFunctionClass{}
	_ FunctionClass = &jsonReplaceFunctionClass{}
	_ FunctionClass = &jsonRemoveFunctionClass{}
	_ FunctionClass = &jsonMergeFunctionClass{}
	_ FunctionClass = &jsonObjectFunctionClass{}
	_ FunctionClass = &jsonArrayFunctionClass{}
	_ FunctionClass = &jsonContainsFunctionClass{}
	_ FunctionClass = &jsonContainsPathFunctionClass{}
	_ FunctionClass = &jsonValidFunctionClass{}
	_ FunctionClass = &jsonArrayAppendFunctionClass{}
	_ FunctionClass = &jsonArrayInsertFunctionClass{}
	_ FunctionClass = &jsonMergePatchFunctionClass{}
	_ FunctionClass = &jsonMergePreserveFunctionClass{}
	_ FunctionClass = &jsonPrettyFunctionClass{}
	_ FunctionClass = &jsonQuoteFunctionClass{}
	_ FunctionClass = &jsonSearchFunctionClass{}
	_ FunctionClass = &jsonStorageSizeFunctionClass{}
	_ FunctionClass = &jsonDepthFunctionClass{}
	_ FunctionClass = &jsonKeysFunctionClass{}
	_ FunctionClass = &jsonLengthFunctionClass{}

	_ BuiltinFunc = &builtinJSONTypeSig{}
	_ BuiltinFunc = &builtinJSONQuoteSig{}
	_ BuiltinFunc = &builtinJSONUnquoteSig{}
	_ BuiltinFunc = &builtinJSONArraySig{}
	_ BuiltinFunc = &builtinJSONArrayAppendSig{}
	_ BuiltinFunc = &builtinJSONArrayInsertSig{}
	_ BuiltinFunc = &builtinJSONObjectSig{}
	_ BuiltinFunc = &builtinJSONExtractSig{}
	_ BuiltinFunc = &builtinJSONSetSig{}
	_ BuiltinFunc = &builtinJSONInsertSig{}
	_ BuiltinFunc = &builtinJSONReplaceSig{}
	_ BuiltinFunc = &builtinJSONRemoveSig{}
	_ BuiltinFunc = &builtinJSONMergeSig{}
	_ BuiltinFunc = &builtinJSONContainsSig{}
	_ BuiltinFunc = &builtinJSONDepthSig{}
	_ BuiltinFunc = &builtinJSONSearchSig{}
	_ BuiltinFunc = &builtinJSONKeysSig{}
	_ BuiltinFunc = &builtinJSONKeys2ArgsSig{}
	_ BuiltinFunc = &builtinJSONLengthSig{}
	_ BuiltinFunc = &builtinJSONValidJSONSig{}
	_ BuiltinFunc = &builtinJSONValidStringSig{}
	_ BuiltinFunc = &builtinJSONValidOthersSig{}
)

type jsonTypeFunctionClass struct {
	BaseFunctionClass
}

type builtinJSONTypeSig struct {
	BaseBuiltinFunc
}

func (b *builtinJSONTypeSig) Clone() BuiltinFunc {
	newSig := &builtinJSONTypeSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (c *jsonTypeFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETString, types.ETJson)
	bf.Tp.Flen = 51 // Flen of JSON_TYPE is length of UNSIGNED INTEGER.
	sig := &builtinJSONTypeSig{bf}
	sig.SetPbCode(tipb.ScalarFuncSig_JsonTypeSig)
	return sig, nil
}

func (b *builtinJSONTypeSig) EvalString(row chunk.Row) (res string, isNull bool, err error) {
	var j json.BinaryJSON
	j, isNull, err = b.Args[0].EvalJSON(b.Ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	return j.Type(), false, nil
}

type jsonExtractFunctionClass struct {
	BaseFunctionClass
}

type builtinJSONExtractSig struct {
	BaseBuiltinFunc
}

func (b *builtinJSONExtractSig) Clone() BuiltinFunc {
	newSig := &builtinJSONExtractSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (c *jsonExtractFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	argTps := make([]types.EvalType, 0, len(args))
	argTps = append(argTps, types.ETJson)
	for range args[1:] {
		argTps = append(argTps, types.ETString)
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETJson, argTps...)
	sig := &builtinJSONExtractSig{bf}
	sig.SetPbCode(tipb.ScalarFuncSig_JsonExtractSig)
	return sig, nil
}

func (b *builtinJSONExtractSig) evalJSON(row chunk.Row) (res json.BinaryJSON, isNull bool, err error) {
	res, isNull, err = b.Args[0].EvalJSON(b.Ctx, row)
	if isNull || err != nil {
		return
	}
	pathExprs := make([]json.PathExpression, 0, len(b.Args)-1)
	for _, arg := range b.Args[1:] {
		var s string
		s, isNull, err = arg.EvalString(b.Ctx, row)
		if isNull || err != nil {
			return res, isNull, err
		}
		pathExpr, err := json.ParseJSONPathExpr(s)
		if err != nil {
			return res, true, err
		}
		pathExprs = append(pathExprs, pathExpr)
	}
	var found bool
	if res, found = res.Extract(pathExprs); !found {
		return res, true, nil
	}
	return res, false, nil
}

type jsonUnquoteFunctionClass struct {
	BaseFunctionClass
}

type builtinJSONUnquoteSig struct {
	BaseBuiltinFunc
}

func (b *builtinJSONUnquoteSig) Clone() BuiltinFunc {
	newSig := &builtinJSONUnquoteSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (c *jsonUnquoteFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETString, types.ETJson)
	DisableParseJSONFlag4Expr(args[0])
	sig := &builtinJSONUnquoteSig{bf}
	sig.SetPbCode(tipb.ScalarFuncSig_JsonUnquoteSig)
	return sig, nil
}

func (b *builtinJSONUnquoteSig) EvalString(row chunk.Row) (res string, isNull bool, err error) {
	var j json.BinaryJSON
	j, isNull, err = b.Args[0].EvalJSON(b.Ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	res, err = j.Unquote()
	return res, err != nil, err
}

type jsonSetFunctionClass struct {
	BaseFunctionClass
}

type builtinJSONSetSig struct {
	BaseBuiltinFunc
}

func (b *builtinJSONSetSig) Clone() BuiltinFunc {
	newSig := &builtinJSONSetSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (c *jsonSetFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	if len(args)&1 != 1 {
		return nil, ErrIncorrectParameterCount.GenWithStackByArgs(c.FuncName)
	}
	argTps := make([]types.EvalType, 0, len(args))
	argTps = append(argTps, types.ETJson)
	for i := 1; i < len(args)-1; i += 2 {
		argTps = append(argTps, types.ETString, types.ETJson)
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETJson, argTps...)
	for i := 2; i < len(args); i += 2 {
		DisableParseJSONFlag4Expr(args[i])
	}
	sig := &builtinJSONSetSig{bf}
	sig.SetPbCode(tipb.ScalarFuncSig_JsonSetSig)
	return sig, nil
}

func (b *builtinJSONSetSig) evalJSON(row chunk.Row) (res json.BinaryJSON, isNull bool, err error) {
	res, isNull, err = jsonModify(b.Ctx, b.Args, row, json.ModifySet)
	return res, isNull, err
}

type jsonInsertFunctionClass struct {
	BaseFunctionClass
}

type builtinJSONInsertSig struct {
	BaseBuiltinFunc
}

func (b *builtinJSONInsertSig) Clone() BuiltinFunc {
	newSig := &builtinJSONInsertSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (c *jsonInsertFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	if len(args)&1 != 1 {
		return nil, ErrIncorrectParameterCount.GenWithStackByArgs(c.FuncName)
	}
	argTps := make([]types.EvalType, 0, len(args))
	argTps = append(argTps, types.ETJson)
	for i := 1; i < len(args)-1; i += 2 {
		argTps = append(argTps, types.ETString, types.ETJson)
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETJson, argTps...)
	for i := 2; i < len(args); i += 2 {
		DisableParseJSONFlag4Expr(args[i])
	}
	sig := &builtinJSONInsertSig{bf}
	sig.SetPbCode(tipb.ScalarFuncSig_JsonInsertSig)
	return sig, nil
}

func (b *builtinJSONInsertSig) evalJSON(row chunk.Row) (res json.BinaryJSON, isNull bool, err error) {
	res, isNull, err = jsonModify(b.Ctx, b.Args, row, json.ModifyInsert)
	return res, isNull, err
}

type jsonReplaceFunctionClass struct {
	BaseFunctionClass
}

type builtinJSONReplaceSig struct {
	BaseBuiltinFunc
}

func (b *builtinJSONReplaceSig) Clone() BuiltinFunc {
	newSig := &builtinJSONReplaceSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (c *jsonReplaceFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	if len(args)&1 != 1 {
		return nil, ErrIncorrectParameterCount.GenWithStackByArgs(c.FuncName)
	}
	argTps := make([]types.EvalType, 0, len(args))
	argTps = append(argTps, types.ETJson)
	for i := 1; i < len(args)-1; i += 2 {
		argTps = append(argTps, types.ETString, types.ETJson)
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETJson, argTps...)
	for i := 2; i < len(args); i += 2 {
		DisableParseJSONFlag4Expr(args[i])
	}
	sig := &builtinJSONReplaceSig{bf}
	sig.SetPbCode(tipb.ScalarFuncSig_JsonReplaceSig)
	return sig, nil
}

func (b *builtinJSONReplaceSig) evalJSON(row chunk.Row) (res json.BinaryJSON, isNull bool, err error) {
	res, isNull, err = jsonModify(b.Ctx, b.Args, row, json.ModifyReplace)
	return res, isNull, err
}

type jsonRemoveFunctionClass struct {
	BaseFunctionClass
}

type builtinJSONRemoveSig struct {
	BaseBuiltinFunc
}

func (b *builtinJSONRemoveSig) Clone() BuiltinFunc {
	newSig := &builtinJSONRemoveSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (c *jsonRemoveFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	argTps := make([]types.EvalType, 0, len(args))
	argTps = append(argTps, types.ETJson)
	for range args[1:] {
		argTps = append(argTps, types.ETString)
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETJson, argTps...)
	sig := &builtinJSONRemoveSig{bf}
	sig.SetPbCode(tipb.ScalarFuncSig_JsonRemoveSig)
	return sig, nil
}

func (b *builtinJSONRemoveSig) evalJSON(row chunk.Row) (res json.BinaryJSON, isNull bool, err error) {
	res, isNull, err = b.Args[0].EvalJSON(b.Ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	pathExprs := make([]json.PathExpression, 0, len(b.Args)-1)
	for _, arg := range b.Args[1:] {
		var s string
		s, isNull, err = arg.EvalString(b.Ctx, row)
		if isNull || err != nil {
			return res, isNull, err
		}
		var pathExpr json.PathExpression
		pathExpr, err = json.ParseJSONPathExpr(s)
		if err != nil {
			return res, true, err
		}
		pathExprs = append(pathExprs, pathExpr)
	}
	res, err = res.Remove(pathExprs)
	if err != nil {
		return res, true, err
	}
	return res, false, nil
}

type jsonMergeFunctionClass struct {
	BaseFunctionClass
}

type builtinJSONMergeSig struct {
	BaseBuiltinFunc
}

func (b *builtinJSONMergeSig) Clone() BuiltinFunc {
	newSig := &builtinJSONMergeSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (c *jsonMergeFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	argTps := make([]types.EvalType, 0, len(args))
	for range args {
		argTps = append(argTps, types.ETJson)
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETJson, argTps...)
	sig := &builtinJSONMergeSig{bf}
	sig.SetPbCode(tipb.ScalarFuncSig_JsonMergeSig)
	return sig, nil
}

func (b *builtinJSONMergeSig) evalJSON(row chunk.Row) (res json.BinaryJSON, isNull bool, err error) {
	values := make([]json.BinaryJSON, 0, len(b.Args))
	for _, arg := range b.Args {
		var value json.BinaryJSON
		value, isNull, err = arg.EvalJSON(b.Ctx, row)
		if isNull || err != nil {
			return res, isNull, err
		}
		values = append(values, value)
	}
	res = json.MergeBinary(values)
	// function "JSON_MERGE" is deprecated since MySQL 5.7.22. Synonym for function "JSON_MERGE_PRESERVE".
	// See https://dev.mysql.com/doc/refman/5.7/en/json-modification-functions.html#function_json-merge
	if b.pbCode == tipb.ScalarFuncSig_JsonMergeSig {
		b.Ctx.GetSessionVars().StmtCtx.AppendWarning(errDeprecatedSyntaxNoReplacement.GenWithStackByArgs("JSON_MERGE"))
	}
	return res, false, nil
}

type jsonObjectFunctionClass struct {
	BaseFunctionClass
}

type builtinJSONObjectSig struct {
	BaseBuiltinFunc
}

func (b *builtinJSONObjectSig) Clone() BuiltinFunc {
	newSig := &builtinJSONObjectSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (c *jsonObjectFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	if len(args)&1 != 0 {
		return nil, ErrIncorrectParameterCount.GenWithStackByArgs(c.FuncName)
	}
	argTps := make([]types.EvalType, 0, len(args))
	for i := 0; i < len(args)-1; i += 2 {
		argTps = append(argTps, types.ETString, types.ETJson)
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETJson, argTps...)
	for i := 1; i < len(args); i += 2 {
		DisableParseJSONFlag4Expr(args[i])
	}
	sig := &builtinJSONObjectSig{bf}
	sig.SetPbCode(tipb.ScalarFuncSig_JsonObjectSig)
	return sig, nil
}

func (b *builtinJSONObjectSig) evalJSON(row chunk.Row) (res json.BinaryJSON, isNull bool, err error) {
	if len(b.Args)&1 == 1 {
		err = ErrIncorrectParameterCount.GenWithStackByArgs(ast.JSONObject)
		return res, true, err
	}
	jsons := make(map[string]interface{}, len(b.Args)>>1)
	var key string
	var value json.BinaryJSON
	for i, arg := range b.Args {
		if i&1 == 0 {
			key, isNull, err = arg.EvalString(b.Ctx, row)
			if err != nil {
				return res, true, err
			}
			if isNull {
				err = errors.New("JSON documents may not contain NULL member names")
				return res, true, err
			}
		} else {
			value, isNull, err = arg.EvalJSON(b.Ctx, row)
			if err != nil {
				return res, true, err
			}
			if isNull {
				value = json.CreateBinary(nil)
			}
			jsons[key] = value
		}
	}
	return json.CreateBinary(jsons), false, nil
}

type jsonArrayFunctionClass struct {
	BaseFunctionClass
}

type builtinJSONArraySig struct {
	BaseBuiltinFunc
}

func (b *builtinJSONArraySig) Clone() BuiltinFunc {
	newSig := &builtinJSONArraySig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (c *jsonArrayFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	argTps := make([]types.EvalType, 0, len(args))
	for range args {
		argTps = append(argTps, types.ETJson)
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETJson, argTps...)
	for i := range args {
		DisableParseJSONFlag4Expr(args[i])
	}
	sig := &builtinJSONArraySig{bf}
	sig.SetPbCode(tipb.ScalarFuncSig_JsonArraySig)
	return sig, nil
}

func (b *builtinJSONArraySig) evalJSON(row chunk.Row) (res json.BinaryJSON, isNull bool, err error) {
	jsons := make([]interface{}, 0, len(b.Args))
	for _, arg := range b.Args {
		j, isNull, err := arg.EvalJSON(b.Ctx, row)
		if err != nil {
			return res, true, err
		}
		if isNull {
			j = json.CreateBinary(nil)
		}
		jsons = append(jsons, j)
	}
	return json.CreateBinary(jsons), false, nil
}

type jsonContainsPathFunctionClass struct {
	BaseFunctionClass
}

type builtinJSONContainsPathSig struct {
	BaseBuiltinFunc
}

func (b *builtinJSONContainsPathSig) Clone() BuiltinFunc {
	newSig := &builtinJSONContainsPathSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (c *jsonContainsPathFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	argTps := []types.EvalType{types.ETJson, types.ETString}
	for i := 3; i <= len(args); i++ {
		argTps = append(argTps, types.ETString)
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETInt, argTps...)
	sig := &builtinJSONContainsPathSig{bf}
	sig.SetPbCode(tipb.ScalarFuncSig_JsonContainsPathSig)
	return sig, nil
}

func (b *builtinJSONContainsPathSig) evalInt(row chunk.Row) (res int64, isNull bool, err error) {
	obj, isNull, err := b.Args[0].EvalJSON(b.Ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	containType, isNull, err := b.Args[1].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	containType = strings.ToLower(containType)
	if containType != json.ContainsPathAll && containType != json.ContainsPathOne {
		return res, true, json.ErrInvalidJSONContainsPathType
	}
	var pathExpr json.PathExpression
	contains := int64(1)
	for i := 2; i < len(b.Args); i++ {
		path, isNull, err := b.Args[i].EvalString(b.Ctx, row)
		if isNull || err != nil {
			return res, isNull, err
		}
		if pathExpr, err = json.ParseJSONPathExpr(path); err != nil {
			return res, true, err
		}
		_, exists := obj.Extract([]json.PathExpression{pathExpr})
		switch {
		case exists && containType == json.ContainsPathOne:
			return 1, false, nil
		case !exists && containType == json.ContainsPathOne:
			contains = 0
		case !exists && containType == json.ContainsPathAll:
			return 0, false, nil
		}
	}
	return contains, false, nil
}

func jsonModify(ctx sessionctx.Context, args []Expression, row chunk.Row, mt json.ModifyType) (res json.BinaryJSON, isNull bool, err error) {
	res, isNull, err = args[0].EvalJSON(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	pathExprs := make([]json.PathExpression, 0, (len(args)-1)/2+1)
	for i := 1; i < len(args); i += 2 {
		// TODO: We can cache pathExprs if args are constants.
		var s string
		s, isNull, err = args[i].EvalString(ctx, row)
		if isNull || err != nil {
			return res, isNull, err
		}
		var pathExpr json.PathExpression
		pathExpr, err = json.ParseJSONPathExpr(s)
		if err != nil {
			return res, true, err
		}
		pathExprs = append(pathExprs, pathExpr)
	}
	values := make([]json.BinaryJSON, 0, (len(args)-1)/2+1)
	for i := 2; i < len(args); i += 2 {
		var value json.BinaryJSON
		value, isNull, err = args[i].EvalJSON(ctx, row)
		if err != nil {
			return res, true, err
		}
		if isNull {
			value = json.CreateBinary(nil)
		}
		values = append(values, value)
	}
	res, err = res.Modify(pathExprs, values, mt)
	if err != nil {
		return res, true, err
	}
	return res, false, nil
}

type jsonContainsFunctionClass struct {
	BaseFunctionClass
}

type builtinJSONContainsSig struct {
	BaseBuiltinFunc
}

func (b *builtinJSONContainsSig) Clone() BuiltinFunc {
	newSig := &builtinJSONContainsSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (c *jsonContainsFunctionClass) verifyArgs(args []Expression) error {
	if err := c.BaseFunctionClass.VerifyArgs(args); err != nil {
		return err
	}
	if evalType := args[0].GetType().EvalType(); evalType != types.ETJson && evalType != types.ETString {
		return json.ErrInvalidJSONData.GenWithStackByArgs(1, "json_contains")
	}
	if evalType := args[1].GetType().EvalType(); evalType != types.ETJson && evalType != types.ETString {
		return json.ErrInvalidJSONData.GenWithStackByArgs(2, "json_contains")
	}
	return nil
}

func (c *jsonContainsFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}

	argTps := []types.EvalType{types.ETJson, types.ETJson}
	if len(args) == 3 {
		argTps = append(argTps, types.ETString)
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETInt, argTps...)
	sig := &builtinJSONContainsSig{bf}
	sig.SetPbCode(tipb.ScalarFuncSig_JsonContainsSig)
	return sig, nil
}

func (b *builtinJSONContainsSig) evalInt(row chunk.Row) (res int64, isNull bool, err error) {
	obj, isNull, err := b.Args[0].EvalJSON(b.Ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	target, isNull, err := b.Args[1].EvalJSON(b.Ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	var pathExpr json.PathExpression
	if len(b.Args) == 3 {
		path, isNull, err := b.Args[2].EvalString(b.Ctx, row)
		if isNull || err != nil {
			return res, isNull, err
		}
		pathExpr, err = json.ParseJSONPathExpr(path)
		if err != nil {
			return res, true, err
		}
		if pathExpr.ContainsAnyAsterisk() {
			return res, true, json.ErrInvalidJSONPathWildcard
		}
		var exists bool
		obj, exists = obj.Extract([]json.PathExpression{pathExpr})
		if !exists {
			return res, true, nil
		}
	}

	if json.ContainsBinary(obj, target) {
		return 1, false, nil
	}
	return 0, false, nil
}

type jsonValidFunctionClass struct {
	BaseFunctionClass
}

func (c *jsonValidFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}

	var sig BuiltinFunc
	argType := args[0].GetType().EvalType()
	switch argType {
	case types.ETJson:
		bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETJson)
		sig = &builtinJSONValidJSONSig{bf}
		sig.SetPbCode(tipb.ScalarFuncSig_JsonValidJsonSig)
	case types.ETString:
		bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETString)
		sig = &builtinJSONValidStringSig{bf}
		sig.SetPbCode(tipb.ScalarFuncSig_JsonValidStringSig)
	default:
		bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETInt, argType)
		sig = &builtinJSONValidOthersSig{bf}
		sig.SetPbCode(tipb.ScalarFuncSig_JsonValidOthersSig)
	}
	return sig, nil
}

type builtinJSONValidJSONSig struct {
	BaseBuiltinFunc
}

func (b *builtinJSONValidJSONSig) Clone() BuiltinFunc {
	newSig := &builtinJSONValidJSONSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalInt evals a builtinJSONValidJSONSig.
// See https://dev.mysql.com/doc/refman/5.7/en/json-attribute-functions.html#function_json-valid
func (b *builtinJSONValidJSONSig) evalInt(row chunk.Row) (res int64, isNull bool, err error) {
	_, isNull, err = b.Args[0].EvalJSON(b.Ctx, row)
	return 1, isNull, err
}

type builtinJSONValidStringSig struct {
	BaseBuiltinFunc
}

func (b *builtinJSONValidStringSig) Clone() BuiltinFunc {
	newSig := &builtinJSONValidStringSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalInt evals a builtinJSONValidStringSig.
// See https://dev.mysql.com/doc/refman/5.7/en/json-attribute-functions.html#function_json-valid
func (b *builtinJSONValidStringSig) evalInt(row chunk.Row) (res int64, isNull bool, err error) {
	val, isNull, err := b.Args[0].EvalString(b.Ctx, row)
	if err != nil || isNull {
		return 0, isNull, err
	}

	data := hack.Slice(val)
	if json2.Valid(data) {
		res = 1
	} else {
		res = 0
	}
	return res, false, nil
}

type builtinJSONValidOthersSig struct {
	BaseBuiltinFunc
}

func (b *builtinJSONValidOthersSig) Clone() BuiltinFunc {
	newSig := &builtinJSONValidOthersSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalInt evals a builtinJSONValidOthersSig.
// See https://dev.mysql.com/doc/refman/5.7/en/json-attribute-functions.html#function_json-valid
func (b *builtinJSONValidOthersSig) evalInt(row chunk.Row) (res int64, isNull bool, err error) {
	return 0, false, nil
}

type jsonArrayAppendFunctionClass struct {
	BaseFunctionClass
}

type builtinJSONArrayAppendSig struct {
	BaseBuiltinFunc
}

func (c *jsonArrayAppendFunctionClass) verifyArgs(args []Expression) error {
	if len(args) < 3 || (len(args)&1 != 1) {
		return ErrIncorrectParameterCount.GenWithStackByArgs(c.FuncName)
	}
	return nil
}

func (c *jsonArrayAppendFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	argTps := make([]types.EvalType, 0, len(args))
	argTps = append(argTps, types.ETJson)
	for i := 1; i < len(args)-1; i += 2 {
		argTps = append(argTps, types.ETString, types.ETJson)
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETJson, argTps...)
	for i := 2; i < len(args); i += 2 {
		DisableParseJSONFlag4Expr(args[i])
	}
	sig := &builtinJSONArrayAppendSig{bf}
	sig.SetPbCode(tipb.ScalarFuncSig_JsonArrayAppendSig)
	return sig, nil
}

func (b *builtinJSONArrayAppendSig) Clone() BuiltinFunc {
	newSig := &builtinJSONArrayAppendSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinJSONArrayAppendSig) evalJSON(row chunk.Row) (res json.BinaryJSON, isNull bool, err error) {
	res, isNull, err = b.Args[0].EvalJSON(b.Ctx, row)
	if err != nil || isNull {
		return res, true, err
	}

	for i := 1; i < len(b.Args)-1; i += 2 {
		// If JSON path is NULL, MySQL breaks and returns NULL.
		s, isNull, err := b.Args[i].EvalString(b.Ctx, row)
		if isNull || err != nil {
			return res, true, err
		}

		// We should do the following checks to get correct values in res.Extract
		pathExpr, err := json.ParseJSONPathExpr(s)
		if err != nil {
			return res, true, json.ErrInvalidJSONPath.GenWithStackByArgs(s)
		}
		if pathExpr.ContainsAnyAsterisk() {
			return res, true, json.ErrInvalidJSONPathWildcard.GenWithStackByArgs(s)
		}

		obj, exists := res.Extract([]json.PathExpression{pathExpr})
		if !exists {
			// If path not exists, just do nothing and no errors.
			continue
		}

		if obj.TypeCode != json.TypeCodeArray {
			// res.Extract will return a json object instead of an array if there is an object at path pathExpr.
			// JSON_ARRAY_APPEND({"a": "b"}, "$", {"b": "c"}) => [{"a": "b"}, {"b", "c"}]
			// We should wrap them to a single array first.
			obj = json.CreateBinary([]interface{}{obj})
		}

		value, isnull, err := b.Args[i+1].EvalJSON(b.Ctx, row)
		if err != nil {
			return res, true, err
		}

		if isnull {
			value = json.CreateBinary(nil)
		}

		obj = json.MergeBinary([]json.BinaryJSON{obj, value})
		res, err = res.Modify([]json.PathExpression{pathExpr}, []json.BinaryJSON{obj}, json.ModifySet)
		if err != nil {
			// We checked pathExpr in the same way as res.Modify do.
			// So err should always be nil, the function should never return here.
			return res, true, err
		}
	}
	return res, false, nil
}

type jsonArrayInsertFunctionClass struct {
	BaseFunctionClass
}

type builtinJSONArrayInsertSig struct {
	BaseBuiltinFunc
}

func (c *jsonArrayInsertFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	if len(args)&1 != 1 {
		return nil, ErrIncorrectParameterCount.GenWithStackByArgs(c.FuncName)
	}

	argTps := make([]types.EvalType, 0, len(args))
	argTps = append(argTps, types.ETJson)
	for i := 1; i < len(args)-1; i += 2 {
		argTps = append(argTps, types.ETString, types.ETJson)
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETJson, argTps...)
	for i := 2; i < len(args); i += 2 {
		DisableParseJSONFlag4Expr(args[i])
	}
	sig := &builtinJSONArrayInsertSig{bf}
	sig.SetPbCode(tipb.ScalarFuncSig_JsonArrayInsertSig)
	return sig, nil
}

func (b *builtinJSONArrayInsertSig) Clone() BuiltinFunc {
	newSig := &builtinJSONArrayInsertSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinJSONArrayInsertSig) evalJSON(row chunk.Row) (res json.BinaryJSON, isNull bool, err error) {
	res, isNull, err = b.Args[0].EvalJSON(b.Ctx, row)
	if err != nil || isNull {
		return res, true, err
	}

	for i := 1; i < len(b.Args)-1; i += 2 {
		// If JSON path is NULL, MySQL breaks and returns NULL.
		s, isNull, err := b.Args[i].EvalString(b.Ctx, row)
		if err != nil || isNull {
			return res, true, err
		}

		pathExpr, err := json.ParseJSONPathExpr(s)
		if err != nil {
			return res, true, json.ErrInvalidJSONPath.GenWithStackByArgs(s)
		}
		if pathExpr.ContainsAnyAsterisk() {
			return res, true, json.ErrInvalidJSONPathWildcard.GenWithStackByArgs(s)
		}

		value, isnull, err := b.Args[i+1].EvalJSON(b.Ctx, row)
		if err != nil {
			return res, true, err
		}

		if isnull {
			value = json.CreateBinary(nil)
		}

		res, err = res.ArrayInsert(pathExpr, value)
		if err != nil {
			return res, true, err
		}
	}
	return res, false, nil
}

type jsonMergePatchFunctionClass struct {
	BaseFunctionClass
}

func (c *jsonMergePatchFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	return nil, errFunctionNotExists.GenWithStackByArgs("FUNCTION", "JSON_MERGE_PATCH")
}

type jsonMergePreserveFunctionClass struct {
	BaseFunctionClass
}

func (c *jsonMergePreserveFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	argTps := make([]types.EvalType, 0, len(args))
	for range args {
		argTps = append(argTps, types.ETJson)
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETJson, argTps...)
	sig := &builtinJSONMergeSig{bf}
	sig.SetPbCode(tipb.ScalarFuncSig_JsonMergePreserveSig)
	return sig, nil
}

type jsonPrettyFunctionClass struct {
	BaseFunctionClass
}

func (c *jsonPrettyFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	return nil, errFunctionNotExists.GenWithStackByArgs("FUNCTION", "JSON_PRETTY")
}

type jsonQuoteFunctionClass struct {
	BaseFunctionClass
}

type builtinJSONQuoteSig struct {
	BaseBuiltinFunc
}

func (b *builtinJSONQuoteSig) Clone() BuiltinFunc {
	newSig := &builtinJSONQuoteSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (c *jsonQuoteFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETString, types.ETJson)
	DisableParseJSONFlag4Expr(args[0])
	sig := &builtinJSONQuoteSig{bf}
	sig.SetPbCode(tipb.ScalarFuncSig_JsonQuoteSig)
	return sig, nil
}

func (b *builtinJSONQuoteSig) EvalString(row chunk.Row) (res string, isNull bool, err error) {
	var j json.BinaryJSON
	j, isNull, err = b.Args[0].EvalJSON(b.Ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	return j.Quote(), false, nil
}

type jsonSearchFunctionClass struct {
	BaseFunctionClass
}

type builtinJSONSearchSig struct {
	BaseBuiltinFunc
}

func (b *builtinJSONSearchSig) Clone() BuiltinFunc {
	newSig := &builtinJSONSearchSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (c *jsonSearchFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	// json_doc, one_or_all, search_str[, escape_char[, path] ...])
	argTps := make([]types.EvalType, 0, len(args))
	argTps = append(argTps, types.ETJson)
	for range args[1:] {
		argTps = append(argTps, types.ETString)
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETJson, argTps...)
	sig := &builtinJSONSearchSig{bf}
	sig.SetPbCode(tipb.ScalarFuncSig_JsonSearchSig)
	return sig, nil
}

func (b *builtinJSONSearchSig) evalJSON(row chunk.Row) (res json.BinaryJSON, isNull bool, err error) {
	// json_doc
	var obj json.BinaryJSON
	obj, isNull, err = b.Args[0].EvalJSON(b.Ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}

	// one_or_all
	var containType string
	containType, isNull, err = b.Args[1].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	if containType != json.ContainsPathAll && containType != json.ContainsPathOne {
		return res, true, errors.AddStack(json.ErrInvalidJSONContainsPathType)
	}

	// search_str & escape_char
	var searchStr string
	searchStr, isNull, err = b.Args[2].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	escape := byte('\\')
	if len(b.Args) >= 4 {
		var escapeStr string
		escapeStr, isNull, err = b.Args[3].EvalString(b.Ctx, row)
		if err != nil {
			return res, isNull, err
		}
		if isNull || len(escapeStr) == 0 {
			escape = byte('\\')
		} else if len(escapeStr) == 1 {
			escape = byte(escapeStr[0])
		} else {
			return res, true, errIncorrectArgs.GenWithStackByArgs("ESCAPE")
		}
	}
	patChars, patTypes := stringutil.CompilePattern(searchStr, escape)

	// result
	result := make([]interface{}, 0)

	// walk json_doc
	walkFn := func(fullpath json.PathExpression, bj json.BinaryJSON) (stop bool, err error) {
		if bj.TypeCode == json.TypeCodeString && stringutil.DoMatch(string(bj.GetString()), patChars, patTypes) {
			result = append(result, fullpath.String())
			if containType == json.ContainsPathOne {
				return true, nil
			}
		}
		return false, nil
	}
	if len(b.Args) >= 5 { // path...
		pathExprs := make([]json.PathExpression, 0, len(b.Args)-4)
		for i := 4; i < len(b.Args); i++ {
			var s string
			s, isNull, err = b.Args[i].EvalString(b.Ctx, row)
			if isNull || err != nil {
				return res, isNull, err
			}
			var pathExpr json.PathExpression
			pathExpr, err = json.ParseJSONPathExpr(s)
			if err != nil {
				return res, true, err
			}
			pathExprs = append(pathExprs, pathExpr)
		}
		err = obj.Walk(walkFn, pathExprs...)
		if err != nil {
			return res, true, err
		}
	} else {
		err = obj.Walk(walkFn)
		if err != nil {
			return res, true, err
		}
	}

	// return
	switch len(result) {
	case 0:
		return res, true, nil
	case 1:
		return json.CreateBinary(result[0]), false, nil
	default:
		return json.CreateBinary(result), false, nil
	}
}

type jsonStorageSizeFunctionClass struct {
	BaseFunctionClass
}

func (c *jsonStorageSizeFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	return nil, errFunctionNotExists.GenWithStackByArgs("FUNCTION", "JSON_STORAGE_SIZE")
}

type jsonDepthFunctionClass struct {
	BaseFunctionClass
}

type builtinJSONDepthSig struct {
	BaseBuiltinFunc
}

func (b *builtinJSONDepthSig) Clone() BuiltinFunc {
	newSig := &builtinJSONDepthSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (c *jsonDepthFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}

	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETJson)
	sig := &builtinJSONDepthSig{bf}
	sig.SetPbCode(tipb.ScalarFuncSig_JsonDepthSig)
	return sig, nil
}

func (b *builtinJSONDepthSig) evalInt(row chunk.Row) (res int64, isNull bool, err error) {
	obj, isNull, err := b.Args[0].EvalJSON(b.Ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}

	return int64(obj.GetElemDepth()), false, nil
}

type jsonKeysFunctionClass struct {
	BaseFunctionClass
}

func (c *jsonKeysFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	argTps := []types.EvalType{types.ETJson}
	if len(args) == 2 {
		argTps = append(argTps, types.ETString)
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETJson, argTps...)
	var sig BuiltinFunc
	switch len(args) {
	case 1:
		sig = &builtinJSONKeysSig{bf}
		sig.SetPbCode(tipb.ScalarFuncSig_JsonKeysSig)
	case 2:
		sig = &builtinJSONKeys2ArgsSig{bf}
		sig.SetPbCode(tipb.ScalarFuncSig_JsonKeys2ArgsSig)
	}
	return sig, nil
}

type builtinJSONKeysSig struct {
	BaseBuiltinFunc
}

func (b *builtinJSONKeysSig) Clone() BuiltinFunc {
	newSig := &builtinJSONKeysSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinJSONKeysSig) evalJSON(row chunk.Row) (res json.BinaryJSON, isNull bool, err error) {
	res, isNull, err = b.Args[0].EvalJSON(b.Ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	if res.TypeCode != json.TypeCodeObject {
		return res, true, json.ErrInvalidJSONData
	}
	return res.GetKeys(), false, nil
}

type builtinJSONKeys2ArgsSig struct {
	BaseBuiltinFunc
}

func (b *builtinJSONKeys2ArgsSig) Clone() BuiltinFunc {
	newSig := &builtinJSONKeys2ArgsSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinJSONKeys2ArgsSig) evalJSON(row chunk.Row) (res json.BinaryJSON, isNull bool, err error) {
	res, isNull, err = b.Args[0].EvalJSON(b.Ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	if res.TypeCode != json.TypeCodeObject {
		return res, true, json.ErrInvalidJSONData
	}

	path, isNull, err := b.Args[1].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}

	pathExpr, err := json.ParseJSONPathExpr(path)
	if err != nil {
		return res, true, err
	}
	if pathExpr.ContainsAnyAsterisk() {
		return res, true, json.ErrInvalidJSONPathWildcard
	}

	res, exists := res.Extract([]json.PathExpression{pathExpr})
	if !exists {
		return res, true, nil
	}
	if res.TypeCode != json.TypeCodeObject {
		return res, true, nil
	}

	return res.GetKeys(), false, nil
}

type jsonLengthFunctionClass struct {
	BaseFunctionClass
}

type builtinJSONLengthSig struct {
	BaseBuiltinFunc
}

func (b *builtinJSONLengthSig) Clone() BuiltinFunc {
	newSig := &builtinJSONLengthSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (c *jsonLengthFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}

	argTps := make([]types.EvalType, 0, len(args))
	argTps = append(argTps, types.ETJson)
	if len(args) == 2 {
		argTps = append(argTps, types.ETString)
	}

	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETInt, argTps...)
	sig := &builtinJSONLengthSig{bf}
	sig.SetPbCode(tipb.ScalarFuncSig_JsonLengthSig)
	return sig, nil
}

func (b *builtinJSONLengthSig) evalInt(row chunk.Row) (res int64, isNull bool, err error) {
	obj, isNull, err := b.Args[0].EvalJSON(b.Ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}

	if obj.TypeCode != json.TypeCodeObject && obj.TypeCode != json.TypeCodeArray {
		return 1, false, nil
	}

	if len(b.Args) == 2 {
		path, isNull, err := b.Args[1].EvalString(b.Ctx, row)
		if isNull || err != nil {
			return res, isNull, err
		}

		pathExpr, err := json.ParseJSONPathExpr(path)
		if err != nil {
			return res, true, err
		}
		if pathExpr.ContainsAnyAsterisk() {
			return res, true, json.ErrInvalidJSONPathWildcard
		}

		var exists bool
		obj, exists = obj.Extract([]json.PathExpression{pathExpr})
		if !exists {
			return res, true, nil
		}
		if obj.TypeCode != json.TypeCodeObject && obj.TypeCode != json.TypeCodeArray {
			return 1, false, nil
		}
	}
	return int64(obj.GetElemCount()), false, nil
}
