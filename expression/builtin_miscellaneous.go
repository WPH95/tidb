// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
// // Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package expression

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"net"
	"strings"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/types/json"
	"github.com/pingcap/tidb/util/chunk"
)

var (
	_ FunctionClass = &sleepFunctionClass{}
	_ FunctionClass = &lockFunctionClass{}
	_ FunctionClass = &releaseLockFunctionClass{}
	_ FunctionClass = &anyValueFunctionClass{}
	_ FunctionClass = &defaultFunctionClass{}
	_ FunctionClass = &inetAtonFunctionClass{}
	_ FunctionClass = &inetNtoaFunctionClass{}
	_ FunctionClass = &inet6AtonFunctionClass{}
	_ FunctionClass = &inet6NtoaFunctionClass{}
	_ FunctionClass = &isFreeLockFunctionClass{}
	_ FunctionClass = &isIPv4FunctionClass{}
	_ FunctionClass = &isIPv4CompatFunctionClass{}
	_ FunctionClass = &isIPv4MappedFunctionClass{}
	_ FunctionClass = &isIPv6FunctionClass{}
	_ FunctionClass = &isUsedLockFunctionClass{}
	_ FunctionClass = &masterPosWaitFunctionClass{}
	_ FunctionClass = &nameConstFunctionClass{}
	_ FunctionClass = &releaseAllLocksFunctionClass{}
	_ FunctionClass = &uuidFunctionClass{}
	_ FunctionClass = &uuidShortFunctionClass{}
)

var (
	_ BuiltinFunc = &builtinSleepSig{}
	_ BuiltinFunc = &builtinLockSig{}
	_ BuiltinFunc = &builtinReleaseLockSig{}
	_ BuiltinFunc = &builtinDecimalAnyValueSig{}
	_ BuiltinFunc = &builtinDurationAnyValueSig{}
	_ BuiltinFunc = &builtinIntAnyValueSig{}
	_ BuiltinFunc = &builtinJSONAnyValueSig{}
	_ BuiltinFunc = &builtinRealAnyValueSig{}
	_ BuiltinFunc = &builtinStringAnyValueSig{}
	_ BuiltinFunc = &builtinTimeAnyValueSig{}
	_ BuiltinFunc = &builtinInetAtonSig{}
	_ BuiltinFunc = &builtinInetNtoaSig{}
	_ BuiltinFunc = &builtinInet6AtonSig{}
	_ BuiltinFunc = &builtinInet6NtoaSig{}
	_ BuiltinFunc = &builtinIsIPv4Sig{}
	_ BuiltinFunc = &builtinIsIPv4CompatSig{}
	_ BuiltinFunc = &builtinIsIPv4MappedSig{}
	_ BuiltinFunc = &builtinIsIPv6Sig{}
	_ BuiltinFunc = &builtinUUIDSig{}

	_ BuiltinFunc = &builtinNameConstIntSig{}
	_ BuiltinFunc = &builtinNameConstRealSig{}
	_ BuiltinFunc = &builtinNameConstDecimalSig{}
	_ BuiltinFunc = &builtinNameConstTimeSig{}
	_ BuiltinFunc = &builtinNameConstDurationSig{}
	_ BuiltinFunc = &builtinNameConstStringSig{}
	_ BuiltinFunc = &builtinNameConstJSONSig{}
)

type sleepFunctionClass struct {
	BaseFunctionClass
}

func (c *sleepFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETReal)
	bf.Tp.Flen = 21
	sig := &builtinSleepSig{bf}
	return sig, nil
}

type builtinSleepSig struct {
	BaseBuiltinFunc
}

func (b *builtinSleepSig) Clone() BuiltinFunc {
	newSig := &builtinSleepSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalInt evals a builtinSleepSig.
// See https://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_sleep
func (b *builtinSleepSig) evalInt(row chunk.Row) (int64, bool, error) {
	val, isNull, err := b.Args[0].EvalReal(b.Ctx, row)
	if err != nil {
		return 0, isNull, err
	}

	sessVars := b.Ctx.GetSessionVars()
	if isNull {
		if sessVars.StrictSQLMode {
			return 0, true, errIncorrectArgs.GenWithStackByArgs("sleep")
		}
		return 0, true, nil
	}
	// processing argument is negative
	if val < 0 {
		if sessVars.StrictSQLMode {
			return 0, false, errIncorrectArgs.GenWithStackByArgs("sleep")
		}
		return 0, false, nil
	}

	if val > math.MaxFloat64/float64(time.Second.Nanoseconds()) {
		return 0, false, errIncorrectArgs.GenWithStackByArgs("sleep")
	}

	dur := time.Duration(val * float64(time.Second.Nanoseconds()))
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	start := time.Now()
	finish := false
	for !finish {
		select {
		case now := <-ticker.C:
			if now.Sub(start) > dur {
				finish = true
			}
		default:
			if atomic.CompareAndSwapUint32(&sessVars.Killed, 1, 0) {
				return 1, false, nil
			}
		}
	}

	return 0, false, nil
}

type lockFunctionClass struct {
	BaseFunctionClass
}

func (c *lockFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETString, types.ETInt)
	sig := &builtinLockSig{bf}
	bf.Tp.Flen = 1
	return sig, nil
}

type builtinLockSig struct {
	BaseBuiltinFunc
}

func (b *builtinLockSig) Clone() BuiltinFunc {
	newSig := &builtinLockSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalInt evals a builtinLockSig.
// See https://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_get-lock
// The lock function will do nothing.
// Warning: get_lock() function is parsed but ignored.
func (b *builtinLockSig) evalInt(_ chunk.Row) (int64, bool, error) {
	return 1, false, nil
}

type releaseLockFunctionClass struct {
	BaseFunctionClass
}

func (c *releaseLockFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETString)
	sig := &builtinReleaseLockSig{bf}
	bf.Tp.Flen = 1
	return sig, nil
}

type builtinReleaseLockSig struct {
	BaseBuiltinFunc
}

func (b *builtinReleaseLockSig) Clone() BuiltinFunc {
	newSig := &builtinReleaseLockSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalInt evals a builtinReleaseLockSig.
// See https://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_release-lock
// The release lock function will do nothing.
// Warning: release_lock() function is parsed but ignored.
func (b *builtinReleaseLockSig) evalInt(_ chunk.Row) (int64, bool, error) {
	return 1, false, nil
}

type anyValueFunctionClass struct {
	BaseFunctionClass
}

func (c *anyValueFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	argTp := args[0].GetType().EvalType()
	bf := NewBaseBuiltinFuncWithTp(ctx, args, argTp, argTp)
	args[0].GetType().Flag |= bf.Tp.Flag
	*bf.Tp = *args[0].GetType()
	var sig BuiltinFunc
	switch argTp {
	case types.ETDecimal:
		sig = &builtinDecimalAnyValueSig{bf}
	case types.ETDuration:
		sig = &builtinDurationAnyValueSig{bf}
	case types.ETInt:
		bf.Tp.Decimal = 0
		sig = &builtinIntAnyValueSig{bf}
	case types.ETJson:
		sig = &builtinJSONAnyValueSig{bf}
	case types.ETReal:
		sig = &builtinRealAnyValueSig{bf}
	case types.ETString:
		bf.Tp.Decimal = types.UnspecifiedLength
		sig = &builtinStringAnyValueSig{bf}
	case types.ETDatetime, types.ETTimestamp:
		bf.Tp.Charset, bf.Tp.Collate, bf.Tp.Flag = mysql.DefaultCharset, mysql.DefaultCollationName, 0
		sig = &builtinTimeAnyValueSig{bf}
	default:
		return nil, errIncorrectArgs.GenWithStackByArgs("ANY_VALUE")
	}
	return sig, nil
}

type builtinDecimalAnyValueSig struct {
	BaseBuiltinFunc
}

func (b *builtinDecimalAnyValueSig) Clone() BuiltinFunc {
	newSig := &builtinDecimalAnyValueSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalDecimal evals a builtinDecimalAnyValueSig.
// See https://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_any-value
func (b *builtinDecimalAnyValueSig) evalDecimal(row chunk.Row) (*types.MyDecimal, bool, error) {
	return b.Args[0].EvalDecimal(b.Ctx, row)
}

type builtinDurationAnyValueSig struct {
	BaseBuiltinFunc
}

func (b *builtinDurationAnyValueSig) Clone() BuiltinFunc {
	newSig := &builtinDurationAnyValueSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalDuration evals a builtinDurationAnyValueSig.
// See https://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_any-value
func (b *builtinDurationAnyValueSig) evalDuration(row chunk.Row) (types.Duration, bool, error) {
	return b.Args[0].EvalDuration(b.Ctx, row)
}

type builtinIntAnyValueSig struct {
	BaseBuiltinFunc
}

func (b *builtinIntAnyValueSig) Clone() BuiltinFunc {
	newSig := &builtinIntAnyValueSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalInt evals a builtinIntAnyValueSig.
// See https://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_any-value
func (b *builtinIntAnyValueSig) evalInt(row chunk.Row) (int64, bool, error) {
	return b.Args[0].EvalInt(b.Ctx, row)
}

type builtinJSONAnyValueSig struct {
	BaseBuiltinFunc
}

func (b *builtinJSONAnyValueSig) Clone() BuiltinFunc {
	newSig := &builtinJSONAnyValueSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalJSON evals a builtinJSONAnyValueSig.
// See https://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_any-value
func (b *builtinJSONAnyValueSig) evalJSON(row chunk.Row) (json.BinaryJSON, bool, error) {
	return b.Args[0].EvalJSON(b.Ctx, row)
}

type builtinRealAnyValueSig struct {
	BaseBuiltinFunc
}

func (b *builtinRealAnyValueSig) Clone() BuiltinFunc {
	newSig := &builtinRealAnyValueSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalReal evals a builtinRealAnyValueSig.
// See https://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_any-value
func (b *builtinRealAnyValueSig) evalReal(row chunk.Row) (float64, bool, error) {
	return b.Args[0].EvalReal(b.Ctx, row)
}

type builtinStringAnyValueSig struct {
	BaseBuiltinFunc
}

func (b *builtinStringAnyValueSig) Clone() BuiltinFunc {
	newSig := &builtinStringAnyValueSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalString evals a builtinStringAnyValueSig.
// See https://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_any-value
func (b *builtinStringAnyValueSig) EvalString(row chunk.Row) (string, bool, error) {
	return b.Args[0].EvalString(b.Ctx, row)
}

type builtinTimeAnyValueSig struct {
	BaseBuiltinFunc
}

func (b *builtinTimeAnyValueSig) Clone() BuiltinFunc {
	newSig := &builtinTimeAnyValueSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalTime evals a builtinTimeAnyValueSig.
// See https://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_any-value
func (b *builtinTimeAnyValueSig) evalTime(row chunk.Row) (types.Time, bool, error) {
	return b.Args[0].EvalTime(b.Ctx, row)
}

type defaultFunctionClass struct {
	BaseFunctionClass
}

func (c *defaultFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	return nil, errFunctionNotExists.GenWithStackByArgs("FUNCTION", "DEFAULT")
}

type inetAtonFunctionClass struct {
	BaseFunctionClass
}

func (c *inetAtonFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETString)
	bf.Tp.Flen = 21
	bf.Tp.Flag |= mysql.UnsignedFlag
	sig := &builtinInetAtonSig{bf}
	return sig, nil
}

type builtinInetAtonSig struct {
	BaseBuiltinFunc
}

func (b *builtinInetAtonSig) Clone() BuiltinFunc {
	newSig := &builtinInetAtonSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalInt evals a builtinInetAtonSig.
// See https://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_inet-aton
func (b *builtinInetAtonSig) evalInt(row chunk.Row) (int64, bool, error) {
	val, isNull, err := b.Args[0].EvalString(b.Ctx, row)
	if err != nil || isNull {
		return 0, true, err
	}
	// ip address should not end with '.'.
	if len(val) == 0 || val[len(val)-1] == '.' {
		return 0, true, nil
	}

	var (
		byteResult, result uint64
		dotCount           int
	)
	for _, c := range val {
		if c >= '0' && c <= '9' {
			digit := uint64(c - '0')
			byteResult = byteResult*10 + digit
			if byteResult > 255 {
				return 0, true, nil
			}
		} else if c == '.' {
			dotCount++
			if dotCount > 3 {
				return 0, true, nil
			}
			result = (result << 8) + byteResult
			byteResult = 0
		} else {
			return 0, true, nil
		}
	}
	// 127 		-> 0.0.0.127
	// 127.255 	-> 127.0.0.255
	// 127.256	-> NULL
	// 127.2.1	-> 127.2.0.1
	switch dotCount {
	case 1:
		result <<= 8
		fallthrough
	case 2:
		result <<= 8
	}
	return int64((result << 8) + byteResult), false, nil
}

type inetNtoaFunctionClass struct {
	BaseFunctionClass
}

func (c *inetNtoaFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETString, types.ETInt)
	bf.Tp.Flen = 93
	bf.Tp.Decimal = 0
	sig := &builtinInetNtoaSig{bf}
	return sig, nil
}

type builtinInetNtoaSig struct {
	BaseBuiltinFunc
}

func (b *builtinInetNtoaSig) Clone() BuiltinFunc {
	newSig := &builtinInetNtoaSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalString evals a builtinInetNtoaSig.
// See https://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_inet-ntoa
func (b *builtinInetNtoaSig) EvalString(row chunk.Row) (string, bool, error) {
	val, isNull, err := b.Args[0].EvalInt(b.Ctx, row)
	if err != nil || isNull {
		return "", true, err
	}

	if val < 0 || uint64(val) > math.MaxUint32 {
		//not an IPv4 address.
		return "", true, nil
	}
	ip := make(net.IP, net.IPv4len)
	binary.BigEndian.PutUint32(ip, uint32(val))
	ipv4 := ip.To4()
	if ipv4 == nil {
		//Not a vaild ipv4 address.
		return "", true, nil
	}

	return ipv4.String(), false, nil
}

type inet6AtonFunctionClass struct {
	BaseFunctionClass
}

func (c *inet6AtonFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETString, types.ETString)
	bf.Tp.Flen = 16
	types.SetBinChsClnFlag(bf.Tp)
	bf.Tp.Decimal = 0
	sig := &builtinInet6AtonSig{bf}
	return sig, nil
}

type builtinInet6AtonSig struct {
	BaseBuiltinFunc
}

func (b *builtinInet6AtonSig) Clone() BuiltinFunc {
	newSig := &builtinInet6AtonSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalString evals a builtinInet6AtonSig.
// See https://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_inet6-aton
func (b *builtinInet6AtonSig) EvalString(row chunk.Row) (string, bool, error) {
	val, isNull, err := b.Args[0].EvalString(b.Ctx, row)
	if err != nil || isNull {
		return "", true, err
	}

	if len(val) == 0 {
		return "", true, nil
	}

	ip := net.ParseIP(val)
	if ip == nil {
		return "", true, nil
	}

	var isMappedIpv6 bool
	if ip.To4() != nil && strings.Contains(val, ":") {
		//mapped ipv6 address.
		isMappedIpv6 = true
	}

	var result []byte
	if isMappedIpv6 || ip.To4() == nil {
		result = make([]byte, net.IPv6len)
	} else {
		result = make([]byte, net.IPv4len)
	}

	if isMappedIpv6 {
		copy(result[12:], ip.To4())
		result[11] = 0xff
		result[10] = 0xff
	} else if ip.To4() == nil {
		copy(result, ip.To16())
	} else {
		copy(result, ip.To4())
	}

	return string(result[:]), false, nil
}

type inet6NtoaFunctionClass struct {
	BaseFunctionClass
}

func (c *inet6NtoaFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETString, types.ETString)
	bf.Tp.Flen = 117
	bf.Tp.Decimal = 0
	sig := &builtinInet6NtoaSig{bf}
	return sig, nil
}

type builtinInet6NtoaSig struct {
	BaseBuiltinFunc
}

func (b *builtinInet6NtoaSig) Clone() BuiltinFunc {
	newSig := &builtinInet6NtoaSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalString evals a builtinInet6NtoaSig.
// See https://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_inet6-ntoa
func (b *builtinInet6NtoaSig) EvalString(row chunk.Row) (string, bool, error) {
	val, isNull, err := b.Args[0].EvalString(b.Ctx, row)
	if err != nil || isNull {
		return "", true, err
	}
	ip := net.IP([]byte(val)).String()
	if len(val) == net.IPv6len && !strings.Contains(ip, ":") {
		ip = fmt.Sprintf("::ffff:%s", ip)
	}

	if net.ParseIP(ip) == nil {
		return "", true, nil
	}

	return ip, false, nil
}

type isFreeLockFunctionClass struct {
	BaseFunctionClass
}

func (c *isFreeLockFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	return nil, errFunctionNotExists.GenWithStackByArgs("FUNCTION", "IS_FREE_LOCK")
}

type isIPv4FunctionClass struct {
	BaseFunctionClass
}

func (c *isIPv4FunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETString)
	bf.Tp.Flen = 1
	sig := &builtinIsIPv4Sig{bf}
	return sig, nil
}

type builtinIsIPv4Sig struct {
	BaseBuiltinFunc
}

func (b *builtinIsIPv4Sig) Clone() BuiltinFunc {
	newSig := &builtinIsIPv4Sig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalInt evals a builtinIsIPv4Sig.
// See https://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_is-ipv4
func (b *builtinIsIPv4Sig) evalInt(row chunk.Row) (int64, bool, error) {
	val, isNull, err := b.Args[0].EvalString(b.Ctx, row)
	if err != nil || isNull {
		return 0, err != nil, err
	}
	if isIPv4(val) {
		return 1, false, nil
	}
	return 0, false, nil
}

// isIPv4 checks IPv4 address which satisfying the format A.B.C.D(0<=A/B/C/D<=255).
// Mapped IPv6 address like '::ffff:1.2.3.4' would return false.
func isIPv4(ip string) bool {
	// acc: keep the decimal value of each segment under check, which should between 0 and 255 for valid IPv4 address.
	// pd: sentinel for '.'
	dots, acc, pd := 0, 0, true
	for _, c := range ip {
		switch {
		case '0' <= c && c <= '9':
			acc = acc*10 + int(c-'0')
			pd = false
		case c == '.':
			dots++
			if dots > 3 || acc > 255 || pd {
				return false
			}
			acc, pd = 0, true
		default:
			return false
		}
	}
	if dots != 3 || acc > 255 || pd {
		return false
	}
	return true
}

type isIPv4CompatFunctionClass struct {
	BaseFunctionClass
}

func (c *isIPv4CompatFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETString)
	bf.Tp.Flen = 1
	sig := &builtinIsIPv4CompatSig{bf}
	return sig, nil
}

type builtinIsIPv4CompatSig struct {
	BaseBuiltinFunc
}

func (b *builtinIsIPv4CompatSig) Clone() BuiltinFunc {
	newSig := &builtinIsIPv4CompatSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalInt evals Is_IPv4_Compat
// See https://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_is-ipv4-compat
func (b *builtinIsIPv4CompatSig) evalInt(row chunk.Row) (int64, bool, error) {
	val, isNull, err := b.Args[0].EvalString(b.Ctx, row)
	if err != nil || isNull {
		return 0, err != nil, err
	}

	ipAddress := []byte(val)
	if len(ipAddress) != net.IPv6len {
		//Not an IPv6 address, return false
		return 0, false, nil
	}

	prefixCompat := []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
	if !bytes.HasPrefix(ipAddress, prefixCompat) {
		return 0, false, nil
	}
	return 1, false, nil
}

type isIPv4MappedFunctionClass struct {
	BaseFunctionClass
}

func (c *isIPv4MappedFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETString)
	bf.Tp.Flen = 1
	sig := &builtinIsIPv4MappedSig{bf}
	return sig, nil
}

type builtinIsIPv4MappedSig struct {
	BaseBuiltinFunc
}

func (b *builtinIsIPv4MappedSig) Clone() BuiltinFunc {
	newSig := &builtinIsIPv4MappedSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalInt evals Is_IPv4_Mapped
// See https://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_is-ipv4-mapped
func (b *builtinIsIPv4MappedSig) evalInt(row chunk.Row) (int64, bool, error) {
	val, isNull, err := b.Args[0].EvalString(b.Ctx, row)
	if err != nil || isNull {
		return 0, err != nil, err
	}

	ipAddress := []byte(val)
	if len(ipAddress) != net.IPv6len {
		//Not an IPv6 address, return false
		return 0, false, nil
	}

	prefixMapped := []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff}
	if !bytes.HasPrefix(ipAddress, prefixMapped) {
		return 0, false, nil
	}
	return 1, false, nil
}

type isIPv6FunctionClass struct {
	BaseFunctionClass
}

func (c *isIPv6FunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETString)
	bf.Tp.Flen = 1
	sig := &builtinIsIPv6Sig{bf}
	return sig, nil
}

type builtinIsIPv6Sig struct {
	BaseBuiltinFunc
}

func (b *builtinIsIPv6Sig) Clone() BuiltinFunc {
	newSig := &builtinIsIPv6Sig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalInt evals a builtinIsIPv6Sig.
// See https://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_is-ipv6
func (b *builtinIsIPv6Sig) evalInt(row chunk.Row) (int64, bool, error) {
	val, isNull, err := b.Args[0].EvalString(b.Ctx, row)
	if err != nil || isNull {
		return 0, err != nil, err
	}
	ip := net.ParseIP(val)
	if ip != nil && !isIPv4(val) {
		return 1, false, nil
	}
	return 0, false, nil
}

type isUsedLockFunctionClass struct {
	BaseFunctionClass
}

func (c *isUsedLockFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	return nil, errFunctionNotExists.GenWithStackByArgs("FUNCTION", "IS_USED_LOCK")
}

type masterPosWaitFunctionClass struct {
	BaseFunctionClass
}

func (c *masterPosWaitFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	return nil, errFunctionNotExists.GenWithStackByArgs("FUNCTION", "MASTER_POS_WAIT")
}

type nameConstFunctionClass struct {
	BaseFunctionClass
}

func (c *nameConstFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	argTp := args[1].GetType().EvalType()
	bf := NewBaseBuiltinFuncWithTp(ctx, args, argTp, types.ETString, argTp)
	*bf.Tp = *args[1].GetType()
	var sig BuiltinFunc
	switch argTp {
	case types.ETDecimal:
		sig = &builtinNameConstDecimalSig{bf}
	case types.ETDuration:
		sig = &builtinNameConstDurationSig{bf}
	case types.ETInt:
		bf.Tp.Decimal = 0
		sig = &builtinNameConstIntSig{bf}
	case types.ETJson:
		sig = &builtinNameConstJSONSig{bf}
	case types.ETReal:
		sig = &builtinNameConstRealSig{bf}
	case types.ETString:
		bf.Tp.Decimal = types.UnspecifiedLength
		sig = &builtinNameConstStringSig{bf}
	case types.ETDatetime, types.ETTimestamp:
		bf.Tp.Charset, bf.Tp.Collate, bf.Tp.Flag = mysql.DefaultCharset, mysql.DefaultCollationName, 0
		sig = &builtinNameConstTimeSig{bf}
	default:
		return nil, errIncorrectArgs.GenWithStackByArgs("NAME_CONST")
	}
	return sig, nil
}

type builtinNameConstDecimalSig struct {
	BaseBuiltinFunc
}

func (b *builtinNameConstDecimalSig) Clone() BuiltinFunc {
	newSig := &builtinNameConstDecimalSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinNameConstDecimalSig) evalDecimal(row chunk.Row) (*types.MyDecimal, bool, error) {
	return b.Args[1].EvalDecimal(b.Ctx, row)
}

type builtinNameConstIntSig struct {
	BaseBuiltinFunc
}

func (b *builtinNameConstIntSig) Clone() BuiltinFunc {
	newSig := &builtinNameConstIntSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinNameConstIntSig) evalInt(row chunk.Row) (int64, bool, error) {
	return b.Args[1].EvalInt(b.Ctx, row)
}

type builtinNameConstRealSig struct {
	BaseBuiltinFunc
}

func (b *builtinNameConstRealSig) Clone() BuiltinFunc {
	newSig := &builtinNameConstRealSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinNameConstRealSig) evalReal(row chunk.Row) (float64, bool, error) {
	return b.Args[1].EvalReal(b.Ctx, row)
}

type builtinNameConstStringSig struct {
	BaseBuiltinFunc
}

func (b *builtinNameConstStringSig) Clone() BuiltinFunc {
	newSig := &builtinNameConstStringSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinNameConstStringSig) EvalString(row chunk.Row) (string, bool, error) {
	return b.Args[1].EvalString(b.Ctx, row)
}

type builtinNameConstJSONSig struct {
	BaseBuiltinFunc
}

func (b *builtinNameConstJSONSig) Clone() BuiltinFunc {
	newSig := &builtinNameConstJSONSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinNameConstJSONSig) evalJSON(row chunk.Row) (json.BinaryJSON, bool, error) {
	return b.Args[1].EvalJSON(b.Ctx, row)
}

type builtinNameConstDurationSig struct {
	BaseBuiltinFunc
}

func (b *builtinNameConstDurationSig) Clone() BuiltinFunc {
	newSig := &builtinNameConstDurationSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinNameConstDurationSig) evalDuration(row chunk.Row) (types.Duration, bool, error) {
	return b.Args[1].EvalDuration(b.Ctx, row)
}

type builtinNameConstTimeSig struct {
	BaseBuiltinFunc
}

func (b *builtinNameConstTimeSig) Clone() BuiltinFunc {
	newSig := &builtinNameConstTimeSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

func (b *builtinNameConstTimeSig) evalTime(row chunk.Row) (types.Time, bool, error) {
	return b.Args[1].EvalTime(b.Ctx, row)
}

type releaseAllLocksFunctionClass struct {
	BaseFunctionClass
}

func (c *releaseAllLocksFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	return nil, errFunctionNotExists.GenWithStackByArgs("FUNCTION", "RELEASE_ALL_LOCKS")
}

type uuidFunctionClass struct {
	BaseFunctionClass
}

func (c *uuidFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETString)
	bf.Tp.Flen = 36
	sig := &builtinUUIDSig{bf}
	return sig, nil
}

type builtinUUIDSig struct {
	BaseBuiltinFunc
}

func (b *builtinUUIDSig) Clone() BuiltinFunc {
	newSig := &builtinUUIDSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalString evals a builtinUUIDSig.
// See https://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_uuid
func (b *builtinUUIDSig) EvalString(_ chunk.Row) (d string, isNull bool, err error) {
	var id uuid.UUID
	id, err = uuid.NewUUID()
	if err != nil {
		return
	}
	d = id.String()
	return
}

type uuidShortFunctionClass struct {
	BaseFunctionClass
}

func (c *uuidShortFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	return nil, errFunctionNotExists.GenWithStackByArgs("FUNCTION", "UUID_SHORT")
}
