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
	"bytes"
	"compress/zlib"
	"crypto/aes"
	"crypto/md5"
	"crypto/rand"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/binary"
	"fmt"
	"hash"
	"io"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/auth"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/encrypt"
)

var (
	_ FunctionClass = &aesDecryptFunctionClass{}
	_ FunctionClass = &aesEncryptFunctionClass{}
	_ FunctionClass = &compressFunctionClass{}
	_ FunctionClass = &decodeFunctionClass{}
	_ FunctionClass = &desDecryptFunctionClass{}
	_ FunctionClass = &desEncryptFunctionClass{}
	_ FunctionClass = &encodeFunctionClass{}
	_ FunctionClass = &encryptFunctionClass{}
	_ FunctionClass = &md5FunctionClass{}
	_ FunctionClass = &oldPasswordFunctionClass{}
	_ FunctionClass = &passwordFunctionClass{}
	_ FunctionClass = &randomBytesFunctionClass{}
	_ FunctionClass = &sha1FunctionClass{}
	_ FunctionClass = &sha2FunctionClass{}
	_ FunctionClass = &uncompressFunctionClass{}
	_ FunctionClass = &uncompressedLengthFunctionClass{}
	_ FunctionClass = &validatePasswordStrengthFunctionClass{}
)

var (
	_ BuiltinFunc = &builtinAesDecryptSig{}
	_ BuiltinFunc = &builtinAesDecryptIVSig{}
	_ BuiltinFunc = &builtinAesEncryptSig{}
	_ BuiltinFunc = &builtinAesEncryptIVSig{}
	_ BuiltinFunc = &builtinCompressSig{}
	_ BuiltinFunc = &builtinMD5Sig{}
	_ BuiltinFunc = &builtinPasswordSig{}
	_ BuiltinFunc = &builtinRandomBytesSig{}
	_ BuiltinFunc = &builtinSHA1Sig{}
	_ BuiltinFunc = &builtinSHA2Sig{}
	_ BuiltinFunc = &builtinUncompressSig{}
	_ BuiltinFunc = &builtinUncompressedLengthSig{}
)

// ivSize indicates the initialization vector supplied to aes_decrypt
const ivSize = aes.BlockSize

// aesModeAttr indicates that the key length and iv attribute for specific block_encryption_mode.
// keySize is the key length in bits and mode is the encryption mode.
// ivRequired indicates that initialization vector is required or not.
type aesModeAttr struct {
	modeName   string
	keySize    int
	ivRequired bool
}

var aesModes = map[string]*aesModeAttr{
	//TODO support more modes, permitted mode values are: ECB, CBC, CFB1, CFB8, CFB128, OFB
	"aes-128-ecb": {"ecb", 16, false},
	"aes-192-ecb": {"ecb", 24, false},
	"aes-256-ecb": {"ecb", 32, false},
	"aes-128-cbc": {"cbc", 16, true},
	"aes-192-cbc": {"cbc", 24, true},
	"aes-256-cbc": {"cbc", 32, true},
	"aes-128-ofb": {"ofb", 16, true},
	"aes-192-ofb": {"ofb", 24, true},
	"aes-256-ofb": {"ofb", 32, true},
	"aes-128-cfb": {"cfb", 16, true},
	"aes-192-cfb": {"cfb", 24, true},
	"aes-256-cfb": {"cfb", 32, true},
}

type aesDecryptFunctionClass struct {
	BaseFunctionClass
}

func (c *aesDecryptFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, c.VerifyArgs(args)
	}
	argTps := make([]types.EvalType, 0, len(args))
	for range args {
		argTps = append(argTps, types.ETString)
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETString, argTps...)
	bf.Tp.Flen = args[0].GetType().Flen // At most.
	types.SetBinChsClnFlag(bf.Tp)

	blockMode, _ := ctx.GetSessionVars().GetSystemVar(variable.BlockEncryptionMode)
	mode, exists := aesModes[strings.ToLower(blockMode)]
	if !exists {
		return nil, errors.Errorf("unsupported block encryption mode - %v", blockMode)
	}
	if mode.ivRequired {
		if len(args) != 3 {
			return nil, ErrIncorrectParameterCount.GenWithStackByArgs("aes_decrypt")
		}
		return &builtinAesDecryptIVSig{bf, mode}, nil
	}
	return &builtinAesDecryptSig{bf, mode}, nil
}

type builtinAesDecryptSig struct {
	BaseBuiltinFunc
	*aesModeAttr
}

func (b *builtinAesDecryptSig) Clone() BuiltinFunc {
	newSig := &builtinAesDecryptSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	newSig.aesModeAttr = b.aesModeAttr
	return newSig
}

// evalString evals AES_DECRYPT(crypt_str, key_key).
// See https://dev.mysql.com/doc/refman/5.7/en/encryption-functions.html#function_aes-decrypt
func (b *builtinAesDecryptSig) EvalString(row chunk.Row) (string, bool, error) {
	// According to doc: If either function argument is NULL, the function returns NULL.
	cryptStr, isNull, err := b.Args[0].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	keyStr, isNull, err := b.Args[1].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	if !b.ivRequired && len(b.Args) == 3 {
		// For modes that do not require init_vector, it is ignored and a warning is generated if it is specified.
		b.Ctx.GetSessionVars().StmtCtx.AppendWarning(errWarnOptionIgnored.GenWithStackByArgs("IV"))
	}

	key := encrypt.DeriveKeyMySQL([]byte(keyStr), b.keySize)
	var plainText []byte
	switch b.modeName {
	case "ecb":
		plainText, err = encrypt.AESDecryptWithECB([]byte(cryptStr), key)
	default:
		return "", true, errors.Errorf("unsupported block encryption mode - %v", b.modeName)
	}
	if err != nil {
		return "", true, nil
	}
	return string(plainText), false, nil
}

type builtinAesDecryptIVSig struct {
	BaseBuiltinFunc
	*aesModeAttr
}

func (b *builtinAesDecryptIVSig) Clone() BuiltinFunc {
	newSig := &builtinAesDecryptIVSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	newSig.aesModeAttr = b.aesModeAttr
	return newSig
}

// evalString evals AES_DECRYPT(crypt_str, key_key, iv).
// See https://dev.mysql.com/doc/refman/5.7/en/encryption-functions.html#function_aes-decrypt
func (b *builtinAesDecryptIVSig) EvalString(row chunk.Row) (string, bool, error) {
	// According to doc: If either function argument is NULL, the function returns NULL.
	cryptStr, isNull, err := b.Args[0].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return "", true, err
	}

	keyStr, isNull, err := b.Args[1].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return "", true, err
	}

	iv, isNull, err := b.Args[2].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	if len(iv) < aes.BlockSize {
		return "", true, errIncorrectArgs.GenWithStack("The initialization vector supplied to aes_decrypt is too short. Must be at least %d bytes long", aes.BlockSize)
	}
	// init_vector must be 16 bytes or longer (bytes in excess of 16 are ignored)
	iv = iv[0:aes.BlockSize]

	key := encrypt.DeriveKeyMySQL([]byte(keyStr), b.keySize)
	var plainText []byte
	switch b.modeName {
	case "cbc":
		plainText, err = encrypt.AESDecryptWithCBC([]byte(cryptStr), key, []byte(iv))
	case "ofb":
		plainText, err = encrypt.AESDecryptWithOFB([]byte(cryptStr), key, []byte(iv))
	case "cfb":
		plainText, err = encrypt.AESDecryptWithCFB([]byte(cryptStr), key, []byte(iv))
	default:
		return "", true, errors.Errorf("unsupported block encryption mode - %v", b.modeName)
	}
	if err != nil {
		return "", true, nil
	}
	return string(plainText), false, nil
}

type aesEncryptFunctionClass struct {
	BaseFunctionClass
}

func (c *aesEncryptFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, c.VerifyArgs(args)
	}
	argTps := make([]types.EvalType, 0, len(args))
	for range args {
		argTps = append(argTps, types.ETString)
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETString, argTps...)
	bf.Tp.Flen = aes.BlockSize * (args[0].GetType().Flen/aes.BlockSize + 1) // At most.
	types.SetBinChsClnFlag(bf.Tp)

	blockMode, _ := ctx.GetSessionVars().GetSystemVar(variable.BlockEncryptionMode)
	mode, exists := aesModes[strings.ToLower(blockMode)]
	if !exists {
		return nil, errors.Errorf("unsupported block encryption mode - %v", blockMode)
	}
	if mode.ivRequired {
		if len(args) != 3 {
			return nil, ErrIncorrectParameterCount.GenWithStackByArgs("aes_encrypt")
		}
		return &builtinAesEncryptIVSig{bf, mode}, nil
	}
	return &builtinAesEncryptSig{bf, mode}, nil
}

type builtinAesEncryptSig struct {
	BaseBuiltinFunc
	*aesModeAttr
}

func (b *builtinAesEncryptSig) Clone() BuiltinFunc {
	newSig := &builtinAesEncryptSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	newSig.aesModeAttr = b.aesModeAttr
	return newSig
}

// evalString evals AES_ENCRYPT(str, key_str).
// See https://dev.mysql.com/doc/refman/5.7/en/encryption-functions.html#function_aes-decrypt
func (b *builtinAesEncryptSig) EvalString(row chunk.Row) (string, bool, error) {
	// According to doc: If either function argument is NULL, the function returns NULL.
	str, isNull, err := b.Args[0].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	keyStr, isNull, err := b.Args[1].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	if !b.ivRequired && len(b.Args) == 3 {
		// For modes that do not require init_vector, it is ignored and a warning is generated if it is specified.
		b.Ctx.GetSessionVars().StmtCtx.AppendWarning(errWarnOptionIgnored.GenWithStackByArgs("IV"))
	}

	key := encrypt.DeriveKeyMySQL([]byte(keyStr), b.keySize)
	var cipherText []byte
	switch b.modeName {
	case "ecb":
		cipherText, err = encrypt.AESEncryptWithECB([]byte(str), key)
	default:
		return "", true, errors.Errorf("unsupported block encryption mode - %v", b.modeName)
	}
	if err != nil {
		return "", true, nil
	}
	return string(cipherText), false, nil
}

type builtinAesEncryptIVSig struct {
	BaseBuiltinFunc
	*aesModeAttr
}

func (b *builtinAesEncryptIVSig) Clone() BuiltinFunc {
	newSig := &builtinAesEncryptIVSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	newSig.aesModeAttr = b.aesModeAttr
	return newSig
}

// evalString evals AES_ENCRYPT(str, key_str, iv).
// See https://dev.mysql.com/doc/refman/5.7/en/encryption-functions.html#function_aes-decrypt
func (b *builtinAesEncryptIVSig) EvalString(row chunk.Row) (string, bool, error) {
	// According to doc: If either function argument is NULL, the function returns NULL.
	str, isNull, err := b.Args[0].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return "", true, err
	}

	keyStr, isNull, err := b.Args[1].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return "", true, err
	}

	iv, isNull, err := b.Args[2].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	if len(iv) < aes.BlockSize {
		return "", true, errIncorrectArgs.GenWithStack("The initialization vector supplied to aes_encrypt is too short. Must be at least %d bytes long", aes.BlockSize)
	}
	// init_vector must be 16 bytes or longer (bytes in excess of 16 are ignored)
	iv = iv[0:aes.BlockSize]

	key := encrypt.DeriveKeyMySQL([]byte(keyStr), b.keySize)
	var cipherText []byte
	switch b.modeName {
	case "cbc":
		cipherText, err = encrypt.AESEncryptWithCBC([]byte(str), key, []byte(iv))
	case "ofb":
		cipherText, err = encrypt.AESEncryptWithOFB([]byte(str), key, []byte(iv))
	case "cfb":
		cipherText, err = encrypt.AESEncryptWithCFB([]byte(str), key, []byte(iv))
	default:
		return "", true, errors.Errorf("unsupported block encryption mode - %v", b.modeName)
	}
	if err != nil {
		return "", true, nil
	}
	return string(cipherText), false, nil
}

type decodeFunctionClass struct {
	BaseFunctionClass
}

func (c *decodeFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}

	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETString, types.ETString, types.ETString)

	bf.Tp.Flen = args[0].GetType().Flen
	sig := &builtinDecodeSig{bf}
	return sig, nil
}

type builtinDecodeSig struct {
	BaseBuiltinFunc
}

func (b *builtinDecodeSig) Clone() BuiltinFunc {
	newSig := &builtinDecodeSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalString evals DECODE(str, password_str).
// See https://dev.mysql.com/doc/refman/5.7/en/encryption-functions.html#function_decode
func (b *builtinDecodeSig) EvalString(row chunk.Row) (string, bool, error) {
	dataStr, isNull, err := b.Args[0].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return "", true, err
	}

	passwordStr, isNull, err := b.Args[1].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return "", true, err
	}

	decodeStr, err := encrypt.SQLDecode(dataStr, passwordStr)
	return decodeStr, false, err
}

type desDecryptFunctionClass struct {
	BaseFunctionClass
}

func (c *desDecryptFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	return nil, errFunctionNotExists.GenWithStackByArgs("FUNCTION", "DES_DECRYPT")
}

type desEncryptFunctionClass struct {
	BaseFunctionClass
}

func (c *desEncryptFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	return nil, errFunctionNotExists.GenWithStackByArgs("FUNCTION", "DES_ENCRYPT")
}

type encodeFunctionClass struct {
	BaseFunctionClass
}

func (c *encodeFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}

	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETString, types.ETString, types.ETString)

	bf.Tp.Flen = args[0].GetType().Flen
	sig := &builtinEncodeSig{bf}
	return sig, nil
}

type builtinEncodeSig struct {
	BaseBuiltinFunc
}

func (b *builtinEncodeSig) Clone() BuiltinFunc {
	newSig := &builtinEncodeSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalString evals ENCODE(crypt_str, password_str).
// See https://dev.mysql.com/doc/refman/5.7/en/encryption-functions.html#function_encode
func (b *builtinEncodeSig) EvalString(row chunk.Row) (string, bool, error) {
	decodeStr, isNull, err := b.Args[0].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return "", true, err
	}

	passwordStr, isNull, err := b.Args[1].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return "", true, err
	}

	dataStr, err := encrypt.SQLEncode(decodeStr, passwordStr)
	return dataStr, false, err
}

type encryptFunctionClass struct {
	BaseFunctionClass
}

func (c *encryptFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	return nil, errFunctionNotExists.GenWithStackByArgs("FUNCTION", "ENCRYPT")
}

type oldPasswordFunctionClass struct {
	BaseFunctionClass
}

func (c *oldPasswordFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	return nil, errFunctionNotExists.GenWithStackByArgs("FUNCTION", "OLD_PASSWORD")
}

type passwordFunctionClass struct {
	BaseFunctionClass
}

func (c *passwordFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETString, types.ETString)
	bf.Tp.Flen = mysql.PWDHashLen + 1
	sig := &builtinPasswordSig{bf}
	return sig, nil
}

type builtinPasswordSig struct {
	BaseBuiltinFunc
}

func (b *builtinPasswordSig) Clone() BuiltinFunc {
	newSig := &builtinPasswordSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalString evals a builtinPasswordSig.
// See https://dev.mysql.com/doc/refman/5.7/en/encryption-functions.html#function_password
func (b *builtinPasswordSig) EvalString(row chunk.Row) (d string, isNull bool, err error) {
	pass, isNull, err := b.Args[0].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return "", err != nil, err
	}

	if len(pass) == 0 {
		return "", false, nil
	}

	// We should append a warning here because function "PASSWORD" is deprecated since MySQL 5.7.6.
	// See https://dev.mysql.com/doc/refman/5.7/en/encryption-functions.html#function_password
	b.Ctx.GetSessionVars().StmtCtx.AppendWarning(errDeprecatedSyntaxNoReplacement.GenWithStackByArgs("PASSWORD"))

	return auth.EncodePassword(pass), false, nil
}

type randomBytesFunctionClass struct {
	BaseFunctionClass
}

func (c *randomBytesFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETString, types.ETInt)
	bf.Tp.Flen = 1024 // Max allowed random bytes
	types.SetBinChsClnFlag(bf.Tp)
	sig := &builtinRandomBytesSig{bf}
	return sig, nil
}

type builtinRandomBytesSig struct {
	BaseBuiltinFunc
}

func (b *builtinRandomBytesSig) Clone() BuiltinFunc {
	newSig := &builtinRandomBytesSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalString evals RANDOM_BYTES(len).
// See https://dev.mysql.com/doc/refman/5.7/en/encryption-functions.html#function_random-bytes
func (b *builtinRandomBytesSig) EvalString(row chunk.Row) (string, bool, error) {
	len, isNull, err := b.Args[0].EvalInt(b.Ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	if len < 1 || len > 1024 {
		return "", false, types.ErrOverflow.GenWithStackByArgs("length", "random_bytes")
	}
	buf := make([]byte, len)
	if n, err := rand.Read(buf); err != nil {
		return "", true, err
	} else if int64(n) != len {
		return "", false, errors.New("fail to generate random bytes")
	}
	return string(buf), false, nil
}

type md5FunctionClass struct {
	BaseFunctionClass
}

func (c *md5FunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETString, types.ETString)
	bf.Tp.Flen = 32
	sig := &builtinMD5Sig{bf}
	return sig, nil
}

type builtinMD5Sig struct {
	BaseBuiltinFunc
}

func (b *builtinMD5Sig) Clone() BuiltinFunc {
	newSig := &builtinMD5Sig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalString evals a builtinMD5Sig.
// See https://dev.mysql.com/doc/refman/5.7/en/encryption-functions.html#function_md5
func (b *builtinMD5Sig) EvalString(row chunk.Row) (string, bool, error) {
	arg, isNull, err := b.Args[0].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	sum := md5.Sum([]byte(arg))
	hexStr := fmt.Sprintf("%x", sum)
	return hexStr, false, nil
}

type sha1FunctionClass struct {
	BaseFunctionClass
}

func (c *sha1FunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETString, types.ETString)
	bf.Tp.Flen = 40
	sig := &builtinSHA1Sig{bf}
	return sig, nil
}

type builtinSHA1Sig struct {
	BaseBuiltinFunc
}

func (b *builtinSHA1Sig) Clone() BuiltinFunc {
	newSig := &builtinSHA1Sig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalString evals SHA1(str).
// See https://dev.mysql.com/doc/refman/5.7/en/encryption-functions.html#function_sha1
// The value is returned as a string of 40 hexadecimal digits, or NULL if the argument was NULL.
func (b *builtinSHA1Sig) EvalString(row chunk.Row) (string, bool, error) {
	str, isNull, err := b.Args[0].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	hasher := sha1.New()
	_, err = hasher.Write([]byte(str))
	if err != nil {
		return "", true, err
	}
	return fmt.Sprintf("%x", hasher.Sum(nil)), false, nil
}

type sha2FunctionClass struct {
	BaseFunctionClass
}

func (c *sha2FunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETString, types.ETString, types.ETInt)
	bf.Tp.Flen = 128 // sha512
	sig := &builtinSHA2Sig{bf}
	return sig, nil
}

type builtinSHA2Sig struct {
	BaseBuiltinFunc
}

func (b *builtinSHA2Sig) Clone() BuiltinFunc {
	newSig := &builtinSHA2Sig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// Supported hash length of SHA-2 family
const (
	SHA0   = 0
	SHA224 = 224
	SHA256 = 256
	SHA384 = 384
	SHA512 = 512
)

// evalString evals SHA2(str, hash_length).
// See https://dev.mysql.com/doc/refman/5.7/en/encryption-functions.html#function_sha2
func (b *builtinSHA2Sig) EvalString(row chunk.Row) (string, bool, error) {
	str, isNull, err := b.Args[0].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	hashLength, isNull, err := b.Args[1].EvalInt(b.Ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	var hasher hash.Hash
	switch int(hashLength) {
	case SHA0, SHA256:
		hasher = sha256.New()
	case SHA224:
		hasher = sha256.New224()
	case SHA384:
		hasher = sha512.New384()
	case SHA512:
		hasher = sha512.New()
	}
	if hasher == nil {
		return "", true, nil
	}

	_, err = hasher.Write([]byte(str))
	if err != nil {
		return "", true, err
	}
	return fmt.Sprintf("%x", hasher.Sum(nil)), false, nil
}

// deflate compresses a string using the DEFLATE format.
func deflate(data []byte) ([]byte, error) {
	var buffer bytes.Buffer
	w := zlib.NewWriter(&buffer)
	if _, err := w.Write(data); err != nil {
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}

// inflate uncompresses a string using the DEFLATE format.
func inflate(compressStr []byte) ([]byte, error) {
	reader := bytes.NewReader(compressStr)
	var out bytes.Buffer
	r, err := zlib.NewReader(reader)
	if err != nil {
		return nil, err
	}
	if _, err = io.Copy(&out, r); err != nil {
		return nil, err
	}
	err = r.Close()
	return out.Bytes(), err
}

type compressFunctionClass struct {
	BaseFunctionClass
}

func (c *compressFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETString, types.ETString)
	srcLen := args[0].GetType().Flen
	compressBound := srcLen + (srcLen >> 12) + (srcLen >> 14) + (srcLen >> 25) + 13
	if compressBound > mysql.MaxBlobWidth {
		compressBound = mysql.MaxBlobWidth
	}
	bf.Tp.Flen = compressBound
	types.SetBinChsClnFlag(bf.Tp)
	sig := &builtinCompressSig{bf}
	return sig, nil
}

type builtinCompressSig struct {
	BaseBuiltinFunc
}

func (b *builtinCompressSig) Clone() BuiltinFunc {
	newSig := &builtinCompressSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalString evals COMPRESS(str).
// See https://dev.mysql.com/doc/refman/5.7/en/encryption-functions.html#function_compress
func (b *builtinCompressSig) EvalString(row chunk.Row) (string, bool, error) {
	str, isNull, err := b.Args[0].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return "", true, err
	}

	// According to doc: Empty strings are stored as empty strings.
	if len(str) == 0 {
		return "", false, nil
	}

	compressed, err := deflate([]byte(str))
	if err != nil {
		return "", true, nil
	}

	resultLength := 4 + len(compressed)

	// append "." if ends with space
	shouldAppendSuffix := compressed[len(compressed)-1] == 32
	if shouldAppendSuffix {
		resultLength++
	}

	buffer := make([]byte, resultLength)
	binary.LittleEndian.PutUint32(buffer, uint32(len(str)))
	copy(buffer[4:], compressed)

	if shouldAppendSuffix {
		buffer[len(buffer)-1] = '.'
	}

	return string(buffer), false, nil
}

type uncompressFunctionClass struct {
	BaseFunctionClass
}

func (c *uncompressFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETString, types.ETString)
	bf.Tp.Flen = mysql.MaxBlobWidth
	types.SetBinChsClnFlag(bf.Tp)
	sig := &builtinUncompressSig{bf}
	return sig, nil
}

type builtinUncompressSig struct {
	BaseBuiltinFunc
}

func (b *builtinUncompressSig) Clone() BuiltinFunc {
	newSig := &builtinUncompressSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalString evals UNCOMPRESS(compressed_string).
// See https://dev.mysql.com/doc/refman/5.7/en/encryption-functions.html#function_uncompress
func (b *builtinUncompressSig) EvalString(row chunk.Row) (string, bool, error) {
	sc := b.Ctx.GetSessionVars().StmtCtx
	payload, isNull, err := b.Args[0].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	if len(payload) == 0 {
		return "", false, nil
	}
	if len(payload) <= 4 {
		// corrupted
		sc.AppendWarning(errZlibZData)
		return "", true, nil
	}
	length := binary.LittleEndian.Uint32([]byte(payload[0:4]))
	bytes, err := inflate([]byte(payload[4:]))
	if err != nil {
		sc.AppendWarning(errZlibZData)
		return "", true, nil
	}
	if length < uint32(len(bytes)) {
		sc.AppendWarning(errZlibZBuf)
		return "", true, nil
	}
	return string(bytes), false, nil
}

type uncompressedLengthFunctionClass struct {
	BaseFunctionClass
}

func (c *uncompressedLengthFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	if err := c.VerifyArgs(args); err != nil {
		return nil, err
	}
	bf := NewBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETString)
	bf.Tp.Flen = 10
	sig := &builtinUncompressedLengthSig{bf}
	return sig, nil
}

type builtinUncompressedLengthSig struct {
	BaseBuiltinFunc
}

func (b *builtinUncompressedLengthSig) Clone() BuiltinFunc {
	newSig := &builtinUncompressedLengthSig{}
	newSig.CloneFrom(&b.BaseBuiltinFunc)
	return newSig
}

// evalInt evals UNCOMPRESSED_LENGTH(str).
// See https://dev.mysql.com/doc/refman/5.7/en/encryption-functions.html#function_uncompressed-length
func (b *builtinUncompressedLengthSig) evalInt(row chunk.Row) (int64, bool, error) {
	sc := b.Ctx.GetSessionVars().StmtCtx
	payload, isNull, err := b.Args[0].EvalString(b.Ctx, row)
	if isNull || err != nil {
		return 0, true, err
	}
	if len(payload) == 0 {
		return 0, false, nil
	}
	if len(payload) <= 4 {
		// corrupted
		sc.AppendWarning(errZlibZData)
		return 0, false, nil
	}
	len := binary.LittleEndian.Uint32([]byte(payload)[0:4])
	return int64(len), false, nil
}

type validatePasswordStrengthFunctionClass struct {
	BaseFunctionClass
}

func (c *validatePasswordStrengthFunctionClass) GetFunction(ctx sessionctx.Context, args []Expression) (BuiltinFunc, error) {
	return nil, errFunctionNotExists.GenWithStackByArgs("FUNCTION", "VALIDATE_PASSWORD_STRENGTH")
}
