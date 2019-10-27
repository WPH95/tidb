// Copyright 2019 PingCAP, Inc.
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

package main

import (
	"bufio"
	"context"
	"fmt"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/plugin"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
)

var Files = make(map[string]*bufio.Reader)

// Validate implements TiDB plugin's Validate SPI.
func Validate(ctx context.Context, m *plugin.Manifest) error {
	fmt.Println("es plugin validate")
	return nil
}

// OnInit implements TiDB plugin's OnInit SPI.
func OnInit(ctx context.Context, manifest *plugin.Manifest) error {
	fmt.Println("es init called")
	return nil
}

// OnShutdown implements TiDB plugin's OnShutdown SPI.
func OnShutdown(ctx context.Context, manifest *plugin.Manifest) error {
	fmt.Println("es shutdown called")
	return nil
}

var pos = 0

type EsDoc struct {
	Id   int64
	Body string
}

func NewEsDoc(ip string, status int, id int64) EsDoc {
	var msg string
	switch status {
	case 200:
		msg = "access web"

	case 401:
		msg = "unauthorized"
	case 500:
		msg = "Server Error"

	default:
		msg = "UNKNOWN"
	}

	return EsDoc{
		Id:   id,
		Body: fmt.Sprintf(`{"status": %d, "IP": "%s", "message": "ip:%s is %s"}`, status, ip, ip, msg),
	}

}

var data = []EsDoc{
	NewEsDoc("1.0.0.0", 200, 1),
	NewEsDoc("2.0.0.0", 200, 2),
	NewEsDoc("3.0.0.0", 200, 3),
	NewEsDoc("3.0.0.0", 401, 4),
	NewEsDoc("1.0.0.0", 200, 5),
	NewEsDoc("1.0.0.0", 200, 6),
	NewEsDoc("2.0.0.0", 200, 7),
	NewEsDoc("2.0.0.0", 200, 8),
	NewEsDoc("1.0.0.0", 200, 9),
	NewEsDoc("1.0.0.0", 200, 10),
}

var data2 = []EsDoc{
	{1, `{"id": "3", "status": 200, "IP": "1.0.0.0"}`},
	{2, "{'msg': '3.0.0.0 is access web'}"},
	{4, "{'msg': '2.0.0.0 is access web'}"},
}

func GetSchema() *expression.Schema {
	var cols []*expression.Column
	cols = append(cols, &expression.Column{
		RetType: types.NewFieldType(mysql.TypeLonglong), Index: 0,
	})
	cols = append(cols, &expression.Column{
		RetType: types.NewFieldType(mysql.TypeString), Index: 1,
	})
	return &expression.Schema{Columns: cols}
}

func OnReaderOpen(ctx context.Context, meta *plugin.ExecutorMeta) error {
	pos = -1
	return nil
}

func OnReaderNext(ctx context.Context, chk *chunk.Chunk, meta *plugin.ExecutorMeta) error {
	chk.Reset()
	pos += 1
	if pos >= len(data) {
		return nil
	}
	DocsToChk(chk, data[pos], meta)

	return nil
}

var SPos = 0

func OnSelectReaderOpen(ctx context.Context, meta *plugin.ExecutorMeta) error {
	SPos = -1
	return nil
}

func DocsToChk(chk *chunk.Chunk, doc EsDoc, meta *plugin.ExecutorMeta) {
	schema := meta.Schema
	if c := schema.FindColumnByName("id"); c != nil {
		if i := schema.ColumnIndex(c); i != -1 {
			chk.AppendInt64(c.Index, doc.Id)
		}
	}
	if c := schema.FindColumnByName("body"); c != nil {
		if i := schema.ColumnIndex(c); i != -1 {
			chk.AppendString(c.Index, doc.Body)
		}
	}
}

func OnSelectReaderNext(ctx context.Context, chk *chunk.Chunk, filter []expression.Expression, meta *plugin.ExecutorMeta) error {
	chk.Reset()
	SPos += 1
	if SPos >= 3 {
		return nil
	}

	DocsToChk(chk, data2[SPos], meta)
	return nil
}
