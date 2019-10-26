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
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/plugin"
	"github.com/pingcap/tidb/util/chunk"
	"io"
	"os"
	"strconv"
	"strings"
)

type ReadExecutor struct {
	pos int
}

var Files = make(map[string]*bufio.Reader)

// Validate implements TiDB plugin's Validate SPI.
func Validate(ctx context.Context, m *plugin.Manifest) error {
	fmt.Println("csv plugin validate")
	return nil
}

// OnInit implements TiDB plugin's OnInit SPI.
func OnInit(ctx context.Context, manifest *plugin.Manifest) error {
	fmt.Println("csv init called")
	return nil
}

// OnShutdown implements TiDB plugin's OnShutdown SPI.
func OnShutdown(ctx context.Context, manifest *plugin.Manifest) error {
	fmt.Println("csv shutdown called")
	return nil
}

func OnReaderOpen(ctx context.Context, meta *plugin.ExecutorMeta) error {
	path := fmt.Sprintf("/tmp/%s.log", meta.Table.Name.L)
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	Files[meta.Table.Name.L] = bufio.NewReader(file)
	return nil
}

func OnReaderNext(ctx context.Context, chk *chunk.Chunk, meta *plugin.ExecutorMeta) error {
	chk.Reset()
	reader := Files[meta.Table.Name.L]
	line, _, err := reader.ReadLine()
	if err != nil {
		if err == io.EOF {
			return nil
		}
		return err
	}
	es := strings.Split(string(line), ",")
	i, err := strconv.Atoi(es[0])
	if err !=nil {
		chk.AppendNull(0)
	} else {
		chk.AppendInt64(0, int64(i))
	}
	chk.AppendString(1, es[1])
	return nil
}

var InsertFiles = make(map[string]*os.File)

func OnInsertOpen(ctx context.Context, meta *plugin.ExecutorMeta) error {
	path := fmt.Sprintf("/tmp/%s.log", meta.Table.Name.L)
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	InsertFiles[meta.Table.Name.L] = f
	return err
}

func OnInsertNext(ctx context.Context, rows [][]expression.Expression, meta *plugin.ExecutorMeta) error {
	for _, row := range rows {
		b := strings.Builder{}
		for _, e := range row {
			b.WriteString(e.String() + ",")
		}
		b.WriteString("\n")
		_, err := InsertFiles[meta.Table.Name.L].WriteString(b.String())
		if err != nil {
			return err
		}
	}
	return nil
}

func OnInsertClose(meta *plugin.ExecutorMeta) error {
	return InsertFiles[meta.Table.Name.L].Close()
}

func OnCreateTable(tblInfo *model.TableInfo) error {
	path := fmt.Sprintf("/tmp/%s.log", tblInfo.Name.L)
	f, err :=os.Create(path)
	if err != nil {
		return err
	}
	return f.Close()
}

func OnDropTable(tblInfo *model.TableInfo) error {
	path := fmt.Sprintf("/tmp/%s.log", tblInfo.Name.L)
	return os.Remove(path)
}
