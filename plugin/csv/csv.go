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
	"context"
	"fmt"
	"github.com/pingcap/tidb/plugin"
	"github.com/pingcap/tidb/util/chunk"
)

type ReadExecutor struct {
	pos int
}

var Files = make(map[string]*ReadExecutor)

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

func OnReaderOpen(ctx context.Context, meta *plugin.ExecutorMeta) {
	Files[meta.Table.Name.L] = &ReadExecutor{
		pos: 0,
	}
}

func OnReaderNext(ctx context.Context, chk *chunk.Chunk, meta *plugin.ExecutorMeta) error {
	chk.Reset()
	if _, ok := Files[meta.Table.Name.L]; !ok {
		fmt.Println("have some problem")
		return nil
	}
	e := Files[meta.Table.Name.L]
	if e.pos > 5 {
		return nil
	}
	chk.AppendInt64(0, int64(e.pos))
	chk.AppendString(1, "233333")
	e.pos += 1
	return nil
}
