package chmock

// Licensed to ClickHouse, Inc. under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. ClickHouse, Inc. licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// This file is a copy of the original file from the clickhouse-go project.
// The original file can be found here:
// https://github.com/ClickHouse/clickhouse-go/blob/226a902d120aa46e3883fbf6a5a2667dfb9e90d2/clickhouse_rows.go

import (
	"database/sql"
	"fmt"
	"io"
	"reflect"
	"sync"

	"github.com/ClickHouse/clickhouse-go/v2/lib/column"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/ClickHouse/clickhouse-go/v2/lib/proto"
)

type Rows struct {
	block     *proto.Block
	structMap *structMap
	colNames  []string
	colTypes  []driver.ColumnType
	values    [][]any
	pos       int
	nextErr   map[int]error
	closeErr  error
}

func (r *Rows) Next() bool {
	return r.pos < len(r.values)
}

func scan(block *proto.Block, row int, dest ...any) error {
	columns := block.Columns
	if len(columns) != len(dest) {
		return &OpError{
			Op:  "Scan",
			Err: fmt.Errorf("expected %d destination arguments in Scan, not %d", len(columns), len(dest)),
		}
	}
	for i, d := range dest {
		if err := columns[i].ScanRow(d, row); err != nil {
			return &OpError{
				Err:        err,
				ColumnName: block.ColumnsNames()[i],
			}
		}
	}
	return nil
}

func (r *Rows) Scan(dest ...any) error {
	if r.pos >= len(r.values) {
		return io.EOF
	}
	if err := scan(r.block, r.pos, dest...); err != nil {
		return err
	}
	if err := r.nextErr[r.pos]; err != nil {
		return err
	}
	r.pos++
	return nil
}

func (r *Rows) ScanStruct(dest any) error {
	if r.pos >= len(r.values) {
		return io.EOF
	}
	if err := scan(r.block, r.pos, r.structMap, dest); err != nil {
		return err
	}
	if err := r.nextErr[r.pos]; err != nil {
		return err
	}
	r.pos++
	return nil
}

func (r *Rows) Totals(dest ...any) error {
	return nil
}

func (r *Rows) Columns() []string {
	return r.colNames
}

func (r *Rows) ColumnTypes() []driver.ColumnType {
	return r.colTypes
}

func (r *Rows) Close() error {
	return r.closeErr
}

func (r *Rows) Err() error {
	return nil
}

type Row struct {
	err  error
	rows *Rows
}

func (r *Row) Err() error {
	return r.err
}

func (r *Row) ScanStruct(dest any) error {
	if r.err != nil {
		return r.err
	}
	if !r.rows.Next() {
		r.rows.Close()
		if err := r.rows.Err(); err != nil {
			return err
		}
		return sql.ErrNoRows
	}
	if err := r.rows.ScanStruct(dest); err != nil {
		return err
	}
	return r.rows.Close()
}

func (r *Row) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	if !r.rows.Next() {
		r.rows.Close()
		if err := r.rows.Err(); err != nil {
			return err
		}
		return sql.ErrNoRows
	}
	if err := r.rows.Scan(dest...); err != nil {
		return err
	}
	return r.rows.Close()
}

func getReflectType(typ string) reflect.Type {
	var reflectType reflect.Type
	switch typ {
	case "Int8":
		reflectType = reflect.TypeOf(int8(0))
	case "Int16":
		reflectType = reflect.TypeOf(int16(0))
	case "Int32":
		reflectType = reflect.TypeOf(int32(0))
	case "Int64":
		reflectType = reflect.TypeOf(int64(0))
	case "UInt8":
		reflectType = reflect.TypeOf(uint8(0))
	case "UInt16":
		reflectType = reflect.TypeOf(uint16(0))
	case "UInt32":
		reflectType = reflect.TypeOf(uint32(0))
	case "UInt64":
		reflectType = reflect.TypeOf(uint64(0))
	case "Float32":
		reflectType = reflect.TypeOf(float32(0))
	case "Float64":
		reflectType = reflect.TypeOf(float64(0))
	case "String":
		reflectType = reflect.TypeOf(string(""))
	case "FixedString":
		reflectType = reflect.TypeOf(string(""))
	case "Date":
		reflectType = reflect.TypeOf(string(""))
	case "DateTime":
		reflectType = reflect.TypeOf(string(""))
	case "UUID":
		reflectType = reflect.TypeOf(string(""))
	case "IPv4":
		reflectType = reflect.TypeOf(string(""))
	case "IPv6":
		reflectType = reflect.TypeOf(string(""))
	case "Array":
		reflectType = reflect.TypeOf([]any{})
	case "Array(String)":
		reflectType = reflect.TypeOf([]string{})
	case "Array(Int64)":
		reflectType = reflect.TypeOf([]int64{})
	case "Array(Float64)":
		reflectType = reflect.TypeOf([]float64{})
	case "Array(Bool)":
		reflectType = reflect.TypeOf([]bool{})
	case "Tuple":
		reflectType = reflect.TypeOf([]any{})
	case "Nullable":
		reflectType = reflect.TypeOf(any(nil))
	case "Nothing":
		reflectType = reflect.TypeOf(any(nil))
	case "Enum8":
		reflectType = reflect.TypeOf(int8(0))
	case "Enum16":
		reflectType = reflect.TypeOf(int16(0))
	case "LowCardinality":
		reflectType = reflect.TypeOf(string(""))
	case "Decimal":
		reflectType = reflect.TypeOf(string(""))
	case "Decimal32":
		reflectType = reflect.TypeOf(int32(0))
	case "Decimal64":
		reflectType = reflect.TypeOf(int64(0))
	case "Decimal128":
		reflectType = reflect.TypeOf(string(""))
	case "Decimal256":
		reflectType = reflect.TypeOf(string(""))
	case "AggregateFunction":
		reflectType = reflect.TypeOf(string(""))
	case "Nested":
		reflectType = reflect.TypeOf(string(""))
	case "SimpleAggregateFunction":
		reflectType = reflect.TypeOf(string(""))
	case "TupleElement":
		reflectType = reflect.TypeOf(string(""))
	}
	return reflectType
}

type ColumnType struct {
	Name string
	Type column.Type
}

func NewRows(columns []ColumnType, values [][]any) *Rows {
	colNames := make([]string, 0, len(columns))
	colTypes := make([]driver.ColumnType, 0, len(columns))
	for _, col := range columns {
		colNames = append(colNames, col.Name)
		reflectType := getReflectType(string(col.Type))
		colTypes = append(colTypes, NewColumnType(col.Name, string(col.Type), false, reflectType))
	}
	block := &proto.Block{}
	for _, col := range columns {
		err := block.AddColumn(col.Name, col.Type)
		if err != nil {
			panic(err)
		}
	}
	for _, row := range values {
		err := block.Append(row...)
		if err != nil {
			panic(err)
		}
	}
	return &Rows{
		block:     block,
		structMap: newStructMap(),
		colNames:  colNames,
		colTypes:  colTypes,
		values:    values,
	}
}

type structMap struct {
	cache sync.Map
}

func (m *structMap) Map(op string, columns []string, s any, ptr bool) ([]any, error) {
	v := reflect.ValueOf(s)
	if v.Kind() != reflect.Ptr {
		return nil, &OpError{
			Op:  op,
			Err: fmt.Errorf("must pass a pointer, not a value, to %s destination", op),
		}
	}
	if v.IsNil() {
		return nil, &OpError{
			Op:  op,
			Err: fmt.Errorf("nil pointer passed to %s destination", op),
		}
	}
	t := reflect.TypeOf(s)
	if v = reflect.Indirect(v); t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	if v.Kind() != reflect.Struct {
		return nil, &OpError{
			Op:  op,
			Err: fmt.Errorf("%s expects a struct dest", op),
		}
	}

	var (
		index  map[string][]int
		values = make([]any, 0, len(columns))
	)

	switch idx, found := m.cache.Load(t); {
	case found:
		index = idx.(map[string][]int)
	default:
		index = structIdx(t)
		m.cache.Store(t, index)
	}
	for _, name := range columns {
		idx, found := index[name]
		if !found {
			return nil, &OpError{
				Op:  op,
				Err: fmt.Errorf("missing destination name %q in %T", name, s),
			}
		}
		switch field := v.FieldByIndex(idx); {
		case ptr:
			values = append(values, field.Addr().Interface())
		default:
			values = append(values, field.Interface())
		}
	}
	return values, nil
}

func structIdx(t reflect.Type) map[string][]int {
	fields := make(map[string][]int)
	for i := 0; i < t.NumField(); i++ {
		var (
			f    = t.Field(i)
			name = f.Name
		)
		if tn := f.Tag.Get("ch"); len(tn) != 0 {
			name = tn
		}
		switch {
		case name == "-", len(f.PkgPath) != 0 && !f.Anonymous:
			continue
		}
		switch {
		case f.Anonymous:
			if f.Type.Kind() != reflect.Ptr {
				for k, idx := range structIdx(f.Type) {
					fields[k] = append(f.Index, idx...)
				}
			}
		default:
			fields[name] = f.Index
		}
	}
	return fields
}

func newStructMap() *structMap {
	return &structMap{}
}

type columnType struct {
	name     string
	chType   string
	nullable bool
	scanType reflect.Type
}

func NewColumnType(name string, chType string, nullable bool, scanType reflect.Type) driver.ColumnType {
	return columnType{
		name:     name,
		chType:   chType,
		nullable: nullable,
		scanType: scanType,
	}
}

func (c columnType) Name() string {
	return c.name
}

func (c columnType) Nullable() bool {
	return c.nullable
}

func (c columnType) ScanType() reflect.Type {
	return c.scanType
}

func (c columnType) DatabaseTypeName() string {
	return c.chType
}
