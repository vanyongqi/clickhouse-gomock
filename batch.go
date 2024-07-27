package clickhouse_gomock

import (
	"github.com/ClickHouse/clickhouse-go/v2/lib/column"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

type batch struct {
	conn  *clickhousemock
	ex    *ExpectedPrepareBatch
	query string
}

func (b *batch) Columns() []column.Interface {
	//TODO implement me
	panic("implement me")
}

type batchcolumn struct {
	conn  *clickhousemock
	ex    *ExpectedPrepareBatch
	query string
}

func (b batchcolumn) Append(any) error {
	return b.ex.appendErr
}

func (b batchcolumn) AppendRow(any) error {
	return b.ex.appendErr
}

func (b *batch) Abort() error {
	return b.ex.abortErr
}

func (b *batch) Append(v ...any) error {
	for _, ex := range b.ex.expected {
		if ap, ok := ex.(*ExpectedAppend); ok && !ap.triggered {
			ap.triggered = true
			break
		}
	}
	return b.ex.appendErr
}

func (b *batch) AppendStruct(v any) error {
	return b.ex.appendStructErr
}

func (b *batch) Column(int) driver.BatchColumn {
	return batchcolumn{conn: b.conn, ex: b.ex, query: b.query}
}

func (b *batch) Flush() error {
	return b.ex.flushErr
}

func (b *batch) Send() error {
	return b.ex.sendErr
}

func (b *batch) IsSent() bool {
	return b.ex.mustBeSent
}

func (b *batch) Rows() int {
	return b.ex.rows
}
