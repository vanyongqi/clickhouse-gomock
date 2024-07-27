package clickhouse_gomock

import (
	"fmt"
	"sync"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/DATA-DOG/go-sqlmock"
)

var clickHousePool *mockClickHouseDriver

type mockClickHouseDriver struct {
	counter int
	conns   map[string]*clickhousemock
	sync.Mutex
}

func init() {
	clickHousePool = &mockClickHouseDriver{
		conns: make(map[string]*clickhousemock),
	}
}

// NewClickHouseNative creates clickhousemock database mock to manage expectations.
func NewClickHouseNative(options *clickhouse.Options) (*clickhousemock, error) {
	return NewClickHouseWithQueryMatcher(options, sqlmock.QueryMatcherEqual)
}

func NewClickHouseWithQueryMatcher(
	options *clickhouse.Options,
	queryMatcher sqlmock.QueryMatcher,
) (*clickhousemock, error) {
	clickHousePool.Lock()
	dsn := fmt.Sprintf("clickhousemock_db_%d", clickHousePool.counter)
	clickHousePool.counter++

	cmock := &clickhousemock{dsn: dsn, drv: clickHousePool, ordered: true, queryMatcher: queryMatcher}
	clickHousePool.conns[dsn] = cmock
	clickHousePool.Unlock()

	return cmock.open(options)
}
