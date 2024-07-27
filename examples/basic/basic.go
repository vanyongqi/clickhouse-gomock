package main

import (
	"context"
	"fmt"
	"log"

	cmock "github.com/vanyongqi/clickhouse-gomock"
)

func main() {
	mock, err := cmock.NewClickHouseNative(nil)
	if err != nil {
		log.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}

	cols := make([]cmock.ColumnType, 0) // 构造数据列
	cols = append(cols, cmock.ColumnType{Type: "Int32", Name: "INTtest"})
	cols = append(cols, cmock.ColumnType{Type: "String", Name: "STRINGtest"})
	cols = append(cols, cmock.ColumnType{Type: "Array(String)", Name: "SLICEtest"})

	// 构造数据
	values := make([][]any, 1) // 构造数据行
	values[0] = make([]any, 3)

	values[0][0] = int32(1)
	values[0][1] = "string test"
	values[0][2] = []string{"1", "2"}

	rows := cmock.NewRows(cols, values)

	mock.
		ExpectQuery("SELECT INTtest, STRINGtest, SLICEtest FROM db ").
		//	WithArgs(42069).
		WillReturnRows(rows)

	returnRows, err := mock.Query(context.Background(), "SELECT INTtest, STRINGtest, SLICEtest FROM db ")
	if err != nil {
		log.Fatalf("an error '%s' was not expected when querying a statement", err)
	}

	cnt := 0
	for returnRows.Next() {
		var INT int32
		var STRING string
		var SLICE []string
		err = returnRows.Scan(&INT, &STRING, &SLICE)
		if err != nil {
			log.Fatalf("an error '%s' was not expected when scanning a row", err)
		}

		if INT != 1 {
			log.Fatalf("expected id to be 42069, but got %d", INT)
		}

		if STRING != "string test" {
			log.Fatalf("expected title to be `string test`, but got `%s`", STRING)
		}

		if 2 != len(SLICE) {
			log.Fatalf("expected tt to be [1, 2], but got %v", SLICE)
		}
		fmt.Println(INT, STRING, SLICE)
		cnt++

		if cnt > 2 {
			log.Fatalf("expected only 1 row, but got more")
			break
		}
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		log.Fatalf("there were unfulfilled expectations: %s", err)
	}
}
