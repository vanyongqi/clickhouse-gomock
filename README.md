
[![GoDoc](https://godoc.org/github.com/DATA-DOG/go-sqlmock?status.svg)](https://godoc.org/github.com/DATA-DOG/go-sqlmock)
# ClickHouse driver mock for Golang

**clickhouse-gomock** is a mock library implementing [sql/driver](https://godoc.org/database/sql/driver). Which has one and only
purpose - to simulate  **clickhouse sql** driver behavior in tests, without needing a real database connection. It helps to
maintain correct **TDD** workflow.

- this library is not complete and stable.
- supports concurrency and multiple connections.
- supports **go1.18** Context related feature mocking and Named sql parameters.
- does not require any modifications to your source code.
- the driver allows to mock any sql driver method behavior.
- has strict by default expectation order matching.
- has third parties dependencies of clickhouse-go && go-sqlmock .


## Looking for maintainers

I do not have much spare time for this library and willing to transfer the repository ownership
to person or an organization motivated to maintain it. Open up a conversation if you are interested. 

## Install

    go get github.com/fanyongqi/clickhouse-gomock

## Run tests

    go test -race

## Change Log

- **2024-07-27** - 1.0.0 - Initial release


## Contributions

Feel free to open a pull request. Note, if you wish to contribute an extension to public (exported methods or types) -
please open an issue before, to discuss whether these changes can be accepted. All backward incompatible changes are
and will be treated cautiously

## License

The [three clause BSD license](http://en.wikipedia.org/wiki/BSD_licenses)

