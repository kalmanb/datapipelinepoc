package main

import "github.com/gocql/gocql"

type cqlSession interface {
	Query(stmt string, values ...interface{}) *gocql.Query
}
