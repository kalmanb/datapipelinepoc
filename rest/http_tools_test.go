package main

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/kalmanb/datapipelinepoc/rest/cassandra"
	"github.com/stretchr/testify/mock"

	"github.com/gorilla/mux"

	"github.com/gocql/gocql"
	log "github.com/sirupsen/logrus"
)

func (s *server) request(req *http.Request) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	s.router.ServeHTTP(w, req)
	return w
}
func newServerTest(t *testing.T) *server {
	log.SetLevel(log.PanicLevel)
	srv := &server{
		router: mux.NewRouter(),
	}
	srv.routes()
	return srv
}

func newServerIntTest(t *testing.T) *server {
	srv := newServerTest(t)

	cluster := gocql.NewCluster("127.0.0.1")
	cluster.DisableInitialHostLookup = true
	createKeySpace(*cluster)
	cluster.Keyspace = "events"
	cluster.Consistency = gocql.Quorum
	db, err := cluster.CreateSession()
	if err != nil {
		panic(err)
	}
	defer db.Close()

	srv.db = cassandra.NewSession(db)
	return srv
}

func (s *server) insertEvent(e Event) {
	fmt.Printf("inserting event\n")

	if err := s.db.Query(`INSERT INTO event (account, id,  amount, field1, field2, field3, field4, field5) VALUES ( ?, ?, ?, ?, ?, ?, ?, ?)`,
		strconv.FormatUint(uint64(e.Account), 10),
		e.ID,
		// e.Timestamp.GetNanos(), // FIXME - this is wrong
		e.Amount,
		e.Field1,
		e.Field2,
		e.Field3,
		e.Field4,
		e.Field5).Exec(); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("inserted %+v\n", e)
}

func matchEverything() interface{} {
	return mock.MatchedBy(func(v []interface{}) bool { return true })
}

func createKeySpace(cluster gocql.ClusterConfig) {
	fmt.Printf("Connecting\n")

	session, err := cluster.CreateSession()
	if err != nil {
		panic(err)
	}

	fmt.Printf("starting\n")
	if err := session.Query("DROP KEYSPACE IF EXISTS events").RetryPolicy(nil).Exec(); err != nil {
		panic(err)
	}
	fmt.Printf("one\n")
	if err := session.Query(`CREATE KEYSPACE events
		WITH replication = {
			'class' : 'SimpleStrategy',
			'replication_factor' : 1
		}`).RetryPolicy(nil).Exec(); err != nil {
		panic(err)
	}
	fmt.Printf("keyspace reset\n")
	session.Close()

	cluster.Keyspace = "events"
	session, err = cluster.CreateSession()
	if err != nil {
		panic(err)
	}
	defer session.Close()

	if err := session.Query(`CREATE TABLE event(
		account   text,
		id        int,
		timestamp bigint,
		amount    bigint,
		field1    text,
		field2    text,
		field3    text,
		field4    text,
		field5    text,
		PRIMARY KEY (account, id))`).Exec(); err != nil {
		panic(err)
	}
	fmt.Printf("created table\n")

}

type Event struct {
	ID      uint32
	Account uint32
	Amount  int64
	Field1  string
	Field2  string
	Field3  string
	Field4  string
	Field5  string
}
