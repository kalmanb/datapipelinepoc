package main

import (
	"context"
	"net/http"
	"time"

	"github.com/gocql/gocql"
	"github.com/gorilla/mux"
	"github.com/kalmanb/datapipelinepoc/rest/cassandra"
	log "github.com/sirupsen/logrus"
)

var batchSize = 20

type server struct {
	db     cassandra.SessionInterface
	router *mux.Router
}

func main() {
	server := newServer()
	server.routes()

	srv := &http.Server{
		Addr:         ":8080",
		Handler:      server.router,
		ReadTimeout:  300 * time.Second,
		WriteTimeout: 300 * time.Second,
	}

	log.Info("Service started...")

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		log.WithError(err).Error("failed to shutdown http server")
	}
}

func (s *server) routes() {
	s.router.Methods("GET").Path("/events/{account}").Handler(s.handleEvents())
}

func newServer() server {
	cluster := gocql.NewCluster("127.0.0.1")
	cluster.Keyspace = "events"
	cluster.Consistency = gocql.Quorum
	db, err := cluster.CreateSession()
	if err != nil {
		panic(err)
	}
	defer db.Close()

	router := mux.NewRouter()

	return server{
		db:     cassandra.NewSession(db),
		router: router,
	}
}
