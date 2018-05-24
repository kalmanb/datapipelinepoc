package main

import (
	"log"
	"net/http"

	"github.com/gorilla/mux"
)

func (s *server) handleEvents() http.HandlerFunc {
	type response struct {
		ID        uint32 `json:"id,omitempty"`
		Timestamp uint64 `json:"timestamp,omitempty"`
		Account   uint32 `json:"account,omitempty"`
		Amount    int64  `json:"amount,omitempty"`
	}
	return func(w http.ResponseWriter, r *http.Request) {
		account := mux.Vars(r)["account"]

		e := response{}

		iter := s.db.Query(`SELECT account, id, timestamp, amount FROM event WHERE account = ?`, account).Iter()

		// Just take the first response
		iter.Scan(&e.Account, &e.ID, &e.Timestamp, &e.Amount)
		if err := iter.Close(); err != nil {
			log.Fatal(err)
		}

		respond(w, r, http.StatusOK, e)
	}
}
