package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	log "github.com/sirupsen/logrus"
)

type adapter func(http.Handler) http.Handler

func adapt(h http.Handler, adapters ...adapter) http.Handler {
	for _, a := range adapters {
		h = a(h)
	}
	return h
}

func withHeader(key, value string) adapter {
	return func(h http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Add(key, value)
			h.ServeHTTP(w, r)
		})
	}
}

func respond(w http.ResponseWriter, r *http.Request, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store, no-cache, must-revalidate, post-check=0, pre-check=0")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(data); err != nil {
		http.Error(w, "Error encoding JSON", http.StatusInternalServerError)
	}
}

type Error struct {
	Status  int    `json:"status"`
	Message string `json:"message"`
}

func errorResponse(w http.ResponseWriter, err error, statusCode int) {
	errorObj := Error{Status: statusCode, Message: err.Error()}
	errorObj.Message = strings.Trim(errorObj.Message, ";")
	bytes, err := json.Marshal(errorObj)

	if err != nil {
		log.WithField("error", err).
			Error("Error marshalling error response JSON")

		w.WriteHeader(http.StatusInternalServerError)
	}

	w.WriteHeader(statusCode)
	fmt.Fprintln(w, string(bytes))
}
