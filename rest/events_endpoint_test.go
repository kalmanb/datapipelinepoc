package main

import (
	"net/http"
	"testing"

	"github.com/kalmanb/datapipelinepoc/rest/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestMocked(t *testing.T) {
	srv := newServerTest(t)
	db := &mocks.SessionInterface{}
	srv.db = db

	iter := &mocks.IterInterface{}
	iter.On("Scan", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(true).Run(func(args mock.Arguments) {
		*(args.Get(0).(*uint32)) = uint32(789) //Account
		*(args.Get(1).(*uint32)) = uint32(123) //ID
		*(args.Get(2).(*uint64)) = uint64(456) //Timestamp
		*(args.Get(3).(*int64)) = int64(999)   //Amount
	}).Once()
	iter.On("Close").Return(nil)

	q := &mocks.QueryInterface{}
	q.On("Iter").Return(iter).Once()
	db.On("Query", mock.AnythingOfType("string"), mock.AnythingOfType("string")).Return(q).Once()

	req, _ := http.NewRequest("GET", "/events/abc", nil)
	w := srv.request(req)

	assert.Equal(t, http.StatusOK, w.Code)
}

func TestIntTest(t *testing.T) {
	t.Skip()
	srv := newServerIntTest(t)

	e := Event{
		ID:      32,
		Account: 123,
		Amount:  456,
		Field1:  "abc",
	}
	srv.insertEvent(e)

	req, _ := http.NewRequest("GET", "/events/abc", nil)
	w := srv.request(req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "{}", w.Body.String())
	assert.Equal(t, "{\"id\":32,\"account\":123,\"amount\":456}\n", w.Body.String())
}
