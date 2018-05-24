package cassandra

import "github.com/gocql/gocql"

// SessionInterface allows gomock mock of gocql.Session
type SessionInterface interface {
	Query(string, ...interface{}) QueryInterface
}

// QueryInterface allows gomock mock of gocql.Query
type QueryInterface interface {
	Bind(...interface{}) QueryInterface
	Exec() error
	Iter() IterInterface
	Scan(...interface{}) error
}

// IterInterface allows gomock mock of gocql.Iter
type IterInterface interface {
	Scan(...interface{}) bool
	Close() error
}

// Session is a wrapper for a session for mockability.
type Session struct {
	session *gocql.Session
}

// Query is a wrapper for a query for mockability.
type Query struct {
	query *gocql.Query
}

// Iter is a wrapper for an iter for mockability.
type Iter struct {
	iter *gocql.Iter
}

// NewSession instantiates a new Session
func NewSession(session *gocql.Session) SessionInterface {
	return &Session{
		session,
	}
}

// NewQuery instantiates a new Query
func NewQuery(query *gocql.Query) QueryInterface {
	return &Query{
		query,
	}
}

// NewIter instantiates a new Iter
func NewIter(iter *gocql.Iter) IterInterface {
	return &Iter{
		iter,
	}
}

// Query wraps the session's query method
func (s *Session) Query(stmt string, values ...interface{}) QueryInterface {
	return NewQuery(s.session.Query(stmt, values...))
}

// Bind wraps the query's Bind method
func (q *Query) Bind(v ...interface{}) QueryInterface {
	return NewQuery(q.query.Bind(v...))
}

// Exec wraps the query's Exec method
func (q *Query) Exec() error {
	return q.query.Exec()
}

// Iter wraps the query's Iter method
func (q *Query) Iter() IterInterface {
	return NewIter(q.query.Iter())
}

// Scan wraps the query's Scan method
func (q *Query) Scan(dest ...interface{}) error {
	return q.query.Scan(dest...)
}

// Scan is a wrapper for the iter's Scan method
func (i *Iter) Scan(dest ...interface{}) bool {
	return i.iter.Scan(dest...)
}

// Close wrapper
func (i *Iter) Close() error {
	return i.iter.Close()
}
