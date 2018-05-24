// Code generated by mockery v1.0.0
package mocks

import cassandra "github.com/kalmanb/datapipelinepoc/rest/cassandra"
import mock "github.com/stretchr/testify/mock"

// SessionInterface is an autogenerated mock type for the SessionInterface type
type SessionInterface struct {
	mock.Mock
}

// Query provides a mock function with given fields: _a0, _a1
func (_m *SessionInterface) Query(_a0 string, _a1 ...interface{}) cassandra.QueryInterface {
	var _ca []interface{}
	_ca = append(_ca, _a0)
	_ca = append(_ca, _a1...)
	ret := _m.Called(_ca...)

	var r0 cassandra.QueryInterface
	if rf, ok := ret.Get(0).(func(string, ...interface{}) cassandra.QueryInterface); ok {
		r0 = rf(_a0, _a1...)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(cassandra.QueryInterface)
		}
	}

	return r0
}