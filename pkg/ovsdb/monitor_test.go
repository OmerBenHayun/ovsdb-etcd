package ovsdb

import (
	"encoding/json"
	"github.com/ibm/ovsdb-etcd/pkg/libovsdb"
	"testing"

	guuid "github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/ibm/ovsdb-etcd/pkg/common"
	"github.com/ibm/ovsdb-etcd/pkg/ovsjson"
)

//FIXME - omer start util rename the functions names
//FIXME - find where these const are defined (or defined them myself using util functions.

const (
	PUT    = "put"
	DELETE = "delete"
	MODIFY = "modify"
)

func getUpdater(Columns []string,isV1 bool) updater {
	return *mcrToUpdater(ovsjson.MonitorCondRequest{Columns: Columns}, isV1)
}

type opData struct{
	event        clientv3.Event
	expRowUpdate *ovsjson.RowUpdate
	err          error
}

func newPutOp(key []byte,value *map[string]interface{})opData{
	return opData{event: clientv3.Event{Type: mvccpb.PUT,
			Kv: &mvccpb.KeyValue{Key: key, Value: data1Json, CreateRevision: 1, ModRevision: 1}},
			expRowUpdate: &ovsjson.RowUpdate{New: value}}
}

func newDeleteOp(key []byte,value *map[string]interface{})opData{
	return opData{event: clientv3.Event{Type: mvccpb.DELETE,
				PrevKv: &mvccpb.KeyValue{Key: []byte("key/db/table/000"),
					Value: data1Json},
				Kv: &mvccpb.KeyValue{Key: []byte("key/db/table/uuid")}},
				expRowUpdate: &ovsjson.RowUpdate{Old: value}}
}

type operation map[string]opData

func generateJsonFromData(t *testing.T,data map[string]interface{})[]byte{
	json, err := json.Marshal(data)
	assert.Nilf(t, err, "marshalling %v, threw %v", data, err)
	return json
}

//TODO make this more generic in the future and not hardcoded ?
//func generateJsonTupleFromData(t *testing.T,data map[string]interface{})([]byte,[]byte){
func generateJsonTupleFromData(t *testing.T)([]byte,[]byte){
	uuid := libovsdb.UUID{GoUUID: guuid.NewString()}
	data := map[string]interface{}{"c1": "v1", "c2": "v2",COL_UUID:uuid}
	data2 := map[string]interface{}{"c1": "v1", "c2": "v3",COL_UUID:uuid}
	return generateJsonFromData(t,data),generateJsonFromData(t,data2)
}

type scenario struct {
		name string  //FIXME consider remove this in the future
		updater updater
		op      operation
}
//TODO refactor this code too
func preformTest(t *testing.T,s scenario) {
	for opName, op := range s.op {
		row, _, err := s.updater.prepareRowUpdate(&op.event)
		if op.err != nil {
			assert.EqualErrorf(t, err, op.err.Error(), "[%s-%s test] expected error %s, got %v", s.name, opName, op.err.Error(), err)
			continue
		} else {
			assert.Nilf(t, err, "[%s-%s test] returned unexpected error %v", s.name, opName, err)
		}
		if op.expRowUpdate == nil {
			assert.Nilf(t, row, "[%s-%s test] returned unexpected row %#v", s.name, opName, row)
		} else {
			assert.NotNil(t, row, "[%s-%s test] returned nil row", s.name, opName)
			if s.updater.isV1 {
				ok, msg := row.ValidateRowUpdate()
				assert.Truef(t, ok, "[%s-%s test]  Row update is not valid %s %#v", s.name, opName, msg, row)
			} else {
				ok, msg := row.ValidateRowUpdate2()
				assert.Truef(t, ok, "[%s-%s test]  Row update is not valid %s %#v", s.name, opName, msg, row)
			}
			assert.EqualValuesf(t, op.expRowUpdate, row, "[%s-%s test] returned wrong row update, expected %#v, got %#v", s.name, opName, *op.expRowUpdate, *row)
		}
	}
}

func TestMonitorAllColumnsV1(t *testing.T) {
	data1Json,data2Json:=generateJsonTupleFromData(t)
	s = scenario{
			updater:getUpdater([]string{},true),
			op:,
	}
	return
}
//FIXME - omer end util

func TestRowUpdate(t *testing.T) {
	data1Json,data2Json:=generateJsonTupleFromData(t)
	tests := map[string]scenario{"allColumns-v1": {updater: *mcrToUpdater(ovsjson.MonitorCondRequest{}, true),
		op: operation{PUT: {event: clientv3.Event{Type: mvccpb.PUT,
			Kv: &mvccpb.KeyValue{Key: []byte("key/db/table/000"),
				Value: data1Json, CreateRevision: 1, ModRevision: 1}},
			expRowUpdate: &ovsjson.RowUpdate{New: &map[string]interface{}{"c1": "v1", "c2": "v2"}}},
			DELETE: {event: clientv3.Event{Type: mvccpb.DELETE,
				PrevKv: &mvccpb.KeyValue{Key: []byte("key/db/table/000"),
					Value: data1Json},
				Kv: &mvccpb.KeyValue{Key: []byte("key/db/table/uuid")}},
				expRowUpdate: &ovsjson.RowUpdate{Old: &map[string]interface{}{"c1": "v1", "c2": "v2"}}},
			MODIFY: {event: clientv3.Event{Type: mvccpb.PUT,
				PrevKv: &mvccpb.KeyValue{Key: []byte("key/db/table/000"), Value: data1Json},
				Kv: &mvccpb.KeyValue{Key: []byte("key/db/table/uuid"),
					Value: data2Json, CreateRevision: 1, ModRevision: 2}},
				expRowUpdate: &ovsjson.RowUpdate{Old: &map[string]interface{}{"c2": "v2"}, New: &map[string]interface{}{"c1": "v1", "c2": "v3"}}}}},
		"SingleColumn-v1": {updater: *mcrToUpdater(ovsjson.MonitorCondRequest{Columns: []string{"c2"}}, true),
			op: operation{PUT: {event: clientv3.Event{Type: mvccpb.PUT,
				Kv: &mvccpb.KeyValue{Key: []byte("key/db/table/000"),
					Value: data1Json, CreateRevision: 1, ModRevision: 1}},
				expRowUpdate: &ovsjson.RowUpdate{New: &map[string]interface{}{"c2": "v2"}}},
				DELETE: {event: clientv3.Event{Type: mvccpb.DELETE,
					PrevKv: &mvccpb.KeyValue{Key: []byte("key/db/table/000"), Value: data1Json},
					Kv:     &mvccpb.KeyValue{Key: []byte("key/db/table/000")}},
					expRowUpdate: &ovsjson.RowUpdate{Old: &map[string]interface{}{"c2": "v2"}}},
				MODIFY: {event: clientv3.Event{Type: mvccpb.PUT,
					PrevKv: &mvccpb.KeyValue{Key: []byte("key/db/table/000"), Value: data1Json},
					Kv:     &mvccpb.KeyValue{Key: []byte("key/db/table/000"), Value: data2Json, CreateRevision: 1, ModRevision: 2}},
					expRowUpdate: &ovsjson.RowUpdate{Old: &map[string]interface{}{"c2": "v2"}, New: &map[string]interface{}{"c2": "v3"}}}}},
		"ZeroColumn-v1": {updater: *mcrToUpdater(ovsjson.MonitorCondRequest{Columns: []string{"c3"}}, true),
			op: operation{PUT: {event: clientv3.Event{Type: mvccpb.PUT,
				Kv: &mvccpb.KeyValue{Key: []byte("key/db/table/000"), Value: data1Json, CreateRevision: 1, ModRevision: 1}},
				expRowUpdate: nil},
				DELETE: {event: clientv3.Event{Type: mvccpb.DELETE,
					PrevKv: &mvccpb.KeyValue{Key: []byte("key/db/table/000"), Value: data1Json},
					Kv:     &mvccpb.KeyValue{Key: []byte("key/db/table/000")}},
					expRowUpdate: nil},
				MODIFY: {event: clientv3.Event{Type: mvccpb.PUT,
					PrevKv: &mvccpb.KeyValue{Key: []byte("key/db/table/000"), Value: data1Json},
					Kv:     &mvccpb.KeyValue{Key: []byte("key/db/table/000"), Value: data2Json, CreateRevision: 1, ModRevision: 2}},
					expRowUpdate: nil}}},

		"allColumns-v2": {updater: *mcrToUpdater(ovsjson.MonitorCondRequest{}, false),
			op: operation{PUT: {event: clientv3.Event{Type: mvccpb.PUT,
				Kv: &mvccpb.KeyValue{Key: []byte("key/db/table/000"), Value: data1Json, CreateRevision: 1, ModRevision: 1}},
				expRowUpdate: &ovsjson.RowUpdate{Insert: &map[string]interface{}{"c1": "v1", "c2": "v2"}}},
				DELETE: {event: clientv3.Event{Type: mvccpb.DELETE,
					PrevKv: &mvccpb.KeyValue{Key: []byte("key/db/table/000"), Value: data1Json},
					Kv:     &mvccpb.KeyValue{Key: []byte("key/db/table/000")}},
					expRowUpdate: &ovsjson.RowUpdate{Delete: true}},
				MODIFY: {event: clientv3.Event{Type: mvccpb.PUT,
					PrevKv: &mvccpb.KeyValue{Key: []byte("key/db/table/000"), Value: data1Json},
					Kv:     &mvccpb.KeyValue{Key: []byte("key/db/table/000"), Value: data2Json, CreateRevision: 1, ModRevision: 2}},
					expRowUpdate: &ovsjson.RowUpdate{Modify: &map[string]interface{}{"c2": "v3"}}}}},
		"SingleColumn-v2": {updater: *mcrToUpdater(ovsjson.MonitorCondRequest{Columns: []string{"c2"}}, false),
			op: operation{PUT: {event: clientv3.Event{Type: mvccpb.PUT,
				Kv: &mvccpb.KeyValue{Key: []byte("key/db/table/000"), Value: data1Json, CreateRevision: 1, ModRevision: 1}},
				expRowUpdate: &ovsjson.RowUpdate{Insert: &map[string]interface{}{"c2": "v2"}}},
				DELETE: {event: clientv3.Event{Type: mvccpb.DELETE,
					PrevKv: &mvccpb.KeyValue{Key: []byte("key/db/table/000"), Value: data1Json},
					Kv:     &mvccpb.KeyValue{Key: []byte("key/db/table/000")}},
					expRowUpdate: &ovsjson.RowUpdate{Delete: true}},
				MODIFY: {event: clientv3.Event{Type: mvccpb.PUT,
					PrevKv: &mvccpb.KeyValue{Key: []byte("key/db/table/000"), Value: data1Json},
					Kv:     &mvccpb.KeyValue{Key: []byte("key/db/table/000"), Value: data2Json, CreateRevision: 1, ModRevision: 2}},
					expRowUpdate: &ovsjson.RowUpdate{Modify: &map[string]interface{}{"c2": "v3"}}}}},
		"ZeroColumn-v2": {updater: *mcrToUpdater(ovsjson.MonitorCondRequest{Columns: []string{"c3"}}, false),
			op: operation{PUT: {event: clientv3.Event{Type: mvccpb.PUT,
				Kv: &mvccpb.KeyValue{Key: []byte("key/db/table/000"), Value: data1Json, CreateRevision: 1, ModRevision: 1}},
				expRowUpdate: nil},
				DELETE: {event: clientv3.Event{Type: mvccpb.DELETE,
					PrevKv: &mvccpb.KeyValue{Key: []byte("key/db/table/000"), Value: data1Json},
					Kv:     &mvccpb.KeyValue{Key: []byte("key/db/table/000")}},
					expRowUpdate: &ovsjson.RowUpdate{Delete: true}},
				MODIFY: {event: clientv3.Event{Type: mvccpb.PUT,
					PrevKv: &mvccpb.KeyValue{Key: []byte("key/db/table/000"), Value: data1Json},
					Kv:     &mvccpb.KeyValue{Key: []byte("key/db/table/000"), Value: data2Json, CreateRevision: 1, ModRevision: 2}},
					expRowUpdate: nil}}},
	}
	for name, ts := range tests {
		updater := ts.updater
		for opName, op := range ts.op {
			row, _, err := updater.prepareRowUpdate(&op.event)
			if op.err != nil {
				assert.EqualErrorf(t, err, op.err.Error(), "[%s-%s test] expected error %s, got %v", name, opName, op.err.Error(), err)
				continue
			} else {
				assert.Nilf(t, err, "[%s-%s test] returned unexpected error %v", name, opName, err)
			}
			if op.expRowUpdate == nil {
				assert.Nilf(t, row, "[%s-%s test] returned unexpected row %#v", name, opName, row)
			} else {
				assert.NotNil(t, row, "[%s-%s test] returned nil row", name, opName)
				if updater.isV1 {
					ok, msg := row.ValidateRowUpdate()
					assert.Truef(t, ok, "[%s-%s test]  Row update is not valid %s %#v", name, opName, msg, row)
				} else {
					ok, msg := row.ValidateRowUpdate2()
					assert.Truef(t, ok, "[%s-%s test]  Row update is not valid %s %#v", name, opName, msg, row)
				}
				assert.EqualValuesf(t, op.expRowUpdate, row, "[%s-%s test] returned wrong row update, expected %#v, got %#v", name, opName, *op.expRowUpdate, *row)
			}
		}
	}
}

func TestAddRemoveUpdaters(t *testing.T) {
	common.SetPrefix("ovsdb/nb")
	compareMonitorStates := func(expected, actual *monitor) {
		assert.Equal(t, expected.handlers, actual.handlers, "Handlers maps should be equals")
		assert.Equal(t, expected.key2Updaters, actual.key2Updaters, "Key to updater maps should be equals")
		assert.Equal(t, expected.upater2handlers, actual.upater2handlers, "Updaters to handlers maps should be equals")
	}
	dbName := "dbtest"
	t1 := "table1"
	t2 := "table2"
	m := newMonitor(dbName, &DatabaseMock{})
	mcr1 := ovsjson.MonitorCondRequest{Columns: []string{"c1", "c3", "c2"}}
	mcr2 := ovsjson.MonitorCondRequest{Columns: []string{"c4"}}
	mcr3 := ovsjson.MonitorCondRequest{Columns: []string{"a1"}}
	u1 := mcrToUpdater(mcr1, true)
	u2 := mcrToUpdater(mcr2, true)
	u3 := mcrToUpdater(mcr3, true)
	k1 := common.NewTableKey(dbName, t1)
	k2 := common.NewTableKey(dbName, t2)

	m1 := Key2Updaters{k1: {*u1, *u2}, k2: {*u3}}
	h1 := handlerKey{jsonValueStr: "jsonValue1"}

	m.addUpdaters(m1, h1)
	expected := &monitor{
		handlers:        map[handlerKey]bool{h1: true},
		key2Updaters:    Key2Updaters{k1: {*u1, *u2}, k2: {*u3}},
		upater2handlers: map[string][]handlerKey{u1.key: {h1}, u2.key: {h1}, u3.key: {h1}}}
	compareMonitorStates(expected, m)

	h2 := handlerKey{jsonValueStr: "jsonValue2"}
	m.addUpdaters(m1, h2)
	expected2 := &monitor{
		handlers:        map[handlerKey]bool{h1: true, h2: true},
		key2Updaters:    Key2Updaters{k1: {*u1, *u2}, k2: {*u3}},
		upater2handlers: map[string][]handlerKey{u1.key: {h1, h2}, u2.key: {h1, h2}, u3.key: {h1, h2}}}
	compareMonitorStates(expected2, m)

	u11 := mcrToUpdater(mcr1, false)
	m11 := Key2Updaters{k1: {*u11}}
	h11 := handlerKey{jsonValueStr: "jsonValue11"}
	m.addUpdaters(m11, h11)
	expected3 := &monitor{
		handlers:        map[handlerKey]bool{h1: true, h2: true, h11: true},
		key2Updaters:    Key2Updaters{k1: {*u1, *u2, *u11}, k2: {*u3}},
		upater2handlers: map[string][]handlerKey{u1.key: {h1, h2}, u2.key: {h1, h2}, u3.key: {h1, h2}, u11.key: {h11}}}
	compareMonitorStates(expected3, m)

	m.removeUpdaters(map[string][]string{t1: {u11.key}}, h11)
	compareMonitorStates(expected2, m)

	m.removeUpdaters(map[string][]string{t1: {u2.key, u1.key}, t2: {u3.key}}, h1)
	expected4 := &monitor{
		handlers:        map[handlerKey]bool{h2: true},
		key2Updaters:    Key2Updaters{k1: {*u1, *u2}, k2: {*u3}},
		upater2handlers: map[string][]handlerKey{u1.key: {h2}, u2.key: {h2}, u3.key: {h2}}}
	compareMonitorStates(expected4, m)
}
