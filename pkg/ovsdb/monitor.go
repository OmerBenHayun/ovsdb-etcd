package ovsdb

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/go-logr/logr"
	clientv3 "go.etcd.io/etcd/client/v3"
	"k8s.io/klog/v2"

	"github.com/ibm/ovsdb-etcd/pkg/common"
	"github.com/ibm/ovsdb-etcd/pkg/libovsdb"
	"github.com/ibm/ovsdb-etcd/pkg/ovsjson"
)

const (
	MONITOR_CANCELED = "monitor_canceled"
	UPDATE           = "update"
	UPDATE2          = "update2"
	UPDATE3          = "update3"
)

type updater struct {
	mcr              ovsjson.MonitorCondRequest
	tableSchema      *libovsdb.TableSchema
	isV1             bool
	notificationType ovsjson.UpdateNotificationType
	jasonValueStr    string
}

type handlerMonitorData struct {
	log logr.Logger

	notificationType ovsjson.UpdateNotificationType

	// updaters from the given json-value, key is the path in the monitor.
	updatersKeys      []common.Key
	dataBaseName      string
	jsonValue         interface{}
	notificationChain chan notificationEvent
}

type notificationEvent struct {
	updates ovsjson.TableUpdates
	wg      *sync.WaitGroup
}

// Map from a key which represents a table paths (prefix/dbname/table) to arrays of updaters
// OVSDB allows specifying an array of <dbMonitor-request> objects for a monitored table
type Key2Updaters map[common.Key][]updater

func (k *Key2Updaters) removeUpdaters(key common.Key, jsonValue string) {
	updaters, ok := (*k)[key]
	if !ok {
		return
	}
	newUpdaters := []updater{}
	for _, u := range updaters {
		if u.jasonValueStr != jsonValue {
			newUpdaters = append(newUpdaters, u)
		}
	}
	if len(newUpdaters) != 0 {
		(*k)[key] = newUpdaters
	} else {
		delete(*k, key)
	}
}

type dbMonitor struct {
	log logr.Logger

	// etcd watcher channel
	watchChannel clientv3.WatchChan
	// cancel function to close the etcd watcher
	cancel context.CancelFunc

	mu sync.Mutex
	// database name that the dbMonitor is watching
	dataBaseName string

	// Map from etcd paths (prefix/dbname/table) to arrays of updaters
	// We use it to link keys from etcd events to updaters. We use array of updaters, because OVSDB allows to specify
	// an array of <dbMonitor-request> objects for a monitored table
	key2Updaters Key2Updaters

	revChecker revisionChecker
	handler    *Handler
}

type revisionChecker struct {
	revision int64
	mu       sync.Mutex
}

func (rc *revisionChecker) isNewRevision(newRevision int64) bool {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	if newRevision > rc.revision {
		rc.revision = newRevision
		return true
	}
	return false
}

func newMonitor(dbName string, handler *Handler, log logr.Logger) *dbMonitor {
	m := dbMonitor{
		log:          log,
		dataBaseName: dbName,
		handler:      handler,
		key2Updaters: Key2Updaters{},
	}
	return &m
}

func (m *dbMonitor) addUpdaters(keyToUpdaters Key2Updaters) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for key, updaters := range keyToUpdaters {
		_, ok := m.key2Updaters[key]
		if !ok {
			m.key2Updaters[key] = []updater{}
		}
		for _, uNew := range updaters {
			for _, u1 := range m.key2Updaters[key] {
				if reflect.DeepEqual(uNew, u1) {
					continue
				}
			}
			m.key2Updaters[key] = append(m.key2Updaters[key], uNew)
		}
	}
}

func (m *dbMonitor) removeUpdaters(keys []common.Key, jsonValue string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, key := range keys {
		m.key2Updaters.removeUpdaters(key, jsonValue)
	}
}

func (m *dbMonitor) hasUpdaters() bool {
	return len(m.key2Updaters) > 0
}

func (m *dbMonitor) start() {
	go func() {
		for wresp := range m.watchChannel {
			if wresp.Canceled {
				m.cancelDbMonitor()
				return
			}
			m.notify(wresp.Events, wresp.Header.Revision, nil)
		}
	}()
}

func (hm *handlerMonitorData) notifier(ch *Handler) {
	// we need some time to allow to the monitor calls return data
	time.Sleep(5 * time.Millisecond)
	for {
		select {
		case <-ch.handlerContext.Done():
			return

		case notificationEvent := <-hm.notificationChain:
			if ch.handlerContext.Err() != nil {
				if notificationEvent.wg != nil {
					notificationEvent.wg.Done()
				}
				return
			}
			if hm.log.V(6).Enabled() {
				hm.log.V(6).Info("send notification", "updates", notificationEvent.updates)
			} else {
				hm.log.V(5).Info("send notification")
			}

			var err error
			switch hm.notificationType {
			case ovsjson.Update:
				err = ch.jrpcServer.Notify(ch.handlerContext, UPDATE, []interface{}{hm.jsonValue, notificationEvent.updates})
			case ovsjson.Update2:
				err = ch.jrpcServer.Notify(ch.handlerContext, UPDATE2, []interface{}{hm.jsonValue, notificationEvent.updates})
			case ovsjson.Update3:
				err = ch.jrpcServer.Notify(ch.handlerContext, UPDATE3, []interface{}{hm.jsonValue, ovsjson.ZERO_UUID, notificationEvent.updates})
			}
			if err != nil {
				// TODO should we do something else
				hm.log.Error(err, "monitor notification failed")
			}
			if notificationEvent.wg != nil {
				hm.log.V(7).Info("sent notification and call wg.done")
				notificationEvent.wg.Done()
			}
		}
	}
}

func (m *dbMonitor) notify(events []*clientv3.Event, revision int64, wg *sync.WaitGroup) {
	var sentToNotifier bool
	defer func() {
		if wg != nil && !sentToNotifier {
			wg.Done()
		}
	}()
	if len(events) == 0 {
		m.log.V(5).Info("there is no events, return")
		return
	}
	m.log.V(5).Info("notify", "revChecker.revision", m.revChecker.revision, "revision", revision, "wg == nil", wg == nil)
	if m.revChecker.isNewRevision(revision) {
		result, err := m.prepareTableUpdate(events)
		if err != nil {
			m.log.Error(err, "prepareTableUpdate failed")
		} else {
			if len(result) == 0 {
				m.log.V(5).Info("there is nothing to notify", "events", fmt.Sprintf("%+v", events))
				for _, e := range events {
					m.log.V(5).Info("there is nothing to notify", "event", fmt.Sprintf("%+v", e))
				}
				return
			}
			for jValue, tableUpdates := range result {
				sentToNotifier = true
				m.log.V(7).Info("notify", "table-update", tableUpdates)
				m.handler.notify(jValue, tableUpdates, wg)
			}
		}
	} else {
		m.log.V(5).Info("revisionChecker returned false", "old-revision", m.revChecker.revision, "notification-revision", revision)
	}

}

func (m *dbMonitor) cancelDbMonitor() {
	m.cancel()
	jasonValues := map[string]string{}
	m.mu.Lock()
	for _, updaters := range m.key2Updaters {
		for _, updater := range updaters {
			jasonValues[updater.jasonValueStr] = updater.jasonValueStr
		}
	}
	m.key2Updaters = Key2Updaters{}
	m.mu.Unlock()
	for jsonValue := range jasonValues {
		m.handler.monitorCanceledNotification(jsonValue)
	}
}

func mcrToUpdater(mcr ovsjson.MonitorCondRequest, jsonValue string, tableSchema *libovsdb.TableSchema, isV1 bool) *updater {
	if mcr.Select == nil {
		mcr.Select = &libovsdb.MonitorSelect{}
	}
	return &updater{mcr: mcr, jasonValueStr: jsonValue, isV1: isV1, tableSchema: tableSchema}
}

func (m *dbMonitor) prepareTableUpdate(events []*clientv3.Event) (map[string]ovsjson.TableUpdates, error) {
	result := map[string]ovsjson.TableUpdates{}
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, ev := range events {
		if ev.Kv == nil {
			m.log.V(5).Info("empty etcd event", "event", fmt.Sprintf("%+v", ev))
			continue
		}
		key, err := common.ParseKey(string(ev.Kv.Key))
		if err != nil {
			m.log.Error(err, "parseKey failed")
			continue
		}
		updaters, ok := m.key2Updaters[key.ToTableKey()]
		if !ok {
			m.log.Info("no monitors for table path", "table-path", key.TableKeyString())
			continue
		}
		for _, updater := range updaters {
			rowUpdate, uuid, err := updater.prepareRowUpdate(ev)
			if err != nil {
				m.log.Error(err, "prepareRowUpdate failed", "updater", updater)
				continue
			}
			if rowUpdate == nil {
				// there is no updates
				m.log.Info("no updates for table path", "table-path", key.TableKeyString())
				continue
			}
			tableUpdates, ok := result[updater.jasonValueStr]
			if !ok {
				tableUpdates = ovsjson.TableUpdates{}
				result[updater.jasonValueStr] = tableUpdates
			}
			tableUpdate, ok := tableUpdates[key.TableName]
			if !ok {
				tableUpdate = ovsjson.TableUpdate{} // map[string]ovsjson.RowUpdate{}
				tableUpdates[key.TableName] = tableUpdate
			}
			// check if there is a rowUpdate for the same uuid
			_, ok = tableUpdate[uuid]
			if ok {
				m.log.Info("duplicate event", "key", key.ShortString(), "table-update", tableUpdate[uuid], "row-update", rowUpdate)
				for n, eLog := range events {
					m.log.V(7).Info("event", "index", n, "type", eLog.Type.String(), "key", string(eLog.Kv.Key), "value", string(eLog.Kv.Value), "prev-key", string(eLog.PrevKv.Key), "prev-value", string(eLog.PrevKv.Value))
				}
			}
			tableUpdate[uuid] = *rowUpdate
		}
	}
	return result, nil
}

func (u *updater) prepareRowUpdate(event *clientv3.Event) (*ovsjson.RowUpdate, string, error) {
	if !event.IsModify() { // the create or delete
		if event.IsCreate() {
			// Create event
			return u.prepareCreateRowUpdate(event)
		} else {
			// Delete event
			return u.prepareDeleteRowUpdate(event)
		}
	}
	// the event is modify
	return u.prepareModifyRowUpdate(event)
}

func (u *updater) prepareDeleteRowUpdate(event *clientv3.Event) (*ovsjson.RowUpdate, string, error) {
	// Delete event
	if !libovsdb.MSIsTrue(u.mcr.Select.Delete) {
		return nil, "", nil
	}
	value := event.PrevKv.Value
	if !u.isV1 {
		// according to https://docs.openvswitch.org/en/latest/ref/ovsdb-server.7/#update2-notification,
		// "<row> is always a null object for a delete update."
		_, uuid, err := u.prepareRow(value)
		if err != nil {
			return nil, "", err
		}
		return &ovsjson.RowUpdate{Delete: true}, uuid, nil
	}

	data, uuid, err := u.prepareRow(value)
	if err != nil {
		return nil, "", err
	}
	if len(data) > 0 {
		// the delete for !u.isV1 we have returned before
		return &ovsjson.RowUpdate{Old: &data}, uuid, nil
	}
	return nil, uuid, nil
}

func (u *updater) prepareCreateRowUpdate(event *clientv3.Event) (*ovsjson.RowUpdate, string, error) {
	// the event is create
	if !libovsdb.MSIsTrue(u.mcr.Select.Insert) {
		return nil, "", nil
	}
	value := event.Kv.Value
	data, uuid, err := u.prepareRow(value)
	if err != nil {
		return nil, "", err
	}
	if len(data) > 0 {
		if !u.isV1 {
			return &ovsjson.RowUpdate{Insert: &data}, uuid, nil
		}
		return &ovsjson.RowUpdate{New: &data}, uuid, nil
	}
	return nil, "", nil
}

func (u *updater) prepareModifyRowUpdate(event *clientv3.Event) (*ovsjson.RowUpdate, string, error) {
	// the event is modify
	if !libovsdb.MSIsTrue(u.mcr.Select.Modify) {
		return nil, "", nil
	}
	modifiedRow, uuid, err := u.prepareRow(event.Kv.Value)
	if err != nil {
		return nil, "", err
	}
	prevRow, prevUUID, err := u.prepareRow(event.PrevKv.Value)
	if err != nil {
		return nil, "", err
	}
	if uuid != prevUUID {
		return nil, "", fmt.Errorf("UUID was changed prev uuid=%q, new uuid=%q", prevUUID, uuid)
	}
	deltaRow := map[string]interface{}{}
	u.compareModifiedRows(modifiedRow, prevRow, deltaRow)
	klog.V(5).Infof("deltaRow size is %d", len(deltaRow))
	if len(deltaRow) > 0 {
		if !u.isV1 {
			return &ovsjson.RowUpdate{Modify: &deltaRow}, uuid, nil
		}
		return &ovsjson.RowUpdate{New: &modifiedRow, Old: &deltaRow}, uuid, nil
	}
	return nil, "", nil
}

func (u *updater) compareModifiedRows(modifiedRow, prevRow, deltaRow map[string]interface{}) error {
	for column, cValue := range modifiedRow {
		if !reflect.DeepEqual(cValue, prevRow[column]) {
			columnSchema, err := u.tableSchema.LookupColumn(column)
			if err != nil {
				return err
			}
			if columnSchema.Type == libovsdb.TypeMap {
				deltaMap, err := u.compareMaps(modifiedRow[column], prevRow[column], columnSchema)
				if err != nil {
					return err
				}
				deltaRow[column] = deltaMap
			} else if columnSchema.Type == libovsdb.TypeSet {
				deltaSet, err := u.compareSets(modifiedRow[column], prevRow[column], columnSchema)
				if err != nil {
					return err
				}
				deltaRow[column] = deltaSet
			} else {
				if u.isV1 {
					deltaRow[column] = prevRow[column]
				} else {
					deltaRow[column] = modifiedRow[column]
				}
			}
		}
	}
	return nil
}

func (u *updater) compareMaps(data, prevData interface{}, columnSchema *libovsdb.ColumnSchema) (*libovsdb.OvsMap, error) {
	deltaMap := libovsdb.OvsMap{GoMap: make(map[interface{}]interface{})}
	v, err := columnSchema.UnmarshalMap(data)
	if err != nil {
		return nil, fmt.Errorf("cannot convert column %v to map: %v", data, err)
	}
	newMap := v.(libovsdb.OvsMap)

	v, err = columnSchema.UnmarshalMap(prevData)
	if err != nil {
		return nil, fmt.Errorf("cannot convert prevData column %v to map: %v", prevData, err)
	}
	prevMap := v.(libovsdb.OvsMap)
	// check new values
	for k, v := range newMap.GoMap {
		pv, ok := prevMap.GoMap[k]
		if !ok || !reflect.DeepEqual(v, pv) {
			deltaMap.GoMap[k] = v
		}
	}
	// we need to find all keys that were in the prev map, but are not in the new one
	for pk, pv := range prevMap.GoMap {
		if _, ok := deltaMap.GoMap[pk]; ok {
			continue
		}
		if _, ok := newMap.GoMap[pk]; !ok {
			deltaMap.GoMap[pk] = pv
		}
	}
	return &deltaMap, nil
}

func (u *updater) compareSets(data, prevData interface{}, columnSchema *libovsdb.ColumnSchema) (*libovsdb.OvsSet, error) {
	v, err := columnSchema.UnmarshalSet(data)
	if err != nil {
		return nil, fmt.Errorf("cannot convert column %v to set: %v", data, err)
	}
	newSet := v.(libovsdb.OvsSet)
	v, err = columnSchema.UnmarshalSet(prevData)
	if err != nil {
		return nil, fmt.Errorf("cannot convert prevData column %v to set: %v", prevData, err)
	}
	prevSet := v.(libovsdb.OvsSet)
	deltaSet := setsDifference(newSet, prevSet)
	return &deltaSet, nil
}

func (u *updater) prepareCreateRowInitial(value *[]byte) (*ovsjson.RowUpdate, string, error) {
	if !libovsdb.MSIsTrue(u.mcr.Select.Initial) {
		return nil, "", nil
	}
	data, uuid, err := u.prepareRow(*value)
	if err != nil {
		return nil, "", err
	}
	if len(data) > 0 {
		if !u.isV1 {
			return &ovsjson.RowUpdate{Initial: &data}, uuid, nil
		}
		return &ovsjson.RowUpdate{New: &data}, uuid, nil
	}
	return nil, uuid, nil
}

func (u *updater) deleteUnselectedColumns(data map[string]interface{}) map[string]interface{} {
	if len(u.mcr.Columns) != 0 {
		newData := map[string]interface{}{}
		for _, column := range u.mcr.Columns {
			value, ok := data[column]
			if ok {
				newData[column] = value
			}
		}
		return newData
	}
	return data
}

func unmarshalData(data []byte) (map[string]interface{}, error) {
	obj := map[string]interface{}{}
	if err := json.Unmarshal(data, &obj); err != nil {
		return nil, err
	}
	return obj, nil
}

func getAndDeleteUUID(data map[string]interface{}) (string, error) {
	uuidInt, ok := data[COL_UUID]
	if !ok {
		return "", fmt.Errorf("row doesn't contain %s", COL_UUID)
	}
	delete(data, COL_UUID)
	uuid, ok := uuidInt.([]interface{})
	if !ok {
		return "", fmt.Errorf("wrong uuid type %T %v", uuidInt, uuidInt)
	}
	// TODO add uuid parsing
	if len(uuid) != 2 {
		return "", fmt.Errorf("wrong uuid type %v", uuid)
	}
	uuidStr, ok := uuid[1].(string)
	if !ok {
		return "", fmt.Errorf("wrong type %T %v", uuidInt, uuidInt)
	}
	return uuidStr, nil
}

func (u *updater) prepareRow(value []byte) (map[string]interface{}, string, error) {
	data, err := unmarshalData(value)
	if err != nil {
		return nil, "", err
	}
	uuid, err := getAndDeleteUUID(data)
	if err != nil {
		return nil, "", err
	}
	data = u.deleteUnselectedColumns(data)
	// TODO handle where
	return data, uuid, nil
}

func setsDifference(set1 libovsdb.OvsSet, set2 libovsdb.OvsSet) libovsdb.OvsSet {
	var diff libovsdb.OvsSet

	// Loop two times, first to find elements from set1 which are not in set2,
	// second loop to find elements from set2 which are not in set1
	for i := 0; i < 2; i++ {
		for _, s1 := range set1.GoSet {
			found := false
			for _, s2 := range set2.GoSet {
				if s1 == s2 {
					found = true
					break
				}
			}
			if !found {
				diff.GoSet = append(diff.GoSet, s1)
			}
		}
		// Swap the sets, only if it was the first loop
		if i == 0 {
			set1, set2 = set2, set1
		}
	}
	return diff
}
