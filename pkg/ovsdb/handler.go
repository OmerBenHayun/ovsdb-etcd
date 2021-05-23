package ovsdb

import (
	"context"
	"fmt"
	"sync"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"k8s.io/klog/v2"

	"github.com/ibm/ovsdb-etcd/pkg/common"
	"github.com/ibm/ovsdb-etcd/pkg/libovsdb"
	"github.com/ibm/ovsdb-etcd/pkg/ovsjson"
)

type ClientConnection interface {
	Wait() error
	Stop()
	Notify(ctx context.Context, method string, params interface{}) error
}

type Handler struct {
	db         Databaser
	etcdClient *clientv3.Client

	connection     ClientConnection
	handlerContext context.Context

	mu sync.Mutex

	// jsonValueStr -> handlerMonitorData
	monitors      map[string]handlerMonitorData
	databaseLocks map[string]Locker
}

func (ch *Handler) Transact(ctx context.Context, param []interface{}) (interface{}, error) {
	klog.V(5).Infof("Transact request %v", param)
	klog.Flush()
	req, err := libovsdb.NewTransact(param)
	if err != nil {
		return nil, err
	}
	txn := NewTransaction(ch.etcdClient, req)
	txn.schemas = ch.db.GetSchemas()
	txn.Commit()
	klog.V(5).Infof("Transact response %s", txn.response)
	return txn.response.Result, nil
}

func (ch *Handler) Cancel(ctx context.Context, param interface{}) (interface{}, error) {
	klog.V(5).Infof("Cancel request, parameters %v", param)

	return "{Cancel}", nil
}

func (ch *Handler) Monitor(ctx context.Context, param ovsjson.CondMonitorParameters) (interface{}, error) {
	klog.V(5).Infof("Monitor request, parameters %v", param)
	updatersMap, err := ch.monitor(param, ovsjson.Update)
	if err != nil {
		klog.Errorf("Monitor: %s", err)
		return nil, err
	}
	return ch.getMonitoredData(updatersMap, true)
}

func (ch *Handler) MonitorCancel(ctx context.Context, param interface{}) (interface{}, error) {
	klog.V(5).Infof("MonitorCancel request, parameters %v", param)
	jsonValue := jsonValueToString(param)
	ch.mu.Lock()
	defer ch.mu.Unlock()
	monitorHandler, ok := ch.monitors[jsonValue]
	if !ok {
		return nil, fmt.Errorf("unknown monitor")
	}
	ch.db.RemoveMonitors(monitorHandler.dataBaseName, monitorHandler.updaters, handlerKey{handler: ch, jsonValueStr: jsonValue})
	delete(ch.monitors, jsonValue)
	return "{}", nil
}

func (ch *Handler) Lock(ctx context.Context, param interface{}) (interface{}, error) {
	klog.V(5).Infof("Lock request, parameters %v", param)
	id, err := common.ParamsToString(param)
	if err != nil {
		return map[string]bool{"locked": false}, err
	}
	ch.mu.Lock()
	myLock, ok := ch.databaseLocks[id]
	ch.mu.Unlock()
	if !ok {
		myLock, err = ch.db.GetLock(ch.handlerContext, id)
		if err != nil {
			klog.Warningf("Lock returned error %v\n", err)
			return nil, err
		}
		ch.mu.Lock()
		// validate that no other locks
		otherLock, ok := ch.databaseLocks[id]
		if !ok {
			ch.databaseLocks[id] = myLock
		} else {
			// What should we do ?
			myLock.cancel()
			myLock = otherLock
		}
		ch.mu.Unlock()
	}
	err = myLock.tryLock()
	if err == nil {
		return map[string]bool{"locked": true}, nil
	} else if err != concurrency.ErrLocked {
		klog.Errorf("Locked %s got error %v", id, err)
		// TOD is it correct?
		return nil, err
	}
	go func() {
		err = myLock.lock()
		if err == nil {
			// Send notification
			klog.V(5).Infoln("%s Locked", id)
			if err := ch.connection.Notify(ch.handlerContext, "locked", []string{id}); err != nil {
				klog.Errorf("notification %v\n", err)
				return
			}
		} else {
			klog.Errorf("Lock %s error %v\n", id, err)
		}
	}()
	return map[string]bool{"locked": false}, nil
}

func (ch *Handler) Unlock(ctx context.Context, param interface{}) (interface{}, error) {
	klog.V(5).Infof("Unlock request, parameters %v", param)
	id, err := common.ParamsToString(param)
	if err != nil {
		return ovsjson.EmptyStruct{}, err
	}
	ch.mu.Lock()
	myLock, ok := ch.databaseLocks[id]
	delete(ch.databaseLocks, id)
	ch.mu.Unlock()
	if !ok {
		klog.V(4).Infof("Unlock non existing lock %s", id)
		return ovsjson.EmptyStruct{}, nil
	}
	myLock.cancel()
	return ovsjson.EmptyStruct{}, nil
}

func (ch *Handler) Steal(ctx context.Context, param interface{}) (interface{}, error) {
	klog.V(5).Infof("Steal request, parameters %v", param)
	// TODO
	return "{Steal}", nil
}

func (ch *Handler) MonitorCond(ctx context.Context, param ovsjson.CondMonitorParameters) (interface{}, error) {
	klog.V(5).Infof("MonitorCond request, parameters %v", param)
	updatersMap, err := ch.monitor(param, ovsjson.Update2)
	if err != nil {
		klog.Errorf("MonitorCond: %s", err)
		return nil, err
	}
	return ch.getMonitoredData(updatersMap, false)
}

func (ch *Handler) MonitorCondChange(ctx context.Context, param []interface{}) (interface{}, error) {
	klog.V(5).Infof("MonitorCondChange request, parameters %v", param)

	return "{Monitor_cond_change}", nil
}

func (ch *Handler) MonitorCondSince(ctx context.Context, param ovsjson.CondMonitorParameters) (interface{}, error) {
	klog.V(5).Infof("MonitorCondSince request, parameters %v", param)
	updatersMap, err := ch.monitor(param, ovsjson.Update3)
	if err != nil {
		klog.Errorf("MonitorCondSince: %s", err)
		return nil, err
	}
	data, err := ch.getMonitoredData(updatersMap, false)
	if err != nil {
		return nil, err
	}
	return []interface{}{false, ovsjson.ZERO_UUID, data}, nil
}

func (ch *Handler) SetDbChangeAware(ctx context.Context, param interface{}) interface{} {
	klog.V(5).Infof("SetDbChangeAware request, parameters %v", param)
	return ovsjson.EmptyStruct{}
}

func NewHandler(tctx context.Context, db Databaser, cli *clientv3.Client) *Handler {
	return &Handler{
		handlerContext: tctx, db: db, databaseLocks: map[string]Locker{}, monitors: map[string]handlerMonitorData{},
		etcdClient: cli,
	}
}

func (ch *Handler) Cleanup() error {
	klog.Info("CLEAN UP do something")
	ch.mu.Lock()
	defer ch.mu.Unlock()
	for _, m := range ch.databaseLocks {
		m.unlock()
	}
	for jsonValueStr, monitorHandler := range ch.monitors {
		ch.db.RemoveMonitors(monitorHandler.dataBaseName, monitorHandler.updaters, handlerKey{handler: ch, jsonValueStr: jsonValueStr})
	}
	return nil
}

func (ch *Handler) SetConnection(con ClientConnection) {
	ch.connection = con
}

func (ch *Handler) notify(jsonValueStr string, updates ovsjson.TableUpdates) {
	klog.V(5).Infof("Monitor notification jsonValue %v", jsonValueStr)
	var err error
	handler, ok := ch.monitors[jsonValueStr]
	if !ok {
		klog.Errorf("Unknown jsonValue %s", jsonValueStr)
		return
	}
	switch handler.notificationType {
	case ovsjson.Update:
		err = ch.connection.Notify(ch.handlerContext, "update", []interface{}{handler.jsonValue, updates})
	case ovsjson.Update2:
		err = ch.connection.Notify(ch.handlerContext, "update2", []interface{}{handler.jsonValue, updates})
	case ovsjson.Update3:
		err = ch.connection.Notify(ch.handlerContext, "update3", []interface{}{handler.jsonValue, ovsjson.ZERO_UUID, updates})
	}
	if err != nil {
		// TODO should we do something else
		klog.Error(err)
	}
}

func (ch *Handler) monitorCanceledNotification(jsonValue interface{}) {
	klog.V(5).Infof("monitorCanceledNotification %v", jsonValue)
	err := ch.connection.Notify(ch.handlerContext, "monitor_canceled", jsonValue)
	if err != nil {
		// TODO should we do something else
		klog.Error(err)
	}
}

func (ch *Handler) monitor(param ovsjson.CondMonitorParameters, notificationType ovsjson.UpdateNotificationType) (Key2Updaters, error) {
	if len(param.DatabaseName) == 0 {
		return nil, fmt.Errorf("DataBase name is not specified")
	}
	jsonValue := param.JsonValue
	JsonValueStr := jsonValueToString(jsonValue)
	ch.mu.Lock()
	defer ch.mu.Unlock()
	if _, ok := ch.monitors[JsonValueStr]; ok {
		return nil, fmt.Errorf("duplicate json-value")
	}
	updatersMap := Key2Updaters{}
	updaterKeys := map[string][]string{}

	for tableName, mcrs := range param.MonitorCondRequests {
		updaters := []updater{}
		keys := []string{}
		for _, mcr := range mcrs {
			updater := mcrToUpdater(mcr, notificationType == ovsjson.Update)
			keys = append(keys, updater.key)
			updaters = append(updaters, *updater)
		}
		updatersMap[common.NewTableKey(param.DatabaseName, tableName)] = updaters
		updaterKeys[tableName] = keys
	}
	ch.monitors[JsonValueStr] = handlerMonitorData{
		dataBaseName:     param.DatabaseName,
		notificationType: notificationType,
		updaters:         updaterKeys,
		jsonValue:        jsonValue}
	ch.db.AddMonitors(param.DatabaseName, updatersMap, handlerKey{jsonValueStr: JsonValueStr, handler: ch})
	return updatersMap, nil
}

func (ch *Handler) getMonitoredData(updatersMap Key2Updaters, isV1 bool) (ovsjson.TableUpdates, error) {
	returnData := ovsjson.TableUpdates{}
	for tableKey, updaters := range updatersMap {
		if len(updaters) == 0 {
			// nothing to update
			continue
		}
		// validate that Initial is required
		reqInitial := false
		for _, updater := range updaters {
			reqInitial := reqInitial || libovsdb.MSIsTrue(updater.Select.Initial)
			if reqInitial {
				break
			}
		}
		resp, err := ch.db.GetData(tableKey, false)
		if err != nil {
			return nil, err
		}
		d1 := ovsjson.TableUpdate{}
		for _, kv := range resp.Kvs {
			for _, updater := range updaters {
				row, uuid, err := updater.prepareCreateRowInitial(&kv.Value)
				if err != nil {
					klog.Errorf("prepareCreateRowInitial returned %s", err)
					return nil, err
				}
				klog.V(8).Infof("processing getMonitoredData %v  row %v", d1, row)
				// TODO merge
				if row != nil {
					d1[uuid] = *row
				} else {
					klog.Info("row is nil")
				}
			}
		}
		returnData[tableKey.TableName] = d1
	}
	klog.V(7).Infof("getMonitoredData: %v", returnData)
	return returnData, nil
}

func jsonValueToString(jsonValue interface{}) string {
	return fmt.Sprintf("%v", jsonValue)
}
