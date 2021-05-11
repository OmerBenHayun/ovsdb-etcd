package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/google/uuid"
	"github.com/ibm/ovsdb-etcd/pkg/libovsdb"
	"github.com/ibm/ovsdb-etcd/pkg/types/_Server"
	"io/ioutil"
	"net"
	"os"
	"os/signal"
	"path"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/creachadair/jrpc2"
	"github.com/creachadair/jrpc2/channel"
	"github.com/creachadair/jrpc2/handler"
	"github.com/creachadair/jrpc2/metrics"
	"k8s.io/klog/v2"

	"github.com/ibm/ovsdb-etcd/pkg/common"
	"github.com/ibm/ovsdb-etcd/pkg/ovsdb"
)

const UNIX_SOCKET = "/tmp/ovsdb-etcd.sock"
const ETCD_LOCALHOST = "localhost:2379"

var (
	tcpAddress         = flag.String("tcp-address", "", "TCP service address")
	unixAddress        = flag.String("unix-address", "", "UNIX service address")
	etcdMembers        = flag.String("etcd-members", ETCD_LOCALHOST, "ETCD service addresses, separated by ',' ")
	schemaBasedir      = flag.String("schema-basedir", ".", "Schema base dir")
	maxTasks           = flag.Int("max", 1, "Maximum concurrent tasks")
	databasePrefix     = flag.String("database-prefix", "ovsdb", "Database prefix")
	serviceName        = flag.String("service-name", "", "Deployment service name, e.g. 'nbdb' or 'sbdb'")
	schemaFile         = flag.String("schema-file", "", "schema-file")
	loadServerDataFlag = flag.Bool("load-server-data", false, "load-server-data")
	pidfile            = flag.String("pid-file", "", "Name of file that will hold the pid")
)

func main() {
	klog.InitFlags(nil)
	flag.Parse()
	defer klog.Flush()

	klog.Infof("start the ovsdb-etcd server with the following arguments:")
	klog.Infof("\ttcpAddress: %s\n\tunixAddressress: %s\n\tetcdMembersress: %s\n\tschemaBasedir: %s\n\tmaxTasks: %d\n\tdatabasePrefix: %s\n\tserviceName: %s\n\tschemaFile: %s\n\tloadServerData: %v\n\tpid_file: %s\n",
		*tcpAddress, *unixAddress, *etcdMembers, *schemaBasedir, *maxTasks, *databasePrefix, *serviceName, *schemaFile, *loadServerDataFlag, *pidfile)

	if len(*tcpAddress) == 0 && len(*unixAddress) == 0 {
		klog.Fatal("You must provide a network-address (TCP and/or UNIX) to listen on")
	}

	if len(*databasePrefix) == 0 || strings.Contains(*databasePrefix, common.KEY_DELIMETER) {
		klog.Fatal("Illegal databasePrefix %s", *databasePrefix)
	}
	if len(*serviceName) == 0 || strings.Contains(*serviceName, common.KEY_DELIMETER) {
		klog.Fatal("Illegal serviceName %s", *serviceName)
	}

	if *pidfile != "" {
		defer delPidfile(*pidfile)
		if err := setupPIDFile(*pidfile); err != nil {
			klog.Fatal(err)
		}
	}

	// several OVSDB deployments can share the same etcd, but for rest of the work, we don't have to separate
	// databasePrefix and serviceName.
	common.SetPrefix(*databasePrefix + common.KEY_DELIMETER + *serviceName)

	if len(*etcdMembers) == 0 {
		klog.Fatal("Wrong ETCD members list", etcdMembers)
	}
	etcdServers := strings.Split(*etcdMembers, ",")

	cli, err := ovsdb.NewEtcdClient(etcdServers)
	if err != nil {
		klog.Fatal(err)
	}
	defer cli.Close()

	db, _ := ovsdb.NewDatabaseEtcd(cli)

	err = db.AddSchema(path.Join(*schemaBasedir, "_server.ovsschema"))
	if err != nil {
		klog.Fatal(err)
	}

	err = db.AddSchema(path.Join(*schemaBasedir, *schemaFile))
	if err != nil {
		klog.Fatal(err)
	}
	err = setServerData(&db)
	if err != nil {
		klog.Fatal(err)
	}
	// TODO for development only, will be remove later
	if *loadServerDataFlag {
		err = loadServerData(db.(*ovsdb.DatabaseEtcd))
		if err != nil {
			klog.Fatal(err)
		}
	}

	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	exitCh := make(chan os.Signal, 1)
	signal.Notify(exitCh,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)
	defer func() {
		signal.Stop(exitCh)
		cancel()
	}()

	servOptions := &jrpc2.ServerOptions{
		Concurrency: *maxTasks,
		Metrics:     metrics.New(),
		AllowPush:   true,
		AllowV1:     true,
	}
	service := ovsdb.NewService(db)
	globServiceMap := createServiceMap(service)
	wg := sync.WaitGroup{}

	loop := func(lst net.Listener) error {
		for {
			conn, err := lst.Accept()
			if err != nil {
				if channel.IsErrClosing(err) {
					err = nil
				} else {
					klog.Infof("Error accepting new connection: %v", err)
				}
				wg.Wait()
				return err
			}
			ch := channel.RawJSON(conn, conn)
			wg.Add(1)
			go func() {
				defer wg.Done()
				tctx, cancel := context.WithCancel(context.Background())
				handler := ovsdb.NewHandler(tctx, db, cli)
				assigner := addClientHandlers(*globServiceMap, handler)
				srv := jrpc2.NewServer(assigner, servOptions)
				handler.SetConnection(srv)
				srv.Start(ch)
				go func() {
					defer cancel()
					srv.Wait()
					handler.Cleanup()
				}()
				stat := srv.WaitStatus()
				if stat.Err != nil {
					klog.Infof("Server exit: %v", stat.Err)
				}
			}()
		}
	}
	if len(*tcpAddress) > 0 {
		lst, err := net.Listen(jrpc2.Network(*tcpAddress), *tcpAddress)
		if err != nil {
			klog.Fatalln("Listen:", err)
		}
		klog.Infof("Listening at %v...", lst.Addr())
		//servOptions.Logger = log.New(os.Stderr, "[TCP.Server] ", log.LstdFlags|log.Lshortfile)

		go loop(lst)
	}
	if runtime.GOOS == "linux" && len(*unixAddress) > 0 {
		if err := os.RemoveAll(*unixAddress); err != nil {
			klog.Fatal(err)
		}
		lst, err := net.Listen(jrpc2.Network(*unixAddress), *unixAddress)
		if err != nil {
			klog.Fatalln("Listen:", err)
		}
		klog.Infof("Listening at %v...", lst.Addr())
		//servOptions.Logger = log.New(os.Stderr, "[UNIX.Server] ", log.LstdFlags|log.Lshortfile)
		go loop(lst)
	}
	select {
	case s := <-exitCh:
		klog.Infof("Received signal %s. Shutting down", s)
		cancel()
	case <-ctx.Done():
	}

}

func createServiceMap(ovsdb *ovsdb.Service) *handler.Map {
	out := make(handler.Map)
	out["list_dbs"] = handler.New(ovsdb.ListDbs)
	out["get_schema"] = handler.New(ovsdb.GetSchema)
	out["get_server_id"] = handler.New(ovsdb.GetServerId)
	out["echo"] = handler.New(ovsdb.Echo)
	out["convert"] = handler.New(ovsdb.Convert)
	return &out
}

// we pass handlerMap by value, so the function gets a proprietary copy of it.
func addClientHandlers(handlerMap handler.Map, ch *ovsdb.Handler) *handler.Map {
	handlerMap["transact"] = handler.New(ch.Transact)
	handlerMap["cancel"] = handler.New(ch.Cancel)
	handlerMap["monitor"] = handler.New(ch.Monitor)
	handlerMap["monitor_cancel"] = handler.New(ch.MonitorCancel)
	handlerMap["lock"] = handler.New(ch.Lock)
	handlerMap["steal"] = handler.New(ch.Steal)
	handlerMap["unlock"] = handler.New(ch.Unlock)
	handlerMap["monitor_cond"] = handler.New(ch.MonitorCond)
	handlerMap["monitor_cond_since"] = handler.New(ch.MonitorCondSince)
	handlerMap["monitor_cond_change"] = handler.New(ch.MonitorCondChange)
	handlerMap["set_db_change_aware"] = handler.New(ch.SetDbChangeAware)
	return &handlerMap
}

func setServerData(con *ovsdb.Databaser) error {
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	for schemaName, schema := range (*con).GetSchemas() {
		schemaSet, err := libovsdb.NewOvsSet(schema.String())
		if err != nil {
			return err
		}
		srv := _Server.Database{Model: "standalone", Name: schemaName, Uuid: libovsdb.UUID{GoUUID: uuid.NewString()},
			Connected: true, Leader: true, Schema: *schemaSet, Version: libovsdb.UUID{GoUUID: uuid.NewString()}}
		key := common.NewDataKey("_Server", "Database", schemaName)
		if err := (*con).PutData(ctx, key, srv); err != nil {
			return err
		}
	}
	return nil
}

func delPidfile(pidfile string) {
	if pidfile != "" {
		if _, err := os.Stat(pidfile); err == nil {
			if err := os.Remove(pidfile); err != nil {
				klog.Errorf("%s delete failed: %v", pidfile, err)
			}
		}
	}
}

func setupPIDFile(pidfile string) error {
	// need to test if already there
	_, err := os.Stat(pidfile)

	// Create if it doesn't exist, else exit with error
	if os.IsNotExist(err) {
		if err := ioutil.WriteFile(pidfile, []byte(fmt.Sprintf("%d", os.Getpid())), 0644); err != nil {
			klog.Errorf("Failed to write pidfile %s (%v). Ignoring..", pidfile, err)
		}
	} else {
		// get the pid and see if it exists
		pid, err := ioutil.ReadFile(pidfile)
		if err != nil {
			return fmt.Errorf("pidfile %s exists but can't be read: %v", pidfile, err)
		}
		_, err1 := os.Stat("/proc/" + string(pid[:]) + "/cmdline")
		if os.IsNotExist(err1) {
			// Left over pid from dead process
			if err := ioutil.WriteFile(pidfile, []byte(fmt.Sprintf("%d", os.Getpid())), 0644); err != nil {
				klog.Errorf("Failed to write pidfile %s (%v). Ignoring..", pidfile, err)
			}
		} else {
			return fmt.Errorf("pidfile %s exists and ovnkube is running", pidfile)
		}
	}

	return nil
}
