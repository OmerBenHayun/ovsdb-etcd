package e2e_test

import (
	"context"
	"encoding/json"
	"flag"
	"net"
	//. "github.com/OmerBenHayun/ovsdb-etcd/tests/e2e"

	"github.com/creachadair/jrpc2"
	"github.com/creachadair/jrpc2/channel"
	. "github.com/onsi/ginkgo"
	"k8s.io/klog/v2"
	//. "github.com/onsi/gomega"
)

var serverAddr = flag.String("server", "127.0.0.1:12345", "Server address")

func listDbs(ctx context.Context, cli *jrpc2.Client) (result []string, err error) {
	err = cli.CallResult(ctx, "list_dbs", nil, &result)
	return
}

var _ = Describe("E2e", func() {
	var cli *jrpc2.Client
	Describe("basic check", func() {
		BeforeEach(func() {
		})
		AfterEach(func() {
			defer cli.Close()
		})
		It("should be able to show the dbs list", func() {
			conn, err := net.Dial(jrpc2.Network(*serverAddr), *serverAddr)
			if err != nil {
				klog.Fatalf("Dial %q: %v", *serverAddr, err)
			}
			klog.Infof("Connected to %v", conn.RemoteAddr())
			// Start up the client, and enable logging to stderr.
			cli = jrpc2.NewClient(channel.RawJSON(conn, conn), &jrpc2.ClientOptions{
				OnNotify: func(req *jrpc2.Request) {
					var params json.RawMessage
					req.UnmarshalParams(&params)
					klog.Infof("[server push] Method %q params %#q", req.Method(), string(params))
				},
				AllowV1: true,
			})
			ctx := context.Background()

			klog.Info("\n-- Sending some individual requests...")

			if dbs, err := listDbs(ctx, cli); err != nil {
				klog.Fatalln("listDbs:", err)
			} else {
				klog.Infof("listDbs result=%v", dbs)
			}
			By("check")
			By("check")
			return
		})
	})
})
