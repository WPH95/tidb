package main

import (
	"flag"
	"fmt"
	. "github.com/pingcap/check"
	"github.com/pingcap/parser"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/plugin"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/store/mockstore/mocktikv"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/testkit"
	"testing"
)

type baseTestSuite struct {
	cluster   *mocktikv.Cluster
	mvccStore mocktikv.MVCCStore
	store     kv.Storage
	domain    *domain.Domain
	*parser.Parser
	ctx *mock.Context
}

var mockTikv = flag.Bool("mockTikv", true, "use mock tikv store in executor test")

func (s *baseTestSuite) SetUpSuite(c *C) {
	s.Parser = parser.New()
	flag.Lookup("mockTikv")
	useMockTikv := *mockTikv
	if useMockTikv {
		s.cluster = mocktikv.NewCluster()
		mocktikv.BootstrapWithSingleStore(s.cluster)
		s.mvccStore = mocktikv.MustNewMVCCStore()
		store, err := mockstore.NewMockTikvStore(
			mockstore.WithCluster(s.cluster),
			mockstore.WithMVCCStore(s.mvccStore),
		)
		c.Assert(err, IsNil)
		s.store = store
		session.SetSchemaLease(0)
		session.DisableStats4Test()
	}
	d, err := session.BootstrapSession(s.store)
	c.Assert(err, IsNil)
	d.SetStatsUpdating(true)
	s.domain = d
}

func (s *baseTestSuite) TearDownSuite(c *C) {
	s.domain.Close()
	s.store.Close()
}

var _ = Suite(&testPlugin{&baseTestSuite{}})

type testPlugin struct{ *baseTestSuite }

func TestPlugin(t *testing.T) {
	TestingT(t)
}

func (s *testPlugin) TestPlugin(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	manifest := &plugin.EngineManifest{
		Manifest: plugin.Manifest{
			Name: "file",
		},
		OnReaderOpen: OnReaderOpen,
		OnReaderNext: OnReaderNext,
		//OnReaderClose: plugin.OnReaderClose,
	}
	plugin.Set(plugin.Engine, &plugin.Plugin{
		Manifest: plugin.ExportManifest(manifest),
		Path:     "",
		Disabled: 0,
		State:    plugin.Ready,
	})

	tk.MustExec("use test")
	tk.MustExec("create table t1(a int, b char(255)) ENGINE = file")
	result := tk.MustQuery("select * from t1")
	result.Check(testkit.Rows("0 233333", "1 233333", "2 233333", "3 233333", "4 233333", "5 233333"))
	result = tk.MustQuery("select * from t1 where a = 2")
	result.Check(testkit.Rows("2 233333", ))
	result = tk.MustQuery("show engines")
	fmt.Println(result.Rows())
	result = tk.MustQuery("show table status")
	fmt.Println(result.Rows())

}
