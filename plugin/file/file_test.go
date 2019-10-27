package main

import (
	"flag"
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
		OnReaderOpen:  OnReaderOpen,
		OnReaderNext:  OnReaderNext,
		OnInsertOpen:  OnInsertOpen,
		OnInsertNext:  OnInsertNext,
		OnInsertClose: OnInsertClose,

		OnCreateTable: OnCreateTable,
		OnDropTable:   OnDropTable,
	}
	plugin.Set(plugin.Engine, &plugin.Plugin{
		Manifest: plugin.ExportManifest(manifest),
		Path:     "",
		Disabled: 0,
		State:    plugin.Ready,
	})

	tk.MustExec("use test")
	tk.MustExec("create table people(city int, name char(255)) ENGINE = file")
	tk.MustExec("insert into people values(1, 'lfkdsk')")
	tk.MustExec("insert into people values(2, 'wph95')")
	tk.MustExec("insert into people values(2, 'guest')")
	result := tk.MustQuery("select * from people")
	result.Check(testkit.Rows("1 lfkdsk", "2 wph95", "2 guest"))
	result = tk.MustQuery("select * from people where city = 2")
	result.Check(testkit.Rows("2 wph95", "2 guest"))
	tk.MustExec("create table city(id int, cityName char(255))")
	tk.MustExec("insert into city values(1, 'beijing')")
	tk.MustExec("insert into city values(2, 'shanghai')")
	result = tk.MustQuery("SELECT city.cityName,people.name FROM city INNER JOIN people ON city.id=people.city")
	result.Check(testkit.Rows("beijing lfkdsk", "shanghai wph95", "shanghai guest"))
}
