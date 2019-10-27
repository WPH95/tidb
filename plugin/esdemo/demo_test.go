package main

import (
	"database/sql"
	"fmt"
	"github.com/go-sql-driver/mysql"
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/plugin"
	. "github.com/pingcap/tidb/server"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/util/testkit"
	"go.uber.org/zap"
	"io/ioutil"
	"log"
	"net/http"
	"testing"
	"time"
)

var defaultDSNConfig = mysql.Config{
	User:   "root",
	Net:    "tcp",
	Addr:   "127.0.0.1:4001",
	DBName: "test",
	Strict: true,
}

type configOverrider func(*mysql.Config)
type TidbTestSuite struct {
	tidbdrv *TiDBDriver
	server  *Server
	domain  *domain.Domain
	store   kv.Storage
}

var suite = new(TidbTestSuite)
var _ = Suite(suite)

func (ts *TidbTestSuite) SetUpSuite(c *C) {
	manifest := &plugin.UDFManifest{
		Manifest: plugin.Manifest{
			Kind:           plugin.UDF,
			Name:           "ip2city",
			Description:    "UDF Expression Release",
			Version:        1,
			RequireVersion: map[string]uint16{},
			License:        "",
			BuildTime:      "2019-10-26 23:41:34.229509 +0800 CST m=+0.001093220",
			Validate:       Validate,
			OnInit:         OnInit,
			OnShutdown:     OnShutdown,
		},
		GetUserDefinedFuncClass: GetUdfIp2city,
	}

	plugin.Set(plugin.UDF, &plugin.Plugin{
		Manifest: plugin.ExportManifest(manifest),
		Path:     "",
		Disabled: 0,
		State:    plugin.Ready,
	})

	manifest2 := &plugin.EngineManifest{
		Manifest: plugin.Manifest{
			Name: "elasticsearch",
		},
		OnReaderNext:       OnReaderNext,
		//GetSchema:          GetSchema,
		OnReaderOpen:       OnReaderOpen,
		OnSelectReaderOpen: OnSelectReaderOpen,
		OnSelectReaderNext: OnSelectReaderNext,
	}
	plugin.Set(plugin.Engine, &plugin.Plugin{
		Manifest: plugin.ExportManifest(manifest2),
		Path:     "",
		Disabled: 0,
		State:    plugin.Ready,
	})

	metrics.RegisterMetrics()
	var err error
	ts.store, err = mockstore.NewMockTikvStore()
	session.DisableStats4Test()
	c.Assert(err, IsNil)
	ts.domain, err = session.BootstrapSession(ts.store)
	c.Assert(err, IsNil)
	ts.tidbdrv = NewTiDBDriver(ts.store)
	cfg := config.NewConfig()
	cfg.Port = 4001
	cfg.Status.ReportStatus = true
	cfg.Status.StatusPort = 10090
	cfg.Performance.TCPKeepAlive = true

	server, err := NewServer(cfg, ts.tidbdrv)
	c.Assert(err, IsNil)
	ts.server = server
	go ts.server.Run()
	waitUntilServerOnline(cfg.Status.StatusPort)
}

// getDSN generates a DSN string for MySQL connection.
func getDSN(overriders ...configOverrider) string {
	var config = defaultDSNConfig
	for _, overrider := range overriders {
		if overrider != nil {
			overrider(&config)
		}
	}
	return config.FormatDSN()
}

const retryTime = 100

func waitUntilServerOnline(statusPort uint) {
	// connect server
	retry := 0
	for ; retry < retryTime; retry++ {
		time.Sleep(time.Millisecond * 10)
		db, err := sql.Open("mysql", getDSN())
		if err == nil {
			db.Close()
			break
		}
	}
	if retry == retryTime {
		log.Fatal("failed to connect DB in every 10 ms", zap.Int("retryTime", retryTime))
	}
	// connect http status
	statusURL := fmt.Sprintf("http://127.0.0.1:%d/status", statusPort)
	for retry = 0; retry < retryTime; retry++ {
		resp, err := http.Get(statusURL)
		if err == nil {
			ioutil.ReadAll(resp.Body)
			resp.Body.Close()
			break
		}
		time.Sleep(time.Millisecond * 10)
	}
	if retry == retryTime {
		log.Fatal("failed to connect HTTP status in every 10 ms", zap.Int("retryTime", retryTime))
	}
}

func (s *TidbTestSuite) TearDownSuite(c *C) {
	s.domain.Close()
	s.store.Close()
}

var _ = Suite(&testPlugin{&TidbTestSuite{}})

type testPlugin struct{ *TidbTestSuite }

func TestPlugin(t *testing.T) {
	TestingT(t)
}

func (s *testPlugin) TestPlugin(c *C) {
	var result *testkit.Result
	tk := testkit.NewTestKit(c, s.store)

	tk.MustExec("use test")
	tk.MustExec("create table logs(ID int, body text, query text) engine=elasticsearch")
	//result = tk.MustQuery("select ID from logs")
	//result.Check(testkit.Rows("1", "2", "3"))
	result = tk.MustQuery("select body->'$.status' as Status  from logs where query='status:200 OR error'")
	result.Check(testkit.Rows("1"))

	//tk.MustExec("drop table if exists blacklist")
	//tk.MustExec("create table blacklist(ip char(255), level int, message char(255))")
	//tk.MustExec(`insert into blacklist values("2.0.0.0",1, "shanghai.0")`)
	//result := tk.MustQuery("select UDF_IP2CITY(ip), ip from blacklist")
	//result.Check(testkit.Rows("SHANGHAI 2.0.0.0", ))
	//tk.MustExec(`insert into blacklist values("1.0.0.0",3, "beijing.1"), ("1.0.0.3", 2, "beijing.3"), ("4.5.6.7", 2, "")`)


	result = tk.MustQuery("select body->'$.status' as Status  from logs where query='status:200 OR error'")
	result.Check(testkit.Rows("1"))



}
