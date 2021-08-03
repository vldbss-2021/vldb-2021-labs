package server

import (
	"bufio"
	"bytes"
	"context"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	tmysql "github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/util/arena"
)

type testSuiteLab4A struct {
	tidbdrv *TiDBDriver
	domain  *domain.Domain
	store   kv.Storage
}

var _ = Suite(&testSuiteLab4A{})

func (ts *testSuiteLab4A) SetUpSuite(c *C) {
	var err error
	ts.store, err = mockstore.NewMockTikvStore()
	session.DisableStats4Test()
	c.Assert(err, IsNil)
	ts.domain, err = session.BootstrapSession(ts.store)
	c.Assert(err, IsNil)
	ts.tidbdrv = NewTiDBDriver(ts.store)
}

func (ts *testSuiteLab4A) TearDownSuite(c *C) {
	if ts.store != nil {
		ts.store.Close()
	}
	if ts.domain != nil {
		ts.domain.Close()
	}
}

func (ts *testSuiteLab4A) prepareCC(c *C) *clientConn {
	var (
		inBuffer  bytes.Buffer
		outBuffer bytes.Buffer
	)
	brc := newBufferedReadConn(&bytesConn{inBuffer})
	pkt := newPacketIO(brc)
	pkt.bufWriter = bufio.NewWriter(&bytesConn{outBuffer})
	qctx, err := ts.tidbdrv.OpenCtx(uint64(0), 0, uint8(tmysql.DefaultCollationID), "test", nil)
	c.Assert(err, IsNil)
	cc := &clientConn{
		connectionID: 1,
		ctx:          qctx,
		salt:         []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x10, 0x11, 0x12, 0x13, 0x14},
		alloc:        arena.NewAllocator(32 * 1024),
		bufReadConn:  pkt.bufReadConn,
		server: &Server{
			capability: defaultCapability,
		},
		pkt: pkt,
	}
	return cc
}

func (ts *testSuiteLab4A) TestRunLoop(c *C) {
	data := append([]byte{3}, "select \"vldbss-2021\""...)
	dataHeader := []byte{byte(len(data)), 0, 0, 0}
	cc := ts.prepareCC(c)
	_, err := cc.pkt.bufReadConn.Conn.(*bytesConn).b.Write(append(dataHeader, data...))
	c.Assert(err, IsNil)
	cc.Run(context.Background())
	time.Sleep(100 * time.Millisecond)
	c.Assert(cc.lastPacket, BytesEquals, data)
	err = cc.Close()
	c.Assert(err, IsNil)
}

func (ts *testSuiteLab4A) TestDispatch(c *C) {
	cc := ts.prepareCC(c)
	data := append([]byte{3}, "select \"vldbss-2021\""...)
	err := cc.dispatch(context.Background(), data)
	c.Assert(err, IsNil)
	// Incorrect SQL should return with error
	data = append([]byte{3}, "select1 \"vldbss-2021\""...)
	err = cc.dispatch(context.Background(), data)
	c.Assert(err, NotNil)
}

func (ts *testSuiteLab4A) TestHandleQuery(c *C) {
	cc := ts.prepareCC(c)
	sql := "select \"vldbss-2021\""
	err := cc.handleQuery(context.Background(), sql)
	c.Assert(err, IsNil)
	sql = "select1 \"vldbss-2021\""
	err = cc.handleQuery(context.Background(), sql)
	c.Assert(err, NotNil)
}
