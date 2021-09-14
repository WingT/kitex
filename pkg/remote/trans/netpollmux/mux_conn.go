/*
 * Copyright 2021 CloudWeGo Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package netpollmux

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/cloudwego/kitex/pkg/gofunc"
	"net"
	"sync"

	"github.com/cloudwego/netpoll"

	"github.com/cloudwego/kitex/pkg/klog"
	"github.com/cloudwego/kitex/pkg/remote"
	np "github.com/cloudwego/kitex/pkg/remote/trans/netpoll"
)

var ErrConnClosed = errors.New("conn closed")

var SharedSize int32 = 32

func newMuxCliConn(connection netpoll.Connection) *muxCliConn {
	c := &muxCliConn{
		muxConn:  newMuxConn(connection),
		seqIDMap: newSharedMap(SharedSize),
	}
	connection.SetOnRequest(c.OnRequest)
	return c
}

type muxCliConn struct {
	muxConn
	seqIDMap *sharedMap // (k,v) is (sequenceID, notify)
	logger   klog.FormatLogger
}

// OnRequest is called when the connection creates.
func (c *muxCliConn) OnRequest(ctx context.Context, connection netpoll.Connection) (err error) {
	// check full
	reader := connection.Reader()
	for {
		l := reader.Len()
		// check full
		if l < 4 {
			return nil
		}
		blen, _ := reader.Peek(4)
		inlen := int(binary.BigEndian.Uint32(blen)) + 4
		if l < inlen {
			return nil
		}
		// read
		// reader is nil if return error
		r2, err := connection.Reader().Slice(inlen)
		if err != nil {
			err = fmt.Errorf("mux read package slice failed: addr(%s), %w", connection.RemoteAddr(), err)
			return c.onError(err, connection)
		}

		// gofunc.GoFunc(ctx, func() {
		gofunc.GoFunc(ctx, func() {
			// check protocol header
			length, seqID, err := parseHeader(r2)
			if err != nil {
				err = fmt.Errorf("%w: addr(%s)", err, connection.RemoteAddr())
				c.onError(err, connection)
			}

			asyncCallback, ok := c.seqIDMap.load(seqID)
			if !ok {
				r2.Skip(length)
				r2.Release()
				return
			}
			bufReader := np.NewReaderByteBuffer(r2)
			asyncCallback.Recv(bufReader, nil)
		})
	}
}

// Close does nothing.
func (c *muxCliConn) Close() error {
	return nil
}

func (c *muxCliConn) close() error {
	c.Connection.Close()
	c.seqIDMap.rangeMap(func(seqID int32, msg EventHandler) {
		msg.Recv(nil, ErrConnClosed)
	})
	return nil
}

func (c *muxCliConn) onError(err error, connection netpoll.Connection) error {
	c.logger.Errorf("KITEX: %s", err.Error())
	connection.Close()
	return err
}

func newMuxSvrConn(connection netpoll.Connection, pool *sync.Pool) *muxSvrConn {
	c := &muxSvrConn{
		muxConn: newMuxConn(connection),
		pool:    pool,
	}
	return c
}

type muxSvrConn struct {
	muxConn
	pool  *sync.Pool // ctx with rpcInfo
	inlen int
}

func newMuxConn(connection netpoll.Connection) muxConn {
	c := muxConn{}
	c.Connection = connection
	writer := np.NewWriterByteBuffer(connection.Writer())
	c.sharedQueue = newSharedQueue(SharedSize, func(gts []BufferGetter) {
		var err error
		var buf remote.ByteBuffer
		var isNil bool
		for _, gt := range gts {
			buf, isNil = gt()
			if !isNil {
				_, err = writer.AppendBuffer(buf)
				if err != nil {
					connection.Close()
					return
				}
			}
		}
	}, func() {
		err := writer.Flush()
		if err != nil {
			connection.Close()
			return
		}
	})
	return c
}

var _ net.Conn = &muxConn{}
var _ netpoll.Connection = &muxConn{}

type muxConn struct {
	netpoll.Connection              // raw conn
	sharedQueue        *sharedQueue // use for write
}

// Put puts the buffer getter back to the queue.
func (c *muxConn) Put(gt BufferGetter) {
	c.sharedQueue.Add(gt)
}
