package local

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
	"unsafe"

	"github.com/grafana/loki/v3/pkg/storage/chunk"
	"github.com/grafana/loki/v3/pkg/storage/chunk/client"
	"github.com/grafana/loki/v3/pkg/storage/config"
	"github.com/pawelgaczynski/giouring"
)

var decodeContextPool = sync.Pool{
	New: func() interface{} {
		return chunk.NewDecodeContext()
	},
}

type IOUringChunkClient struct {
	innerClient client.Client
	ring        *giouring.Ring
	schema      config.SchemaConfig
	dirname     string
	bufferPool  sync.Pool
}

type ReadRequest struct {
	Index int
	File  *os.File
	Buf   []byte
	Size  uint32
}

func NewIOUringFSClient(innerClient client.Client, dirname string, schema config.SchemaConfig, bufSizeKB int) (*IOUringChunkClient, error) {
	size := 256
	ring, err := giouring.CreateRing(uint32(size), giouring.WithSQPolling(), giouring.WithSQThreadIdle(2*time.Second))
	if err != nil {
		return nil, err
	}

	return &IOUringChunkClient{
		innerClient: innerClient,
		dirname:     dirname,
		schema:      schema,
		ring:        ring,
		bufferPool: sync.Pool{
			New: func() interface{} {
				buf := make([]byte, bufSizeKB*1024)
				return buf
			},
		},
	}, nil

}

func (c *IOUringChunkClient) Stop() {
	c.innerClient.Stop()
}

func (c *IOUringChunkClient) PutChunks(ctx context.Context, chunks []chunk.Chunk) error {
	return c.innerClient.PutChunks(ctx, chunks)
}

func (c *IOUringChunkClient) GetChunks(ctx context.Context, chunks []chunk.Chunk) ([]chunk.Chunk, error) {
	maxPrepReadWorkers := 10
	maxDecodeWorker := 10

	chunkBuffer := make([][]byte, len(chunks))
	openedFiles := make([]*os.File, len(chunks)) // keep the open files, so their FDs don't get recycled because of GC
	size := make([]int, len(chunks))

	readReqChan := make(chan *ReadRequest, len(chunks))
	chunkChan := make(chan int, len(chunks))
	for i := 0; i < min(maxPrepReadWorkers, len(chunks)); i++ {
		go c.makeReadReqs(readReqChan, chunkChan, &chunks)
	}
	for i := 0; i < len(chunks); i++ {
		chunkChan <- i
	}
	close(chunkChan)

	completionChan := make(chan struct{}, len(chunks))
	go c.pollCompletion(completionChan)

	decodeDoneChan := make(chan struct{}, len(chunks))
	decodeChan := make(chan int, len(chunks))
	for i := 0; i < min(maxDecodeWorker, len(chunks)); i++ {
		go c.decodeWorker(chunks, chunkBuffer, size, decodeChan, decodeDoneChan)
	}

	decoded := 0
	prepared := 0

OUTER:
	for {
		select {
		case readReq := <-readReqChan:
			var sqe *giouring.SubmissionQueueEntry
			for sqe = c.ring.GetSQE(); sqe == nil; {
				sqe = c.ring.GetSQE()
			}

			openedFiles[readReq.Index] = readReq.File
			chunkBuffer[readReq.Index] = readReq.Buf
			size[readReq.Index] = int(readReq.Size)

			bufPtr := uintptr(unsafe.Pointer(&(readReq.Buf)[0]))
			sqe.PrepareRead(int(readReq.File.Fd()), bufPtr, readReq.Size, 0)
			sqe.SetData64(uint64(readReq.Index))

			prepared += 1
			if prepared == len(chunks) || prepared%10 == 0 {
				_, err := c.ring.Submit()
				if err != nil {
					panic(fmt.Sprintf("err on submit, err:%s", err))
				}
			}

		case <-completionChan:
			cqeNum := 0
			c.ring.ForEachCQE(func(cqe *giouring.CompletionQueueEvent) {
				cqeNum += 1
				chunkIndex := cqe.GetData64()
				decodeChan <- int(chunkIndex)
			})
			c.ring.CQAdvance(uint32(cqeNum))

		case <-decodeDoneChan:
			decoded += 1
			if decoded == len(chunks) {
				break OUTER
			}
		}
	}
	return chunks, nil
}

func (c *IOUringChunkClient) makeReadReqs(readReqChan chan<- *ReadRequest, chunkChan <-chan int, chunks *[]chunk.Chunk) {
	for chunkIndex := range chunkChan {
		// client.FSEncoder takes quite a bit of time, so we make it its own goroutine
		key := client.FSEncoder(c.schema, (*chunks)[chunkIndex])
		chunkFile := filepath.Join(c.dirname, filepath.FromSlash(key))
		fl, err := os.Open(chunkFile)
		if err != nil {
			panic(fmt.Sprintf("err on open file, err:%s", err))
		}

		stats, err := fl.Stat()
		if err != nil {
			panic(err)
		}

		size := stats.Size()
		buf := c.bufferPool.Get().([]byte)

		readReqChan <- &ReadRequest{
			Index: chunkIndex,
			File:  fl,
			Buf:   buf,
			Size:  uint32(size),
		}
	}
}

func (c *IOUringChunkClient) pollCompletion(completionChan chan<- struct{}) {
	for {
		c.ring.WaitCQE()
		completionChan <- struct{}{}
	}
}

func (c *IOUringChunkClient) decodeWorker(chunks []chunk.Chunk, chunkBuffer [][]byte, sizes []int, decodeChan <-chan int, doneCh chan<- struct{}) {
	for chunkIndex := range decodeChan {
		logChunk := chunks[chunkIndex]
		buffer := chunkBuffer[chunkIndex]
		decodeContext := decodeContextPool.Get().(*chunk.DecodeContext)
		size := sizes[chunkIndex]

		err := logChunk.Decode(decodeContext, buffer[:size])
		if err != nil {
			panic(err)
		}
		c.bufferPool.Put(buffer)
		decodeContextPool.Put(decodeContext)
		doneCh <- struct{}{}
	}
}

func (c *IOUringChunkClient) DeleteChunk(ctx context.Context, userID, chunkID string) error {
	return c.innerClient.DeleteChunk(ctx, userID, chunkID)
}

func (c *IOUringChunkClient) IsChunkNotFoundErr(err error) bool {
	return c.innerClient.IsChunkNotFoundErr(err)
}

func (c *IOUringChunkClient) IsRetryableErr(err error) bool {
	return c.innerClient.IsRetryableErr(err)
}
