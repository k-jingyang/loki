package local

import (
	"context"
	"math/rand"
	"testing"
	"time"

	"github.com/grafana/loki/v3/pkg/storage/chunk"
	"github.com/grafana/loki/v3/pkg/storage/chunk/client/testutils"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/require"
)

type fileTest struct {
	name          string
	chunkDuration time.Duration // determines the chunk size
	bufSizeKB     int           // for io_uring impl to allocate buffer
	chunksNum     int
}

var (
	batchSize = 1

	fileSizeTest []fileTest = []fileTest{
		{
			name:          "235KB chunks 100 files",
			chunkDuration: 12 * time.Hour,
			bufSizeKB:     256,
			chunksNum:     100,
		},
		{
			name:          "470KB chunks 100 files",
			chunkDuration: 24 * time.Hour,
			bufSizeKB:     512,
			chunksNum:     100,
		},
		{
			name:          "700KB chunks 100 files",
			chunkDuration: 36 * time.Hour,
			bufSizeKB:     768,
			chunksNum:     100,
		},
		{
			name:          "900KB chunks 100 files",
			chunkDuration: 48 * time.Hour,
			bufSizeKB:     1024,
			chunksNum:     100,
		},
		{
			name:          "1.1MB chunks 100 files",
			chunkDuration: 60 * time.Hour,
			bufSizeKB:     1280,
			chunksNum:     100,
		},
		{
			name:          "1.40MB chunks 100 files",
			chunkDuration: 72 * time.Hour,
			bufSizeKB:     1536,
			chunksNum:     100,
		},
	}

	fileCountTest []fileTest = []fileTest{
		{
			name:          "900KB chunks 50 files",
			chunkDuration: 48 * time.Hour,
			bufSizeKB:     1024,
			chunksNum:     50,
		},
		{
			name:          "900KB chunks 100 files",
			chunkDuration: 48 * time.Hour,
			bufSizeKB:     1024,
			chunksNum:     100,
		},
		{
			name:          "900KB chunks 200 files",
			chunkDuration: 48 * time.Hour,
			bufSizeKB:     1024,
			chunksNum:     200,
		},
		{
			name:          "900KB chunks 300 files",
			chunkDuration: 48 * time.Hour,
			bufSizeKB:     1024,
			chunksNum:     300,
		},
		{
			name:          "900KB chunks 400 files",
			chunkDuration: 48 * time.Hour,
			bufSizeKB:     1024,
			chunksNum:     400,
		},
		{
			name:          "900KB chunks 500 files",
			chunkDuration: 48 * time.Hour,
			bufSizeKB:     1024,
			chunksNum:     500,
		},
	}
)

func BenchmarkSyncReadChunkFileSizes(b *testing.B) {
	for _, test := range fileSizeTest {
		b.Run(test.name, func(b *testing.B) {
			SyncReadChunk(b, test)
		})
	}
}

func BenchmarkSyncReadChunkFileNum(b *testing.B) {
	for _, test := range fileCountTest {
		b.Run(test.name, func(b *testing.B) {
			SyncReadChunk(b, test)
		})
	}
}

func SyncReadChunk(b *testing.B, test fileTest) {
	fixture := Fixtures[0]
	_, chunkClient, _, schemaCfg, close, _ := fixture.Clients()
	defer close.Close()

	ctx := context.Background()

	// Prepare testing data of chunks
	written := []string{}
	testChunks := []chunk.Chunk{}
	for i := 0; i < test.chunksNum; i++ {
		keys, chunks, err := testutils.CreateChunks(schemaCfg, i, batchSize, model.Now().Add(-1*test.chunkDuration), model.Now())
		require.NoError(b, err)
		written = append(written, keys...)
		err = chunkClient.PutChunks(ctx, chunks)
		require.NoError(b, err)

		for _, testChunk := range chunks {
			testChunks = append(testChunks, chunk.Chunk{ChunkRef: testChunk.ChunkRef})
		}
	}

	// Randomize so that files are not read sequentially
	rand.New(rand.NewSource(time.Now().UnixNano()))
	rand.Shuffle(len(testChunks), func(i, j int) { testChunks[i], testChunks[j] = testChunks[j], testChunks[i] })

	// Bench
	for b.Loop() {
		_, _ = chunkClient.GetChunks(ctx, testChunks)
	}

}

func BenchmarkIOUringReadChunkFileSizes(b *testing.B) {
	for _, test := range fileSizeTest {
		b.Run(test.name, func(b *testing.B) {
			IOUringReadChunk(b, test)
		})
	}
}

func BenchmarkIOUringReadChunkFileNum(b *testing.B) {
	for _, test := range fileCountTest {
		b.Run(test.name, func(b *testing.B) {
			IOUringReadChunk(b, test)
		})
	}
}

func IOUringReadChunk(b *testing.B, test fileTest) {
	testFixture := Fixtures[0]
	_, chunkClient, _, schemaCfg, close, _ := testFixture.Clients()
	defer close.Close()

	ctx := context.Background()

	localFixture, ok := testFixture.(*fixture)
	require.True(b, ok)

	iouringClient, err := NewIOUringFSClient(chunkClient, localFixture.dirname, schemaCfg, test.bufSizeKB)
	require.NoError(b, err)

	// Prepare testing data of chunks
	written := []string{}
	testChunks := []chunk.Chunk{}
	for i := 0; i < test.chunksNum; i++ {
		keys, chunks, err := testutils.CreateChunks(schemaCfg, i, batchSize, model.Now().Add(-1*test.chunkDuration), model.Now())
		require.NoError(b, err)
		written = append(written, keys...)
		err = iouringClient.PutChunks(ctx, chunks)
		require.NoError(b, err)
		for _, testChunk := range chunks {
			testChunks = append(testChunks, chunk.Chunk{ChunkRef: testChunk.ChunkRef})
		}
	}

	// Randomize so that files are not read sequentially
	rand.New(rand.NewSource(time.Now().UnixNano()))
	rand.Shuffle(len(testChunks), func(i, j int) { testChunks[i], testChunks[j] = testChunks[j], testChunks[i] })

	// // Profile
	// f, err := os.Create("iouring.prof")
	// if err != nil {
	// 	b.Fatalf("could not create CPU profile file: %v", err)
	// }
	// defer f.Close()

	// if err := pprof.StartCPUProfile(f); err != nil {
	// 	b.Fatalf("could not start CPU profile: %v", err)
	// }
	// defer pprof.StopCPUProfile()

	// Bench
	for b.Loop() {
		_, _ = iouringClient.GetChunks(ctx, testChunks)
	}
}
