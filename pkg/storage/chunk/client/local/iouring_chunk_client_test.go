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

var (
	chunksNum                   = 50
	batchSize                   = 5
	chunkDuration time.Duration = 1 * time.Hour
)

func BenchmarkSyncReadChunk(b *testing.B) {
	fixture := Fixtures[0]
	_, chunkClient, _, schemaCfg, close, _ := fixture.Clients()
	defer close.Close()

	ctx := context.Background()

	// Prepare testing data of chunks
	written := []string{}
	testChunks := []chunk.Chunk{}
	for i := 0; i < chunksNum; i++ {
		keys, chunks, err := testutils.CreateChunks(schemaCfg, i, batchSize, model.Now().Add(-chunkDuration), model.Now())
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

	// Profile
	// f, err := os.Create("sync.prof")
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
		_, _ = chunkClient.GetChunks(ctx, testChunks)
	}
}

func BenchmarkIOUringReadChunk(b *testing.B) {
	testFixture := Fixtures[0]
	_, chunkClient, _, schemaCfg, close, _ := testFixture.Clients()
	defer close.Close()

	ctx := context.Background()

	localFixture, ok := testFixture.(*fixture)
	require.True(b, ok)

	iouringClient, err := NewIOUringChunkClient(chunkClient, localFixture.dirname, schemaCfg)
	require.NoError(b, err)

	// Prepare testing data of chunks
	written := []string{}
	testChunks := []chunk.Chunk{}
	for i := 0; i < chunksNum; i++ {
		keys, chunks, err := testutils.CreateChunks(schemaCfg, i, batchSize, model.Now().Add(-1*chunkDuration), model.Now())
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

	// Profile
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
