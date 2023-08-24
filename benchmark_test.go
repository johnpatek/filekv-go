package filekv_test

import (
	"os"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/johnpatek/filekv-go"
)

func clientLoop(kv *filekv.FileKV[string, int]) {
	key := uuid.NewString()
	_ = kv.Create(key, 0)

	running := true
	for running {
		value, _ := kv.Read(key)
		if value < 10 {
			_ = kv.Update(key, func(current int) int {
				return current + 1
			})
		} else {
			running = false
		}
	}

	kv.Delete(key)
}

func BenchmarkFileKV(b *testing.B) {
	kv, _ := filekv.New[string, int](os.Getenv("FILEKV_BENCHMARK_DIRECTORY"))
	waitGroup := new(sync.WaitGroup)
	for index := 0; index < 500; index++ {
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			clientLoop(kv)
		}()
	}
	waitGroup.Wait()
}
