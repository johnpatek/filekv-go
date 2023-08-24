package filekv_test

import (
	"testing"

	"github.com/google/uuid"
)

func BenchmarkFileKV(b *testing.B) {
	_, _ = uuid.NewRandom()
}
