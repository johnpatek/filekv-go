package filekv_test

import (
	"fmt"
	"os"
	"testing"

	"github.com/johnpatek/filekv-go"
	"github.com/stretchr/testify/assert"
)

type ValueType struct {
	String string
}

var directory string

func TestFileKV(t *testing.T) {
	directory = os.Getenv("FILEKV_INTEGRATION_DIRECTORY")
	kv, err := filekv.New[string, *ValueType](directory)
	assert.NotNil(t, kv)
	assert.NoError(t, err)
	for count := 0; count < 100; count++ {
		key := fmt.Sprintf("key%d", count)
		value := &ValueType{
			String: fmt.Sprintf("value%d", count),
		}
		err = kv.Create(key, value)
		assert.NoError(t, err)
	}

	for count := 0; count < 100; count++ {
		key := fmt.Sprintf("key%d", count)
		expectedValue := &ValueType{
			String: fmt.Sprintf("value%d", count),
		}
		value, err := kv.Read(key)
		assert.Equal(t, expectedValue, value)
		assert.NoError(t, err)
	}
}
