package filekv

import (
	"fmt"
	"hash/fnv"
	"os"
	"path"
	"syscall"
	"testing"

	"github.com/stretchr/testify/assert"
)

type ValidType struct {
	String string
}

type InvalidType struct {
	Channel chan int
}

var (
	validDirectory            string
	invalidDirectoryValueType string
	invalidDirectoryDNE       string
	invalidDirectoryInfo      string
	invalidDirectoryPerm      string
	invalidOption             Option = Option{
		key:   "BadKey",
		value: 100,
	}
	hashString = func(key string) []byte {
		hash := fnv.New64()
		_, _ = hash.Write([]byte(key))
		return hash.Sum(nil)
	}
	zeroHash = func(key string) []byte {
		return make([]byte, 75)
	}

	validTestKV *FileKV[string, *ValidType]
)

func TestNew(t *testing.T) {
	kv, err := New[string, *ValidType](invalidDirectoryDNE)
	assert.Nil(t, kv)
	assert.Error(t, err)
	kv, err = New[string, *ValidType](invalidDirectoryInfo)
	assert.Nil(t, kv)
	assert.Error(t, err)
	kv, err = New[string, *ValidType](validDirectory, invalidOption)
	assert.Nil(t, kv)
	assert.Error(t, err)
	kv, err = New[string, *ValidType](invalidDirectoryPerm)
	assert.Nil(t, kv)
	assert.Error(t, err)
	kv, err = New[string, *ValidType](invalidDirectoryPerm, HashFunction(hashString))
	assert.Nil(t, kv)
	assert.Error(t, err)
}

func TestBucket(t *testing.T) {
	directory := path.Join(validDirectory, "testbucket")
	_ = os.Mkdir(directory, 0777)
	bucket, err := newBucket[string, ValidType](directory, 0)
	assert.NotNil(t, bucket)
	assert.NoError(t, err)
	_ = os.Chmod(directory, 0444)
	nilBucket, err := newBucket[string, ValidType](directory, 0)
	assert.Nil(t, nilBucket)
	assert.Error(t, err)
	_ = os.Chmod(directory, 0777)
}

func TestCRUD(t *testing.T) {
	validKey := "valid-key"
	invalidKey := "invalid-key"
	initialValue := &ValidType{
		String: "initial-string",
	}
	updatedValue := &ValidType{
		String: "updated-string",
	}
	assert.NoError(t, validTestKV.Create(validKey, initialValue))
	assert.Error(t, validTestKV.Create(validKey, initialValue))
	value, err := validTestKV.Read(validKey)
	assert.Equal(t, initialValue, value)
	assert.NoError(t, err)
	_, err = validTestKV.Read(invalidKey)
	assert.Error(t, err)
	assert.NoError(t, validTestKV.Update(validKey, func(value *ValidType) *ValidType {
		return updatedValue
	}))
	assert.Error(t, validTestKV.Update(invalidKey, func(value *ValidType) *ValidType {
		return updatedValue
	}))
	value, err = validTestKV.Read(validKey)
	assert.Equal(t, updatedValue, value)
	assert.NoError(t, err)
	assert.NoError(t, validTestKV.Delete(validKey))
	assert.Error(t, validTestKV.Delete(invalidKey))
	assert.Error(t, validTestKV.Delete(validKey))
	assert.Zero(t, validTestKV.size)
	rehashTestEntryCount := 1 + int(validTestKV.loadFactor*float64(len(validTestKV.buckets)))
	for index := 0; index < rehashTestEntryCount; index++ {
		key := fmt.Sprintf("rehash-test-key-%d", index)
		value := &ValidType{
			String: fmt.Sprintf("rehash-test-key-%d", index),
		}
		assert.NoError(t, validTestKV.Create(key, value))
	}

	for index := 0; index < rehashTestEntryCount; index++ {
		key := fmt.Sprintf("rehash-test-key-%d", index)
		expectedValue := &ValidType{
			String: fmt.Sprintf("rehash-test-key-%d", index),
		}
		value, err := validTestKV.Read(key)
		assert.Equal(t, expectedValue, value)
		assert.NoError(t, err)
	}

}

func TestPrime(t *testing.T) {
	assert.True(t, isPrime(2))
	assert.False(t, isPrime(4))
	assert.False(t, isPrime(28))
	assert.Equal(t, nextPrime(7), 11)
}

func TestBucketIndex(t *testing.T) {
	path := path.Join(validDirectory, "bucket")
	_ = os.Mkdir(path, 0777)
	kv, _ := New[string, ValidType](path, HashFunction(zeroHash))
	assert.Zero(t, kv.getBucketIndex("test-key"))
}

func TestMain(m *testing.M) {
	syscall.Umask(0)
	unitTestDirectory, _ := os.MkdirTemp(os.TempDir(), "filekv-unit-*")
	integrationTestDirectory, _ := os.MkdirTemp(os.TempDir(), "filekv-integration-*")
	benchmarkTestDirectory, _ := os.MkdirTemp(os.TempDir(), "filekv-benchmark-*")
	_ = os.Setenv("FILEKV_INTEGRATION_DIRECTORY", integrationTestDirectory)
	_ = os.Setenv("FILEKV_BENCHMARK_DIRECTORY", benchmarkTestDirectory)
	invalidDirectoryDNE = unitTestDirectory + "invalid"
	invalidDirectoryInfo = path.Join(unitTestDirectory, "file")
	file, _ := os.Create(invalidDirectoryInfo)
	file.Close()
	validDirectory = path.Join(unitTestDirectory, "data")
	_ = os.Mkdir(validDirectory, 0777)
	invalidDirectoryPerm = path.Join(unitTestDirectory, "readonly")
	_ = os.Mkdir(invalidDirectoryPerm, 0444)
	invalidDirectoryValueType, _ = os.MkdirTemp(unitTestDirectory, "badtype")
	validTestKV, _ = New[string, *ValidType](validDirectory, LoadFactor(0.75), BucketCount(7))
	code := m.Run()
	unitTestEntries, _ := os.ReadDir(validDirectory)
	if len(unitTestEntries) < 2 {
		os.RemoveAll(unitTestDirectory)
	}
	integrationTestEntries, _ := os.ReadDir(integrationTestDirectory)
	if len(integrationTestEntries) == 0 {
		os.RemoveAll(integrationTestDirectory)
	}
	benchmarkTestEntries, _ := os.ReadDir(benchmarkTestDirectory)
	if len(benchmarkTestEntries) == 0 {
		os.RemoveAll(benchmarkTestDirectory)
	}
	os.Exit(code)
}
