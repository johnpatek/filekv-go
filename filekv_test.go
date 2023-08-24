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

	validTestKV   *FileKV[string, *ValidType]
	invalidTestKV *FileKV[string, *InvalidType]
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
	validTestKV, err = New[string, *ValidType](validDirectory, HashFunction(hashString), LoadFactor(2.0), BucketCount(7))
	assert.NotNil(t, validTestKV)
	assert.NoError(t, err)
	invalidTestKV, err = New[string, *InvalidType](invalidDirectoryValueType)
	assert.NotNil(t, invalidTestKV)
	assert.NoError(t, err)
}

func TestCreate(t *testing.T) {
	err := invalidTestKV.Create("test-store-fail", &InvalidType{
		Channel: make(chan int),
	})
	assert.Error(t, err)
	err = validTestKV.Create("test-value", &ValidType{
		String: "test-value",
	})
	assert.NoError(t, err)
	err = validTestKV.Create("test-value", &ValidType{
		String: "test-duplicate-fail",
	})
	assert.Error(t, err)
	validTestKV.directory = invalidDirectoryPerm
	err = validTestKV.Create("test-load-fail", &ValidType{
		String: "test-load-fail",
	})
	assert.Error(t, err)
	validTestKV.directory = validDirectory
	count := validTestKV.bucketCount
	requiredEntries := int(float64(validTestKV.bucketCount) * validTestKV.loadFactor)
	for entriesAdded := 0; entriesAdded < requiredEntries; entriesAdded++ {
		kv := fmt.Sprintf("test-rehash-entry%d", entriesAdded)
		err = validTestKV.Create(kv, &ValidType{String: kv})
		assert.NoError(t, err)
	}
	newCount := validTestKV.bucketCount
	assert.NotEqual(t, count, newCount)
}

func TestRead(t *testing.T) {
	value, err := validTestKV.Read("test-value")
	assert.NotNil(t, value)
	assert.NoError(t, err)
	value, err = validTestKV.Read("test-value-dne")
	assert.Nil(t, value)
	assert.Error(t, err)
	validTestKV.directory = invalidDirectoryDNE
	invalid, err := validTestKV.Read("test-value")
	assert.Nil(t, invalid)
	assert.Error(t, err)
	validTestKV.directory = validDirectory
}

func TestUpdate(t *testing.T) {
	updateFunc := func(value *ValidType) *ValidType {
		return value
	}
	err := validTestKV.Update("test-value", updateFunc)
	assert.NoError(t, err)
	err = validTestKV.Update("test-value-dne", updateFunc)
	assert.Error(t, err)
	validTestKV.directory = invalidDirectoryDNE
	err = validTestKV.Update("test-value", updateFunc)
	assert.Error(t, err)
	validTestKV.directory = validDirectory
	index := validTestKV.getBucketIndex("test-value")
	path := path.Join(validDirectory, fmt.Sprintf("bucket%d", index))
	_ = os.Chmod(path, 0400)
	err = validTestKV.Update("test-value", updateFunc)
	assert.Error(t, err)
	_ = os.Chmod(path, 0666)
}

func TestDelete(t *testing.T) {
	err := validTestKV.Delete("invalid-key-dne")
	assert.Error(t, err)
	validTestKV.directory = invalidDirectoryDNE
	err = validTestKV.Delete("test-value")
	assert.Error(t, err)
	validTestKV.directory = validDirectory
	index := validTestKV.getBucketIndex("test-value")
	path := path.Join(validDirectory, fmt.Sprintf("bucket%d", index))
	_ = os.Chmod(path, 0400)
	err = validTestKV.Delete("test-value")
	assert.Error(t, err)
	_ = os.Chmod(path, 0666)
	err = validTestKV.Delete("test-value")
	assert.NoError(t, err)
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
