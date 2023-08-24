/*
Copyright 2023 John R Patek Sr

Permission is hereby granted, free of charge, to any person obtaining
a copy of this software and associated documentation files (the “Software”),
to deal in the Software without restriction, including without limitation the
rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
sell copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
*/

package filekv

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"math"
	"os"
	"path"
	"sync"
	"sync/atomic"
)

const (
	defaultBucketCount int     = 113
	defaultLoadFactor  float64 = 0.75
)

func defaultHashFunction[K comparable](key K) []byte {
	buffer := bytes.NewBuffer(nil)
	encoder := gob.NewEncoder(buffer)
	_ = encoder.Encode(key)
	hash := fnv.New64()
	_, _ = hash.Write(buffer.Bytes())
	return hash.Sum(nil)
}

type entry[K comparable, V any] struct {
	Key   K
	Value V
}

// FlieKV Generic KV storage
type FileKV[K comparable, V any] struct {
	directory     string
	mutex         *sync.RWMutex
	size          int
	bucketCount   int
	bucketMutexes []*sync.RWMutex
	loadFactor    float64
	hashFunction  func(K) []byte
	needRehash    atomic.Bool
}

// Option FileKV configuration options
type Option struct {
	key   string
	value interface{}
}

/*
BucketCount create a config option for initial bucket count
*/
func BucketCount(count int) Option {
	return Option{
		key:   "BucketCount",
		value: count,
	}
}

// HashFunction create config option for custom hash function
func HashFunction[K comparable](hashFunction func(K) []byte) Option {
	return Option{
		key:   "HashFunction",
		value: hashFunction,
	}
}

// LoadFactor create config option for load factor
func LoadFactor(loadFactor float64) Option {
	return Option{
		key:   "LoadFactor",
		value: loadFactor,
	}
}

func New[K comparable, V any](directory string, options ...Option) (*FileKV[K, V], error) {
	fileKV := &FileKV[K, V]{
		directory:     directory,
		mutex:         new(sync.RWMutex),
		size:          0,
		bucketCount:   defaultBucketCount,
		bucketMutexes: []*sync.RWMutex{},
		loadFactor:    defaultLoadFactor,
		hashFunction:  defaultHashFunction[K],
		needRehash:    atomic.Bool{},
	}

	err := fileKV.verifyDirectory()
	if err != nil {
		return nil, fmt.Errorf("New: invalid directory: %v", err)
	}

	err = fileKV.parseOptions(options)
	if err != nil {
		return nil, fmt.Errorf("New: invalid options: %v", err)
	}

	err = fileKV.createBuckets(fileKV.bucketCount)
	if err != nil {
		return nil, fmt.Errorf("New: failed to create buckets: %v", err)
	}

	return fileKV, nil
}

func (fileKV *FileKV[K, V]) Create(key K, value V) error {
	bucketIndex := fileKV.getBucketIndex(key)
	bucketPath := path.Join(fileKV.directory, fmt.Sprintf("bucket%d", bucketIndex))
	fileKV.mutex.RLock()
	fileKV.bucketMutexes[bucketIndex].Lock()
	rehashed := false
	defer func() {
		if !rehashed {
			fileKV.bucketMutexes[bucketIndex].Unlock()
			fileKV.mutex.RUnlock()
		}
	}()
	entries, err := fileKV.loadBucket(bucketPath)
	if err != nil {
		return fmt.Errorf("Create: failed to load bucket: %v", err)
	}
	for _, existingEntry := range entries {
		if key == existingEntry.Key {
			return errors.New("Create: entry already exists")
		}
	}
	entries = append(entries, entry[K, V]{key, value})
	err = fileKV.storeBucket(bucketPath, entries)
	if err != nil {
		return fmt.Errorf("Create: failed to store bucket: %v", err)
	}
	fileKV.size++
	if float64(fileKV.size)/float64(fileKV.bucketCount) >= fileKV.loadFactor {
		fileKV.bucketMutexes[bucketIndex].Unlock()
		fileKV.mutex.RUnlock()
		fileKV.mutex.Lock()
		defer fileKV.mutex.Unlock()
		fileKV.rehash()
		rehashed = true
	}
	return nil
}

func (fileKV *FileKV[K, V]) Read(key K) (V, error) {
	var result V
	bucketIndex := fileKV.getBucketIndex(key)
	bucketPath := path.Join(fileKV.directory, fmt.Sprintf("bucket%d", bucketIndex))
	fileKV.mutex.RLock()
	defer fileKV.mutex.RUnlock()
	fileKV.bucketMutexes[bucketIndex].RLock()
	defer fileKV.bucketMutexes[bucketIndex].RUnlock()

	entries, err := fileKV.loadBucket(bucketPath)
	if err != nil {
		return result, fmt.Errorf("Read: failed to load bucket: %v", err)
	}
	found := false
	for _, existingEntry := range entries {
		if !found && key == existingEntry.Key {
			result = existingEntry.Value
			found = true
		}
	}
	if !found {
		return result, errors.New("Read: entry does not exist")
	}
	return result, nil
}

func (fileKV *FileKV[K, V]) Update(key K, updateFunc func(value V) V) error {
	bucketIndex := fileKV.getBucketIndex(key)
	bucketPath := path.Join(fileKV.directory, fmt.Sprintf("bucket%d", bucketIndex))
	fileKV.mutex.RLock()
	defer fileKV.mutex.RUnlock()
	fileKV.bucketMutexes[bucketIndex].Lock()
	defer fileKV.bucketMutexes[bucketIndex].Unlock()
	entries, err := fileKV.loadBucket(bucketPath)
	if err != nil {
		return fmt.Errorf("Update: failed to load bucket: %v", err)
	}
	found := false
	for _, existingEntry := range entries {
		if !found && key == existingEntry.Key {
			existingEntry.Value = updateFunc(existingEntry.Value)
			found = true
		}
	}
	if !found {
		return errors.New("Update: entry does not exist")
	}
	err = fileKV.storeBucket(bucketPath, entries)
	if err != nil {
		return fmt.Errorf("Update: failed to store bucket: %v", err)
	}
	return nil
}

func (fileKV *FileKV[K, V]) Delete(key K) error {
	bucketIndex := fileKV.getBucketIndex(key)
	bucketPath := path.Join(fileKV.directory, fmt.Sprintf("bucket%d", bucketIndex))
	fileKV.mutex.RLock()
	defer fileKV.mutex.RUnlock()
	fileKV.bucketMutexes[bucketIndex].RLock()
	defer fileKV.bucketMutexes[bucketIndex].RUnlock()

	entries, err := fileKV.loadBucket(bucketPath)
	if err != nil {
		return fmt.Errorf("Delete: failed to load bucket: %v", err)
	}
	found := false
	updatedEntries := []entry[K, V]{}
	for _, existingEntry := range entries {
		if key != existingEntry.Key {
			updatedEntries = append(updatedEntries, existingEntry)
		} else {
			found = true
		}
	}
	if !found {
		return errors.New("Delete: entry does not exist")
	}
	err = fileKV.storeBucket(bucketPath, updatedEntries)
	if err != nil {
		return fmt.Errorf("Delete: failed to store bucket: %v", err)
	}
	return nil
}

func (fileKV *FileKV[K, V]) verifyDirectory() error {
	directoryInfo, err := os.Stat(fileKV.directory)
	if err != nil {
		return fmt.Errorf("New: stat failed")
	} else if !directoryInfo.IsDir() {
		return fmt.Errorf("New: %s is not a directory", fileKV.directory)
	}
	return nil
}

func (fileKV *FileKV[K, V]) parseOptions(options []Option) error {
	for _, option := range options {
		switch option.key {
		case "BucketCount":
			fileKV.bucketCount = option.value.(int)
		case "HashFunction":
			fileKV.hashFunction = option.value.(func(K) []byte)
		case "LoadFactor":
			fileKV.loadFactor = option.value.(float64)
		default:
			return fmt.Errorf("New: invalid option \"%s\"", option.key)
		}
	}
	return nil
}

func (fileKV *FileKV[K, V]) createBuckets(count int) error {
	fileKV.bucketMutexes = make([]*sync.RWMutex, count)
	for bucketIndex := 0; bucketIndex < count; bucketIndex++ {
		fileKV.bucketMutexes[bucketIndex] = new(sync.RWMutex)
		file, err := os.Create(path.Join(fileKV.directory, fmt.Sprintf("bucket%d", bucketIndex)))
		if err != nil {
			return fmt.Errorf("createBuckets: create file failed: %v", err)
		}
		_ = file.Close()
	}
	fileKV.bucketCount = count
	return nil
}

func (fileKV *FileKV[K, V]) rehash() {
	existingBucketCount := fileKV.bucketCount

	for bucketIndex := 0; bucketIndex < existingBucketCount; bucketIndex++ {
		bucketPath := path.Join(fileKV.directory, fmt.Sprintf("bucket%d", bucketIndex))
		_ = os.Rename(bucketPath, bucketPath+".old")
	}

	_ = fileKV.createBuckets(nextPrime(existingBucketCount))

	moveEntry := func(existing entry[K, V]) {
		index := fileKV.getBucketIndex(existing.Key)
		path := path.Join(fileKV.directory, fmt.Sprintf("bucket%d", index))
		fileKV.bucketMutexes[index].Lock()
		defer fileKV.bucketMutexes[index].Unlock()
		file, _ := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
		defer file.Close()
		encoder := json.NewEncoder(file)
		_ = encoder.Encode(existing)
	}

	waitGroup := new(sync.WaitGroup)
	waitGroup.Add(existingBucketCount)
	fileKV.size = 0
	for bucketIndex := 0; bucketIndex < existingBucketCount; bucketIndex++ {
		bucketPath := path.Join(fileKV.directory, fmt.Sprintf("bucket%d.old", bucketIndex))
		go func() {
			defer waitGroup.Done()
			entries, _ := fileKV.loadBucket(bucketPath)
			for _, bucketEntry := range entries {
				moveEntry(bucketEntry)
			}
		}()
	}
	waitGroup.Wait()

	for bucketIndex := 0; bucketIndex < existingBucketCount; bucketIndex++ {
		oldBucketPath := path.Join(fileKV.directory, fmt.Sprintf("bucket%d.old", bucketIndex))
		_ = os.Remove(oldBucketPath)
	}
}

func (fileKV *FileKV[K, V]) getBucketIndex(key K) int {
	hashReader := bytes.NewReader(fileKV.hashFunction(key))
	value := uint64(0)
	block := make([]byte, 8)
	for hashReader.Len() > 0 {
		readCount, _ := hashReader.Read(block)
		for paddingIndex := readCount; paddingIndex < 8; paddingIndex++ {
			block[paddingIndex] = 0
		}
		readValue := binary.BigEndian.Uint64(block)
		value = value ^ readValue
	}
	return int(value % uint64(fileKV.bucketCount))
}

func (fileKV *FileKV[K, V]) loadBucket(path string) ([]entry[K, V], error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %v", err)
	}
	defer file.Close()
	result := []entry[K, V]{}
	decoder := json.NewDecoder(file)
	for decoder.More() {
		next := entry[K, V]{}
		_ = decoder.Decode(&next)
		result = append(result, next)
	}
	return result, nil
}

func (fileKV *FileKV[K, V]) storeBucket(path string, entries []entry[K, V]) error {
	file, err := os.OpenFile(path, os.O_WRONLY, 0)
	if err != nil {
		return fmt.Errorf("failed to open file: %v", err)
	}
	defer file.Close()
	encoder := json.NewEncoder(file)
	for _, bucketEnry := range entries {
		err := encoder.Encode(bucketEnry)
		if err != nil {
			return fmt.Errorf("failed to encode entry: %v", err)
		}
	}
	return nil
}

func isPrime(value int) bool {
	for i := 2; i <= int(math.Floor(float64(value)/2)); i++ {
		if value%i == 0 {
			return false
		}
	}
	return value > 1
}

func nextPrime(value int) int {
	next := value + 1
	for !isPrime(next) {
		next++
	}
	return next
}
