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
	"fmt"
	"hash/fnv"
	"math"
	"os"
	"path"
	"regexp"
	"strings"
	"sync"
)

const (
	defaultBucketCount     int     = 113
	defaultLoadFactor      float64 = 0.75
	defaultRehashIncrement int     = 15
	recoveryFile           string  = ".filekv"
)

func defaultHashFunction[K comparable](key K) []byte {
	buffer := bytes.NewBuffer(nil)
	encoder := gob.NewEncoder(buffer)
	_ = encoder.Encode(key)
	hash := fnv.New64()
	_, _ = hash.Write(buffer.Bytes())
	return hash.Sum(nil)
}

type entryKV[K comparable, V any] struct {
	Key   K
	Value V
}

type bucketKV[K comparable, V any] struct {
	path  string
	mutex *sync.RWMutex
}

func newBucket[K comparable, V any](directory string, index int) (*bucketKV[K, V], error) {
	path := path.Join(directory, fmt.Sprintf("bucket%d", index))
	file, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("newBucket: failed to create bucket file: %v", err)
	}
	file.Close()
	return &bucketKV[K, V]{
		path:  path,
		mutex: new(sync.RWMutex),
	}, nil
}

func recoverBucket[K comparable, V any](path string) (*bucketKV[K, V], error) {
	statInfo, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("recoverBucket: failed to stat bucket: %v", err)
	}
	if !statInfo.Mode().IsRegular() {
		return nil, fmt.Errorf("recoverBucket: not a file: %s", path)
	}
	return &bucketKV[K, V]{
		path:  path,
		mutex: new(sync.RWMutex),
	}, nil
}

func (bucket *bucketKV[K, V]) load(handleEntries func([]entryKV[K, V]) error) error {
	file, err := os.Open(bucket.path)
	if err != nil {
		return fmt.Errorf("failed to open bucket: %v", err)
	}
	defer file.Close()
	entries := []entryKV[K, V]{}
	decoder := json.NewDecoder(file)
	for decoder.More() {
		entry := entryKV[K, V]{}
		_ = decoder.Decode(&entry)
		entries = append(entries, entry)
	}
	err = handleEntries(entries)
	if err != nil {
		return fmt.Errorf("handler returned error: %v", err)
	}
	return nil
}

func (bucket *bucketKV[K, V]) store(handleEntries func() ([]entryKV[K, V], error), append bool) error {
	flags := os.O_RDWR | os.O_TRUNC
	if append {
		flags = os.O_RDWR | os.O_APPEND
	}
	file, err := os.OpenFile(bucket.path, flags, 0)
	if err != nil {
		return fmt.Errorf("failed to open bucket: %v", err)
	}
	entries, err := handleEntries()
	if err != nil {
		return fmt.Errorf("handler returned error: %v", err)
	}
	if len(entries) > 0 {
		encoder := json.NewEncoder(file)
		for _, entry := range entries {
			err = encoder.Encode(entry)
			if err != nil {
				return fmt.Errorf("failed to encode entry: %v", err)
			}
		}
	}
	return nil
}

func (bucket *bucketKV[K, V]) loadStore(handleEntries func([]entryKV[K, V]) ([]entryKV[K, V], error), append bool) error {
	existingEntries := []entryKV[K, V]{}
	err := bucket.load(func(entries []entryKV[K, V]) error {
		existingEntries = entries
		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to load bucket: %v", err)
	}
	newEntries, err := handleEntries(existingEntries)
	if err != nil {
		return fmt.Errorf("failed to process bucket: %v", err)
	}
	err = bucket.store(func() ([]entryKV[K, V], error) {
		return newEntries, nil
	}, append)
	if err != nil {
		return fmt.Errorf("failed to store bucket: %v", err)
	}
	return nil
}

func (bucket *bucketKV[K, V]) lock() {
	bucket.mutex.Lock()
}

func (bucket *bucketKV[K, V]) unlock() {
	bucket.mutex.Unlock()
}

func (bucket *bucketKV[K, V]) rLock() {
	bucket.mutex.RLock()
}

func (bucket *bucketKV[K, V]) rUnlock() {
	bucket.mutex.RUnlock()
}

func (bucket *bucketKV[K, V]) expire() error {
	newPath := bucket.path
	if !strings.HasSuffix(newPath, ".old") {
		newPath = bucket.path + ".old"
	}
	err := os.Rename(bucket.path, newPath)
	if err != nil {
		return fmt.Errorf("expire: rename failed: %v", err)
	}
	bucket.path = newPath
	return nil
}

func (bucket *bucketKV[K, V]) remove() error {
	err := os.Remove(bucket.path)
	if err != nil {
		return fmt.Errorf("remove: remove failed: %v", err)
	}
	return nil
}

type optionsKV[K comparable] struct {
	bucketCount  int
	hashFunction func(K) []byte
	loadFactor   float64
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

// FlieKV Generic KV storage
type FileKV[K comparable, V any] struct {
	directory    string
	mutex        *sync.RWMutex
	buckets      []*bucketKV[K, V]
	size         int
	loadFactor   float64
	hashFunction func(K) []byte
}

type recoveryKV struct {
	LoadFactor float64
}

func (fileKV *FileKV[K, V]) storeRecovery() error {
	path := path.Join(fileKV.directory, recoveryFile)
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		return fmt.Errorf("storeRecovery: failed to open recovery file")
	}
	defer file.Close()
	encoder := json.NewEncoder(file)
	_ = encoder.Encode(recoveryKV{
		LoadFactor: fileKV.loadFactor,
	})
	return nil
}

func (fileKV *FileKV[K, V]) loadRecovery() error {
	path := path.Join(fileKV.directory, recoveryFile)
	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("loadRecovery: failed to open recovery file")
	}
	defer file.Close()
	recovery := new(recoveryKV)
	decoder := json.NewDecoder(file)
	_ = decoder.Decode(recovery)
	fileKV.loadFactor = recovery.LoadFactor
	return nil
}

func New[K comparable, V any](directory string, options ...Option) (*FileKV[K, V], error) {
	parsedOptions, err := parseOptions[K](options)
	if err != nil {
		return nil, fmt.Errorf("New: invalid options: %v", err)
	}

	fileKV := &FileKV[K, V]{
		directory:    directory,
		mutex:        new(sync.RWMutex),
		size:         0,
		loadFactor:   parsedOptions.loadFactor,
		hashFunction: parsedOptions.hashFunction,
	}

	err = verifyDirectory(directory)
	if err != nil {
		return nil, fmt.Errorf("New: invalid directory: %v", err)
	}

	err = fileKV.createBuckets(parsedOptions.bucketCount)
	if err != nil {
		return nil, fmt.Errorf("New: failed to create buckets: %v", err)
	}

	fileKV.storeRecovery()

	return fileKV, nil
}

func Recover[K comparable, V any](directory string, hashFunction func(K) []byte) (*FileKV[K, V], error) {
	fileKV := &FileKV[K, V]{
		directory:    directory,
		hashFunction: hashFunction,
	}

	err := fileKV.loadRecovery()
	if err != nil {
		return nil, fmt.Errorf("Recover: failed to load recovery file: %v", err)
	}

	files, err := os.ReadDir(directory)
	if err != nil {
		return nil, fmt.Errorf("Recover: failed to read directory: %v", err)
	}
	regex, _ := regexp.Compile("bucket*")
	buckets := []*bucketKV[K, V]{}
	for _, file := range files {
		if regex.Match([]byte(file.Name())) {
			bucket, err := recoverBucket[K, V](path.Join(directory, file.Name()))
			if err != nil {
				return nil, fmt.Errorf("Recover: failed to recover bucket: %v", err)
			}
			buckets = append(buckets, bucket)
		}
	}
	size := 0
	for _, bucket := range buckets {
		err = bucket.load(func(entries []entryKV[K, V]) error {
			size += len(entries)
			return nil
		})
		if err != nil {
			return nil, fmt.Errorf("Recover: failed to load bucket: %v", err)
		}
	}
	return fileKV, nil
}

func (fileKV *FileKV[K, V]) Create(key K, value V) error {
	fileKV.mutex.RLock()
	index := fileKV.getBucketIndex(key)
	bucket := fileKV.buckets[index]
	bucket.lock()
	err := bucket.loadStore(func(entries []entryKV[K, V]) ([]entryKV[K, V], error) {
		for _, entry := range entries {
			if key == entry.Key {
				return nil, fmt.Errorf("entry %v already exists", key)
			}
		}
		return []entryKV[K, V]{
			{
				Key:   key,
				Value: value,
			},
		}, nil
	}, true)
	if err != nil {
		bucket.unlock()
		fileKV.mutex.RUnlock()
		return fmt.Errorf("Create: loadStore failed: %v", err)
	}
	bucket.unlock()
	fileKV.mutex.RUnlock()
	fileKV.mutex.Lock()
	defer fileKV.mutex.Unlock()
	fileKV.size++
	if float64(fileKV.size)/float64(len(fileKV.buckets)) >= fileKV.loadFactor {
		fileKV.rehash()
	}
	return nil
}

func (fileKV *FileKV[K, V]) Read(key K) (V, error) {
	var result V
	fileKV.mutex.RLock()
	defer fileKV.mutex.RUnlock()
	index := fileKV.getBucketIndex(key)
	bucket := fileKV.buckets[index]
	bucket.rLock()
	defer bucket.rUnlock()
	err := bucket.load(func(entries []entryKV[K, V]) error {
		found := false
		index := 0
		for !found && index < len(entries) {
			entry := entries[index]
			if key == entry.Key {
				result = entry.Value
				found = true
			}
			index++
		}
		if !found {
			return fmt.Errorf("entry %v does not exist", key)
		}
		return nil
	})
	if err != nil {
		return result, fmt.Errorf("Read: load failed: %v", err)
	}
	return result, nil
}

func (fileKV *FileKV[K, V]) Update(key K, updateFunc func(value V) V) error {
	fileKV.mutex.RLock()
	defer fileKV.mutex.RUnlock()
	index := fileKV.getBucketIndex(key)
	bucket := fileKV.buckets[index]
	bucket.lock()
	defer bucket.unlock()
	err := bucket.loadStore(func(entries []entryKV[K, V]) ([]entryKV[K, V], error) {
		found := false
		newEntries := []entryKV[K, V]{}
		for index := 0; index < len(entries); index++ {
			entry := entries[index]
			if key == entry.Key {
				newEntries = append(newEntries, entryKV[K, V]{
					Key:   key,
					Value: updateFunc(entry.Value),
				})
				found = true
			} else {
				newEntries = append(newEntries, entry)
			}
		}
		if !found {
			return nil, fmt.Errorf("entry %v does not exist", key)
		}
		return newEntries, nil
	}, false)
	if err != nil {
		return fmt.Errorf("Read: load failed: %v", err)
	}
	return nil
}

func (fileKV *FileKV[K, V]) Delete(key K) error {
	fileKV.mutex.RLock()
	index := fileKV.getBucketIndex(key)
	bucket := fileKV.buckets[index]
	bucket.lock()
	err := bucket.loadStore(func(entries []entryKV[K, V]) ([]entryKV[K, V], error) {
		found := false
		newEntries := []entryKV[K, V]{}
		for index := 0; index < len(entries); index++ {
			entry := entries[index]
			if key == entry.Key {
				found = true
			} else {
				newEntries = append(newEntries, entry)
			}
		}
		if !found {
			return nil, fmt.Errorf("entry %v does not exist", key)
		}
		return newEntries, nil
	}, false)
	bucket.unlock()
	fileKV.mutex.RUnlock()
	if err != nil {
		return fmt.Errorf("Delete: loadStore failed: %v", err)
	}
	fileKV.mutex.Lock()
	defer fileKV.mutex.Unlock()
	fileKV.size--
	return nil
}

func (fileKV *FileKV[K, V]) ForEach(callbackFunc func(K, V)) {
	fileKV.mutex.Lock()
	defer fileKV.mutex.Lock()
	waitGroup := new(sync.WaitGroup)
	for index := range fileKV.buckets {
		waitGroup.Add(1)
		func(bucket *bucketKV[K, V]) {
			defer waitGroup.Done()
			_ = bucket.load(func(entries []entryKV[K, V]) error {
				for _, entry := range entries {
					callbackFunc(entry.Key, entry.Value)
				}
				return nil
			})
		}(fileKV.buckets[index])
	}
	waitGroup.Wait()
}

func verifyDirectory(path string) error {
	directoryInfo, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("stat failed: %v", err)
	} else if !directoryInfo.IsDir() {
		return fmt.Errorf("%s is not a directory", path)
	}
	return nil
}

func parseOptions[K comparable](options []Option) (*optionsKV[K], error) {
	result := &optionsKV[K]{
		bucketCount:  defaultBucketCount,
		hashFunction: defaultHashFunction[K],
		loadFactor:   defaultLoadFactor,
	}
	for _, option := range options {
		switch option.key {
		case "BucketCount":
			result.bucketCount = option.value.(int)
		case "HashFunction":
			result.hashFunction = option.value.(func(K) []byte)
		case "LoadFactor":
			result.loadFactor = option.value.(float64)
		default:
			return nil, fmt.Errorf("New: invalid option \"%s\"", option.key)
		}
	}
	return result, nil
}

func (fileKV *FileKV[K, V]) createBuckets(count int) error {
	fileKV.buckets = make([]*bucketKV[K, V], count)
	for index := range fileKV.buckets {
		bucket, err := newBucket[K, V](fileKV.directory, index)
		if err != nil {
			return fmt.Errorf("createBuckets: failed to create new bucket: %v", err)
		}
		fileKV.buckets[index] = bucket
	}
	return nil
}

func (fileKV *FileKV[K, V]) rehash() {
	oldBuckets := make([]*bucketKV[K, V], len(fileKV.buckets))
	copy(oldBuckets, fileKV.buckets)
	for _, oldBucket := range oldBuckets {
		_ = oldBucket.expire()
	}

	_ = fileKV.createBuckets(nextPrime(len(oldBuckets), 15))

	waitGroup := new(sync.WaitGroup)
	waitGroup.Add(len(fileKV.buckets))
	channels := make([]chan entryKV[K, V], len(fileKV.buckets))
	for bucketIndex := range fileKV.buckets {
		channels[bucketIndex] = make(chan entryKV[K, V])
		go func(bucket *bucketKV[K, V], channel chan entryKV[K, V]) {
			defer waitGroup.Done()
			for entry := range channel {
				_ = bucket.store(func() ([]entryKV[K, V], error) {
					return []entryKV[K, V]{
						entry,
					}, nil
				}, true)
			}
		}(fileKV.buckets[bucketIndex], channels[bucketIndex])
	}

	for _, oldBucket := range oldBuckets {
		_ = oldBucket.load(func(entries []entryKV[K, V]) error {
			for _, entry := range entries {
				index := fileKV.getBucketIndex(entry.Key)
				channels[index] <- entry
			}
			return nil
		})
	}

	for _, channel := range channels {
		close(channel)
	}

	waitGroup.Wait()
	for _, oldBucket := range oldBuckets {
		_ = oldBucket.remove()
	}
}

func (fileKV *FileKV[K, V]) getBucketIndex(key K) int {
	bucketCount := len(fileKV.buckets)
	if bucketCount == 0 {
		return -1
	}
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
	return int(value % uint64(bucketCount))
}

func isPrime(value int) bool {
	for i := 2; i <= int(math.Floor(float64(value)/2)); i++ {
		if value%i == 0 {
			return false
		}
	}
	return value > 1
}

func nextPrime(value int, offset int) int {
	next := value
	for count := 0; count < offset; count++ {
		next = next + 1
		for !isPrime(next) {
			next++
		}
	}
	return next
}
