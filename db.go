package minidb

import (
	"io"
	"os"
	"path/filepath"
	"sync"
)

type Bucket struct {
	indexes map[string]int64 // 内存中的索引信息
	dbFile  *DBFile          // 数据文件
	dbName  string           // 数据目录
	dirName string
	mu      sync.RWMutex
}

type DB struct {
	dir string
}

func Open(dirname string) (*DB, error) {
	// 如果数据库目录不存在，则新建一个
	if _, err := os.Stat(dirname); os.IsNotExist(err) {
		if err := os.MkdirAll(dirname, os.ModePerm); err != nil {
			return nil, err
		}
		return &DB{dir: dirname}, nil
	}
	return &DB{dir: dirname}, nil
}

// Open 开启一个数据库实例
func (db *DB) Bucket(filename string) (*Bucket, error) {
	dbFile, err := NewDBFile(db.dir, filename)
	if err != nil {
		return nil, err
	}
	bucket := &Bucket{
		dbFile:  dbFile,
		indexes: make(map[string]int64),
		dbName:  filename,
		dirName: db.dir,
	}

	// 加载索引
	bucket.loadIndexesFromFile()
	return bucket, nil
}

// Merge 合并数据文件，在rosedb当中是 Reclaim 方法
func (bucket *Bucket) Merge() error {
	// 没有数据，忽略
	if bucket.dbFile.Offset == 0 {
		return nil
	}

	var (
		validEntries []*Entry
		offset       int64
	)

	// 读取原数据文件中的 Entry
	for {
		e, err := bucket.dbFile.Read(offset)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		// 内存中的索引状态是最新的，直接对比过滤出有效的 Entry
		if off, ok := bucket.indexes[string(e.Key)]; ok && off == offset {
			validEntries = append(validEntries, e)
		}
		offset += e.GetSize()
	}

	// 新建临时文件
	mergeDBFile, err := NewMergeDBFile(bucket.dirName, bucket.dbName)
	if err != nil {
		return err
	}
	defer func() {
		_ = os.Remove(mergeDBFile.File.Name())
	}()

	bucket.mu.Lock()
	defer bucket.mu.Unlock()

	// 重新写入有效的 entry
	for _, entry := range validEntries {
		writeOff := mergeDBFile.Offset
		err = mergeDBFile.Write(entry)
		if err != nil {
			return err
		}

		// 更新索引
		bucket.indexes[string(entry.Key)] = writeOff
	}

	// 获取文件名
	dbFileName := bucket.dbFile.File.Name()
	// 关闭文件
	_ = bucket.dbFile.File.Close()
	// 删除旧的数据文件
	_ = os.Remove(dbFileName)
	_ = mergeDBFile.File.Close()
	// 获取文件名
	mergeDBFileName := mergeDBFile.File.Name()
	// 临时文件变更为新的数据文件
	_ = os.Rename(mergeDBFileName, filepath.Join(bucket.dirName, bucket.dbName))

	dbFile, err := NewDBFile(bucket.dirName, bucket.dbName)
	if err != nil {
		return err
	}

	bucket.dbFile = dbFile
	return nil
}

// Put 写入数据
func (bucket *Bucket) Put(key []byte, value []byte) (err error) {
	if len(key) == 0 {
		return
	}

	bucket.mu.Lock()
	defer bucket.mu.Unlock()

	offset := bucket.dbFile.Offset
	// 封装成 Entry
	entry := NewEntry(key, value, PUT)
	// 追加到数据文件当中
	err = bucket.dbFile.Write(entry)

	// 写到内存
	bucket.indexes[string(key)] = offset
	return
}

// exist key值是否存在与数据库
// 若存在返回偏移量；不存在返回ErrKeyNotFound
func (bucket *Bucket) exist(key []byte) (int64, error) {
	// 从内存当中取出索引信息
	offset, ok := bucket.indexes[string(key)]
	// key 不存在
	if !ok {
		return 0, ErrKeyNotFound
	}
	return offset, nil
}

// Get 取出数据
func (bucket *Bucket) Get(key []byte) (val []byte, err error) {
	if len(key) == 0 {
		return
	}

	bucket.mu.RLock()
	defer bucket.mu.RUnlock()

	offset, err := bucket.exist(key)
	if err == ErrKeyNotFound {
		return
	}

	// 从磁盘中读取数据
	var e *Entry
	e, err = bucket.dbFile.Read(offset)
	if err != nil && err != io.EOF {
		return
	}
	if e != nil {
		val = e.Value
	}
	return
}

// Del 删除数据
func (bucket *Bucket) Del(key []byte) (err error) {
	if len(key) == 0 {
		return
	}

	bucket.mu.Lock()
	defer bucket.mu.Unlock()

	_, err = bucket.exist(key)
	if err == ErrKeyNotFound {
		err = nil
		return
	}

	// 封装成 Entry 并写入
	e := NewEntry(key, nil, DEL)
	err = bucket.dbFile.Write(e)
	if err != nil {
		return
	}

	// 删除内存中的 key
	delete(bucket.indexes, string(key))
	return
}

// 从文件当中加载索引
func (bucket *Bucket) loadIndexesFromFile() {
	if bucket.dbFile == nil {
		return
	}

	var offset int64
	for {
		e, err := bucket.dbFile.Read(offset)
		if err != nil {
			// 读取完毕
			if err == io.EOF {
				break
			}
			return
		}

		// 设置索引状态
		bucket.indexes[string(e.Key)] = offset

		if e.Mark == DEL {
			// 删除内存中的 key
			delete(bucket.indexes, string(e.Key))
		}

		offset += e.GetSize()
	}
}

// Close 关闭 db 实例
func (bucket *Bucket) Close() error {
	if bucket.dbFile == nil {
		return ErrInvalidDBFile
	}

	return bucket.dbFile.File.Close()
}
