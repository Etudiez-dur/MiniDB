package minidb

import (
	"io"
	"os"
	"runtime"
	"sync"
)

type Bucket struct {
	indexes *sync.Map // 内存中的索引信息
	dbFile  *DBFile
	name    string
}

type DB struct {
	dbFile  *DBFile // 数据文件
	dbName  string  // 数据目录
	buckets *sync.Map
}

func Open(dbname string) (*DB, error) {
	dbFile, err := NewDBFile(dbname)
	if err != nil {
		return nil, err
	}
	db := &DB{dbFile: dbFile, dbName: dbname, buckets: &sync.Map{}}
	db.loadIndexesFromFile()
	return db, nil
}

// Open 开启一个数据库实例
func (db *DB) Bucket(name string) *Bucket {
	bucket, _ := db.buckets.LoadOrStore(name, &Bucket{
		dbFile:  db.dbFile,
		name:    name,
		indexes: &sync.Map{},
	})
	b := bucket.(*Bucket)
	return b
}

// Merge 合并数据文件，在rosedb当中是 Reclaim 方法
func (db *DB) Merge() error {
	// 没有数据，忽略
	if db.dbFile.Offset == 0 {
		return nil
	}

	var (
		validEntries []*Entry
		offset       int64
	)

	// 读取原数据文件中的 Entry
	for {
		e, err := db.dbFile.Read(offset)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		b, _ := db.buckets.Load(string(e.Bucket))
		bucket := b.(*Bucket)
		// 内存中的索引状态是最新的，直接对比过滤出有效的 Entry
		if off, ok := bucket.indexes.Load(string(e.Key)); ok && off == offset {
			validEntries = append(validEntries, e)
		}
		offset += e.GetSize()
	}
	// 新建临时文件
	mergeDBFile, err := NewMergeDBFile(db.dbName)
	if err != nil {
		return err
	}
	defer func() {
		_ = os.Remove(mergeDBFile.File.Name())
	}()

	// 重新写入有效的 entry
	for _, entry := range validEntries {
		writeOff := mergeDBFile.Offset
		err = mergeDBFile.Write(entry)
		if err != nil {
			return err
		}
		b, _ := db.buckets.Load(string(entry.Bucket))
		bucket := b.(*Bucket)
		// 更新索引
		bucket.indexes.Store(string(entry.Key), writeOff)
	}

	// 获取文件名
	dbFileName := db.dbFile.File.Name()
	// 关闭文件
	_ = db.dbFile.File.Close()
	// 删除旧的数据文件
	_ = os.Remove(dbFileName)
	_ = mergeDBFile.File.Close()
	// 获取文件名
	mergeDBFileName := mergeDBFile.File.Name()
	// 临时文件变更为新的数据文件
	_ = os.Rename(mergeDBFileName, db.dbName)

	dbFile, err := NewDBFile(db.dbName)
	if err != nil {
		return err
	}

	db.dbFile = dbFile
	return nil
}

// Put 写入数据
func (bucket *Bucket) Put(key []byte, value []byte) (err error) {
	if len(key) == 0 {
		return
	}

	offset := bucket.dbFile.Offset
	// 封装成 Entry
	entry := NewEntry(key, value, []byte(bucket.name), PUT)
	// 追加到数据文件当中
	err = bucket.dbFile.Write(entry)

	// 写到内存
	bucket.indexes.Store(string(key), offset)
	return
}

// exist key值是否存在与数据库
// 若存在返回偏移量；不存在返回ErrKeyNotFound
func (bucket *Bucket) exist(key []byte) (int64, error) {
	// 从内存当中取出索引信息
	offset, ok := bucket.indexes.Load(string(key))
	// key 不存在
	if !ok {
		return 0, ErrKeyNotFound
	}
	return offset.(int64), nil
}

// Get 取出数据
func (bucket *Bucket) Get(key []byte) (val []byte, err error) {
	if len(key) == 0 {
		return
	}

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
	println(runtime.Caller(1))
	_, err = bucket.exist(key)
	if err == ErrKeyNotFound {
		err = nil
		return
	}

	// 封装成 Entry 并写入
	e := NewEntry(key, nil, []byte(bucket.name), DEL)
	err = bucket.dbFile.Write(e)
	if err != nil {
		return
	}

	// 删除内存中的 key
	bucket.indexes.Delete(string(key))
	return
}

// 从文件当中加载索引
func (db *DB) loadIndexesFromFile() {
	if db.dbFile == nil {
		return
	}

	var offset int64
	for {
		e, err := db.dbFile.Read(offset)
		if err != nil {
			// 读取完毕
			if err == io.EOF {
				break
			}
			return
		}
		b, _ := db.buckets.LoadOrStore(string(e.Bucket), &Bucket{indexes: &sync.Map{}, name: string(e.Bucket), dbFile: db.dbFile})
		bucket := b.(*Bucket)
		// 设置索引状态
		bucket.indexes.Store(string(e.Key), offset)

		if e.Mark == DEL {
			// 删除内存中的 key
			bucket.indexes.Delete(string(e.Key))
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
