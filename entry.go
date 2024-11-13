package minidb

import "encoding/binary"

const entryHeaderSize = 14

const (
	PUT uint16 = iota
	DEL
)

// Entry 写入文件的记录
type Entry struct {
	Bucket     []byte
	Key        []byte
	Value      []byte
	BucketSize uint32
	KeySize    uint32
	ValueSize  uint32
	Mark       uint16
}

func NewEntry(key, value []byte, bucket []byte, mark uint16) *Entry {
	return &Entry{
		Key:        key,
		Value:      value,
		Bucket:     bucket,
		KeySize:    uint32(len(key)),
		ValueSize:  uint32(len(value)),
		BucketSize: uint32(len(bucket)),
		Mark:       mark,
	}
}

func (e *Entry) GetSize() int64 {
	return int64(entryHeaderSize + e.KeySize + e.ValueSize + e.BucketSize)
}

// Encode 编码 Entry，返回字节数组
func (e *Entry) Encode() ([]byte, error) {
	buf := make([]byte, e.GetSize())
	binary.BigEndian.PutUint32(buf[0:4], e.KeySize)
	binary.BigEndian.PutUint32(buf[4:8], e.ValueSize)
	binary.BigEndian.PutUint32(buf[8:12], e.BucketSize)
	binary.BigEndian.PutUint16(buf[12:14], e.Mark)
	copy(buf[entryHeaderSize:entryHeaderSize+e.KeySize], e.Key)
	copy(buf[entryHeaderSize+e.KeySize:], e.Value)
	copy(buf[entryHeaderSize+e.KeySize+e.ValueSize:], e.Bucket)
	return buf, nil
}

// Decode 解码 buf 字节数组，返回 Entry
func Decode(buf []byte) (*Entry, error) {
	ks := binary.BigEndian.Uint32(buf[0:4])
	vs := binary.BigEndian.Uint32(buf[4:8])
	bs := binary.BigEndian.Uint32(buf[8:12])
	mark := binary.BigEndian.Uint16(buf[12:14])
	return &Entry{KeySize: ks, ValueSize: vs, BucketSize: bs, Mark: mark}, nil
}
