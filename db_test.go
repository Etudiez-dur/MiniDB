package minidb

import (
	"math/rand"
	"strconv"
	"testing"
	"time"
)

var db *DB

func init() {
	db, _ = Open("./database")
}

func BenchmarkMiniBitcask_Put(b *testing.B) {

	rand.Seed(time.Now().UnixNano())
	keyPrefix := "test_key_"
	valPrefix := "test_val_"
	for n := 0; n < b.N; n++ {
		key := []byte(keyPrefix + strconv.Itoa(n))
		val := []byte(valPrefix + strconv.FormatInt(rand.Int63(), 10))
		db.Bucket("test").Put(key, val)
	}
}

func BenchmarkMiniBitcask_Get(b *testing.B) {

	for n := 0; n < b.N; n++ {
		db.Bucket("test").Get([]byte("test_key_" + strconv.Itoa(n)))
	}
}

func TestMiniBitcask_Del(t *testing.T) {

	db, err := Open("database")
	if err != nil {
		t.Error(err)
	}

	key := []byte("test_key_1")
	err = db.Bucket("test").Del(key)

	if err != nil {
		t.Error("del err: ", err)
	}
}

func TestMiniBitcask_Merge(t *testing.T) {

	db, err := Open("database")
	if err != nil {
		t.Error(err)
	}
	err = db.Merge()
	if err != nil {
		t.Error("merge err: ", err)
	}
}
