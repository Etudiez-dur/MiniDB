package minidb

import (
	"math/rand"
	"strconv"
	"testing"
	"time"
)

func TestOpen(t *testing.T) {

	db, err := Open("database")
	if err != nil {
		t.Error(err)
	}
	bucket, err := db.Bucket("test1")
	if err != nil {
		t.Error(err)
	}
	t.Log(bucket)
}

func TestMiniBitcask_Put(t *testing.T) {

	db, err := Open("database")
	if err != nil {
		t.Error(err)
	}
	bucket, err := db.Bucket("test1")
	if err != nil {
		t.Error(err)
	}

	rand.Seed(time.Now().UnixNano())
	keyPrefix := "test_key_"
	valPrefix := "test_val_"
	for i := 0; i < 10000; i++ {
		key := []byte(keyPrefix + strconv.Itoa(i%5))
		val := []byte(valPrefix + strconv.FormatInt(rand.Int63(), 10))
		err = bucket.Put(key, val)
	}

	if err != nil {
		t.Log(err)
	}
}

func TestMiniBitcask_Get(t *testing.T) {

	db, err := Open("database")
	if err != nil {
		t.Error(err)
	}
	bucket, err := db.Bucket("test1")
	if err != nil {
		t.Error(err)
	}

	getVal := func(key []byte) {
		val, err := bucket.Get(key)
		if err != nil {
			t.Error("read val err: ", err)
		} else {
			t.Logf("key = %s, val = %s\n", string(key), string(val))
		}
	}

	getVal([]byte("test_key_0"))
	getVal([]byte("test_key_1"))
	getVal([]byte("test_key_2"))
	getVal([]byte("test_key_3"))
	getVal([]byte("test_key_4"))

	_, err = bucket.Get([]byte("test_key_5"))
	if err == nil {
		t.Error("expected test_Key_5 does not exist")
	}
}

func TestMiniBitcask_Del(t *testing.T) {

	db, err := Open("database")
	if err != nil {
		t.Error(err)
	}
	bucket, err := db.Bucket("test1")
	if err != nil {
		t.Error(err)
	}

	key := []byte("test_key_1")
	err = bucket.Del(key)

	if err != nil {
		t.Error("del err: ", err)
	}
}

func TestMiniBitcask_Merge(t *testing.T) {

	db, err := Open("database")
	if err != nil {
		t.Error(err)
	}
	bucket, err := db.Bucket("test1")
	if err != nil {
		t.Error(err)
	}
	err = bucket.Merge()
	if err != nil {
		t.Error("merge err: ", err)
	}
}
