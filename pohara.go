package pohara

import (
	"bytes"
	"encoding/binary"
	"github.com/boltdb/bolt"
	"log"
	"os"
)

type Pohara struct {
	file   *os.File
	index  *bolt.DB
	bucket *bolt.Bucket
	offset int
}

type Entry struct {
	offset int
	length int
}

func Create(fname string) (*Pohara, error) {
	f, err := os.Create(fname)
	if err != nil {
		return nil, err
	}

	pohara := new(Pohara)
	pohara.file = f

	pohara.index, err = bolt.Open(fname+".db", 0600, nil)
	if err != nil {
		return nil, err
	}

	pohara.offset = 0
	return pohara, nil
}

func (sm *Pohara) Add(key []byte, value []byte) error {
	_, err := sm.file.Write(value)
	if err != nil {
		log.Println(err)
		return err
	}

	length := len(value)
	sm.WriteIndex(key, length)
	if err != nil {
		log.Println(err)
		return err
	}
	sm.offset += length
	return nil
}

func (sm *Pohara) WriteIndex(key []byte, length int) error {
	entry := new(Entry)
	entry.offset = sm.offset
	entry.length = length

	var buf bytes.Buffer
	binary.Write(&buf, binary.BigEndian, entry)

	err := sm.bucket.Put(key, []byte(buf.Bytes()))

	return err
}
