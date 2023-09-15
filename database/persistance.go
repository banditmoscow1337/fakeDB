package database

import (
	"compress/gzip"
	"io"
	"os"
	"sync"
	"time"

	"github.com/goccy/go-json"
)

func toFile(id int) error {
	storLock[id].Lock()

	data, err := json.Marshal(storages[id])
	if err != nil {
		return err
	}

	storages[id].Persistent.lastSave = time.Now().Unix()
	storages[id].Persistent.writeCount = 0

	fi, err := os.OpenFile(storages[id].Name+".db", os.O_WRONLY|os.O_CREATE, 0660)
	if err != nil {
		return err
	}

	storLock[id].Unlock()

	w := gzip.NewWriter(fi)
	w.Write(data)
	w.Close()

	return nil
}

func (c Connection) Snapshot() error {
	return toFile(c.id)
}

func LoadDB(name string) (c Connection, err error) {
	file, err := os.Open(name + ".db")
	if err != nil {
		return
	}

	var s Storage

	r, err := gzip.NewReader(file)
	if err != nil {
		return
	}

	data, err := io.ReadAll(r)
	if err != nil {
		return
	}
	r.Close()

	err = json.Unmarshal(data, &s)

	if err != nil {
		return
	}

	mainLock.Lock()
	storages = append(storages, s)
	storLock = append(storLock, sync.Mutex{})
	storList[name] = len(storages) - 1

	c = Connection{len(storages) - 1, name}

	mainLock.Unlock()

	return
}
