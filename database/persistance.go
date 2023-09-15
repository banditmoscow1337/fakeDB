package database

import (
	"compress/gzip"
	"io"
	"os"
	"strings"
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

	fi, err := os.OpenFile(storages[id].Name+".db", os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0660)
	if err != nil {
		return err
	}

	storLock[id].Unlock()

	w := gzip.NewWriter(fi)
	w.Write(data)
	w.Close()

	return nil
}

func (c *Connection) Snapshot() error {
	return toFile(c.id)
}

func LoadDB(name string, storageType interface{}) (*Connection, error) {
	file, err := os.Open(name)
	if err != nil {
		return nil, err
	}

	var s Storage

	r, err := gzip.NewReader(file)
	if err != nil {
		return nil, err
	}

	data, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}
	r.Close()

	err = json.Unmarshal(data, &s)

	if err != nil {
		return nil, err
	}

	realname := name[:strings.Index(name, ".db")]
	realname = realname[strings.LastIndex(realname, "/")+1:]

	mainLock.Lock()
	storages = append(storages, s)
	storList[realname] = len(storages) - 1

	c := Connection{len(storages) - 1, realname}
	mainLock.Unlock()

	return &c, nil
}
