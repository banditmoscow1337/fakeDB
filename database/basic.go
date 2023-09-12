package database

import (
	"errors"
	"sync"
	"time"
)

func Init() {
	go checkSnapshots()
	go recalcSearches()
}

func checkSnapshots() {
	for {
		current := time.Now().Unix()

		for i, s := range storages {
			if s.Persistent.Writes == 0 && s.Persistent.Period == 0 {
				continue
			}

			if (current-(s.Persistent.lastSave+s.Persistent.Period) > 0) && (s.Persistent.Writes-s.Persistent.writeCount < 0) {
				go toFile(i)
			}
		}

		time.Sleep(time.Second * 1)
	}
}

func New(name string, storagetype interface{}, writes int64, time int64) (*Connection, error) {
	if _, ok := storList[name]; ok {
		return nil, errors.New(name + " storage already exists")
	}

	s := Storage{
		Name:        name,
		StorageType: storagetype,
		DB:          make(map[string]Capsule),
		SearchMap:   make(map[string]SearchMapper),
	}

	s.Persistent.Period = time
	s.Persistent.Writes = writes

	mainLock.Lock()

	storages = append(storages, s)
	storLock = append(storLock, sync.Mutex{})

	storList[name] = len(storages) - 1
	mainLock.Unlock()

	c := &Connection{len(storages) - 1, name}

	return c, nil
}

func (c *Connection) Get(key string) interface{} {
	if cp, ok := storages[c.id].DB[key]; ok {
		return cp.Data
	}
	return nil
}

func (c *Connection) Set(key string, val interface{}) {
	storLock[c.id].Lock()

	capsule, ok := storages[c.id].DB[key]
	if !ok {
		capsule.Timestamp = time.Now().Unix()
	}

	capsule.Data = val
	storages[c.id].DB[key] = capsule
	storages[c.id].Persistent.writeCount++
	storages[c.id].searchWritesCount++

	storLock[c.id].Unlock()
}

func (c *Connection) Delete(key string) {
	storLock[c.id].Lock()
	delete(storages[c.id].DB, key)
	storLock[c.id].Unlock()
}

func (c *Connection) Reset() {
	storLock[c.id].Lock()
	storages[c.id].DB = make(map[string]Capsule)
	storages[c.id].Persistent.lastSave = time.Now().Unix()
	storLock[c.id].Unlock()
}

func Conn(name string) (*Connection, error) {
	if id, ok := storList[name]; ok {
		return &Connection{id, name}, nil
	}

	return nil, errors.New(name + "storage not found")
}
