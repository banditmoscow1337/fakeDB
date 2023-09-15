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

func New(name string, storagetype string, writes int64, time int64) (c Connection, err error) {
	if _, ok := storList[name]; ok {
		err = errors.New(name + " storage already exists")
		return
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

	c = Connection{len(storages) - 1, name}
	mainLock.Unlock()

	return
}

func (c Connection) Get(key string) interface{} {
	if cp, ok := storages[c.id].DB[key]; ok {
		switch cp.Data.(type) { //bad fix after json bullshittery
		case float64:
			switch storages[c.id].StorageType {
			case "int":
				return int(cp.Data.(float64))
				break
			case "int16":
				return int16(cp.Data.(float64))
				break
			case "int32":
				return int32(cp.Data.(float64))
				break
			case "int64":
				return int64(cp.Data.(float64))
				break
			}
		}

		return cp.Data
	}
	return nil
}

func (c Connection) Set(key string, val interface{}) {
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

func (c Connection) Delete(key string) {
	storLock[c.id].Lock()
	delete(storages[c.id].DB, key)
	storLock[c.id].Unlock()
}

func (c Connection) Reset() {
	storLock[c.id].Lock()
	storages[c.id].DB = make(map[string]Capsule)
	storages[c.id].Persistent.lastSave = time.Now().Unix()
	storLock[c.id].Unlock()
}

func Conn(name string) (c Connection, err error) {
	if id, ok := storList[name]; ok {
		c = Connection{id, name}
		return
	}

	err = errors.New(name + "storage not found")

	return
}
