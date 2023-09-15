package database

import (
	"reflect"
	"sync"
)

type Storage struct {
	Name         string
	DB           map[string]Capsule
	StorageType  string
	strorageType reflect.Type
	Persistent   Persistent
	SearchMap    map[string]SearchMapper

	searchArrays      [][]SearchBlock
	searchWritesCount int64
}

type Persistent struct {
	Period     int64
	Writes     int64
	writeCount int64
	lastSave   int64
}

type SearchBlock struct {
	Capsule string
	Value   interface{}
}

type SearchMapper struct {
	Pointer    int
	Partial    bool
	RecalcTime int64
}

type Capsule struct {
	Data      interface{}
	Timestamp int64
}

var (
	mainLock sync.RWMutex
	storLock = []sync.Mutex{}
	storList = make(map[string]int)
	storages = []Storage{}
)

type Connection struct {
	id   int
	Name string
}
