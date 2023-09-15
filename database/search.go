package database

import (
	"errors"
	"reflect"
	"strings"
	"time"
)

func (c *Connection) ExactMatch(field string, value interface{}) (keys []string, err error) {
	search, ok := storages[c.id].SearchMap[field]
	if !ok {
		err = errors.New("search not exist")
		return
	}

	for _, v := range storages[c.id].searchArrays[search.Pointer] {
		if v.Value == value {
			keys = append(keys, v.Capsule)
		}
	}

	if len(keys) == 0 {
		err = errors.New("not found")
	}

	return
}

func (c *Connection) PartialMatch(field, value string, reverse bool) (keys []string, err error) {
	search, ok := storages[c.id].SearchMap[field]
	if !ok {
		err = errors.New("search not exist")
		return
	}

	if !search.Partial {
		err = errors.New("partial match not supported for this field")
		return
	}

	for _, v := range storages[c.id].searchArrays[search.Pointer] { //Why only string, moron?
		if reverse {
			if strings.Contains(value, v.Value.(string)) {
				keys = append(keys, v.Capsule)
			}
		} else {
			if strings.Contains(v.Value.(string), value) {
				keys = append(keys, v.Capsule)
			}
		}
	}

	if len(keys) == 0 {
		err = errors.New("not found")
	}

	return
}

func makeSearch(id int, field string) (sa []SearchBlock, err error) {
	var checked bool

	for k, v := range storages[id].DB {
		var sb SearchBlock
		sb.Capsule = k

		if field == "" {
			sb.Value = v.Data
		} else {
			r := reflect.ValueOf(v.Data)
			v := reflect.Indirect(r).FieldByName(field)
			if !v.IsValid() {
				err = errors.New("field not exist")
				return
			}
			sb.Value = v.Interface()
		}

		if !checked {
			if reflect.ValueOf(sb.Value).Type().String() == "string" {
				m := storages[id].SearchMap[field]
				m.Partial = true
				storages[id].SearchMap[field] = m
			}

			checked = true
		}

		sa = append(sa, sb)
	}
	return
}

func (c *Connection) MakeSearch(field string, recalc int64) (err error) {
	if _, ok := storages[c.id].SearchMap[field]; ok {
		err = errors.New("search exist")
		return
	}

	var sm SearchMapper

	storLock[c.id].Lock()

	sm.Pointer = len(storages[c.id].searchArrays)
	if recalc > 0 {
		recalc = 50
	}
	sm.RecalcTime = recalc
	storages[c.id].SearchMap[field] = sm

	sa, err := makeSearch(c.id, field)
	if err != nil {
		return
	}

	storages[c.id].searchArrays = append(storages[c.id].searchArrays, sa)

	storLock[c.id].Unlock()
	return
}

func recalcSearches() {
	for {
		for i, s := range storages {
			for mn, m := range s.SearchMap {
				if s.searchWritesCount > m.RecalcTime {
					go makeSearch(i, mn)
				}
			}
		}
		time.Sleep(time.Second * 1)
	}
}
