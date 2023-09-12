package database

import (
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"
)

func parallelShit(tf func(i int)) {
	var wg sync.WaitGroup
	limitCh := make(chan struct{}, 10)

	for t := 1; t < 11; t++ {
		wg.Add(1)
		limitCh <- struct{}{}

		go func(t int) {
			defer func() {
				wg.Done()
				<-limitCh
			}()

			for i := 0; i < 1000000; i++ {
				tf(t * i)
			}
		}(t)
	}

	wg.Wait()
}

func TestWriteString(t *testing.T) {
	conn, err := New("amogus_str", "", 0, 0)
	if err != nil {
		t.Fatal(err)
	}

	start := time.Now()

	parallelShit(func(i int) { conn.Set(strconv.Itoa(i), "puk") })

	t.Log("10m writes in", time.Since(start).Milliseconds(), "milliseconds")

	start2 := time.Now()

	parallelShit(func(i int) {
		if conn.Get(strconv.Itoa(i)).(string) != "puk" {
			t.Fail()
		}
	})

	t.Log("10m reads in", time.Since(start2).Milliseconds(), "milliseconds")

	start3 := time.Now()

	if err := toFile(conn.id); err != nil {
		t.Fatal(err)
	}

	t.Log("10m snapshotted in", time.Since(start3).Milliseconds(), "milliseconds")
}

type GeeGurda struct {
	Name  string
	Xueim int64
	Piska bool
	siska float64
}

func TestWriteStruct(t *testing.T) {
	conn, err := New("amogus_struct", GeeGurda{}, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	parallelShit(func(i int) { conn.Set(strconv.Itoa(i), GeeGurda{"puks", 228, true, 0.666}) })
	t.Log("10m writes in", time.Since(start).Milliseconds(), "milliseconds")

	start2 := time.Now()
	parallelShit(func(i int) {
		if conn.Get(strconv.Itoa(i)).(GeeGurda).Name != "puks" {
			t.Fail()
		}
	})
	t.Log("10m reads in", time.Since(start2).Milliseconds(), "milliseconds")

	start3 := time.Now()

	if err := toFile(conn.id); err != nil {
		t.Fatal(err)
	}

	t.Log("10m snapshotted in", time.Since(start3).Milliseconds(), "milliseconds")
}

func TestNewDB(t *testing.T) {
	start := time.Now()
	parallelShit(func(i int) {
		_, err := New(strconv.Itoa(i), GeeGurda{}, 0, 0)
		if err != nil {
			t.Fatal(err)
		}
	})
	t.Log("10m new dbs in", time.Since(start).Milliseconds(), "milliseconds")
}

func TestSearch(t *testing.T) {
	Init()
	db, err := New("test", "", 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i < 666; i++ {
		val := "sus"
		if i == 228 {
			val = "amogus"
		}
		db.Set(strconv.Itoa(i), val)
	}

	if err := db.MakeSearch("", 0); err != nil {
		t.Fatal(err)
	}

	time.Sleep(3 * time.Second)

	res, err := db.PartialMatch("", "amo", false)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(res)
}
