package main

import (
	"fmt"
	deadsimpledb "github/putto11262002/dead_simple_go_db"
	"log"
)

func main() {
	db := deadsimpledb.NewDB("test.db")
	defer db.Close()
	if err := db.Open(); err != nil {
		log.Fatalf("db.Open: %v", err)
	}
	db.Set([]byte("put"), []byte("thai"))
	value, ok := db.Get([]byte("put"))
	fmt.Printf("ok: %v, value: %s\n", ok, value)
	db.Set([]byte("tham"), []byte("English"))
	value, ok = db.Get([]byte("tham"))
	fmt.Printf("ok: %v, value: %s\n", ok, value)
	db.Del([]byte("put"))
	value, ok = db.Get([]byte("put"))
	fmt.Printf("ok: %v, value: %s\n", ok, value)

}
