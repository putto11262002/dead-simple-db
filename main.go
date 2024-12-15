package main

import (
	"fmt"
	"log"

	"example.com/db/btree"
	"example.com/db/kv"
)

func main() {
	db := kv.DB{}
	if err := db.Open("test.db"); err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	if val, err := db.Get([]byte("foo")); err != nil {
		if err == btree.ErrKeyNotFound {
			fmt.Println("key not found")
		} else if err == btree.ErrEmptyTree {
			fmt.Println("empty tree")

		} else {
			log.Fatal(err)
		}
	} else {
		fmt.Println(string(val))
	}

	if val, err := db.Get([]byte("bar")); err != nil {
		if err == btree.ErrKeyNotFound {
			fmt.Println("key not found")
		} else if err == btree.ErrEmptyTree {
			fmt.Println("empty tree")

		} else {
			log.Fatal(err)
		}
	} else {
		fmt.Println(string(val))
	}

	db.Set([]byte("foo"), []byte("foooozzzzzzz"))

	db.Set([]byte("bar"), []byte("barzzzzzzz"))

	if val, err := db.Get([]byte("foo")); err != nil {
		if err == btree.ErrKeyNotFound {
			fmt.Println("key not found")
		} else if err == btree.ErrEmptyTree {
			fmt.Println("empty tree")

		} else {
			log.Fatal(err)
		}
	} else {
		fmt.Println(string(val))
	}

	if val, err := db.Get([]byte("bar")); err != nil {
		if err == btree.ErrKeyNotFound {
			fmt.Println("key not found")
		} else if err == btree.ErrEmptyTree {
			fmt.Println("empty tree")

		} else {
			log.Fatal(err)
		}
	} else {
		fmt.Println(string(val))
	}

}
