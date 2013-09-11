// --------
// store.go ::: in memory store
// --------
// Copyright (c) 2013-Present, Scott Cagno. All rights reserved.
// This source code is governed by a BSD-style license.

package main

import (
	"fmt"
	"mdb"
	"time"
)

const COUNT = 100

func main() {

	db := mdb.MemDb(1)
	st := db.GetStore("order")
	st.Expire(10)

	fmt.Printf("Creating %d records, each containing random data\n", COUNT)
	for i := 0; i < COUNT; i++ {
		st.Set(string(i), mdb.Random())
	}

	y, m, d := time.Now().Date()
	fmt.Printf("Saving snapshot 'order_%v_%v_%v' to backups\n", m, d, y)
	db.Save("backups/", fmt.Sprintf("order_%v_%v_%v", m, d, y))
	
	time.Sleep(time.Duration(30) * time.Second)
	fmt.Printf("Loading snapshot 'order_%v_%v_%v' from backups\n", m, d, y)
	db.Load("backups/", fmt.Sprintf("order_%v_%v_%v", m, d, y))

	// wait
	func() { fmt.Println("any key to continue..."); var n int; fmt.Scanln(&n) }()
}
