// ------
// Mdb.go ::: in memory db
// ------
// Copyright (c) 2013-Present, Scott Cagno. All rights reserved.
// This source code is governed by a BSD-style license.

package mdb

import (
	"fmt"
	"runtime"
	"sync"
	"time"
)

// store
type Mdb struct {
	Stores map[string]*store
	Rate   int64
	mu     sync.Mutex
}

// return store instance
func MemDb(n int64) *Mdb {
	m := &Mdb{Stores: make(map[string]*store), Rate: n}
	if n > 0 {
		go m.rungc(m.Rate)
	}
	return m
}

// check to see if store exists in Stores
func (self *Mdb) HasStore(id string) bool {
	_, ok := self.Stores[id]
	return ok
}

// remove store
func (self *Mdb) DelStore(id string) {
	self.mu.Lock()
	if st, ok := self.Stores[id]; ok {
		st.Items = nil
		delete(self.Stores, id)
	}
	self.mu.Unlock()
	runtime.GC()
}

// get registered store or return a new one
func (self *Mdb) GetStore(id string) *store {
	self.mu.Lock()
	if st, ok := self.Stores[id]; ok {
		st.Ts = time.Now().Unix()
		self.mu.Unlock()
		return st
	}
	self.Stores[id] = NewStore(id)
	self.mu.Unlock()
	return self.Stores[id]
}

// run garbage collector
func (self *Mdb) rungc(n int64) {
	if len(self.Stores) > 0 {
		self.gc()
	}
	fmt.Println(len(self.Stores), "stores in memory")
	time.AfterFunc(time.Duration(n)*time.Second, func() { self.rungc(n) })
}

// garbage collector
func (self *Mdb) gc() {
	self.mu.Lock()
	for id, st := range self.Stores {
		if st.expired() {
			st.Items = nil
			delete(self.Stores, id)
		}
	}
	self.mu.Unlock()
}
