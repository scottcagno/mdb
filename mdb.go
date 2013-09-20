// * 
// * Copyright 2013, Scott Cagno. All rights Reserved
// * License: sites.google.com/site/bsdc3license
// * 
// * ------
// * mdb.go ::: in memory database
// * ------
// * 

package mdb

import (
	"runtime"
	"sync"
	"time"
)

// store
type DataBase struct {
	Stores		map[string]*Store
	Marked		map[string]int64
	mu 			sync.Mutex
}

// return store instance
func InitDB(rate int64) *DataBase {
	self := &DataBase{
		Stores: 	make(map[string]*Store),
		Marked: 	make(map[string]int64),
	}
	if rate > 0 {
		go self.RunGC(rate)
	}
	return self
}

// run garbage collector
func (self *DataBase) RunGC(n int64) {
	if len(self.Marked) > 0 {
		self.GC()
	}
	time.AfterFunc(time.Duration(n)*time.Second, func() { self.RunGC(n) })
}

// garbage collector
func (self *DataBase) GC() {
	self.mu.Lock()
	for k, ttl := range self.Marked {
		if ttl <= time.Now().Unix() {
			self.Stores[k].Items = nil
			delete(self.Stores, k)
			delete(self.Marked, k)
		}
	}
	self.mu.Unlock()
}

// check to see if store exists in Stores
func (self *DataBase) HasStore(id string) int {
	self.mu.Lock()
	_, ok := self.Stores[id]
	self.mu.Unlock()
	return Btoi(ok)
}

// get registered store or return a new one
func (self *DataBase) GetStore(id string, rate int64) *Store {
	self.mu.Lock()
	if st, ok := self.Stores[id]; ok {
		self.mu.Unlock()
		return st
	}
	self.Stores[id] = InitStore(rate)
	self.mu.Unlock()
	return self.Stores[id]
}

// remove store
func (self *DataBase) DelStore(id string) int {
	self.mu.Lock()
	st, ok := self.Stores[id] 
	if ok {
		st.Items = nil
		delete(self.Stores, id)
	}
	self.mu.Unlock()
	runtime.GC()
	return Btoi(ok)
}

// expire a store
func (self *DataBase) ExpStore(id string, ttl int64) int {
	self.mu.Lock()
	self.Marked[id] = time.Now().Unix() + ttl
	_, ok := self.Marked[id]
	self.mu.Unlock()
	return Btoi(ok)
}

// check ttl
func (self *DataBase) TTLStore(id string) int64 {
	self.mu.Lock()
	if ttl, ok := self.Marked[id]; ok {
		self.mu.Unlock()
		return ttl - time.Now().Unix()
	}
	self.mu.Unlock()
	return 0
}