// * 
// * Copyright 2013, Scott Cagno. All rights Reserved
// * License: sites.google.com/site/bsdc3license
// * 
// * -------
// * data.go ::: in memory database
// * -------
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
func InitDB() *DataBase {
	self := &DataBase{
		Stores: 	make(map[string]*Store),
		Marked: 	make(map[string]int64),
	}
	go self.RunGC(DB_GC_RATE)	// default 1 second
	return self
}

// run garbage collector
func (self *DataBase) RunGC(rate int64) {
	if len(self.Marked) > 0 {
		self.GC()
	}
	time.AfterFunc(time.Duration(rate)*time.Second, func() { self.RunGC(rate) })
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

// get registered store or return a new one
func (self *DataBase) ReturnStore(id string, rate int64) *Store {
	self.mu.Lock()
	if st, ok := self.Stores[id]; ok {
		self.mu.Unlock()
		return st
	}
	self.Stores[id] = InitStore()
	self.mu.Unlock()
	return self.Stores[id]
}

// check to see if store exists in Stores
func (self *DataBase) HasStore(id string) int {
	self.mu.Lock()
	_, ok := self.Stores[id]
	self.mu.Unlock()
	return Btoi(ok)
}

// create a new store if it doesn't exist
func (self *DataBase) AddStore(id string) int {
	self.mu.Lock()
	_, ok := self.Stores[id]
	if ok {
		self.mu.Unlock()
		return Btoi(!ok)
	}
	self.Stores[id] = InitStore()
	self.mu.Unlock()
	return Btoi(!ok)
}

// get a stores keys if it exsts
func (self *DataBase) GetStore(id string) []string {
	self.mu.Lock()
	if st, ok := self.Stores[id]; ok {
		var ss []string
		for k, _ := range st.Items {
			ss = append(ss, k)
		}
		self.mu.Unlock()
		return ss
	}
	self.mu.Unlock()
	return nil
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