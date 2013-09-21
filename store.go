// * 
// * Copyright 2013, Scott Cagno. All rights Reserved
// * License: sites.google.com/site/bsdc3license
// * 
// * --------
// * store.go ::: stores for main database
// * --------
// * 

package mdb

import (
	"runtime"
	"sync"
	"time"
)

// store
type Store struct {
	PK 			int64
	Items   	map[string][]string
	Marked		map[string]int64
	mu 			sync.Mutex
}

// return new store
func InitStore() *Store {
	self := &Store{
		PK:			0,
		Items: 		make(map[string][]string),
		Marked:		make(map[string]int64),
	}
	go self.RunGC(1)
	return self
}

// run garbage collector
func (self *Store) RunGC(rate int64) {
	if len(self.Marked) > 0 {
		count := len(self.Marked)
		self.GC()
		if len(self.Marked) < (count/4) && count >= 1000 {
			runtime.GC()
		}
	}
	time.AfterFunc(time.Duration(rate)*time.Second, func(){ self.RunGC(rate) })
}

//garbage collector
func (self *Store) GC() {
	self.mu.Lock()
	for k, ttl := range self.Marked {
		if ttl <= time.Now().Unix() {
			delete(self.Items, k)
			delete(self.Marked, k)
		}
	}
	self.mu.Unlock()
}

// check for existence of key
func (self *Store) Has(k string) int {
	self.mu.Lock()
	_, ok := self.Items[k]
	self.mu.Unlock()
	return Btoi(ok)
}

// add to an items values (safely)
func (self *Store) Add(k string, v ...string) int {
 	self.mu.Lock()
 	if _, ok := self.Items[k]; ok {
 		for _, val := range v {
 			self.Items[k] = append(self.Items[k], val)
 		}
 		self.mu.Unlock()
 		return Btoi(ok)
 	} 
 	self.Items[k] = v
 	self.mu.Unlock()
 	return Btoi(true)
} 

// get an items values from store
func (self *Store) Get(k string) []string {
	self.mu.Lock()
	if v, ok := self.Items[k]; ok {
		self.mu.Unlock()
		return v
	}
	self.mu.Unlock()
	return nil
}

// delete item in store
func (self *Store) Del(k string) int {
	self.mu.Lock()
	delete(self.Items, k)
	_, ok := self.Items[k]
	self.mu.Unlock()
	return Btoi(!ok)
}

// set expire rate for item
func (self *Store) Exp(k string, ttl int64) int {
	self.mu.Lock()
	self.Marked[k] = time.Now().Unix() + ttl
	_, ok := self.Marked[k]
	self.mu.Unlock()
	return Btoi(ok)
}

// check ttl for item
func (self *Store) TTL(k string) int64 {
	self.mu.Lock()
	if ttl, ok := self.Marked[k]; ok {
		self.mu.Unlock()
		return ttl - time.Now().Unix()
	}
	self.mu.Unlock()
	return 0
}

// add or modify item in store (considered to be an unsafe update, overwrites item)
func (self *Store) Set(k string, v ...string) int {
	self.mu.Lock()
	self.Items[k] = v
	_, ok := self.Items[k]
	self.mu.Unlock()
	return Btoi(ok)
}

// check for existence of value
func (self *Store) Find(s string) []string {
	self.mu.Lock()
	var ss []string
	for k, v := range self.Items {
		for _, n := range v {
			if n == s {
				ss = append(ss, k)
			}
		}
	}
	self.mu.Unlock()
	return ss
}