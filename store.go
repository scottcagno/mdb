// --------
// store.go ::: in memory store
// --------
// Copyright (c) 2013-Present, Scott Cagno. All rights reserved.
// This source code is governed by a BSD-style license.

package mdb

import (
	"sync"
	"time"
)

// store
type store struct {
	Id      string
	Ts, Exp int64
	Items   map[string]interface{}
	mu      sync.Mutex
}

// return new store
func NewStore(id string) *store {
	return &store{
		Id:    id,
		Ts:    time.Now().Unix(),
		Items: make(map[string]interface{}),
	}
}

// set expire rate
func (self *store) Expire(n int64) *store {
	self.mu.Lock()
	self.Exp = n
	self.Ts = time.Now().Unix()
	self.Items["exp"] = n
	self.mu.Unlock()
	return self
}

// check for existence of key
func (self *store) HasKey(k string) bool {
	_, ok := self.Items[k]
	return ok
}

// add or modify item in store
func (self *store) Set(k string, v interface{}) {
	self.mu.Lock()
	self.Items[k] = v
	if self.Exp > 0 {
		self.Ts = time.Now().Unix()
	}
	self.mu.Unlock()
}

// get item in store
func (self *store) Get(k string) interface{} {
	self.mu.Lock()
	if v, ok := self.Items[k]; ok {
		if self.Exp > 0 {
			self.Ts = time.Now().Unix()
		}
		self.mu.Unlock()
		return v
	}
	self.mu.Unlock()
	return nil
}

// delete item in store
func (self *store) Del(k string) {
	self.mu.Lock()
	delete(self.Items, k)
	if self.Exp > 0 {
		self.Ts = time.Now().Unix()
	}
	self.mu.Unlock()
}

// return expired bool
func (self *store) expired() bool {
	if self.Exp > 0 {
		return (self.Ts + self.Exp) <= time.Now().Unix()
	}
	return false
}
