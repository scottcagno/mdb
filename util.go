// -------
// util.go ::: utilities
// -------
// Copyright (c) 2013-Present, Scott Cagno. All rights reserved.
// This source code is governed by a BSD-style license.

package mdb

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"runtime"
	"strings"
	"time"
	"crypto/md5"
	"crypto/rand"
	"encoding/base64"
)

// write json data to an io.Writer
func (self *Mdb) save(w io.Writer) error {
	enc := json.NewEncoder(w)
	err := enc.Encode(self.Stores)
	return err
}

// read json data from and io.Reader 
func (self *Mdb) load(r io.Reader) error {
	dec := json.NewDecoder(r)
	stores := make(map[string]*store)
	err := dec.Decode(&stores)
	if err == nil {
		for id, st := range stores {
			if _, ok := self.Stores[id]; !ok {
				self.Stores[id] = st
				self.Stores[id].Ts = time.Now().Unix()
			}
		}
	}
	return err
}

// backup data in json format
func (self *Mdb) Save(path, file string) {
	self.mu.Lock()
	fmt.Printf("saving data to snapshot...  ")
	os.MkdirAll(path, 0755)
	if !strings.HasSuffix(path, "/") {
		path = path + "/"
	}
	if !strings.HasSuffix(file, ".json") {
		file = file + ".json"
	}
	f, err := os.Create(path + file)
	if err != nil {
		log.Println(err)
	}
	err = self.save(f)
	if err != nil {
		f.Close()
		log.Println(err)
	}
	f.Close()
	self.mu.Unlock()
	runtime.GC()
	fmt.Println("done.")
}

// load backup into memory
func (self *Mdb) Load(path, file string) {
	self.mu.Lock()
	fmt.Printf("loading data from snapshot...  ")
	os.MkdirAll(path, 0755)
	if !strings.HasSuffix(path, "/") {
		path = path + "/"
	}
	if !strings.HasSuffix(file, ".json") {
		file = file + ".json"
	}
	f, err := os.Open(path + file)
	if err != nil {
		log.Println(err)
	}
	err = self.load(f)
	if err != nil {
		f.Close()
		log.Println(err)
	}
	f.Close()
	self.mu.Unlock()
	runtime.GC()
	fmt.Println("done.")
}

// get specific memory stat
func MemStat(s string) string {
	runtime.GC()
	m := new(runtime.MemStats)
	runtime.ReadMemStats(m)
	var val string
	switch s {
	case "alloc":
		if ((m.HeapSys / 1024) / 1024) > 0 {
			val = fmt.Sprintf("%d mb", ((m.HeapSys / 1024) / 1024))
			break
		}
		val = fmt.Sprintf("%d bytes", m.HeapSys)
		break
	case "inuse":
		if ((m.HeapInuse / 1024) / 1024) > 0 {
			val = fmt.Sprintf("%d mb", ((m.HeapInuse / 1024) / 1024))
			break
		}
		val = fmt.Sprintf("%d bytes", m.HeapInuse)
		break
	default:
		val = "err"
		break
	}
	return val
}


// return random hash (6*10^49)
func Random() string {
	e := make([]byte, 32)
	rand.Read(e)
	seed := make([]byte, base64.URLEncoding.EncodedLen(len(e)))
	base64.URLEncoding.Encode(seed, e)
	h := md5.New()
	i := 3
	for i > 0 {
		io.WriteString(h, string(seed))
		i--
	}
	return fmt.Sprintf("%x", h.Sum(nil))
}