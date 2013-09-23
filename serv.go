// * 
// * Copyright 2013, Scott Cagno. All rights Reserved
// * License: sites.google.com/site/bsdc3license
// * 
// * -------
// * serv.go ::: socket server for data store
// * -------
// * 

package mdb

import (
	"strings"
	"bufio"
	"bytes"
	"time"
	"fmt"
	"log"
	"net"
	"io"
)

// socket server
type Server struct {
	db 		*DataBase
	count 	int
}

// initialize server
func InitServer() *Server {
	return &Server {
		db: InitDB(),
	}
}

func (self *Server) Run() {
	addr, err := net.ResolveTCPAddr("tcp", SERVER_LISTEN)
	if err != nil {
		log.Panicln(err)
	}
	l, err := net.ListenTCP("tcp", addr)
	if err != nil {
		log.Panicln(err)
	}
	fmt.Println("MDB v1.0 alpha", "\n==============")
	log.Printf("Listening on %q...", SERVER_LISTEN)
	for {
		c, err := l.AcceptTCP()
		if err != nil {
			log.Panicln(err)
		}
		self.count++
		log.Printf("ACCEPTED CLIENT REQUEST %q\n", c.RemoteAddr())
		log.Printf("(TOTAL: %d)\n", self.count)
		go self.connHandler(c)
	}
}

// string wrapper for c.Write
func send(c *net.TCPConn, s string, a ...interface{}) {
	c.Write([]byte(fmt.Sprintf(s + "\r\n", a...)))
}

// parse incoming commands
func parse(b []byte) (string, string, []string) {
	raw := string(bytes.ToLower(bytes.TrimRight(b, "\r\n")))
	if raw != "" {
		arg := strings.Split(raw, " ")
		if len(arg) == 1 {
			return arg[0], "", nil
		}
		if len(arg) == 2 {
			return arg[0], arg[1], nil
		}
		return arg[0], arg[1], arg[2:]
	}
	return "", "", nil
}

// handle connection
func (self *Server) connHandler(c *net.TCPConn) {
	// read incoming bytes
	r := bufio.NewReader(c)
	self.extendConnTTL(c, CLIENT_TIMEOUT)
	for {
		b, err := r.ReadBytes('\n')
		if err == io.EOF {
			break
		} else if err != nil {
			self.closeConn(c)
			return
		} else {
			self.extendConnTTL(c, CLIENT_TIMEOUT)
		}
		// parse incoming bytes, return command, store, and args (if able) 
		cmd, store, args := parse(b)
		switch cmd {
		case "":
			// send error
			send(c, "recvd no data")
		case "rand":
			// send random hash
			send(c, "%v", Random())
		case "help":
			// send help
			send(c, "-----\ncommands\n-----\nrand, help, exit, save, load, pk, has, add, get, del, exp, ttl, set, find")
		case "exit":
			// disconnect a client
			send(c, "Goodbye!")
			c.SetDeadline(time.Now())
		case "save":
			// save a snapshot or archive to disk
			self.db.Save(ARCHIVE_PATH, "archive.json")
			send(c, "saved %v/archive.json", ARCHIVE_PATH)
		case "load":
			// load a snapshot or archive from disk
			self.db.Load(ARCHIVE_PATH, "archive.json")
			send(c, "loaded %v/archive.json", ARCHIVE_PATH)
		case "pk":
			// increment return stores pk, and return
			if args == nil && self.db.HasStore(store) == 1 {
				self.db.Stores[store].PK++
				v := self.db.Stores[store].PK
				send(c, "%v", v)
				break
			}
			if args != nil && self.db.HasStore(store) == 1 {
				if args[0] == "reset" {
					self.db.Stores[store].PK = 0
					send(c, "%v's pk reset", store)
					break	
				}
				send(c, "pk err")
			}
			send(c, "pk err")
		case "has":
			// check to see if a store exists
			if store != "" && args == nil {
				v := self.db.HasStore(store)
				send(c, "%v", v)
				break
			}
			// check to see if a key in a store exists
			if args != nil && self.db.HasStore(store) == 1 {
				v := self.db.Stores[store].Has(args[0])
				send(c, "%v", v)
				break
			}
			send(c, "has err")
		case "add":
			// add a store safely (will not overwrite existing values)
			if store != "" && args == nil {
				v := self.db.AddStore(store)
				send(c, "%v", v)
				break
			}
			// add and items values safely (will not overwrite existing values)
			if args != nil && self.db.HasStore(store) == 1 {
				v := self.db.Stores[store].Add(args[0], args[1:]...)
				send(c, "%v", v)
				break
			}
			send(c, "add err")
		case "get": 
			// get store keys
			if store != "" && args == nil {
				v := self.db.GetStore(store)
				send(c, "%v", v)
				break
			}
			// get items keys in store
			if args != nil && self.db.HasStore(store) == 1 {
				v := self.db.Stores[store].Get(args[0])
				send(c, "%v", v)
				break
			}
			send(c, "get err")
		case "del":
			// delete a store
			if store != "" && args == nil {
				v := self.db.DelStore(store)
				send(c, "%v", v)
				break
			}
			// delete item in a store
			if args != nil && self.db.HasStore(store) == 1 {
				v := self.db.Stores[store].Del(args[0])
				send(c, "%v", v)
				break
			}
			send(c, "del err")
		case "exp":
			// expire a store (set in seconds)
			if store != "" && len(args) == 1 {
				v := self.db.ExpStore(store, Atoi(args[0]))
				send(c, "%v", v)
				break
			}
			// expire item in store (set in seconds)
			if args != nil && self.db.HasStore(store) == 1 {
				v := self.db.Stores[store].Exp(args[0], Atoi(args[1]))
				send(c, "%v", v)
				break
			}
			send(c, "exp err")
		case "ttl":
			// check the time to live on a store that has been set to expire
			if store != "" && args == nil { 
				v := self.db.TTLStore(store)
				send(c, "%v", v)
				break
			}
			// check the time to live on a key that has been set to expire
			if args != nil && self.db.HasStore(store) == 1 {
				v := self.db.Stores[store].TTL(args[0])
				send(c, "%v", v)
				break
			}
			send(c, "ttl err")
		case "set":
			// update an items values (will overwrite an item, considered not safe)
			if args != nil && self.db.HasStore(store) == 1 {
				v := self.db.Stores[store].Set(args[0], args[1:]...)
				send(c, "%v", v)
				break
			}
			send(c, "set err")
		case "find":
			// search a store for a value, returns found items key(s)
			if args != nil && self.db.HasStore(store) == 1 {
				v := self.db.Stores[store].Find(args[0])
				send(c, "%v", v)
				break
			}
			send(c, "find err")
		default:
			// send error
			send(c, "err")
		}
	}
}

// close connection
func (self *Server) closeConn(c *net.TCPConn) {
	c.Write([]byte("Goodbye\r\n"))
	log.Printf("CLOSED CONNECTION TO CLIENT [%s]\n", c.RemoteAddr().String())
	c.Close()
	c = nil
}

// extend connection ttl
func (self *Server) extendConnTTL(c *net.TCPConn, ttl float64) {
	if ttl > 0 {
		c.SetDeadline(time.Now().Add(time.Duration(ttl) * time.Second))
	}
}
