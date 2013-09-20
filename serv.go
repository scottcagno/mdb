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
	"strconv"
	"strings"
	"bufio"
	"bytes"
	"time"
	"fmt"
	"log"
	"net"
	"io"
)

var GREETING = []string{
	`      ___           ___           ___     `,
	`     /\__\         /\  \         /\  \    `,
	`    /::|  |       /::\  \       /::\  \   `,
	`   /:|:|  |      /:/\:\  \     /:/\:\  \  `,
	`  /:/|:|__|__   /:/  \:\__\   /::\~\:\__\ `,
	` /:/ |::::\__\ /:/__/ \:|__| /:/\:\ \:|__|`,
	` \/__/~~/:/  / \:\  \ /:/  / \:\~\:\/:/  /`,
	`       /:/  /   \:\  /:/  /   \:\ \::/  / `,
	`      /:/  /     \:\/:/  /     \:\/:/  /  `,
	`     /:/  /       \::/  /       \::/  /   `,
	`     \/__/         \/__/         \/__/    `,
	"\n",
}

var HELP = []string{
		"data syntax",
		"-----------",
		"has <store>",
		"exp <store> <ttl>",
		"del <store>",
		"ttl <store>",
		"\n",
		"item syntax",
		"-----------",
		"set <store> <key> <val>...",
		"app <store> <key> <val>...",
		"get <store> <key>",
		"del <store> <key>",
		"has <store> <key>",
		"exp <store> <key>",
		"ttl <store> <key>",
		"\n",
		"general cmd",
		"-----------",
		"db help",
		"db exit",
		"save <snapshot>",
		"load <snapshot>",
}

// socket server
type Server struct {
	DB 		*DataBase
	count 	int
}

// initialize server
func InitDBServer() *Server {
	return &Server {
		DB: InitDB(DB_GC),
	}
}

func (self *Server) Serve() {
	addr, err := net.ResolveTCPAddr("tcp", HOST)
	if err != nil {
		log.Panicln(err)
	}
	l, err := net.ListenTCP("tcp", addr)
	if err != nil {
		log.Panicln(err)
	}
	fmt.Println(strings.Join(GREETING, "\n"))
	log.Printf("Listening on %q...", HOST)
	for {
		c, err := l.AcceptTCP()
		if err != nil {
			log.Panicln(err)
		}
		self.count++
		log.Printf("ACCEPTED CLIENT REQUEST %q\n", c.RemoteAddr())
		log.Printf("(TOTAL: %d)\n", self.count)
		go self.HandleConnection(c)
	}
}

// string wrapper for c.Write
func send(c *net.TCPConn, s string, a ...interface{}) {
	c.Write([]byte(fmt.Sprintf(s + "\r\n", a...)))
}

// parse incoming commands
func parse_args(b []byte) (string, []string) {
	raw := string(bytes.ToLower(bytes.TrimRight(b, "\r\n")))
	arg := strings.Split(raw, " ")
	if len(arg) >= 2 {
		return arg[0], arg[1:]
	}
	return "", nil
}

// handle connection
func (self *Server) HandleConnection(c *net.TCPConn) {
	r := bufio.NewReader(c)
	self.ExtendClientTTL(c, CLIENT_TIMEOUT)
	for {
		b, err := r.ReadBytes('\n')
		if err == io.EOF {
			break
		} else if err != nil {
			self.CloseConnection(c)
			return
		} else {
			self.ExtendClientTTL(c, CLIENT_TIMEOUT)
		}
		cmd, args := parse_args(b)
		if args == nil {
			send(c, "0")
			break
		}
		switch cmd {
		case "set":
			if len(args) >= 3 {
				ok := self.DB.GetStore(args[0], ST_GC).Set(args[1], args[2:]...)
				send(c, "%v", ok)
				break
			}
			send(c, "0")
			break
		case "app":
			if len(args) >= 3 {
				ok := self.DB.GetStore(args[0], ST_GC).App(args[1], args[2:]...)
				send(c, "%v", ok)
				break
			}
			send(c, "0")
			break
		case "get":
			if len(args) == 2 {
				v := self.DB.GetStore(args[0], ST_GC).Get(args[1])
				send(c, "%v", v)
				break
			}
			send(c, "0") 
			break
		case "del":
			if len(args) == 1 {
				ok := self.DB.DelStore(args[0])
				send(c, "%v", ok)
				break
			}
			if len(args) == 2 {
				ok := self.DB.GetStore(args[0], ST_GC).Del(args[1])
				send(c, "%v", ok)
				break
			}
			send(c, "0")
			break
		case "exp":
			if len(args) == 2 {
				ttl, err := strconv.ParseInt(args[1], 10, 64)
				if err != nil {
					send(c, "0")
					break
				}
				ok := self.DB.ExpStore(args[0], ttl)
				send(c, "%v", ok)
				break
			}
			if len(args) == 3 {
				ttl, err := strconv.ParseInt(args[2], 10, 64)
				if err != nil {
					send(c, "0")
					break
				}
				ok := self.DB.GetStore(args[0], ST_GC).Exp(args[1], ttl)
				send(c, "%v", ok)
				break
			}
			send(c, "0")
			break
		case "ttl":
			if len(args) == 1 {
				ttl := self.DB.TTLStore(args[0])
				send(c, "%v", ttl)
				break
			}
			if len(args) == 2 {
				ttl := self.DB.GetStore(args[0], ST_GC).TTL(args[1])
				send(c, "%v", ttl)
				break
			}
			send(c, "0")
			break
		case "has":
			if len(args) == 1 {
				ok := self.DB.HasStore(args[0])
				send(c, "%v", ok)
				break
			}
			if len(args) == 2 {
				ok := self.DB.GetStore(args[0], ST_GC).HasKey(args[1])
				send(c, "%v", ok)
				break
			}
			if len(args) == 3 && args[0] == "val" {
				ok := self.DB.GetStore(args[1], ST_GC).HasVal(args[2])
				send(c, "%v", ok)
				break
			}
			send(c, "0")
			break
		case "save":
			if len(args) == 1 {
				self.DB.Save(SNAPSHOT_PATH, args[0])
				send(c, "saved %v%v", SNAPSHOT_PATH, args[0])
				break
			}
			send(c, "0")
			break
		case "load":
			if len(args) == 1 {
				self.DB.Load(SNAPSHOT_PATH, args[0])
				send(c, "loaded %v%v", SNAPSHOT_PATH, args[0])
				break
			}
			send(c, "0")
			break
		case "db":
			if len(args) == 1 && args[0] == "exit" {
				c.SetDeadline(time.Now())
				break
			}
			if len(args) == 1 && args[0] == "help" {
				send(c, "%v", strings.Join(HELP, "\n"))
				break
			}
			send(c, "0")
			break
		}
	}
}

// close connection
func (self *Server) CloseConnection(c *net.TCPConn) {
	c.Write([]byte("Goodbye\r\n"))
	log.Printf("CLOSED CONNECTION TO CLIENT [%s]\n", c.RemoteAddr().String())
	c.Close()
	c = nil
}

// extend connection ttl
func (self *Server) ExtendClientTTL(c *net.TCPConn, ttl float64) {
	if ttl > 0 {
		c.SetDeadline(time.Now().Add(time.Duration(ttl) * time.Second))
	}
}
