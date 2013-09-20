// * 
// * Copyright 2013, Scott Cagno. All rights Reserved
// * License: sites.google.com/site/bsdc3license
// * 
// * -------
// * main.go ::: main database implementation
// * -------
// * 

package main

import "github.com/scottcagno/mdb"

func main() {
	mdb.InitDBServer().Serve()
}
