// * 
// * Copyright 2013, Scott Cagno. All rights Reserved
// * License: sites.google.com/site/bsdc3license
// * 
// * -------
// * conf.go ::: configuration file
// * -------
// * 

package mdb

const (

	SERVER_LISTEN 	= ":5555"
	CLIENT_TIMEOUT 	= 5*60		// 5 min client timeout
	DB_GC_RATE 		= 1
	ST_GC_RATE		= 1
	ARCHIVE_PATH	= "arch"
	LOGGING_PATH	= "logs"

)