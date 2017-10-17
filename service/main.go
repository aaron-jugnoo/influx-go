// Copyright 2016 Eleme. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.



package main

import (
	"encoding/json"
	"errors"
	"flag"
	"log"
	"net/http"
	"os"
	"time"

	lumberjack "gopkg.in/natefinch/lumberjack.v2"
	//redis "gopkg.in/redis.v5"
	//redis "github.com/go-redis/redis"

	"../backend"
	//"github.com/yozora-hitagi/influx-proxy/backend"
	"fmt"
)

var (
	ErrConfig   = errors.New("config parse error")
	ConfigFile  string
	NodeName    string
	RedisAddr   string
	LogFilePath string
)

func init() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds | log.Lshortfile)

	flag.StringVar(&LogFilePath, "log-file-path", "influx-go.log", "output file")
	flag.StringVar(&ConfigFile, "config", "proxy.json", "config file")
	flag.StringVar(&NodeName, "node", "default", "node name")
	//flag.StringVar(&RedisAddr, "redis", "localhost:6379", "config file")
	flag.Parse()
}

//type Config struct {
//	redis.Options
//	CfgFile string
//	Node string
//}

func LoadJson(configfile string, cfg interface{}) (err error) {
	file, err := os.Open(configfile)
	if err != nil {
		return
	}
	defer file.Close()

	dec := json.NewDecoder(file)
	err = dec.Decode(&cfg)
	return
}

func initLog() {
	if LogFilePath == "" {
		log.SetOutput(os.Stdout)
	} else {
		log.SetOutput(&lumberjack.Logger{
			Filename:   LogFilePath,
			MaxSize:    10,
			MaxBackups: 5,
			MaxAge:     7,
		})
	}
}

func main() {
	initLog()

	var err error
	//var cfg Config

	//if ConfigFile != "" {
	//	err = LoadJson(ConfigFile, &cfg)
	//	if err != nil {
	//		log.Print("load config failed: ", err)
	//		return
	//	}
	//	log.Printf("json loaded.")
	//}
	//
	//if NodeName != "" {
	//	cfg.Node = NodeName
	//}
	//
	//if RedisAddr != "" {
	//	cfg.Addr = RedisAddr
	//}



	var cfgsource *backend.ConfigSource

	//if RedisAddr != "" {
	//	var ops redis.Options
	//	ops.Addr=RedisAddr
	//	cfgsource=backend.NewRedisConfigSource(&ops, NodeName)
	//}

	cfgsource,err=backend.NewConfigSource(ConfigFile,NodeName)
	if err != nil {
		fmt.Println(err)
		return
	}

	//rcs := backend.NewRedisConfigSource(&cfg.Options, cfg.Node)

	//nodecfg, err := cfgsource.LoadNode()
	//if err != nil {
	//	log.Printf("config source load failed.")
	//	return
	//}

	nodecfg:=cfgsource.NODES[cfgsource.Node]

	ic := backend.NewInfluxCluster(cfgsource, &nodecfg)
	ic.LoadConfig()

	mux := http.NewServeMux()
	NewHttpService(ic, nodecfg.DB).Register(mux)


	server := &http.Server{
		Addr:        nodecfg.ListenAddr,
		Handler:     mux,
		IdleTimeout: time.Duration(nodecfg.IdleTimeout) * time.Second,
	}
	log.Printf("http service start : %s",nodecfg.ListenAddr)

	if nodecfg.IdleTimeout <= 0 {
		server.IdleTimeout = 10 * time.Second
	}
	err = server.ListenAndServe()
	if err != nil {
		log.Print(err)
		return
	}
}
