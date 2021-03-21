package config

import (
	"github.com/zhenligod/thingo/mysql"
	"github.com/zhenligod/thingo/yamlconf"
)

var (
	// AppEnv app_env
	AppEnv string

	// AppDebug app debug
	AppDebug bool

	conf   *yamlconf.ConfigEngine
	dbConf = &mysql.DbConf{}
)
