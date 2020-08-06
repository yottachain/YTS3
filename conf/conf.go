package conf

import (
	"github.com/go-ini/ini"
)

type Config struct {
	conf *ini.File
}

func CreateConfig(url string) (*Config, error) {
	cfg, err := ini.Load(url)
	if err != nil {
		return nil, err
	}
	return &Config{conf: cfg}, nil
}

//GetHTTPInfo 根据key获取配置文件相关信息
func (c *Config) GetHTTPInfo(expectKey string) string {
	return c.conf.Section("http").Key(expectKey).String()
}

//GetConfigInfo 根据key获取配置文件相关信息
func (c *Config) GetConfigInfo(expectKey string) string {
	// cfg, err := ini.Load("../conf/yotta_config.yaml")

	// if err != nil {
	// 	panic(err)
	// }
	return c.conf.Section("mongo").Key(expectKey).String()
}

//GetRecieveInfo 从配置文件读取要写的数据库信息
func (c *Config) GetRecieveInfo(expectKey string) string {
	// cfg, err := ini.Load("../conf/yotta_config.yaml")

	// if err != nil {
	// 	panic(err)
	// }
	return c.conf.Section("receive").Key(expectKey).String()
}
