package common

import "github.com/caarlos0/env/v6"

type osenv struct {
	ApolloHost string `env:"APOLLO_HOST,notEmpty"`
	Appid      string `env:"APP_ID" envDefault:"contract-match"`
	Namespace  string `env:"APOLLO_NAMESPACE" envDefault:"application"`
}

func InitEnv() (*osenv, error) {
	e := osenv{}
	if err := env.Parse(&e); err != nil {
		return &e, err
	}
	return &e, nil
}
