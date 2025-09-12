package config

import (
	"github.com/apolloconfig/agollo/v4/storage"
)

type CustomChangeListener struct {
}

func (c *CustomChangeListener) OnChange(changeEvent *storage.ChangeListener) {
	//write your code here
}
