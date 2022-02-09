package dev

import (
	"encoding/json"
	"fmt"
)

func Println(data interface{})  {
	marshal, _ := json.Marshal(data)
	fmt.Println(string(marshal))
}
