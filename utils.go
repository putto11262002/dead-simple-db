package deadsimpledb

import "fmt"

func assert(v bool, s string, args ...interface{}) {
	if !v {
		panic(fmt.Sprintf(s, args...))
	}
}
