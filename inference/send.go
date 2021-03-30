package main 

import (
	"github.com/changli-pts/high_concurrency/tree/master/common"
	"fmt"
)


var (
	JobNum = 10
	requests = 100 / JobNum
)


func main() {
	//c := make(chan bool, 10)
	
	result := common.JobNum
	fmt.Println(result)
	
  
	//<- c
}
