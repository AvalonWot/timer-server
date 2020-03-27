package timer

import (
	"fmt"
	"log"
	"testing"
	"time"
)

func TestAddTimer(t *testing.T) {
	Init("192.168.31.158:6379")
	for i := 0; i < 9; i++ {
		id := AddTimer(time.Now().Add(5*time.Second).Unix(), fmt.Sprintf("test-%d", i))
		log.Printf("添加计时器: %d\n", id)
	}
	AddTimer(time.Now().Add(time.Duration(40)*time.Second).Unix(), fmt.Sprintf("test-del"))
	// RemoveTimer(id)
	time.Sleep(time.Second)
	n := 0
	for n < 10 {
		timers := GetExpiredTimer(20)
		n += len(timers)
		for _, t := range timers {
			log.Printf("task: %s 到期: %v\n", t.Value, time.Unix(t.Deadline, 0))
		}
		time.Sleep(100 * time.Millisecond)
	}
}
