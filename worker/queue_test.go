package worker

import (
	log "github.com/wj008/goyee/logger"
	"testing"
	"time"
)

func TestNewQueue(t *testing.T) {
	queue := NewQueue(10, func(post ...any) {
		time.Sleep(1 * time.Second)
		log.Println(post...)
	})
	for i := 0; i < 100; i++ {
		queue.Add(i, i)
	}
	queue.Wait()
}
