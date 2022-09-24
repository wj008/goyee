package worker

import (
	"sync"
)

type Queue struct {
	wg  sync.WaitGroup
	job chan any
}

func NewQueue(workerCount int, fn func(any)) *Queue {
	q := &Queue{
		job: make(chan any, workerCount),
	}
	worker := func(jobChan <-chan any, workId int) {
		q.wg.Add(1)
		defer q.wg.Done()
		for value := range jobChan {
			if fn != nil {
				fn(value)
				//log.Println("success:workId", workId)
			}
		}
	}
	for i := 0; i < workerCount; i++ {
		go worker(q.job, i)
	}
	return q
}

// Add 添加函数值
func (q *Queue) Add(value any) {
	q.job <- value
}

// Wait 等待任务完成
func (q *Queue) Wait() {
	close(q.job)
	q.wg.Wait()
}
