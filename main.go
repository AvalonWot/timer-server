package main

import (
	"fmt"
	"sort"
	"sync"
	"time"
)

const (
	// 在时间线上划分的间隔的基数, 单位是秒
	TimelineRadix = 2 * 60
)

var (
	_timeline TimeLine
	_pool     TimerSets
)

// 每个timer的唯一标识
type TimerID uint64

type TimeLine struct {
	m sync.RWMutex
	v map[TimeIndex]*TimeSlice
}

// 所有timer的集合
type TimerSets struct {
	sets map[TimerID]*TimerCell
}

// 在时间线上的索引点, 由TimelineRadix整除确定
type TimeIndex int64

type TimerList []*TimerCell

func (l TimerList) Len() int {
	return len(l)
}

func (l TimerList) Less(i, j int) bool {
	return l[i].deadline.Before(l[j].deadline)
}

func (l TimerList) Swap(i, j int) {
	l[i], l[j] = l[j], l[i]
}

// 每一个TimeIndex下的内容
type TimeSlice struct {
	sync.Mutex
	index TimeIndex
	// 无序timer
	disorder map[TimerID]*TimerCell
	// 有序timer
	order TimerList
	// 是否已经经过了排序
	sorted bool
}

type TimerCell struct {
	id        uint64
	deadline  time.Time
	discarded bool
	repeat    bool
	interval  time.Duration
	ext       interface{}
}

func (t *TimeSlice) Sort() {
	l := make(TimerList, 0, len(t.disorder))
	for _, v := range t.disorder {
		l = append(l, v)
	}
	sort.Sort(l)
	t.sorted = true
}

func (t *TimeLine) GetSlice(index TimeIndex) (slice *TimeSlice, ok bool) {
	t.m.Lock()
	defer t.m.Unlock()
	slice, ok = t.v[index]
	return
}

func (t *TimeLine) AddTimer(cell *TimerCell) bool {
	t.m.Lock()
	defer t.m.Unlock()
	index := TimeIndex(cell.deadline.Unix() / TimelineRadix)
	nowIndex := TimeIndex(time.Now().Unix() / TimelineRadix)
	if index <= nowIndex {
		return false
	}
	slice, ok := t.v[index]
	if !ok {
		slice = &TimeSlice{}
	}
	return slice
}

func ant() {
	var slice *TimeSlice
	for {
		index := TimeIndex(time.Now().Unix() / TimelineRadix)
		if slice == nil {
			v, ok := _timeline.GetSliceAndDelete(index)
			if !ok {
				time.Sleep(time.Millisecond)
				continue
			}
			slice = v
		}
		if !slice.sorted {
			slice.Sort()
		}
		newstart = 0
		for i, t := range slice.order {
			if t.deadline.Before(time.Now()) {
				// TODO: 发送到到期ringbuffer去
			}
		}
	}
}

func main() {
	fmt.Printf("")
}
