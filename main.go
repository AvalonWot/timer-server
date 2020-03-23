package main

import (
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// 在时间线上划分的间隔的基数, 单位是秒
	TimelineRadix = 2 * 60
)

var (
	_cellId   uint64
	_timeline TimeLine
	_pool     TimerSets
)

type LockMap struct {
	sync.Mutex
	v map[TimerID]*TimerCell
}

// 每个timer的唯一标识
type TimerID uint64

type TimeLine struct {
	v     map[TimeIndex]*TimeSlice
	dirty LockMap
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
	index TimeIndex
	// 无序timer
	v map[TimerID]*TimerCell
}

type TimerCell struct {
	id       TimerID
	deadline time.Time
	index    TimeIndex
	repeat   bool
	interval time.Duration
	ext      interface{}
}

// TODO:  init 部分一定要记得处理 _cellId 的初始值, 从当前 timers 中找到最大值+1

func NewTimerCell(deadline int64, repeat bool, interval int, ext interface{}) *TimerCell {
	return &TimerCell{
		id:       TimerID(atomic.AddUint64(&_cellId, 1)),
		deadline: time.Unix(deadline, 0),
		index:    TimeIndex(deadline / TimelineRadix),
		repeat:   repeat,
		interval: time.Duration(interval) * time.Second,
		ext:      ext,
	}
}

func (t *TimeSlice) sort() TimerList {
	// TODO: 这里可以不用新生成一个 切片, 而是用一个既有的
	l := make(TimerList, 0, len(t.v))
	for _, cell := range t.v {
		l = append(l, cell)
	}
	sort.Sort(l)
	return l
}

func (t *TimeLine) pushDirty(cell *TimerCell) {
	t.dirty.Lock()
	defer t.dirty.Unlock()
	if t.dirty.v == nil {
		t.dirty.v = make(map[TimerID]*TimerCell)
	}
	t.dirty.v[cell.id] = cell
}

func (t *TimeLine) popDirty(out map[TimerID]*TimerCell) {
	t.dirty.Lock()
	r := t.dirty.v
	t.dirty.v = nil
	t.dirty.Unlock()
	for k, v := range r {
		out[k] = v
	}
}

func (t *TimeLine) GetSliceAndDelete(index TimeIndex) (slice *TimeSlice, ok bool) {
	slice, ok = t.v[index]
	if ok {
		delete(t.v, index)
	}
	return
}

func (t *TimeLine) addTimer(cell *TimerCell) bool {
	if cell.deadline.Before(time.Now().Add(5 * time.Second)) {
		return false
	}
	t.pushDirty(cell)
	return true
}

// 将代理放入到对应的timeslice中
func (t *TimeLine) putLine(cell *TimerCell) {
	slice, ok := t.v[cell.index]
	if !ok {
		slice = &TimeSlice{}
		t.v[cell.index] = slice
	}
	slice.v[cell.id] = cell
}

func merge(index TimeIndex, dirty map[TimerID]*TimerCell) {
	for id, cell := range dirty {
		if cell.deadline.Before(time.Now()) {
			emit(cell)
			delete(dirty, id)
			continue
		}
		if cell.index > index {
			_timeline.putLine(cell)
		}
		// 注意 cell.index == index 时不要处理, 留着在 before 那里去触发
		// 因为在两个 index 临界的时候, 很容易把 timer 插入到不会再处理的 slice 中去
	}
}

func ant() {
	dirty := make(map[TimerID]*TimerCell)
	for {
		index := TimeIndex(time.Now().Unix() / TimelineRadix)
		slice, ok := _timeline.v[index]
		if ok {
			l := slice.sort()
			for TimeIndex(time.Now().Unix()/TimelineRadix) < index || len(l) > 0 {
				for i, cell := range l {
					if cell.deadline.Before(time.Now()) {
						emit(cell)
					} else {
						l = l[i:]
						break
					}
				}
				_timeline.popDirty(dirty)
				merge(index, dirty)
			}
		}
		time.Sleep(time.Millisecond)
	}
}

func emit(cell *TimerCell) {
	// TODO: 将到期 cell 发送到 ringbuff 等待读取
}

func main() {
	fmt.Printf("")
}
