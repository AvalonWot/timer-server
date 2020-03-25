package timer

import (
	"log"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"example.com/timer-server/queue"
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

// 每个timer的唯一标识
type TimerID uint64

type TimeLine struct {
	v     map[TimeIndex]*TimeSlice
	dirty *queue.EsQueue
}

// 所有timer的集合
type TimerSets struct {
	m    sync.RWMutex
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
	ext      interface{}
}

func init() {
	_timeline.dirty = queue.NewQueue(1024 * 1024)
}

func Run() {
	go ant()
}

// TODO:  init 部分一定要记得处理 _cellId 的初始值, 从当前 timers 中找到最大值+1

// 添加计时器
func AddTimer(deadline int64, ext interface{}) (id TimerID, ok bool) {
	d := time.Unix(deadline, 0)
	if d.Before(time.Now()) {
		return TimerID(0), false
	}
	cell := &TimerCell{
		id:       TimerID(atomic.AddUint64(&_cellId, 1)),
		deadline: time.Unix(deadline, 0),
		index:    TimeIndex(deadline / TimelineRadix),
		ext:      ext,
	}
	if !_pool.add(cell) {
		log.Printf("[ERR] 添加计时器 %d 失败\n", cell.id)
		return TimerID(0), false
	}
	log.Printf("[INFO] 添加计时器 %d\n", cell.id)
	_timeline.pushDirty(cell)
	return cell.id, true
}

// 查找计时器是否存在, 若存在折该计时器还未触发
func IsTimerAlive(id TimerID) bool {
	_, ok := _pool.find(id)
	return ok
}

// 移除计时器
func RemoveTimer(id TimerID) bool {
	return _pool.remove(id)
}

func (s *TimerSets) add(cell *TimerCell) bool {
	s.m.Lock()
	defer s.m.Unlock()
	if _, ok := s.sets[cell.id]; ok {
		log.Printf("[ERR] %d 计时器已经存在\n", cell.id)
		return false
	}
	s.sets[cell.id] = cell
	return true
}

func (s *TimerSets) remove(id TimerID) bool {
	s.m.Lock()
	defer s.m.Unlock()
	if _, ok := s.sets[id]; ok {
		delete(s.sets, id)
		return true
	}
	return false
}

func (s *TimerSets) find(id TimerID) (cell *TimerCell, ok bool) {
	s.m.RLock()
	defer s.m.RUnlock()
	cell, ok = s.sets[id]
	return
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
	ok := false
	for !ok {
		ok, _ = _timeline.dirty.Put(cell)
	}
}

func (t *TimeLine) popDirty(out map[TimerID]*TimerCell) {
	for {
		v, ok, _ := _timeline.dirty.Get()
		if !ok {
			return
		}
		cell := v.(*TimerCell)
		out[cell.id] = cell
	}
}

func (t *TimeLine) getSliceAndDelete(index TimeIndex) (slice *TimeSlice, ok bool) {
	slice, ok = t.v[index]
	if ok {
		delete(t.v, index)
	}
	return
}

// 将代理放入到对应的timeslice中
func (t *TimeLine) putLine(cell *TimerCell) {
	slice, ok := t.v[cell.index]
	if !ok {
		slice = &TimeSlice{cell.index, make(map[TimerID]*TimerCell)}
		t.v[cell.index] = slice
	}
	slice.v[cell.id] = cell
}

// 将添加进来的timer(在dirty中的),合并到对应的slice中
func merge(index TimeIndex, dirty map[TimerID]*TimerCell) {
	for id, cell := range dirty {
		if cell.deadline.Before(time.Now()) {
			emit(cell)
			delete(dirty, id)
			continue
		}
		if cell.index > index {
			_timeline.putLine(cell)
			delete(dirty, id)
		}
		// 注意 cell.index == index 时不要处理, 留着在 before 那里去触发
		// 因为在两个 index 临界的时候, 很容易把 timer 插入到不会再处理的 slice 中去
	}
}

func ant() {
	dirty := make(map[TimerID]*TimerCell)
	index := TimeIndex(time.Now().Unix() / TimelineRadix)
	for {
		slice, ok := _timeline.getSliceAndDelete(index)
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
				runtime.Gosched()
			}
		} else {
			for TimeIndex(time.Now().Unix()/TimelineRadix) < index {
				_timeline.popDirty(dirty)
				merge(index, dirty)
				time.Sleep(100 * time.Millisecond)
			}
		}
		runtime.Gosched()
		// 保证每一个slice都被遍历一次
		index++
	}
}

func syncdb(cell *TimerCell) {
	// 同步到持久化部分, 若出现问题及时panic
}

func emit(cell *TimerCell) {
	// TODO: 将到期 cell 发送到 ringbuff 等待读取
}
