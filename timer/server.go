package timer

import (
	"log"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"example.com/timer-server/queue"
	"github.com/gomodule/redigo/redis"
)

const (
	// 在时间线上划分的间隔的基数, 单位是秒
	TimelineRadix   = 2 * 60
	RedisServerAddr = "192.168.31.158:6379"
)

var (
	_cellId   uint64
	_timeline TimeLine
	_pool     TimerSets
	_redis    *redis.Pool
	_expired  = queue.NewQueue(1024 * 1024)
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
	id       TimerID   `redis:"id"`
	deadline time.Time `redis:"deadline"`
	index    TimeIndex `redis:"index"`
	value    string    `redis:"value"`
}

func init() {
	_timeline.dirty = queue.NewQueue(1024 * 1024)
	_redis = &redis.Pool{
		MaxIdle:     3,
		IdleTimeout: 240 * time.Second,
		Wait:        true,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", RedisServerAddr, redis.DialConnectTimeout(5*time.Second))
			if err != nil {
				return c, err
			}
			return c, err
		},
	}
	recovery()
}

func recovery() error {
	c := _redis.Get()
	defer c.Close()
	cache := make(map[TimerID]*TimerCell, 1024*1024)
	iter := "0"
	for {
		reply, err := redis.Values(redis.DoWithTimeout(c, 5*time.Second, "SCAN", iter))
		if err != nil {
			return err
		}
		iter, err = redis.String(reply[0], nil)
		if err != nil {
			return err
		}
		var ids []interface{}
		ids, err = redis.Values(reply[1], nil)
		if err != nil {
			return err
		}
		for _, id := range ids {
			v, err := redis.Values(c.Do("HGETALL", id))
			if err != nil {
				return err
			}
			cell := &TimerCell{}
			if err := redis.ScanStruct(v, cell); err != nil {
				return err
			}
			// 选取当前已有最大的timerid作为之后新timer的计数基准
			if uint64(cell.id) > _cellId {
				_cellId = uint64(cell.id)
			}
			cache[cell.id] = cell
		}
		if iter == "0" {
			break
		}
	}
	log.Printf("[INFO] 读取到timer个数: %d", len(cache))
	go ant()
	go func(vs map[TimerID]*TimerCell) {
		for _, cell := range vs {
			_timeline.pushDirty(cell)
		}
		log.Printf("所有timer导入完成")
	}(cache)
	return nil
}

// TODO:  init 部分一定要记得处理 _cellId 的初始值, 从当前 timers 中找到最大值+1

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

func (s *TimerSets) removes(vs []*TimerCell) {
	s.m.Lock()
	defer s.m.Unlock()
	for _, cell := range vs {
		if _, ok := s.sets[cell.id]; !ok {
			log.Printf("[WARRING] 要移除的timer: %+v 不存在", cell)
		}
		delete(s.sets, cell.id)
	}
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

func emit(cell *TimerCell) {
	_expired.Put(cell)
}

func dbadd(cell *TimerCell) bool {
	// FIXME: 实现原子操作: 不存在才添加
	c := _redis.Get()
	defer c.Close()

	if _, err := c.Do("HMSET", redis.Args{}.Add(cell.id).AddFlat(cell)...); err != nil {
		log.Panicf("写入redis发生错误: %v", err)
	}
	return true
}

func dbremove(id TimerID) {
	c := _redis.Get()
	defer c.Close()

	result, err := redis.Int(c.Do("DEL", id))
	if err != nil {
		log.Panicf("从redis删除数据时发生错误: %v", err)
	}
	if result == 0 {
		log.Printf("[WARRING] 从数据库中删除 timer: %d 不存在\n", id)
	}
}

func dbremoves(cells []*TimerCell) {
	c := _redis.Get()
	defer c.Close()

	ids := make([]TimerID, 0, len(cells))
	for i, cell := range cells {
		ids[i] = cell.id
	}
	result, err := redis.Int(c.Do("DEL", ids))
	if err != nil {
		log.Panicf("从redis删除数据时发生错误: %v", err)
	}
	if result != len(ids) {
		log.Printf("[WARRING] 要求删除的个数为: %d, 实际删除的个数为: %d", len(ids), result)
	}
}

// 添加计时器
func AddTimer(deadline int64, value string) (id TimerID, ok bool) {
	d := time.Unix(deadline, 0)
	if d.Before(time.Now()) {
		return TimerID(0), false
	}
	cell := &TimerCell{
		id:       TimerID(atomic.AddUint64(&_cellId, 1)),
		deadline: time.Unix(deadline, 0),
		index:    TimeIndex(deadline / TimelineRadix),
		value:    value,
	}
	if !_pool.add(cell) {
		log.Printf("[ERR] 添加计时器 %d 失败\n", cell.id)
		return TimerID(0), false
	}
	dbadd(cell)
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
func RemoveTimer(id TimerID) {
	if !_pool.remove(id) {
		log.Printf("要移除的timer: %d 不存在", id)
	}
	dbremove(id)
}

// 获取到期计时器
func GetExpiredTimer(limit uint32) []*TimerCell {
	values, _ := _expired.Gets(limit)
	cells := make([]*TimerCell, 0, len(values))
	for i, v := range values {
		cell := v.(*TimerCell)
		cells[i] = cell
	}
	_pool.removes(cells)
	dbremoves(cells)
	return cells
}
