package timer

import (
	"fmt"
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
	TimelineRadix = 2 * 60
)

var (
	_cellId    uint64
	_timeline  TimeLine
	_pool      TimerSets
	_redis     *redis.Pool
	_expired   = queue.NewQueue(1024 * 1024)
	_addscript = redis.NewScript(1, fmt.Sprintf("--%d\nif redis.call('exists', KEYS[1]) ~= 0 then return redis.status_reply('FAIL') else return redis.call('hmset', KEYS[1], unpack(ARGV)) end", time.Now().UnixNano()))
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
	value    string
	removed  int32
}

type Timer struct {
	Id       TimerID `redis:"id"`
	Deadline int64   `redis:"deadline"`
	Value    string  `redis:"value"`
}

func Init(redisAddr string) {
	_timeline.dirty = queue.NewQueue(1024 * 1024)
	_redis = &redis.Pool{
		MaxIdle:     3,
		IdleTimeout: 240 * time.Second,
		Wait:        true,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", redisAddr, redis.DialConnectTimeout(5*time.Second))
			if err != nil {
				return c, err
			}
			return c, err
		},
	}
	err := recovery()
	if err != nil {
		log.Panicf("恢复数据发生错误: %v", err)
	}
}

func recovery() error {
	c := _redis.Get()
	defer c.Close()
	cache := make([]*TimerCell, 0, 10240)
	iter := "0"
	for {
		reply, err := redis.Values(redis.DoWithTimeout(c, 5*time.Second, "SCAN", iter, "MATCH", "timer:*"))
		if err != nil {
			return err
		}
		iter, err = redis.String(reply[0], nil)
		if err != nil {
			return err
		}
		var keys []interface{}
		keys, err = redis.Values(reply[1], nil)
		if err != nil {
			return err
		}
		for _, key := range keys {
			v, err := redis.Values(c.Do("HGETALL", key))
			if err != nil {
				return err
			}
			var t Timer
			if err := redis.ScanStruct(v, &t); err != nil {
				return err
			}
			cell := &TimerCell{
				id:       t.Id,
				deadline: time.Unix(t.Deadline, 0),
				index:    TimeIndex(t.Deadline / TimelineRadix),
				value:    t.Value,
			}
			// 选取当前已有最大的timerid作为之后新timer的计数基准
			if uint64(cell.id) > _cellId {
				_cellId = uint64(cell.id)
			}
			cache = append(cache, cell)
		}
		if iter == "0" {
			break
		}
	}
	log.Printf("[INFO] 读取到timer个数: %d", len(cache))
	_pool.init(cache)
	go ant()
	go func(vs []*TimerCell) {
		for _, cell := range vs {
			_timeline.pushDirty(cell)
		}
		log.Printf("所有timer导入完成")
	}(cache)
	return nil
}

func (s *TimerSets) init(values []*TimerCell) {
	s.m.Lock()
	defer s.m.Unlock()
	s.sets = make(map[TimerID]*TimerCell, 1024*1024)
	for _, c := range values {
		s.sets[c.id] = c
	}
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
	if t, ok := s.sets[id]; ok {
		t.flagRemoved()
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
	if t.v == nil {
		t.v = make(map[TimeIndex]*TimeSlice)
	}
	slice, ok = t.v[index]
	if ok {
		delete(t.v, index)
	}
	return
}

// 将代理放入到对应的timeslice中
func (t *TimeLine) putLine(cell *TimerCell) {
	if t.v == nil {
		t.v = make(map[TimeIndex]*TimeSlice)
	}
	slice, ok := t.v[cell.index]
	if !ok {
		slice = &TimeSlice{cell.index, make(map[TimerID]*TimerCell)}
		t.v[cell.index] = slice
	}
	slice.v[cell.id] = cell
}

func (c *TimerCell) flagRemoved() {
	atomic.AddInt32(&c.removed, 1)
}

func (c *TimerCell) isRemoved() bool {
	if atomic.LoadInt32(&c.removed) == 1 {
		return true
	} else {
		return false
	}
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
	dirty := make(map[TimerID]*TimerCell, 10240)
	index := TimeIndex(time.Now().Unix() / TimelineRadix)
	for {
		slice, ok := _timeline.getSliceAndDelete(index)
		if ok {
			l := slice.sort()
			for TimeIndex(time.Now().Unix()/TimelineRadix) < index || len(l) > 0 {
				newi := func(timers TimerList) int {
					for i, cell := range timers {
						if cell.deadline.Before(time.Now()) {
							emit(cell)
						} else {
							return i
						}
					}
					return len(timers)
				}(l)
				l = l[newi:]
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
	if !cell.isRemoved() {
		_expired.Put(cell)
	} else {
		log.Printf("[DEBUG] timer emit: %d 已经被移除", cell.id)
	}
}

func dbadd(cell *TimerCell) {
	// FIXME: 实现原子操作: 不存在才添加
	c := _redis.Get()
	defer c.Close()
	t := &Timer{
		Id:       cell.id,
		Deadline: cell.deadline.Unix(),
		Value:    cell.value,
	}
	r, err := redis.String(_addscript.Do(c, redis.Args{}.Add(fmt.Sprintf("timer:%d", cell.id)).AddFlat(t)...))
	if err != nil {
		log.Panicf("写入redis发生错误: %v", err)
	}
	if r != "OK" {
		log.Panicf("[ERR] 数据库和pool发生冲突, pool中没有timer: %d, 但数据库含有", cell.id)
	}
}

func dbremove(id TimerID) {
	c := _redis.Get()
	defer c.Close()

	result, err := redis.Int(c.Do("DEL", fmt.Sprintf("timer:%d", id)))
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

	keys := make([]interface{}, len(cells))
	for i, cell := range cells {
		keys[i] = fmt.Sprintf("timer:%d", cell.id)
	}
	result, err := redis.Int(c.Do("DEL", keys...))
	if err != nil {
		log.Panicf("从redis删除数据时发生错误: %v", err)
	}
	if result != len(keys) {
		log.Printf("[WARRING] 要求删除的个数为: %d, 实际删除的个数为: %d", len(keys), result)
	}
}

// 添加计时器
func AddTimer(deadline int64, value string) TimerID {
	cell := &TimerCell{
		deadline: time.Unix(deadline, 0),
		index:    TimeIndex(deadline / TimelineRadix),
		value:    value,
	}
	for {
		cell.id = TimerID(atomic.AddUint64(&_cellId, 1))
		if _pool.add(cell) {
			break
		}
	}
	dbadd(cell)
	log.Printf("[INFO] 添加计时器 %d\n", cell.id)
	_timeline.pushDirty(cell)
	return cell.id
}

// 查找计时器是否存在, 若存在折该计时器还未触发
func IsTimerAlive(id TimerID) bool {
	_, ok := _pool.find(id)
	return ok
}

// 移除计时器
func RemoveTimer(id TimerID) {
	log.Printf("[INFO] 主动移除timer: %d", id)
	if !_pool.remove(id) {
		log.Printf("要移除的timer: %d 不存在", id)
	}
	dbremove(id)
}

// 获取到期计时器
func GetExpiredTimer(limit uint32) []*Timer {
	values, _ := _expired.Gets(limit)
	if len(values) > 0 {
		cells := make([]*TimerCell, len(values))
		for i, v := range values {
			cell := v.(*TimerCell)
			cells[i] = cell
		}
		_pool.removes(cells)
		dbremoves(cells)
		timers := make([]*Timer, len(cells))
		for i, v := range cells {
			timers[i] = &Timer{
				Id:       v.id,
				Deadline: v.deadline.Unix(),
				Value:    v.value,
			}
		}
		return timers
	}
	return nil
}
