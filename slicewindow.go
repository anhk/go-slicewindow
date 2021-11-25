package slicewindow

import (
	"fmt"
	"runtime"
	"sync/atomic"
)

// Bucket represent a slot to record metrics
type Bucket struct {
	// The start of timestamp of this statistic bucket
	BucketStart uint64
	// The actual data struct to record the metrics
	Value atomic.Value
}

// BucketGenerator Generic interface to generate bucket
type BucketGenerator interface {
	// NewEmptyBucket called when timestamp entry a new slot interval
	NewEmptyBucket() interface{}

	// ResetBucketTo reset the Bucket, clear all data
	ResetBucketTo(b *Bucket, startTime uint64) *Bucket
}

// DeprecatedCallbackFunc this function will be called when bucket deprecated
type DeprecatedCallbackFunc func([]*Bucket)

// SliceWindow slice window
type SliceWindow struct {
	sampleCount    int // 采样数量
	intervalInMs   int // 采样间隔
	windowSizeInMs int // 窗口时长
	generator      BucketGenerator
	array          []*Bucket
	deprecatedFunc DeprecatedCallbackFunc

	updateLock     mutex // update lock
	deprecatedLock mutex // deprecated lock
}

func (sw *SliceWindow) CurrentBucket() (*Bucket, error) {
	return sw.currentBucketOfTime(CurrentTimeMillsWithTicker())
}

func (sw *SliceWindow) currentBucketOfTime(now uint64) (*Bucket, error) {
	if now <= 0 {
		return nil, fmt.Errorf("Current time is less than 0.")
	}

	idx := sw.calculateTimeIdx(now)                      // 计算索引
	bucketStart := now - (now % uint64(sw.intervalInMs)) // 抹零

	for {
		old := sw.array[idx]
		if old == nil {
			n := &Bucket{BucketStart: bucketStart, Value: atomic.Value{}}
			n.Value.Store(sw.generator.NewEmptyBucket())
			sw.array[idx] = n
			return n, nil
		} else if bucketStart == old.BucketStart {
			return old, nil
		} else if bucketStart > old.BucketStart {
			sw.callDeprecatedCallbackFunc()
			if sw.updateLock.TryLock() {
				old := sw.generator.ResetBucketTo(old, bucketStart)
				sw.updateLock.Unlock()
				return old, nil
			} else {
				runtime.Gosched()
			}
		} else if bucketStart < old.BucketStart {
			return nil, fmt.Errorf("Provided time timeMillis=%d is alread behind old.BucketStart=%d",
				bucketStart, old.BucketStart)
		}
	}
}

func (sw *SliceWindow) calculateTimeIdx(now uint64) int {
	timeId := now / uint64(sw.intervalInMs)
	return int(timeId) % sw.sampleCount
}

func (sw *SliceWindow) Values() []*Bucket {
	return sw.valuesWithTime(CurrentTimeMillsWithTicker())
}

func (sw *SliceWindow) valuesWithTime(now uint64) []*Bucket {
	if now <= 0 {
		return make([]*Bucket, 0)
	}
	ret := make([]*Bucket, 0, sw.sampleCount)
	for i := 0; i < sw.sampleCount; i++ {
		b := sw.array[i]
		if b == nil || sw.isBucketDeprecated(now, b) {
			continue
		}
		ret = append(ret, b)
	}
	return ret
}

func (sw *SliceWindow) isBucketDeprecated(now uint64, b *Bucket) bool {
	bucketStart := b.BucketStart
	return (now - bucketStart) > uint64(sw.windowSizeInMs+sw.intervalInMs)
}

func (sw *SliceWindow) callDeprecatedCallbackFunc() {
	if sw.deprecatedFunc != nil && sw.deprecatedLock.TryLock() {
		values := sw.Values()
		sw.deprecatedFunc(values)
		sw.deprecatedLock.Unlock()
	}
}

func (sw *SliceWindow) SetDeprecatedFunc(deprecatedFunc DeprecatedCallbackFunc) {
	sw.deprecatedFunc = deprecatedFunc
}

func NewSliceWindow(sampleCount, intervalInMs int, generator BucketGenerator) (*SliceWindow, error) {
	if sampleCount <= 0 || intervalInMs <= 0 {
		return nil, fmt.Errorf("Invalid parameters, intervalInMs is %d, sampleCount is %d", intervalInMs, sampleCount)
	}
	if generator == nil {
		return nil, fmt.Errorf("Invalid parameters, BucketGenerator is nil")
	}
	return &SliceWindow{
		sampleCount:    sampleCount,
		intervalInMs:   intervalInMs,
		generator:      generator,
		windowSizeInMs: sampleCount * intervalInMs,
		array:          make([]*Bucket, sampleCount),
	}, nil
}
