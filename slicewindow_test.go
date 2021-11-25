package slicewindow

import (
	"fmt"
	"testing"
	"time"
)

type MockGenerator struct {
}

func (g *MockGenerator) NewEmptyBucket() interface{} {
	return 0
}

func (g *MockGenerator) ResetBucketTo(bw *Bucket, startTime uint64) *Bucket {
	bw.Value.Store(g.NewEmptyBucket())
	bw.BucketStart = startTime
	return bw
}

// 过期时的函数
func Deprecated(buckets []*Bucket) {
	fmt.Println("---------------------")
	for _, v := range buckets {
		fmt.Println(v.BucketStart, v.Value.Load())
	}
}

func Test_SliceWindow_001(t *testing.T) {
	sw, _ := NewSliceWindow(5, 1000, &MockGenerator{})
	sw.SetDeprecatedFunc(Deprecated)

	for i := 1; i < 100; i++ {
		b, _ := sw.CurrentBucket()
		b.Value.Store(i)

		time.Sleep(time.Second)
	}
}
