package main

import (
	"context"
	"fmt"
	"time"

	"github.com/golang/glog"
	"github.com/opencensus-integrations/redigo/redis"

	ub "github.com/treachery/uint64-buckets"
)

func main() {
	testCtx := context.Background()
	rb := ub.NewRedisBitmap(newPool("redis://127.0.0.1:6379"), "test_bm", 1000)

	nums := []uint64{0, 100, 1000000, 1000000000, 1000000000000, 1000000000000000, 1000000000000000000}
	if err := rb.Add(testCtx, nums...); err != nil {
		panic(err)
	}

	size, err := rb.Size(testCtx)
	if err != nil || size != 7 {
		glog.Fatalf("size=%d err=%v", size, err)
	}

	min, err := rb.Min(testCtx)
	if err != nil || min != 0 {
		glog.Fatalf("min=%d err=%v", min, err)
	}

	max, err := rb.Max(testCtx)
	if err != nil || max != 1000000000000000000 {
		glog.Fatalf("max=%d err=%v", max, err)
	}

	for _, num := range nums {
		contain, err := rb.Contains(testCtx, num)
		if !contain || err != nil {
			glog.Fatalf("contain(%d)=%v err=%v", num, contain, err)
		}
	}
	contain, err := rb.Contains(testCtx, uint64(13561))
	if contain || err != nil {
		glog.Fatalf("contain(%d)=%v err=%v", 13561, contain, err)
	}

	var seq = 0
	fmt.Println("Range:")
	for {
		ids, next, err := rb.Range(testCtx, seq)
		if err != nil {
			glog.Fatalln(err)
		}
		if next < 0 {
			break
		}
		seq = next
		fmt.Println("ids:", ids)
	}
}

func newPool(addr string) *redis.Pool {
	return &redis.Pool{
		MaxIdle:     20,
		IdleTimeout: 240 * time.Second,
		MaxActive:   20,
		Wait:        true,
		Dial: func() (redis.Conn, error) {
			return redis.DialURL(addr)
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			if time.Since(t) < time.Minute {
				return nil
			}
			_, err := c.Do("PING")
			return err
		},
	}
}
