package uint64_buckets

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/opencensus-integrations/redigo/redis"
)

var testCtx = context.Background()

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

func Test_Bitmap(t *testing.T) {
	rb := NewRedisBitmap(newPool("redis://127.0.0.1:6379"), "test_bm", 100)
	nums := []uint64{0, 100, 1000000, 1000000000, 1000000000000, 1000000000000000, 1000000000000000000}
	if err := rb.Add(testCtx, nums...); err != nil {
		panic(err)
	}

	size, err := rb.Size(testCtx)
	if err != nil || size != 7 {
		t.Fatalf("size=%d err=%v", size, err)
	}

	min, err := rb.Min(testCtx)
	if err != nil || min != 0 {
		t.Fatalf("min=%d err=%v", min, err)
	}

	max, err := rb.Max(testCtx)
	if err != nil || max != 1000000000000000000 {
		t.Fatalf("max=%d err=%v", max, err)
	}

	for _, num := range nums {
		contain, err := rb.Contains(testCtx, num)
		if !contain || err != nil {
			t.Fatalf("contain(%d)=%v err=%v", num, contain, err)
		}
	}
	contain, err := rb.Contains(testCtx, uint64(13561))
	if contain || err != nil {
		t.Fatalf("contain(%d)=%v err=%v", 13561, contain, err)
	}
}

func Test_Range(t *testing.T) {
	rb := NewRedisBitmap(newPool("redis://127.0.0.1:6379"), "test_bm", 100)

	var seq = 0
	for {
		ids, next, err := rb.Range(testCtx, seq)
		if err != nil {
			t.Fatal(err)
		}
		if next < 0 {
			break
		}
		seq = next
		fmt.Println("ids:", ids)
	}
}

func Test_BucketsCount(t *testing.T) {
	rb := NewRedisBitmap(newPool("redis://127.0.0.1:6379"), "test_bm", 100)

	n, err := rb.(*RedisBitmap).BucketsCount(testCtx)
	fmt.Println("n:", n)
	fmt.Println("err:", err)
}
