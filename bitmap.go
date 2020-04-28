package uint64_buckets

import (
	"context"
	"fmt"
	"strconv"

	"github.com/opencensus-integrations/redigo/redis"
	"github.com/pkg/errors"
)

var _ bitmaper = &RedisBitmap{}

type bitmaper interface {
	Add(ctx context.Context, ids ...uint64) error
	Contains(ctx context.Context, id uint64) (bool, error)
	Size(ctx context.Context) (uint64, error)
	Min(ctx context.Context) (uint64, error)
	Max(ctx context.Context) (uint64, error)
	Range(ctx context.Context, seq int) (ids []uint64, next int, err error)

	//TODO
	//交集
	//并集
}

/*
每个bitmap在redis内存储两类key
rb_buckets_{key}: 用zset存储所有bucketid
rb_{key}_{bucketid}: 用bitmap存储每个bucket的offsets
*/
type RedisBitmap struct {
	pool *redis.Pool

	key string
}

func NewRedisBitmap(pool *redis.Pool, key string) bitmaper {
	return &RedisBitmap{
		pool: pool,
		key:  key,
	}
}

func (r *RedisBitmap) getZsetKey() string {
	return fmt.Sprintf("rb_%s", r.key)
}

func (r *RedisBitmap) getBucketBmKey(bucketid uint64) string {
	return fmt.Sprintf("rb_%s_%x", r.key, bucketid)
}

func (r *RedisBitmap) Add(ctx context.Context, ids ...uint64) error {
	conn, err := r.pool.GetContext(ctx)
	if err != nil {
		return errors.Wrapf(err, "key(%s) Add ids(%+v)", r.key, ids)
	}
	defer conn.Close()

	for _, id := range ids {
		bucketid, offset := getBucketId(id), getOffset(id)
		//1. 加bucket
		if err := conn.Send("ZADD", r.getZsetKey(), bucketid, bucketid); err != nil {
			return errors.Wrapf(err, "key(%s) Add ids(%+v)", r.key, ids)
		}
		//2. 设置offset
		if err := conn.Send("SETBIT", r.getBucketBmKey(bucketid), offset, 1); err != nil {
			return errors.Wrapf(err, "key(%s) Add ids(%+v)", r.key, ids)
		}
	}
	if err := conn.Flush(); err != nil {
		return errors.Wrapf(err, "key(%s) Add ids(%+v)", r.key, ids)
	}

	for i := len(ids) * 2; i > 0; i-- {
		if _, err := conn.Receive(); err != nil {
			return errors.Wrapf(err, "key(%s) Add ids(%+v)", r.key, ids)
		}
	}
	return nil
}

func (r *RedisBitmap) Contains(ctx context.Context, id uint64) (bool, error) {
	conn, err := r.pool.GetContext(ctx)
	if err != nil {
		return false, errors.Wrapf(err, "key(%s) Contains id(%+v)", r.key, id)
	}
	defer conn.Close()

	bucketid := getBucketId(id)
	offset := getOffset(id)

	exist, err := redis.Bool(conn.Do("GETBIT", r.getBucketBmKey(bucketid), offset))
	if err != nil {
		if err == redis.ErrNil {
			return false, nil
		}
		return false, errors.Wrapf(err, "key(%s) Contains id(%+v)", r.key, id)
	}
	return exist, nil
}

func (r *RedisBitmap) Size(ctx context.Context) (uint64, error) {
	conn, err := r.pool.GetContext(ctx)
	if err != nil {
		return 0, errors.Wrapf(err, "Size")
	}
	defer conn.Close()

	var (
		bucketids   = []uint64{}
		zsetkey     = r.getZsetKey()
		start, stop = 0, 1000
	)
	for {
		strs, err := redis.Strings(conn.Do("ZRANGE", zsetkey, start, stop))
		if err != nil {
			return 0, errors.Wrapf(err, "Size")
		}
		if len(strs) == 0 {
			break
		}
		for _, str := range strs {
			id, err := strconv.ParseUint(str, 10, 64)
			if err != nil {
				return 0, errors.Wrapf(err, "Size")
			}
			bucketids = append(bucketids, id)
		}
		start, stop = start+1000, stop+1000
	}

	var size = uint64(0)
	for _, id := range bucketids {
		count, err := redis.Uint64(conn.Do("BITCOUNT", r.getBucketBmKey(id), 0, 128))
		if err != nil {
			return 0, errors.Wrapf(err, "Size")
		}
		size += count
	}
	return size, nil
}

func (r *RedisBitmap) Min(ctx context.Context) (uint64, error) {
	conn, err := r.pool.GetContext(ctx)
	if err != nil {
		return 0, errors.Wrapf(err, "key(%s) Min", r.key)
	}
	defer conn.Close()

	//第一个bucket
	bids, err := redis.Int64s(conn.Do("ZRANGE", r.getZsetKey(), 0, 0))
	if err != nil {
		return 0, errors.Wrapf(err, "key(%s) Max", r.key)
	}
	if len(bids) != 1 {
		return 0, errors.New("empty zset")
	}
	bucketid, err := strconv.ParseUint(fmt.Sprint(bids[0]), 10, 64)
	if err != nil {
		return 0, errors.Wrapf(err, "key(%s) Max", r.key)
	}
	bucketoffset, err := redis.Uint64(conn.Do("BITPOS", r.getBucketBmKey(bucketid), 1))
	if err != nil {
		return 0, errors.Wrapf(err, "key(%s) Max", r.key)
	}
	return bucketid*capacity + bucketoffset, nil
}

func (r *RedisBitmap) Max(ctx context.Context) (uint64, error) {
	conn, err := r.pool.GetContext(ctx)
	if err != nil {
		return 0, errors.Wrapf(err, "key(%s) Max", r.key)
	}
	defer conn.Close()

	//最后一个bucket
	bids, err := redis.Int64s(conn.Do("ZRANGE", r.getZsetKey(), -1, -1))
	if err != nil {
		return 0, errors.Wrapf(err, "key(%s) Max", r.key)
	}
	if len(bids) != 1 {
		return 0, errors.New("empty zset")
	}
	bucketid, err := strconv.ParseUint(fmt.Sprint(bids[0]), 10, 64)
	if err != nil {
		return 0, errors.Wrapf(err, "key(%s) Max", r.key)
	}
	ids, err := r.bitRange(conn, r.getBucketBmKey(bucketid), 0, capacity)
	if err != nil {
		return 0, errors.Wrapf(err, "key(%s) Max", r.key)
	}
	if len(ids) == 0 {
		return 0, errors.New("empty bucket")
	}
	return bucketid*capacity + ids[len(ids)-1], nil
}

func (r *RedisBitmap) Range(ctx context.Context, seq int) (ids []uint64, next int, err error) {
	if seq < 0 {
		return ids, -1, errors.Wrapf(errors.New("invalid seq"), "key(%s) Range seq(%d)", r.key, seq)
	}
	conn, err := r.pool.GetContext(ctx)
	if err != nil {
		return ids, -1, errors.Wrapf(err, "key(%s) Range seq(%d)", r.key, seq)
	}
	defer conn.Close()
	bids, err := redis.Strings(conn.Do("ZRANGE", r.getZsetKey(), seq, seq))
	if err != nil {
		return ids, -1, errors.Wrapf(err, "key(%s) Range seq(%d)", r.key, seq)
	}
	if len(bids) != 1 {
		return ids, -1, nil
	}
	bucketid, err := strconv.ParseUint(bids[0], 10, 64)
	if err != nil {
		return ids, -1, errors.Wrapf(err, "key(%s) Range seq(%d)", r.key, seq)
	}
	offsets, err := r.bitRange(conn, r.getBucketBmKey(bucketid), 0, capacity)
	if err != nil {
		return ids, -1, errors.Wrapf(err, "key(%s) Range seq(%d)", r.key, seq)
	}
	for _, offset := range offsets {
		ids = append(ids, bucketid*capacity+offset)
	}
	next = seq + 1
	return
}
