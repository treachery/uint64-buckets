package uint64_buckets

import (
	"github.com/opencensus-integrations/redigo/redis"
	"github.com/pkg/errors"
)

var nums = [8]uint8{
	uint8(1 << 7),
	uint8(1 << 6),
	uint8(1 << 5),
	uint8(1 << 4),
	uint8(1 << 3),
	uint8(1 << 2),
	uint8(1 << 1),
	uint8(1 << 0),
}

// BitRange 计算下标表
// str: 计算的字符串
// start: 开始的座标
// offset: 偏移值
// size: 查询个数
func bitRange(str []byte, start, offset, size uint64) []uint64 {
	var bits []uint64
	k := uint64(0)
	for i, b := range str {
		// 按位，依次判断0-7下标每位是否为真
		for j, num := range nums {
			if b&num != num {
				continue
			}
			// redis 存储offset 是从左向右存
			k = uint64(i*8 + j)
			if offset <= k && k < offset+size {
				bits = append(bits, start*8+k)
			}
		}
	}
	return bits
}

// GetBitRange 按位查询 返回bit位为1的下标
// client: redis的client
// key: redis 存储的key
// cur: 开始位置
// size: 查询个数
func (r *RedisBitmap) bitRange(conn redis.Conn, key string, cur, size uint64) ([]uint64, error) {
	start := cur / 8
	// end必须按8取整
	end := (cur + size + 7) / 8
	str, err := redis.Bytes(conn.Do("GETRANGE", key, start, end))
	if err != nil {
		return nil, errors.Wrapf(err, "BitRange key(%s) cur(%d) size(%d)", key, cur, size)
	}
	bits := bitRange(str, start, cur%8, size)
	return bits, nil
}
