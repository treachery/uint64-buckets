package uint64_buckets

const (
	capacity = 256
)

func getBucketId(number uint64) uint64 {
	return number / capacity
}

func getOffset(number uint64) uint64 {
	return number % capacity
}
