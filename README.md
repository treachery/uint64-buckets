# 使用分片bitmap存储uint64数值

## 业务场景中经常需要存储一批数值，一般情况下使用redis bitmap, 但有两个大缺点
1. redis bitmap最大只支持存储uin32
2. redis bitmap占用内存的大小由最大offset决定，导致存储稀疏时内存浪费严重

## 思路
- 参考roaring-bitmap,将一个超大bitmap切片成多个，每个bucket只存储自己的offset。
- 每个bucket单独存储到redis，这样可以直接在redis操作，避免分布式环境下使用roaring-bitmap需要读到内存序列化

存储uint64需要(2的64次方)位长度的bitmap, 将其切分为(2的48次方)个长度位128位的bitmap.
没有数据的bucket，我们不需要存储，因此可以大大节省内存.

## example
http://github.com/treachery/uint64-buckets/example/main.go

## TODO
支持交叉并集
