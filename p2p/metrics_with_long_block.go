package p2p

import (
	"strconv"
)

var (
	CacheMetricLongBlock []cacheMetricsMsg
)

type cacheMetricsMsg struct {
	fromPeer string
	toPeer   string
	chID     string
	typeIs   string
	size     int
	rawByte  string
}

func ResetCacheMetrics() {
	CacheMetricLongBlock = []cacheMetricsMsg{}
}

func ToStrings() (n [][]string) {
	for _, j := range CacheMetricLongBlock {
		n = append(n, []string{j.fromPeer, j.toPeer, j.chID, j.typeIs, strconv.Itoa(j.size), j.rawByte})
	}
	return n
}
