package p2p

import (
	"strconv"
)

var (
	CacheMetricLongBlock []cacheMetricsMsg
)

type cacheMetricsMsg struct {
	typeIs string
	size   int
	// rawJson  string
	fromPeer string
	// toPeer   string
	chID string
}

func ResetCacheMetrics() {
	CacheMetricLongBlock = []cacheMetricsMsg{}
}

func ToStrings() (n [][]string) {
	for _, j := range CacheMetricLongBlock {
		n = append(n, []string{j.typeIs, strconv.Itoa(j.size), j.chID, j.fromPeer})
	}
	return n
}

// n := cacheMetricsMsg{
// 	toPeer:   string(e.Src.ID()),
// 	fromPeer: string(p.ID()),
// }
// CacheMetricLongBlock = append(CacheMetricLongBlock, n)
