package p2p

import (
	"strconv"
)

var (
	CacheMetricLongBlock []cacheMetricsMsg
)

func init() {
	CacheMetricLongBlock = []cacheMetricsMsg{}
}

type cacheMetricsMsg struct {
	FromPeer string
	ToPeer   string
	ChID     string
	TypeIs   string
	Size     int
	// RawByte  string
}

func ResetCacheMetrics() {
	CacheMetricLongBlock = []cacheMetricsMsg{}
}

func ToStrings() (n [][]string) {
	for _, j := range CacheMetricLongBlock {
		n = append(n, []string{j.FromPeer, j.ToPeer, j.ChID, j.TypeIs, strconv.Itoa(j.Size)})
	}
	return n
}
