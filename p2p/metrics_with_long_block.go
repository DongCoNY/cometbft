package p2p

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
	RawByte  string
	Content  string
}

func ResetCacheMetrics() {
	CacheMetricLongBlock = []cacheMetricsMsg{}
}
