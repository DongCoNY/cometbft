package consensus

import (
	"fmt"
	"strings"
	"time"

	"github.com/go-kit/kit/metrics"

	cstypes "github.com/cometbft/cometbft/consensus/types"
	cmtproto "github.com/cometbft/cometbft/proto/tendermint/types"
)

const (
	// MetricsSubsystem is a subsystem shared by all metrics exposed by this
	// package.
	MetricsThresholdSubsystem = "2consensus2"
)

//go:generate go run ../scripts/metricsgen -struct=Metrics

// Metrics contains metrics exposed by this package.
type MetricsThreshold struct {
	// Height of the chain.
	Height metrics.Gauge

	// Last height signed by this validator if the node is a validator.
	ValidatorLastSignedHeight metrics.Gauge `metrics_labels:"validator_address"`

	// Number of rounds.
	Rounds metrics.Gauge

	// Histogram of round duration.
	RoundDurationSeconds metrics.Histogram `metrics_buckettype:"exprange" metrics_bucketsizes:"0.1, 100, 8"`

	// Number of validators.
	Validators metrics.Gauge
	// Total power of all validators.
	ValidatorsPower metrics.Gauge
	// Power of a validator.
	ValidatorPower metrics.Gauge `metrics_labels:"validator_address"`
	// Amount of blocks missed per validator.
	ValidatorMissedBlocks metrics.Gauge `metrics_labels:"validator_address"`
	// Number of validators who did not sign.
	MissingValidators metrics.Gauge
	// Total power of the missing validators.
	MissingValidatorsPower metrics.Gauge
	// Number of validators who tried to double sign.
	ByzantineValidators metrics.Gauge
	// Total power of the byzantine validators.
	ByzantineValidatorsPower metrics.Gauge

	// Time between this and the last block.
	BlockIntervalSeconds metrics.Histogram

	// Number of transactions.
	NumTxs metrics.Gauge
	// Size of the block.
	BlockSizeBytes metrics.Gauge
	// Total number of transactions.
	TotalTxs metrics.Gauge
	// The latest block height.
	CommittedHeight metrics.Gauge `metrics_name:"latest_block_height"`
	// Whether or not a node is block syncing. 1 if yes, 0 if no.
	// BlockSyncing metrics.Gauge
	// Whether or not a node is state syncing. 1 if yes, 0 if no.
	// StateSyncing metrics.Gauge

	// Number of block parts transmitted by each peer.
	BlockParts metrics.Counter `metrics_labels:"peer_id"`

	// Histogram of durations for each step in the consensus protocol.
	StepDurationSeconds metrics.Histogram `metrics_labels:"step" metrics_buckettype:"exprange" metrics_bucketsizes:"0.1, 100, 8"`
	stepStart           time.Time

	// Number of block parts received by the node, separated by whether the part
	// was relevant to the block the node is trying to gather or not.
	BlockGossipPartsReceived metrics.Counter `metrics_labels:"matches_current"`

	// QuroumPrevoteMessageDelay is the interval in seconds between the proposal
	// timestamp and the timestamp of the earliest prevote that achieved a quorum
	// during the prevote step.
	//
	// To compute it, sum the voting power over each prevote received, in increasing
	// order of timestamp. The timestamp of the first prevote to increase the sum to
	// be above 2/3 of the total voting power of the network defines the endpoint
	// the endpoint of the interval. Subtract the proposal timestamp from this endpoint
	// to obtain the quorum delay.
	//metrics:Interval in seconds between the proposal timestamp and the timestamp of the earliest prevote that achieved a quorum.
	QuorumPrevoteDelay metrics.Gauge `metrics_labels:"proposer_address"`

	// FullPrevoteDelay is the interval in seconds between the proposal
	// timestamp and the timestamp of the latest prevote in a round where 100%
	// of the voting power on the network issued prevotes.
	//metrics:Interval in seconds between the proposal timestamp and the timestamp of the latest prevote in a round where all validators voted.
	FullPrevoteDelay metrics.Gauge `metrics_labels:"proposer_address"`

	// ProposalReceiveCount is the total number of proposals received by this node
	// since process start.
	// The metric is annotated by the status of the proposal from the application,
	// either 'accepted' or 'rejected'.
	ProposalReceiveCount metrics.Counter `metrics_labels:"status"`

	// ProposalCreationCount is the total number of proposals created by this node
	// since process start.
	ProposalCreateCount metrics.Counter

	// RoundVotingPowerPercent is the percentage of the total voting power received
	// with a round. The value begins at 0 for each round and approaches 1.0 as
	// additional voting power is observed. The metric is labeled by vote type.
	RoundVotingPowerPercent metrics.Gauge `metrics_labels:"vote_type"`

	// LateVotes stores the number of votes that were received by this node that
	// correspond to earlier heights and rounds than this node is currently
	// in.
	LateVotes metrics.Counter `metrics_labels:"vote_type"`

	IsOutTime bool

	timeOldHeight time.Time

	timeThreshold time.Duration

	oldMetric OldMetricsCache
}

type OldMetricsCache struct {
	height                     int64
	cacheMarkProposalProcessed bool
	cacheMarkVoteReceived      []cacheMarkVoteReceived
	cacheLateVote              []string
	round                      int32
	st                         time.Time

	cacheValidatorPowerLastSignedMiss []MarkLabelVal
	validatorsSize                    int
	validatorsPower                   int64
	missingValidators                 int
	missingValidatorsPower            int64
	byzantineValidatorsCount          int64
	byzantineValidatorsPower          int64

	numTxs         int
	totalTxs       int
	blockSizeBytes int

	cacheBlockParts []string

	cacheNotBlockGossipPartsReceived bool

	caheOldQuorumPrevoteDelay []caheOldQuorumPrevoteDelay

	cacheFullPrevoteDelay cacheFullPrevoteDelay

	cacheProposalCreateCount cacheProposalCreateCount

	// cacheSyncing bool
}

func (m *MetricsThreshold) handleIfOutTime() {
	fmt.Println("hannnnnnnnnnnn")
	// ProposalProcessed
	m.handleMarkProposalProcessed()

	// RoundVotingPowerPercent
	m.handleRoundVotingPowerPercent()

	// Round
	m.handleRoundOld()

	// lateVote
	m.handleLastVote()

	// ValidatorLastSignedHeight
	m.handleMarkValidatorPowerLastSignedMiss()

	m.Validators.Set(float64(m.oldMetric.validatorsSize))
	m.ValidatorsPower.Set(float64(m.oldMetric.validatorsPower))

	m.MissingValidators.Set(float64(m.oldMetric.missingValidators))
	m.MissingValidatorsPower.Set(float64(m.oldMetric.missingValidatorsPower))

	m.ByzantineValidators.Set(float64(m.oldMetric.byzantineValidatorsCount))
	m.ByzantineValidatorsPower.Set(float64(m.oldMetric.byzantineValidatorsPower))

	m.NumTxs.Set(float64(m.oldMetric.numTxs))
	m.TotalTxs.Add(float64(m.oldMetric.totalTxs))
	m.BlockSizeBytes.Set(float64(m.oldMetric.blockSizeBytes))
	m.CommittedHeight.Set(float64(m.oldMetric.height))

	m.handleBlockParts()

	m.handleBlockGossipPartsReceived()

	m.handleQuorumPrevoteDelay()

	m.handleFullPrevoteDelay()

	m.handleProposalCreateCount()

}

func (m *MetricsThreshold) MarkStep(s cstypes.RoundStepType) {
	if !m.stepStart.IsZero() {
		stepTime := time.Since(m.stepStart).Seconds()
		stepName := strings.TrimPrefix(s.String(), "RoundStep")
		m.StepDurationSeconds.With("step", stepName).Observe(stepTime)
	}

	m.stepStart = time.Now()
}

func (m *MetricsThreshold) handleRoundOld() {
	m.Rounds.Set(float64(m.oldMetric.round))
	roundTime := time.Since(m.oldMetric.st).Seconds()
	m.RoundDurationSeconds.Observe(roundTime)
	pvt := cmtproto.PrevoteType
	pvn := strings.ToLower(strings.TrimPrefix(pvt.String(), "SIGNED_MSG_TYPE_"))
	m.RoundVotingPowerPercent.With("vote_type", pvn).Set(0)
	pct := cmtproto.PrecommitType
	pcn := strings.ToLower(strings.TrimPrefix(pct.String(), "SIGNED_MSG_TYPE_"))
	m.RoundVotingPowerPercent.With("vote_type", pcn).Set(0)
}

type MarkLabelVal struct {
	markValidatorPower            bool
	markValidatorLastSignedHeight bool
	markValidatorMissedBlocks     bool
	votingPower                   int64
	label                         []string
}

func (m *MetricsThreshold) handleMarkValidatorPowerLastSignedMiss() {
	for _, markLabel := range m.oldMetric.cacheValidatorPowerLastSignedMiss {
		if markLabel.markValidatorPower {
			m.ValidatorPower.With(markLabel.label...).Set(float64(markLabel.votingPower))
		}

		if markLabel.markValidatorLastSignedHeight {
			m.ValidatorLastSignedHeight.With(markLabel.label...).Set(float64(m.oldMetric.height))
		}
		if markLabel.markValidatorMissedBlocks {
			m.ValidatorMissedBlocks.With(markLabel.label...).Add(float64(1))
		}
	}
	// release memory
}

func (m *MetricsThreshold) handleBlockParts() {
	for _, id := range m.oldMetric.cacheBlockParts {
		m.BlockParts.With("peer_id", id).Add(1)
	}
	// release memory
}

func (m *MetricsThreshold) handleLastVote() {
	for _, value := range m.oldMetric.cacheLateVote {
		m.LateVotes.With("vote_type", value).Add(1)
	}
	m.oldMetric.cacheLateVote = []string{}
}

func (m *MetricsThreshold) handleRoundVotingPowerPercent() {
	for _, old := range m.oldMetric.cacheMarkVoteReceived {
		m.RoundVotingPowerPercent.With("vote_type", old.n).Add(old.p)
	}
	// release memory
}

func (m *MetricsThreshold) handleBlockGossipPartsReceived() {
	if m.oldMetric.cacheNotBlockGossipPartsReceived {
		m.BlockGossipPartsReceived.With("matches_current", "false").Add(1)
	} else {
		m.BlockGossipPartsReceived.With("matches_current", "true").Add(1)
	}

}

func (m *MetricsThreshold) handleQuorumPrevoteDelay() {
	for _, j := range m.oldMetric.caheOldQuorumPrevoteDelay {
		m.QuorumPrevoteDelay.With("proposer_address", j.add).Set(j.time)
	}
	// release memory
}

type cacheFullPrevoteDelay struct {
	isHasAll bool
	address  string
	time     float64
}

func (m *MetricsThreshold) handleFullPrevoteDelay() {
	if m.oldMetric.cacheFullPrevoteDelay.isHasAll {
		m.FullPrevoteDelay.With("proposer_address", m.oldMetric.cacheFullPrevoteDelay.address).Set(m.oldMetric.cacheFullPrevoteDelay.time)
	}
}

type cacheProposalCreateCount struct {
	noValidBlocks bool
	count         int64
}

func (m *MetricsThreshold) handleProposalCreateCount() {
	if m.oldMetric.cacheProposalCreateCount.noValidBlocks {
		m.ProposalCreateCount.Add(float64(m.oldMetric.cacheProposalCreateCount.count))
	}
}

func (m *MetricsThreshold) handleMarkProposalProcessed() {
	status := "accepted"
	if !m.oldMetric.cacheMarkProposalProcessed {
		status = "rejected"
	}
	m.ProposalReceiveCount.With("status", status).Add(1)
}

type cacheMarkVoteReceived struct {
	n string
	p float64
}

type caheOldQuorumPrevoteDelay struct {
	add  string
	time float64
}

// type cacheSyncing struct {
// 	blockSync bool
// 	stateSync bool
// }
