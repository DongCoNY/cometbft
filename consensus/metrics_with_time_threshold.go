package consensus

import (
	"encoding/csv"
	"os"
	"path/filepath"
	"strconv"

	"strings"
	"time"

	"github.com/go-kit/kit/metrics"

	cstypes "github.com/cometbft/cometbft/consensus/types"
	cmtos "github.com/cometbft/cometbft/libs/os"
	cmtproto "github.com/cometbft/cometbft/proto/tendermint/types"
)

const (
	// MetricsSubsystem is a subsystem shared by all metrics exposed by this
	// package.
	MetricsThresholdSubsystem = "2consensus2"
)

var (
	pathblockProposalStep string
	pathblockVoteStep     string
	pathblock             string
	pathblockOnlyTimeStep string
)

func init() {
	home, _ := os.UserHomeDir()

	metricspath := filepath.Join(home, "cometbft-metrics")
	if !cmtos.FileExists(metricspath) {
		// create dir metrics
		os.MkdirAll(metricspath, os.ModePerm)

		// create files
		pathblockProposalStep = metricspath + "/blockProposalStep.csv"
		file1, _ := os.Create(pathblockProposalStep)
		defer file1.Close()

		pathblockVoteStep = metricspath + "/blockVoteStep.csv"
		file2, _ := os.Create(pathblockVoteStep)
		defer file2.Close()

		pathblock = metricspath + "/block.csv"
		file3, _ := os.Create(pathblock)
		defer file3.Close()

		pathblockOnlyTimeStep = metricspath + "/blockOnlyTimeStep.csv"
		file4, _ := os.Create(pathblockOnlyTimeStep)
		defer file4.Close()
	} else {
		pathblockProposalStep = metricspath + "/blockProposalStep.csv"
		pathblockVoteStep = metricspath + "/blockVoteStep.csv"
		pathblock = metricspath + "/block.csv"
		pathblockOnlyTimeStep = metricspath + "/blockOnlyTimeStep.csv"
	}
}

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
	BlockSyncing metrics.Gauge
	// Whether or not a node is state syncing. 1 if yes, 0 if no.
	StateSyncing metrics.Gauge

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

	// Time threshold is said to be timeout
	timeThreshold time.Duration
	// Time at the last height update
	timeOldHeight time.Time
	// Cache stores old metric values
	metricsCache metricsCache
}

type metricsCache struct {
	height                      int64
	round                       int32
	numTxs                      int
	blockSizeBytes              int
	blockIntervalSeconds        float64
	proposalProcessed           bool
	blockPartsReceived          []uint32
	notBlockGossipPartsReceived []bool
	quorumPrevoteDelay          []caheOldQuorumPrevoteDelay
	fullPrevoteDelay            cacheFullPrevoteDelay
	lateVote                    []string

	validatorsPower               int64
	missingValidatorsPower        int64
	missingValidatorsPowerPrevote int64

	blockPartsToTal uint32
	blockParts      []uint32

	voteReceived                 []cacheVoteReceived
	st                           time.Time
	validatorPowerLastSignedMiss []cacheLabelVal
	validatorsSize               int
	missingValidators            int
	byzantineValidatorsCount     int64
	byzantineValidatorsPower     int64
	totalTxs                     int

	proposalCreateCount cacheProposalCreateCount
	syncing             cacheSyncing
	step                map[string]float64
}

func (m *MetricsThreshold) handleIfOutTime() {
	m.metricsCache.blockParts = removeDuplicates(m.metricsCache.blockParts)
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

	m.Validators.Set(float64(m.metricsCache.validatorsSize))
	m.ValidatorsPower.Set(float64(m.metricsCache.validatorsPower))

	m.MissingValidators.Set(float64(m.metricsCache.missingValidators))
	m.MissingValidatorsPower.Set(float64(m.metricsCache.missingValidatorsPower))

	m.ByzantineValidators.Set(float64(m.metricsCache.byzantineValidatorsCount))
	m.ByzantineValidatorsPower.Set(float64(m.metricsCache.byzantineValidatorsPower))

	m.NumTxs.Set(float64(m.metricsCache.numTxs))
	m.TotalTxs.Add(float64(m.metricsCache.totalTxs))
	m.BlockSizeBytes.Set(float64(m.metricsCache.blockSizeBytes))
	m.CommittedHeight.Set(float64(m.metricsCache.height))
	m.BlockIntervalSeconds.Observe(m.metricsCache.blockIntervalSeconds)

	m.handleBlockParts()
	m.handleBlockGossipPartsReceived()
	m.handleQuorumPrevoteDelay()
	m.handleFullPrevoteDelay()
	m.handleProposalCreateCount()
	m.handleSyncing()
	m.handleStepDurationSeconds()

	m.handleWriteToFileCSVForEachHeight()
	m.handCSVTimeSet()
	m.handleWriteToFileCSVForVoteStep()
	m.handleWriteToFileCSVForProposalStep()
}

func NopCacheStep() map[string]float64 {
	step := make(map[string]float64, 0)
	step["NewHeight"] = 0
	step["NewRound"] = 0
	step["Propose"] = 0
	step["Prevote"] = 0
	step["PrevoteWait"] = 0
	step["Precommit"] = 0
	step["RoundStepPrecommitWait"] = 0
	step["Commit"] = 0

	return step
}

func (m *MetricsThreshold) handleStepDurationSeconds() {
	for stepName, stepTime := range m.metricsCache.step {
		m.StepDurationSeconds.With("step", stepName).Observe(stepTime)
	}
}

func (m *MetricsThreshold) MarkStep(s cstypes.RoundStepType) {
	if !m.stepStart.IsZero() {
		stepTime := time.Since(m.stepStart).Seconds()
		stepName := strings.TrimPrefix(s.String(), "RoundStep")
		m.metricsCache.step[stepName] = stepTime
	}

	m.stepStart = time.Now()
}

func (m *MetricsThreshold) handleRoundOld() {
	m.Rounds.Set(float64(m.metricsCache.round))
	roundTime := time.Since(m.metricsCache.st).Seconds()
	m.RoundDurationSeconds.Observe(roundTime)
	pvt := cmtproto.PrevoteType
	pvn := strings.ToLower(strings.TrimPrefix(pvt.String(), "SIGNED_MSG_TYPE_"))
	m.RoundVotingPowerPercent.With("vote_type", pvn).Set(0)
	pct := cmtproto.PrecommitType
	pcn := strings.ToLower(strings.TrimPrefix(pct.String(), "SIGNED_MSG_TYPE_"))
	m.RoundVotingPowerPercent.With("vote_type", pcn).Set(0)
}

type cacheLabelVal struct {
	markValidatorPower            bool
	markValidatorLastSignedHeight bool
	markValidatorMissedBlocks     bool
	votingPower                   int64
	label                         []string
}

func (m *MetricsThreshold) handleMarkValidatorPowerLastSignedMiss() {
	for _, markLabel := range m.metricsCache.validatorPowerLastSignedMiss {
		if markLabel.markValidatorPower {
			m.ValidatorPower.With(markLabel.label...).Set(float64(markLabel.votingPower))
		}

		if markLabel.markValidatorLastSignedHeight {
			m.ValidatorLastSignedHeight.With(markLabel.label...).Set(float64(m.metricsCache.height))
		}
		if markLabel.markValidatorMissedBlocks {
			m.ValidatorMissedBlocks.With(markLabel.label...).Add(float64(1))
		}
	}
	// release memory
}

func (m *MetricsThreshold) handleBlockParts() {
	for _, id := range m.metricsCache.blockPartsReceived {
		m.BlockParts.With("peer_id", strconv.FormatInt(int64(id), 10)).Add(1)
	}
	// release memory
}

func (m *MetricsThreshold) handleLastVote() {
	for _, value := range m.metricsCache.lateVote {
		m.LateVotes.With("vote_type", value).Add(1)
	}
	m.metricsCache.lateVote = []string{}
}

func (m *MetricsThreshold) handleRoundVotingPowerPercent() {
	for _, old := range m.metricsCache.voteReceived {
		m.RoundVotingPowerPercent.With("vote_type", old.n).Add(old.p)
	}
	// release memory
}

func (m *MetricsThreshold) handleBlockGossipPartsReceived() {
	for _, j := range m.metricsCache.notBlockGossipPartsReceived {
		if j {
			m.BlockGossipPartsReceived.With("matches_current", "false").Add(1)
		} else {
			m.BlockGossipPartsReceived.With("matches_current", "true").Add(1)
		}
	}

}

func (m *MetricsThreshold) handleQuorumPrevoteDelay() {
	for _, j := range m.metricsCache.quorumPrevoteDelay {
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
	if m.metricsCache.fullPrevoteDelay.isHasAll {
		m.FullPrevoteDelay.With("proposer_address", m.metricsCache.fullPrevoteDelay.address).Set(m.metricsCache.fullPrevoteDelay.time)
	}
}

type cacheProposalCreateCount struct {
	noValidBlocks bool
	count         int64
}

func (m *MetricsThreshold) handleProposalCreateCount() {
	if m.metricsCache.proposalCreateCount.noValidBlocks {
		m.ProposalCreateCount.Add(float64(m.metricsCache.proposalCreateCount.count))
	}
}

func (m *MetricsThreshold) handleMarkProposalProcessed() {
	status := "accepted"
	if !m.metricsCache.proposalProcessed {
		status = "rejected"
	}
	m.ProposalReceiveCount.With("status", status).Add(1)
}

type cacheVoteReceived struct {
	n string
	p float64
}

type caheOldQuorumPrevoteDelay struct {
	add  string
	time float64
}

type cacheSyncing struct {
	switchToConsensus bool
	blockSync         bool
	stateSync         bool
	stateSync2        bool
}

func (m *MetricsThreshold) MarkStateSync() {
	m.metricsCache.syncing.stateSync = true
}

func (m *MetricsThreshold) MarkStateSync2() {
	m.metricsCache.syncing.stateSync2 = true
}

func (m *MetricsThreshold) MarkBlockSync() {
	m.metricsCache.syncing.blockSync = true
}

func (m *MetricsThreshold) handleSyncing() {
	if m.metricsCache.syncing.switchToConsensus {
		m.BlockSyncing.Set(0)
		m.StateSyncing.Set(0)
	}

	if m.metricsCache.syncing.blockSync {
		m.StateSyncing.Set(0)
		m.BlockSyncing.Set(1)
	}

	if m.metricsCache.syncing.stateSync {
		m.StateSyncing.Set(1)
	}

	if m.metricsCache.syncing.stateSync2 {
		m.BlockSyncing.Set(1)
	}
}

func (m *MetricsThreshold) handCSVTimeSet() error {
	file, err := os.OpenFile(pathblockOnlyTimeStep, os.O_WRONLY|os.O_APPEND, os.ModeAppend)
	if err != nil {

		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	err = writer.Write(m.metricsCache.StringForEachStep())
	if err != nil {
		return err
	}
	return nil
}

func (m MetricsThreshold) handleWriteToFileCSVForEachHeight() error {
	file, err := os.OpenFile(pathblock, os.O_WRONLY|os.O_APPEND, os.ModeAppend)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	err = writer.Write(m.metricsCache.StringForEachHeight())
	if err != nil {
		return err
	}
	return nil
}

func (m MetricsThreshold) handleWriteToFileCSVForVoteStep() error {
	file, err := os.OpenFile(pathblockVoteStep, os.O_WRONLY|os.O_APPEND, os.ModeAppend)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	err = writer.Write(m.metricsCache.StringForVotingPrecommitStep())
	if err != nil {
		return err
	}
	return nil
}

func (m MetricsThreshold) handleWriteToFileCSVForProposalStep() error {
	file, err := os.OpenFile(pathblockProposalStep, os.O_WRONLY|os.O_APPEND, os.ModeAppend)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	err = writer.Write(m.metricsCache.StringForProposalStep())
	if err != nil {
		return err
	}
	return nil
}

func (m metricsCache) StringForEachHeight() []string {
	forheight := []string{}
	// Height,
	forheight = append(forheight, strconv.FormatInt(m.height, 10))
	// Rounds,
	forheight = append(forheight, strconv.Itoa(int(m.round)))
	// BlockIntervalSeconds,
	forheight = append(forheight, strconv.FormatFloat(m.blockIntervalSeconds, 'f', -1, 64))
	// NumTxs,
	forheight = append(forheight, strconv.Itoa(m.numTxs))
	// BlockSizeBytes,
	forheight = append(forheight, strconv.Itoa(m.blockSizeBytes))
	// BlockParts,
	forheight = append(forheight, strconv.Itoa(len(m.blockPartsReceived)))

	for _, value := range m.blockPartsReceived {
		forheight = append(forheight, strconv.FormatInt(int64(value), 10))
	}

	// BlockGossipPartsReceived
	for _, j := range m.notBlockGossipPartsReceived {
		if j {
			forheight = append(forheight, "false")
		} else {
			forheight = append(forheight, "true")
		}
	}
	// QuorumPrevoteDelay,
	forheight = append(forheight, strconv.Itoa(len(m.quorumPrevoteDelay)))
	for _, j := range m.quorumPrevoteDelay {
		forheight = append(forheight, j.add)
		forheight = append(forheight, strconv.FormatFloat(j.time, 'f', -1, 64))
	}

	// FullPrevoteDelay,
	forheight = append(forheight, strconv.FormatBool(m.fullPrevoteDelay.isHasAll), m.fullPrevoteDelay.address, strconv.FormatFloat(m.fullPrevoteDelay.time, 'f', -1, 64))
	// ProposalReceiveCount,
	status := "accepted"
	if !m.proposalProcessed {
		status = "rejected"
	}
	forheight = append(forheight, status)
	// LateVotes
	forheight = append(forheight, strconv.Itoa(len(m.lateVote)))
	forheight = append(forheight, m.lateVote...)

	return forheight
}

func (m metricsCache) StringForEachStep() []string {
	forStep := []string{strconv.FormatInt(m.height, 10)}

	for _, timeStep := range m.step {
		forStep = append(forStep, strconv.FormatFloat(timeStep, 'f', -1, 64))
	}
	return forStep
}

func (m metricsCache) StringForVotingPrecommitStep() []string {
	forheight := []string{}

	forheight = append(forheight, strconv.FormatInt(m.height, 10))
	forheight = append(forheight, strconv.Itoa(int(m.round)))

	forheight = append(forheight, strconv.FormatInt(m.validatorsPower, 10))
	forheight = append(forheight, strconv.FormatInt(m.missingValidatorsPowerPrevote, 10))
	forheight = append(forheight, strconv.FormatInt(m.missingValidatorsPower, 10))

	return forheight
}

func (m metricsCache) StringForProposalStep() []string {
	forheight := []string{}

	forheight = append(forheight, strconv.FormatInt(m.height, 10))
	forheight = append(forheight, strconv.Itoa(int(m.round)))

	forheight = append(forheight, strconv.Itoa(len(m.blockPartsReceived)))
	for _, value := range m.blockPartsReceived {
		forheight = append(forheight, strconv.FormatInt(int64(value), 10))
	}

	edundantBlockpartsReceived := filterSlice(m.blockParts, m.blockPartsReceived)
	forheight = append(forheight, strconv.Itoa(len(edundantBlockpartsReceived)))
	forheight = append(forheight, edundantBlockpartsReceived...)

	forheight = append(forheight, strconv.FormatInt(int64(m.blockPartsToTal), 10))
	return forheight
}

func filterSlice(a, b []uint32) []string {
	result := []string{}

	// Create a map to store the elements of slice b for quick inspection
	bMap := make(map[uint32]bool)
	for _, v := range b {
		bMap[v] = true
	}
	// Filter elements of slice a
	for _, v := range a {
		if !bMap[v] {
			result = append(result, strconv.FormatInt(int64(v), 10))
		}
	}

	return result
}

func removeDuplicates(s []uint32) []uint32 {
	encountered := map[uint32]bool{}
	result := []uint32{}

	for _, v := range s {
		if !encountered[v] {
			encountered[v] = true
			result = append(result, v)
		}
	}

	return result
}
