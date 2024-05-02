package consensus

import (
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"strconv"

	"strings"
	"time"

	cstypes "github.com/cometbft/cometbft/consensus/types"
	cmtos "github.com/cometbft/cometbft/libs/os"
	"github.com/cometbft/cometbft/p2p"
)

var (
	pathBlockProposalStep string
	pathBlockVoteStep     string
	pathBlock             string
	pathBlockOnlyTimeStep string
	pathBlockP2P          string
)

func init() {
	home, _ := os.UserHomeDir()

	metricspath := filepath.Join(home, "cometbft-metrics")
	if !cmtos.FileExists(metricspath) {
		// create dir metrics
		os.MkdirAll(metricspath, os.ModePerm)

		// create files
		pathBlockProposalStep = metricspath + "/blockProposalStep.csv"
		file1, _ := os.Create(pathBlockProposalStep)
		defer file1.Close()

		pathBlockVoteStep = metricspath + "/blockVoteStep.csv"
		file2, _ := os.Create(pathBlockVoteStep)
		defer file2.Close()

		pathBlock = metricspath + "/block.csv"
		file3, _ := os.Create(pathBlock)
		defer file3.Close()

		pathBlockOnlyTimeStep = metricspath + "/blockOnlyTimeStep.csv"
		file4, _ := os.Create(pathBlockOnlyTimeStep)
		defer file4.Close()

		pathBlockP2P = metricspath + "/blockP2P.csv"
		file5, _ := os.Create(pathBlockP2P)
		defer file5.Close()
	} else {
		pathBlockProposalStep = metricspath + "/blockProposalStep.csv"
		pathBlockVoteStep = metricspath + "/blockVoteStep.csv"
		pathBlock = metricspath + "/block.csv"
		pathBlockOnlyTimeStep = metricspath + "/blockOnlyTimeStep.csv"
		pathBlockP2P = metricspath + "/blockP2P.csv"
	}
}

func NopMetricsThreshold() *MetricsThreshold {
	return &MetricsThreshold{
		stepStart:    time.Now(),
		metricsCache: NopCacheMetricsCache(),
	}
}

// Metrics contains metrics exposed by this package.
type MetricsThreshold struct {
	stepStart time.Time
	// Time threshold is said to be timeout
	timeThreshold time.Duration
	// Time at the last height update
	timeOldHeight time.Time
	// Cache stores old metric values
	metricsCache metricsCache
}

type metricsCache struct {
	height               int64
	round                int32
	numTxs               int
	blockSizeBytes       int
	blockIntervalSeconds float64
	proposalProcessed    bool

	notBlockGossipPartsReceived []bool
	quorumPrevoteDelay          []caheOldQuorumPrevoteDelay
	fullPrevoteDelay            cacheFullPrevoteDelay
	lateVote                    []string

	validatorsPower               int64
	missingValidatorsPower        int64
	missingValidatorsPowerPrevote int64

	numblockParts      uint32
	blockParts         []uint32
	blockPartsReceived []uint32

	proposalCreateCount cacheProposalCreateCount

	// step name and duration step
	steps map[string]float64
}

func (m *MetricsThreshold) handleIfOutTime() {
	m.metricsCache.blockParts = removeDuplicates(m.metricsCache.blockParts)

	m.handleWriteToFileCSVForEachHeight()
	m.handCSVTimeSet()
	m.handleWriteToFileCSVForVoteStep()
	m.handleWriteToFileCSVForProposalStep()
	m.handleP2P()
}

func NopCacheMetricsCache() metricsCache {
	return metricsCache{
		height:               -1,
		round:                -1,
		numTxs:               -1,
		blockSizeBytes:       -1,
		blockIntervalSeconds: -1.0,
		proposalProcessed:    false,

		notBlockGossipPartsReceived: []bool{},
		quorumPrevoteDelay:          []caheOldQuorumPrevoteDelay{},
		fullPrevoteDelay:            cacheFullPrevoteDelay{},
		lateVote:                    []string{},

		validatorsPower:               -1,
		missingValidatorsPower:        -1,
		missingValidatorsPowerPrevote: -1,

		numblockParts:      0,
		blockParts:         []uint32{},
		blockPartsReceived: []uint32{},

		proposalCreateCount: cacheProposalCreateCount{},

		steps: NopCacheStep(),
	}
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

func (m *MetricsThreshold) MarkStep(s cstypes.RoundStepType) {
	if !m.stepStart.IsZero() {
		stepTime := time.Since(m.stepStart).Seconds()
		stepName := strings.TrimPrefix(s.String(), "RoundStep")
		m.metricsCache.steps[stepName] = stepTime
	}

	m.stepStart = time.Now()
}

type cacheLabelVal struct {
	markValidatorPower            bool
	markValidatorLastSignedHeight bool
	markValidatorMissedBlocks     bool
	votingPower                   int64
	label                         []string
}

type cacheFullPrevoteDelay struct {
	isHasAll bool
	address  string
	time     float64
}

type cacheProposalCreateCount struct {
	noValidBlocks bool
	count         int64
}

type caheOldQuorumPrevoteDelay struct {
	add  string
	time float64
}

func (m *MetricsThreshold) handCSVTimeSet() error {
	file, err := os.OpenFile(pathBlockOnlyTimeStep, os.O_WRONLY|os.O_APPEND, os.ModeAppend)
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
	file, err := os.OpenFile(pathBlock, os.O_WRONLY|os.O_APPEND, os.ModeAppend)
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
	file, err := os.OpenFile(pathBlockVoteStep, os.O_WRONLY|os.O_APPEND, os.ModeAppend)
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
	file, err := os.OpenFile(pathBlockProposalStep, os.O_WRONLY|os.O_APPEND, os.ModeAppend)
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

func (m MetricsThreshold) handleP2P() error {
	file, err := os.OpenFile(pathBlockP2P, os.O_WRONLY|os.O_APPEND, os.ModeAppend)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	for _, j := range p2p.ToStrings() {
		fmt.Println("222222")
		n := []string{}
		// Height,
		n = append(n, strconv.FormatInt(m.metricsCache.height, 10))
		// Rounds,
		n = append(n, strconv.Itoa(int(m.metricsCache.round)))

		n = append(n, j...)

		err = writer.Write(n)
		if err != nil {
			return err
		}
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

	for _, timeStep := range m.steps {
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

	forheight = append(forheight, strconv.FormatInt(int64(m.numblockParts), 10))
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
