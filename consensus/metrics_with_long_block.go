package consensus

import (
	"encoding/csv"
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
	metricTimeOut         MetricsThreshold
	pathBlockProposalStep string
	pathBlockVoteStep     string
	pathBlock             string
	pathBlockOnlyTimeStep string
	pathBlockP2P          string
)

func init() {
	metricTimeOut.NopMetricsThreshold()
	metricTimeOut.timeThreshold = 5 * time.Second

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

func (m *MetricsThreshold) NopMetricsThreshold() {
	m.stepStart = time.Now()
	m.metricsCache = NopCacheMetricsCache()
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

type blockHeight struct {
	height                   int64
	numRound                 int
	numTxs                   int
	blockSizeBytes           int
	blockIntervalSeconds     float64
	blockParts               uint32
	blockGossipPartsReceived int
	quorumPrevoteDelay       float64
	fullPrevoteDelay         float64
	proposalReceiveCount     int
	proposalCreateCount      int64
}

type stepProposal struct {
	height  int64
	roundId int64
	step    string

	numblockParts      uint32
	blockPartsReceived int
}

type stepVote struct {
	height  int64
	roundId int64
	step    string

	validatorsPower               int64
	missingValidatorsPowerPrevote int64
}

type stepTime struct {
	height   int64
	roundId  uint32
	stepName string
	stepTime float64
}

type stepMessageP2P struct {
	height  int64
	roundId int64
	step    string

	fromPeer string
	toPeer   string
	chID     string
	msgType  string
	size     int
	rawByte  string
}

type metricsCache struct {
	eachHeight blockHeight

	eachTime     []stepTime
	eachProposal []stepProposal
	eachVote     []stepVote
	eachMsg      []stepMessageP2P

	validatorsPowerTemporary               int64
	missingValidatorsPowerPrevoteTemporary int64

	numblockPartsTemporary      uint32
	blockPartsReceivedTemporary int
}

func (m *MetricsThreshold) WriteToFileCSV() {
	m.CSVEachHeight()
	m.CSVP2P()
	m.CSVProposalStep()
	m.CSVTimeStep()
	m.CSVVoteStep()
}

func NopCacheMetricsCache() metricsCache {
	return metricsCache{
		eachHeight: blockHeight{
			height:                   0,
			numRound:                 0,
			numTxs:                   0,
			blockSizeBytes:           0,
			blockIntervalSeconds:     0.0,
			blockParts:               0,
			blockGossipPartsReceived: 0,
			quorumPrevoteDelay:       0,
			proposalCreateCount:      0,
			proposalReceiveCount:     0,
		},

		eachTime:     []stepTime{},
		eachProposal: []stepProposal{},
		eachVote:     []stepVote{},
		eachMsg:      []stepMessageP2P{},

		validatorsPowerTemporary:               0,
		missingValidatorsPowerPrevoteTemporary: 0,
		numblockPartsTemporary:                 0,
		blockPartsReceivedTemporary:            0,
	}
}

func (m *MetricsThreshold) ResetCache() {
	m.metricsCache.eachHeight.blockGossipPartsReceived = 0
	m.metricsCache.eachHeight.numRound = 0
	m.metricsCache.eachHeight.quorumPrevoteDelay = 0
	m.metricsCache.eachHeight.proposalCreateCount = 0
	m.metricsCache.eachHeight.proposalReceiveCount = 0

	m.metricsCache.eachTime = []stepTime{}
	m.metricsCache.eachProposal = []stepProposal{}
	m.metricsCache.eachVote = []stepVote{}
	m.metricsCache.eachMsg = []stepMessageP2P{}

	m.metricsCache.blockPartsReceivedTemporary = 0
}

func (m MetricsThreshold) CSVEachHeight() error {
	file, err := os.OpenFile(pathBlock, os.O_WRONLY|os.O_APPEND, os.ModeAppend)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	err = writer.Write(m.metricsCache.eachHeight.StringForEachHeight())
	if err != nil {
		return err
	}
	return nil
}

func (m MetricsThreshold) CSVTimeStep() error {
	file, err := os.OpenFile(pathBlockOnlyTimeStep, os.O_WRONLY|os.O_APPEND, os.ModeAppend)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	for _, j := range m.metricsCache.StringEachStep() {
		err = writer.Write(j)
		if err != nil {
			return err
		}
	}

	return nil
}

func (m MetricsThreshold) CSVVoteStep() error {
	file, err := os.OpenFile(pathBlockVoteStep, os.O_WRONLY|os.O_APPEND, os.ModeAppend)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	for _, j := range m.metricsCache.StringEachVoteStep() {
		err = writer.Write(j)
		if err != nil {
			return err
		}
	}

	return nil
}

func (m MetricsThreshold) CSVProposalStep() error {
	file, err := os.OpenFile(pathBlockProposalStep, os.O_WRONLY|os.O_APPEND, os.ModeAppend)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	for _, j := range m.metricsCache.StringForProposalStep() {
		err = writer.Write(j)
		if err != nil {
			return err
		}
	}

	return nil
}

func (m MetricsThreshold) CSVP2P() error {
	file, err := os.OpenFile(pathBlockP2P, os.O_WRONLY|os.O_APPEND, os.ModeAppend)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	for _, j := range m.metricsCache.StringForP2PStep() {
		err = writer.Write(j)
		if err != nil {
			return err
		}
	}

	return nil
}

func (m blockHeight) StringForEachHeight() []string {
	forheight := []string{}
	// Height,
	forheight = append(forheight, strconv.FormatInt(m.height, 10))
	// Rounds,
	forheight = append(forheight, strconv.Itoa(m.numRound))
	// BlockIntervalSeconds,
	forheight = append(forheight, strconv.FormatFloat(m.blockIntervalSeconds, 'f', -1, 64))
	// NumTxs,
	forheight = append(forheight, strconv.Itoa(m.numTxs))
	// BlockSizeBytes,
	forheight = append(forheight, strconv.Itoa(m.blockSizeBytes))
	// BlockParts,
	forheight = append(forheight, strconv.Itoa(int(m.blockParts)))

	// BlockGossipPartsReceived
	forheight = append(forheight, strconv.Itoa(m.blockGossipPartsReceived))

	// QuorumPrevoteDelay,
	forheight = append(forheight, strconv.FormatFloat(m.quorumPrevoteDelay, 'f', -1, 64))

	// full delay
	forheight = append(forheight, strconv.FormatFloat(m.fullPrevoteDelay, 'f', -1, 64))

	// ProposalReceiveCount,
	forheight = append(forheight, strconv.Itoa(m.proposalReceiveCount))

	return forheight
}

func (m metricsCache) StringEachStep() [][]string {
	forStep := [][]string{}

	for _, timeStep := range m.eachTime {
		tmp := []string{}
		tmp = append(tmp, strconv.FormatInt(timeStep.height, 10))
		tmp = append(tmp, strconv.FormatInt(int64(timeStep.roundId), 10))
		tmp = append(tmp, timeStep.stepName)
		tmp = append(tmp, strconv.FormatFloat(timeStep.stepTime, 'f', -1, 64))

		forStep = append(forStep, tmp)
	}
	return forStep
}

func (m metricsCache) StringEachVoteStep() [][]string {
	forStep := [][]string{}
	for _, voteStep := range m.eachVote {
		tmp := []string{}
		tmp = append(tmp, strconv.FormatInt(voteStep.height, 10))
		tmp = append(tmp, strconv.FormatInt(voteStep.roundId, 10))
		tmp = append(tmp, voteStep.step)
		tmp = append(tmp, strconv.FormatInt(voteStep.validatorsPower, 10))
		tmp = append(tmp, strconv.FormatInt(voteStep.missingValidatorsPowerPrevote, 10))

		forStep = append(forStep, tmp)
	}
	return forStep
}

func (m metricsCache) StringForProposalStep() [][]string {
	forStep := [][]string{}
	for _, voteStep := range m.eachProposal {
		tmp := []string{}
		tmp = append(tmp, strconv.FormatInt(voteStep.height, 10))
		tmp = append(tmp, strconv.FormatInt(int64(voteStep.roundId), 10))
		tmp = append(tmp, voteStep.step)
		tmp = append(tmp, strconv.FormatInt(int64(voteStep.numblockParts), 10))
		tmp = append(tmp, strconv.FormatInt(int64(voteStep.blockPartsReceived), 10))

		forStep = append(forStep, tmp)
	}
	return forStep
}

func (m metricsCache) StringForP2PStep() [][]string {
	forStep := [][]string{}
	for _, voteStep := range m.eachMsg {
		tmp := []string{}
		tmp = append(tmp, strconv.FormatInt(voteStep.height, 10))
		tmp = append(tmp, strconv.FormatInt(int64(voteStep.roundId), 10))
		tmp = append(tmp, voteStep.step)
		tmp = append(tmp, voteStep.fromPeer)
		tmp = append(tmp, voteStep.toPeer)
		tmp = append(tmp, voteStep.chID)
		tmp = append(tmp, voteStep.msgType)
		tmp = append(tmp, strconv.Itoa(voteStep.size))
		tmp = append(tmp, voteStep.rawByte)

		forStep = append(forStep, tmp)
	}
	return forStep
}

func (m *MetricsThreshold) MarkStepTimes(s cstypes.RoundStepType, height int64, roundID uint32) {
	if !m.stepStart.IsZero() {
		stepT := time.Since(m.stepStart).Seconds()
		stepN := strings.TrimPrefix(s.String(), "RoundStep")
		m.metricsCache.eachTime = append(m.metricsCache.eachTime, stepTime{height: height, roundId: roundID, stepName: stepN, stepTime: stepT})
	}

	m.stepStart = time.Now()
}

func (m *MetricsThreshold) handleSaveNewStep(height int64, roundId int64, step string) {
	step = strings.TrimPrefix(step, "RoundStep")
	m.metricsCache.eachProposal = append(m.metricsCache.eachProposal, stepProposal{
		height:             height,
		roundId:            roundId,
		step:               step,
		numblockParts:      m.metricsCache.numblockPartsTemporary,
		blockPartsReceived: m.metricsCache.blockPartsReceivedTemporary,
	})

	m.metricsCache.eachVote = append(m.metricsCache.eachVote, stepVote{
		height:                        height,
		roundId:                       roundId,
		step:                          step,
		validatorsPower:               m.metricsCache.validatorsPowerTemporary,
		missingValidatorsPowerPrevote: m.metricsCache.missingValidatorsPowerPrevoteTemporary,
	})

	for _, msg := range p2p.CacheMetricLongBlock {
		m.metricsCache.eachMsg = append(m.metricsCache.eachMsg, stepMessageP2P{
			height:  height,
			roundId: roundId,
			step:    step,

			fromPeer: msg.FromPeer,
			toPeer:   msg.ToPeer,
			chID:     msg.ChID,
			msgType:  msg.TypeIs,
			size:     msg.Size,
			rawByte:  msg.RawByte,
		})
	}
	m.metricsCache.numblockPartsTemporary = 0
	m.metricsCache.blockPartsReceivedTemporary = 0

	m.metricsCache.validatorsPowerTemporary = 0
	m.metricsCache.missingValidatorsPowerPrevoteTemporary = 0
	p2p.ResetCacheMetrics()
}
