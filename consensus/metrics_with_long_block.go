package consensus

import (
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"

	"strings"
	"time"

	cstypes "github.com/cometbft/cometbft/consensus/types"
	cmtos "github.com/cometbft/cometbft/libs/os"
	"github.com/cometbft/cometbft/p2p"
	"github.com/cometbft/cometbft/types"
)

var (
	metricTimeOut         MetricsThreshold
	pathBlockProposalStep string
	pathBlockVoteStep     string
	pathBlock             string
	pathBlockOnlyTimeStep string
	pathBlockP2P          string
	pathRoundVoteSet      string
)

func init() {
	metricTimeOut.metricsCache = NopCacheMetricsCache()
	metricTimeOut.timeThreshold = 0 * time.Second

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

		pathRoundVoteSet = metricspath + "/RoundVoteSet.csv"
		file6, _ := os.Create(pathRoundVoteSet)
		defer file6.Close()
	} else {
		pathBlockProposalStep = metricspath + "/blockProposalStep.csv"
		pathBlockVoteStep = metricspath + "/blockVoteStep.csv"
		pathBlock = metricspath + "/block.csv"
		pathBlockOnlyTimeStep = metricspath + "/blockOnlyTimeStep.csv"
		pathBlockP2P = metricspath + "/blockP2P.csv"
		pathRoundVoteSet = metricspath + "/RoundVoteSet.csv"
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

type blockHeight struct {
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

type roundProposal struct {
	roundId   int64
	blockSize int
	numTxs    int

	numBlockParts      uint32
	blockPartsSend     int
	blockPartsReceived int
}

type stepVote struct {
	roundId int64
	step    string

	numVoteReceived               int
	numVoteSent                   int
	validatorsPower               int64
	missingValidatorsPowerPrevote int64
}

type stepTime struct {
	roundId  uint32
	stepName string
	stepTime float64
}

type stepMessageP2P struct {
	roundId int64
	step    string

	fromPeer string
	toPeer   string
	chID     string
	msgType  string
	size     int
	rawByte  string
	content  string
}

// Prevote và precommit for round
type roundVoteSet struct {
	roundId uint32
	votes   []*types.Vote
}

type metricsCache struct {
	height      int64
	isLongBlock bool

	eachHeight   blockHeight
	eachTime     []stepTime
	eachProposal []roundProposal
	eachVote     []stepVote
	eachMsg      []stepMessageP2P
	roundVotes   []roundVoteSet

	numVoteSentTemporary                   int
	numVoteReceivedTemporary               int
	validatorsPowerTemporary               int64
	missingValidatorsPowerPrevoteTemporary int64

	blockSizeTemporary          int
	blockPartsSendTemporary     int
	numTxsTemporary             int
	numblockPartsTemporary      uint32
	blockPartsReceivedTemporary int
}

func (m *MetricsThreshold) WriteToFileCSV() {
	if metricTimeOut.metricsCache.eachHeight.blockIntervalSeconds > 5 {
		m.metricsCache.isLongBlock = true
		m.CSVP2P()
	} else {
		m.metricsCache.isLongBlock = false
	}
	m.CSVEachHeight()
	m.CSVProposalStep()
	m.CSVTimeStep()
	m.CSVVoteStep()
	m.CSVRoundVoteSet()
}

func NopCacheMetricsCache() metricsCache {
	return metricsCache{
		height: 0,
		eachHeight: blockHeight{
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
		eachProposal: []roundProposal{},
		eachVote:     []stepVote{},
		eachMsg:      []stepMessageP2P{},
		roundVotes:   []roundVoteSet{},

		validatorsPowerTemporary:               0,
		missingValidatorsPowerPrevoteTemporary: 0,
		numVoteSentTemporary:                   0,
		numVoteReceivedTemporary:               0,

		numTxsTemporary:             0,
		blockSizeTemporary:          0,
		blockPartsSendTemporary:     0,
		numblockPartsTemporary:      0,
		blockPartsReceivedTemporary: 0,
	}
}

func (m *MetricsThreshold) ResetCache() {
	m.metricsCache.eachHeight.blockGossipPartsReceived = 0
	m.metricsCache.eachHeight.numRound = 0
	m.metricsCache.eachHeight.quorumPrevoteDelay = 0
	m.metricsCache.eachHeight.proposalCreateCount = 0
	m.metricsCache.eachHeight.proposalReceiveCount = 0

	m.metricsCache.eachTime = []stepTime{}
	m.metricsCache.roundVotes = []roundVoteSet{}
	m.metricsCache.eachProposal = []roundProposal{}
	m.metricsCache.eachVote = []stepVote{}
	m.metricsCache.eachMsg = []stepMessageP2P{}
}

func (m MetricsThreshold) CSVEachHeight() error {
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

func (m MetricsThreshold) CSVTimeStep() error {
	file, err := os.OpenFile(pathBlockOnlyTimeStep, os.O_WRONLY|os.O_APPEND, os.ModeAppend)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	for _, j := range m.metricsCache.StringEachTimeStep() {
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

func (m MetricsThreshold) CSVRoundVoteSet() error {
	file, err := os.OpenFile(pathRoundVoteSet, os.O_WRONLY|os.O_APPEND, os.ModeAppend)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	for _, j := range m.metricsCache.StringForVoteSet() {
		err = writer.Write(j)
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
	// islongblock
	forheight = append(forheight, strconv.FormatBool(m.isLongBlock))
	// Rounds,
	forheight = append(forheight, strconv.Itoa(m.eachHeight.numRound))
	// BlockIntervalSeconds,
	forheight = append(forheight, strconv.FormatFloat(m.eachHeight.blockIntervalSeconds, 'f', -1, 64))
	// NumTxs,
	forheight = append(forheight, strconv.Itoa(m.eachHeight.numTxs))
	// BlockSizeBytes,
	forheight = append(forheight, strconv.Itoa(m.eachHeight.blockSizeBytes))
	// BlockParts,
	forheight = append(forheight, strconv.Itoa(int(m.eachHeight.blockParts)))

	// BlockGossipPartsReceived
	forheight = append(forheight, strconv.Itoa(m.eachHeight.blockGossipPartsReceived))

	// QuorumPrevoteDelay,
	forheight = append(forheight, strconv.FormatFloat(m.eachHeight.quorumPrevoteDelay, 'f', -1, 64))

	// full delay
	forheight = append(forheight, strconv.FormatFloat(m.eachHeight.fullPrevoteDelay, 'f', -1, 64))

	// ProposalReceiveCount,
	forheight = append(forheight, strconv.Itoa(m.eachHeight.proposalReceiveCount))

	// proposalCreateCount,
	forheight = append(forheight, strconv.Itoa(int(m.eachHeight.proposalCreateCount)))

	return forheight
}

func (m metricsCache) StringEachTimeStep() [][]string {
	forStep := [][]string{}

	for _, timeStep := range m.eachTime {
		tmp := []string{}
		tmp = append(tmp, strconv.FormatInt(m.height, 10))
		tmp = append(tmp, strconv.FormatBool(m.isLongBlock))
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
		tmp = append(tmp, strconv.FormatInt(m.height, 10))
		tmp = append(tmp, strconv.FormatBool(m.isLongBlock))
		tmp = append(tmp, strconv.FormatInt(voteStep.roundId, 10))
		tmp = append(tmp, voteStep.step)
		tmp = append(tmp, strconv.FormatInt(int64(voteStep.numVoteReceived), 10))
		tmp = append(tmp, strconv.FormatInt(int64(voteStep.numVoteSent), 10))
		tmp = append(tmp, strconv.FormatInt(voteStep.missingValidatorsPowerPrevote, 10))
		tmp = append(tmp, strconv.FormatInt(voteStep.validatorsPower, 10))

		forStep = append(forStep, tmp)
	}
	return forStep
}

func (m metricsCache) StringForProposalStep() [][]string {
	forStep := [][]string{}
	for _, proposal := range m.eachProposal {
		tmp := []string{}
		tmp = append(tmp, strconv.FormatInt(m.height, 10))
		tmp = append(tmp, strconv.FormatBool(m.isLongBlock))
		tmp = append(tmp, strconv.FormatInt(int64(proposal.roundId), 10))
		tmp = append(tmp, strconv.FormatInt(int64(proposal.blockSize), 10))
		tmp = append(tmp, strconv.FormatInt(int64(proposal.numTxs), 10))
		tmp = append(tmp, strconv.FormatInt(int64(proposal.blockPartsSend), 10))
		tmp = append(tmp, strconv.FormatInt(int64(proposal.blockPartsReceived), 10))
		tmp = append(tmp, strconv.FormatInt(int64(proposal.numBlockParts), 10))

		forStep = append(forStep, tmp)
	}
	return forStep
}

func (m metricsCache) StringForP2PStep() [][]string {
	forStep := [][]string{}
	for _, msg := range m.eachMsg {
		tmp := []string{}
		tmp = append(tmp, strconv.FormatInt(m.height, 10))
		tmp = append(tmp, strconv.FormatInt(int64(msg.roundId), 10))
		tmp = append(tmp, msg.step)
		tmp = append(tmp, msg.fromPeer)
		tmp = append(tmp, msg.toPeer)
		tmp = append(tmp, msg.chID)
		tmp = append(tmp, msg.msgType)
		tmp = append(tmp, strconv.Itoa(msg.size))
		// tmp = append(tmp, msg.rawByte)
		tmp = append(tmp, handleContent(msg.msgType, msg.content))

		forStep = append(forStep, tmp)
	}
	return forStep
}

func handleContent(msgTypes, content string) string {
	if msgTypes == "consensus_HasVote" {
		re := regexp.MustCompile(`(type:\S+).*?(index:\d+)`)
		match := re.FindStringSubmatch(content)
		result := strings.Join(match[1:], " ")
		return result
	}
	if msgTypes == "consensus_Vote" {
		re := regexp.MustCompile(`(type:\S+).*?(validator_index:\d+)`)
		match := re.FindStringSubmatch(content)
		result := strings.Join(match[1:], " ")
		return result
	}
	if msgTypes == "mempool_Txs" {
		return ""
	}
	return content
}

func (m metricsCache) StringForVoteSet() [][]string {
	forStep := [][]string{}
	fmt.Println("votttttttttt l=", len(m.roundVotes))

	for _, round := range m.roundVotes {
		for _, j := range round.votes {
			tmp := []string{}
			tmp = append(tmp, strconv.FormatInt(m.height, 10))
			tmp = append(tmp, strconv.FormatBool(m.isLongBlock))
			tmp = append(tmp, strconv.FormatInt(int64(round.roundId), 10))
			tmp = append(tmp, j.Type.String())
			tmp = append(tmp, j.BlockID.String())
			tmp = append(tmp, j.Timestamp.GoString())
			tmp = append(tmp, j.ValidatorAddress.String())
			tmp = append(tmp, strconv.FormatInt(int64(j.ValidatorIndex), 10))
			tmp = append(tmp, fmt.Sprintf("%v", j.Signature))
			forStep = append(forStep, tmp)
		}
	}
	return forStep
}

func (m *MetricsThreshold) MarkStepTimes(s cstypes.RoundStepType, roundID uint32) {
	if !m.stepStart.IsZero() {
		stepT := time.Since(m.stepStart).Seconds()
		stepN := strings.TrimPrefix(s.String(), "RoundStep")
		m.metricsCache.eachTime = append(m.metricsCache.eachTime, stepTime{roundId: roundID, stepName: stepN, stepTime: stepT})
	}

	m.stepStart = time.Now()
}

func (m *MetricsThreshold) handleSaveNewStep(roundId int64, step string) {
	step = strings.TrimPrefix(step, "RoundStep")

	m.metricsCache.eachVote = append(m.metricsCache.eachVote, stepVote{
		roundId:                       roundId,
		step:                          step,
		numVoteReceived:               m.metricsCache.numVoteReceivedTemporary,
		numVoteSent:                   m.metricsCache.numVoteSentTemporary,
		validatorsPower:               m.metricsCache.validatorsPowerTemporary,
		missingValidatorsPowerPrevote: m.metricsCache.missingValidatorsPowerPrevoteTemporary,
	})

	for _, msg := range p2p.CacheMetricLongBlock {
		m.metricsCache.eachMsg = append(m.metricsCache.eachMsg, stepMessageP2P{
			roundId: roundId,
			step:    step,

			fromPeer: msg.FromPeer,
			toPeer:   msg.ToPeer,
			chID:     msg.ChID,
			msgType:  msg.TypeIs,
			size:     msg.Size,
			rawByte:  msg.RawByte,
			content:  msg.Content,
		})
	}

	m.metricsCache.numVoteReceivedTemporary = 0
	m.metricsCache.numVoteSentTemporary = 0
	m.metricsCache.validatorsPowerTemporary = 0
	m.metricsCache.missingValidatorsPowerPrevoteTemporary = 0
	p2p.ResetCacheMetrics()
}

func (m *MetricsThreshold) handleSaveNewRound(roundId int64) {
	m.metricsCache.eachProposal = append(m.metricsCache.eachProposal, roundProposal{
		roundId:            roundId,
		blockSize:          m.metricsCache.blockSizeTemporary,
		numTxs:             m.metricsCache.numTxsTemporary,
		blockPartsSend:     m.metricsCache.blockPartsSendTemporary,
		numBlockParts:      m.metricsCache.numblockPartsTemporary,
		blockPartsReceived: m.metricsCache.blockPartsReceivedTemporary,
	})

	m.metricsCache.blockSizeTemporary = 0
	m.metricsCache.blockPartsSendTemporary = 0
	m.metricsCache.numTxsTemporary = 0
	m.metricsCache.numblockPartsTemporary = 0
	m.metricsCache.blockPartsReceivedTemporary = 0
}
