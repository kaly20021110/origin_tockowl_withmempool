package consensus

import (
	"bft/mvba/core"
	"bft/mvba/crypto"
	"bft/mvba/logger"
	"bytes"
	"encoding/gob"
	"reflect"
	"strconv"
)

// const MAXCOUNT int = 500 //和这个数量有关系，共识延迟，具体而言应该就是,这个是超级参数，如果值太大会导致mempool一直请求；但是会导致共识延迟很大
const (
	STOP0 int8 = iota
	STOP1
	STOP2
	STOP3
	STOP4
	STOP5
	STOP6
	STOP7
	STOP8
	STOP9
)

const (
	CBC_ONE_PHASE int8 = iota
	CBC_TWO_PHASE
	CBC_THREE_PHASE
	LAST
)

const (
	VOTE_FLAG_YES int8 = iota
	VOTE_FLAG_NO
)

type Validator interface {
	Verify(core.Committee) bool
}

type ConsensusBlock struct {
	Proposer   core.NodeID
	Epoch      int64
	PreHash    crypto.Digest
	PreNodeId  core.NodeID
	Referrence []crypto.Digest
	// Maxcount   int
}

// , maxcount int
func NewConsensusBlock(proposer core.NodeID, Epoch int64, prehash crypto.Digest, PreNodeId core.NodeID, referrence []crypto.Digest) *ConsensusBlock {
	return &ConsensusBlock{
		Proposer:   proposer,
		Epoch:      Epoch,
		PreHash:    prehash,
		PreNodeId:  PreNodeId,
		Referrence: referrence,
		// Maxcount:   maxcount,
	}
}

func (b *ConsensusBlock) Encode() ([]byte, error) {
	buf := bytes.NewBuffer(nil)
	if err := gob.NewEncoder(buf).Encode(b); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (b *ConsensusBlock) Decode(data []byte) error {
	buf := bytes.NewBuffer(data)
	if err := gob.NewDecoder(buf).Decode(b); err != nil {
		return err
	}
	return nil
}

func (b *ConsensusBlock) Hash() crypto.Digest {
	hasher := crypto.NewHasher()
	hasher.Add(strconv.AppendInt(nil, int64(b.Proposer), 2))
	hasher.Add(strconv.AppendInt(nil, b.Epoch, 2))
	hasher.Add(b.PreHash[:])
	hasher.Add(strconv.AppendInt(nil, int64(b.PreNodeId), 2))
	// for i := 0; i < b.maxcount; i++ {
	// 	hasher.Add(b.referrence[i][:])
	// }
	for _, d := range b.Referrence {
		hasher.Add(d[:])
	}
	return hasher.Sum256(nil)
}

// 是否要定义Halt消息我不确定
const (
	CBCProposalType int = iota
	CBCVoteType
	FinishType
	ElectShareType
	BestMsgType
	RequestBlockType
	ReplyBlockType
)

var DefaultMessageTypeMap = map[int]reflect.Type{
	CBCProposalType:  reflect.TypeOf(CBCProposal{}),
	CBCVoteType:      reflect.TypeOf(CBCVote{}),
	FinishType:       reflect.TypeOf(Finish{}),
	ElectShareType:   reflect.TypeOf(ElectShare{}),
	BestMsgType:      reflect.TypeOf(BestMsg{}),
	RequestBlockType: reflect.TypeOf(RequestBlockMsg{}),
	ReplyBlockType:   reflect.TypeOf(ReplyBlockMsg{}),
}

type CBCProposal struct {
	Author    core.NodeID
	Epoch     int64
	Phase     int8
	B         *ConsensusBlock
	ParentQC1 QuorumCert
	VoteQC    []byte
	Signature crypto.Signature
}

func NewCBCProposal(author core.NodeID, epoch int64, phase int8, B *ConsensusBlock, parentqc1 QuorumCert, qc []byte, sigService *crypto.SigService) (*CBCProposal, error) {
	p := &CBCProposal{
		Author:    author,
		Epoch:     epoch,
		Phase:     phase,
		B:         B,
		ParentQC1: parentqc1,
		VoteQC:    qc,
	}
	sig, err := sigService.RequestSignature(p.Hash())
	if err != nil {
		return nil, err
	}
	p.Signature = sig
	return p, nil
}

func (p *CBCProposal) Verify(committee core.Committee) bool {
	pub := committee.Name(p.Author)
	return p.Signature.Verify(pub, p.Hash())
}

func (p *CBCProposal) Hash() crypto.Digest {
	hasher := crypto.NewHasher()
	hasher.Add(strconv.AppendInt(nil, int64(p.Author), 2))
	hasher.Add(strconv.AppendInt(nil, p.Epoch, 2))
	hasher.Add(strconv.AppendInt(nil, int64(p.Phase), 2))
	if p.B != nil {
		d := p.B.Hash()
		hasher.Add(d[:])
	}
	if p.VoteQC != nil {
		hasher.Add(p.VoteQC)
	}
	return hasher.Sum256(nil)

}

func (*CBCProposal) MsgType() int {
	return CBCProposalType
}

func (*CBCProposal) Module() string {
	return "consensus"
}

type CBCVote struct {
	Author    core.NodeID
	Proposer  core.NodeID
	Epoch     int64
	Phase     int8
	BlockHash crypto.Digest
	Signature crypto.SignatureShare //什么时候用部分签名，什么时候用全签名 这个地方我觉得应该用signshare
}

func NewCBCVote(Author, Proposer core.NodeID, BlockHash crypto.Digest, Epoch int64, Phase int8, sigService *crypto.SigService) (*CBCVote, error) {
	vote := &CBCVote{
		Author:    Author,
		Proposer:  Proposer,
		BlockHash: BlockHash,
		Epoch:     Epoch,
		Phase:     Phase,
	}
	sig, err := sigService.RequestTsSugnature(vote.Hash())
	//sig, err := sigService.RequestTsSignature(vote.Hash())
	if err != nil {
		logger.Debug.Printf("requestsifnature error\n")
		return nil, err
	}
	vote.Signature = sig
	return vote, nil
}

func (v *CBCVote) Verify(committee core.Committee) bool {
	//_ := committee.Name(v.Author)
	return v.Signature.Verify(v.Hash())
}

func (v *CBCVote) Hash() crypto.Digest {
	hasher := crypto.NewHasher()
	//hasher.Add(strconv.AppendInt(nil, int64(v.Author), 2))
	hasher.Add(strconv.AppendInt(nil, int64(v.Phase), 2))
	hasher.Add(strconv.AppendInt(nil, int64(v.Epoch), 2))
	hasher.Add(strconv.AppendInt(nil, int64(v.Proposer), 2))
	hasher.Add(v.BlockHash[:])
	return hasher.Sum256(nil)
}
func (*CBCVote) MsgType() int {
	return CBCVoteType
}

func (*CBCVote) Module() string {
	return "consensus"
}

type Finish struct { //这个消息不需要，只需要定义一个判断函数即可
	Author    core.NodeID
	Epoch     int64
	Signature crypto.Signature
}

func NewFinish(Author core.NodeID, Epoch int64, sigService *crypto.SigService) (*Finish, error) {
	d := &Finish{
		Author: Author,
		Epoch:  Epoch,
	}
	sig, err := sigService.RequestSignature(d.Hash())
	if err != nil {
		return nil, err
	}
	d.Signature = sig
	return d, nil
}

func (d *Finish) Verify(committee core.Committee) bool {
	pub := committee.Name(d.Author)
	return d.Signature.Verify(pub, d.Hash())
}

func (d *Finish) Hash() crypto.Digest {
	hasher := crypto.NewHasher()
	hasher.Add(strconv.AppendInt(nil, int64(d.Author), 2))
	hasher.Add(strconv.AppendInt(nil, d.Epoch, 2))
	return hasher.Sum256(nil)
}

func (d *Finish) MsgType() int {
	return FinishType
}

func (*Finish) Module() string {
	return "consensus"
}

type ElectShare struct {
	Author   core.NodeID
	Epoch    int64
	SigShare crypto.SignatureShare
}

func NewElectShare(Author core.NodeID, Epoch int64, sigService *crypto.SigService) (*ElectShare, error) {
	e := &ElectShare{
		Author: Author,
		Epoch:  Epoch,
	}
	sig, err := sigService.RequestTsSugnature(e.Hash())
	if err != nil {
		return nil, err
	}
	e.SigShare = sig
	return e, nil
}

func (e *ElectShare) Verify(committee core.Committee) bool {
	_ = committee.Name(e.Author)
	return e.SigShare.Verify(e.Hash())
}

func (e *ElectShare) Hash() crypto.Digest {
	hasher := crypto.NewHasher()
	hasher.Add(strconv.AppendInt(nil, e.Epoch, 2))
	return hasher.Sum256(nil)
}

func (*ElectShare) MsgType() int {
	return ElectShareType
}
func (*ElectShare) Module() string {
	return "consensus"
}

type QuorumCert struct {
	Epoch int64
	//QC            []byte
	Priorityindex int //优先级序列
	node          core.NodeID
	//Signature crypto.Signature
}
type BestMessage struct { //本质上应该带一个QC证明
	BestNode  core.NodeID
	BestIndex int
	BestQC    crypto.Digest //this maybe nil
}
type BestMsg struct {
	Author    core.NodeID
	Epoch     int64
	BestV     BestMessage
	BestQ1    BestMessage
	BestQ2    BestMessage
	BestQ3    BestMessage
	Signature crypto.Signature
}

func NewBestMsg(author core.NodeID, Epoch int64, bestv BestMessage, bestq1 BestMessage, bestq2 BestMessage, bestq3 BestMessage, sigService *crypto.SigService) (*BestMsg, error) {
	bms := &BestMsg{
		Author: author,
		Epoch:  Epoch,
		BestV:  bestv,
		BestQ1: bestq1,
		BestQ2: bestq2,
		BestQ3: bestq3,
	}
	sig, err := sigService.RequestSignature(bms.Hash())
	if err != nil {
		return nil, err
	}
	bms.Signature = sig
	return bms, nil
}

func (bms *BestMsg) Verify(committee core.Committee) bool {
	pub := committee.Name(bms.Author)
	return bms.Signature.Verify(pub, bms.Hash())
}
func (bms *BestMsg) Hash() crypto.Digest {
	hasher := crypto.NewHasher()
	hasher.Add(strconv.AppendInt(nil, int64(bms.Author), 2))
	hasher.Add(strconv.AppendInt(nil, bms.Epoch, 2))
	hasher.Add(strconv.AppendInt(nil, int64(bms.BestV.BestNode), 2))
	hasher.Add(strconv.AppendInt(nil, int64(bms.BestQ1.BestNode), 2))
	hasher.Add(strconv.AppendInt(nil, int64(bms.BestQ2.BestNode), 2))
	hasher.Add(strconv.AppendInt(nil, int64(bms.BestQ3.BestNode), 2))
	return hasher.Sum256(nil)
}

func (*BestMsg) MsgType() int {
	return BestMsgType
}
func (*BestMsg) Module() string {
	return "consensus"
}

// RequestBlock
type RequestBlockMsg struct {
	Author    core.NodeID
	MissBlock crypto.Digest
	ReqID     int
	Ts        int64
	Signature crypto.Signature
}

func NewRequestBlock(
	Author core.NodeID,
	MissBlock crypto.Digest,
	ReqID int,
	Ts int64, //请求时间
	sigService *crypto.SigService,
) (*RequestBlockMsg, error) {
	msg := &RequestBlockMsg{
		Author:    Author,
		MissBlock: MissBlock,
		ReqID:     ReqID,
		Ts:        Ts,
	}
	sig, err := sigService.RequestSignature(msg.Hash())
	if err != nil {
		return nil, err
	}
	msg.Signature = sig
	return msg, nil
}

func (msg *RequestBlockMsg) Verify(committee core.Committee) bool {
	return msg.Signature.Verify(committee.Name(msg.Author), msg.Hash())
}

func (msg *RequestBlockMsg) Hash() crypto.Digest {
	hasher := crypto.NewHasher()
	hasher.Add(strconv.AppendInt(nil, int64(msg.Author), 2))
	hasher.Add(msg.MissBlock[:])
	hasher.Add(strconv.AppendInt(nil, int64(msg.ReqID), 2))
	hasher.Add(strconv.AppendInt(nil, msg.Ts, 2))
	return hasher.Sum256(nil)
}

func (msg *RequestBlockMsg) MsgType() int {
	return RequestBlockType
}

func (msg *RequestBlockMsg) Module() string {
	return "consensus"
}

// ReplyBlockMsg  返回一个公式区块
type ReplyBlockMsg struct {
	Author    core.NodeID
	Blocks    *ConsensusBlock
	ReqID     int
	Signature crypto.Signature
}

func NewReplyBlockMsg(Author core.NodeID, B *ConsensusBlock, ReqID int, sigService *crypto.SigService) (*ReplyBlockMsg, error) {
	msg := &ReplyBlockMsg{
		Author: Author,
		Blocks: B,
		ReqID:  ReqID,
	}
	sig, err := sigService.RequestSignature(msg.Hash())
	if err != nil {
		return nil, err
	}
	msg.Signature = sig
	return msg, nil
}

func (msg *ReplyBlockMsg) Verify(committee core.Committee) bool {
	return msg.Signature.Verify(committee.Name(msg.Author), msg.Hash())
}

func (msg *ReplyBlockMsg) Hash() crypto.Digest {
	hasher := crypto.NewHasher()
	hasher.Add(strconv.AppendInt(nil, int64(msg.Author), 2))
	hasher.Add(strconv.AppendInt(nil, int64(msg.ReqID), 2))
	return hasher.Sum256(nil)
}

func (msg *ReplyBlockMsg) MsgType() int {
	return ReplyBlockType
}

func (msg *ReplyBlockMsg) Module() string {
	return "consensus"
}
