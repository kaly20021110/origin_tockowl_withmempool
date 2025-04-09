package consensus

import (
	"bft/mvba/core"
	"bft/mvba/crypto"
	"bft/mvba/logger"
	"bft/mvba/pool"
	"bytes"
	"encoding/gob"
	"reflect"
	"strconv"
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

type Block struct {
	Proposer core.NodeID
	Batch    pool.Batch
	Epoch    int64
	PreHash  crypto.Digest
}

func NewBlock(proposer core.NodeID, Batch pool.Batch, Epoch int64, prehash crypto.Digest) *Block {
	return &Block{
		Proposer: proposer,
		Batch:    Batch,
		Epoch:    Epoch,
		PreHash:  prehash,
	}
}

func (b *Block) Encode() ([]byte, error) {
	buf := bytes.NewBuffer(nil)
	if err := gob.NewEncoder(buf).Encode(b); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (b *Block) Decode(data []byte) error {
	buf := bytes.NewBuffer(data)
	if err := gob.NewDecoder(buf).Decode(b); err != nil {
		return err
	}
	return nil
}

func (b *Block) Hash() crypto.Digest {
	hasher := crypto.NewHasher()
	hasher.Add(strconv.AppendInt(nil, int64(b.Proposer), 2))
	hasher.Add(strconv.AppendInt(nil, b.Epoch, 2))
	hasher.Add(strconv.AppendInt(nil, int64(b.Batch.ID), 2))
	hasher.Add(b.PreHash[:])
	return hasher.Sum256(nil)
}

// 是否要定义Halt消息我不确定
const (
	CBCProposalType int = iota
	CBCVoteType
	FinishType
	ElectShareType
	BestMsgType
)

var DefaultMessageTypeMap = map[int]reflect.Type{
	CBCProposalType: reflect.TypeOf(CBCProposal{}),
	CBCVoteType:     reflect.TypeOf(CBCVote{}),
	FinishType:      reflect.TypeOf(Finish{}),
	ElectShareType:  reflect.TypeOf(ElectShare{}),
	BestMsgType:     reflect.TypeOf(BestMsg{}),
}

type CBCProposal struct {
	Author    core.NodeID
	Epoch     int64
	Phase     int8
	B         *Block
	ParentQC1 QuorumCert
	Signature crypto.Signature
}

func NewCBCProposal(author core.NodeID, epoch int64, phase int8, B *Block, parentqc1 QuorumCert, sigService *crypto.SigService) (*CBCProposal, error) {
	p := &CBCProposal{
		Author:    author,
		Epoch:     epoch,
		Phase:     phase,
		B:         B,
		ParentQC1: parentqc1,
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
	return hasher.Sum256(nil)

}

func (*CBCProposal) MsgType() int {
	return CBCProposalType
}

type CBCVote struct {
	Author    core.NodeID
	Proposer  core.NodeID
	Epoch     int64
	Phase     int8
	BlockHash crypto.Digest
	Signature crypto.Signature //什么时候用部分签名，什么时候用全签名 这个地方我觉得应该用signshare
}

func NewCBCVote(Author, Proposer core.NodeID, BlockHash crypto.Digest, Epoch int64, Phase int8, sigService *crypto.SigService) (*CBCVote, error) {
	vote := &CBCVote{
		Author:    Author,
		Proposer:  Proposer,
		BlockHash: BlockHash,
		Epoch:     Epoch,
		Phase:     Phase,
	}
	sig, err := sigService.RequestSignature(vote.Hash())
	if err != nil {
		logger.Debug.Printf("requestsifnature error\n")
		return nil, err
	}
	vote.Signature = sig
	return vote, nil
}

func (v *CBCVote) Verify(committee core.Committee) bool {
	pub := committee.Name(v.Author)
	return v.Signature.Verify(pub, v.Hash())
}

func (v *CBCVote) Hash() crypto.Digest {
	hasher := crypto.NewHasher()
	hasher.Add(strconv.AppendInt(nil, int64(v.Author), 2))
	hasher.Add(strconv.AppendInt(nil, int64(v.Phase), 2))
	hasher.Add(strconv.AppendInt(nil, int64(v.Epoch), 2))
	hasher.Add(strconv.AppendInt(nil, int64(v.Proposer), 2))
	hasher.Add(v.BlockHash[:])
	return hasher.Sum256(nil)
}
func (*CBCVote) MsgType() int {
	return CBCVoteType
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

type QuorumCert struct {
	Epoch int64
	//QC            []byte
	Priorityindex int //优先级序列
	node          core.NodeID
	//Signature crypto.Signature
}

type BestMessage struct {
	BestNode  core.NodeID
	BestIndex int
	BestQC    *crypto.Digest //this maybe nil
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
