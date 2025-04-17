package consensus

import (
	"bft/mvba/core"
	"bft/mvba/crypto"
	"bft/mvba/logger"
	"sync"
	"sync/atomic"
)

type Promote struct {
	C        *Core
	Epoch    int64
	Proposer core.NodeID

	mHash     sync.RWMutex
	blockHash *crypto.Digest

	mUnhandle        sync.Mutex
	unhandleProposal []*CBCProposal
	unhandleVote     []*CBCVote
	mVoteCnt         sync.Mutex
	voteCnts         map[int8]int

	keyFlag    atomic.Bool
	lockFlag   atomic.Bool
	finishFlag atomic.Bool
}

func NewPromote(c *Core, epoch int64, proposer core.NodeID) *Promote {

	p := &Promote{
		C:                c,
		Epoch:            epoch,
		Proposer:         proposer,
		unhandleProposal: make([]*CBCProposal, 0),
		unhandleVote:     make([]*CBCVote, 0),
		mUnhandle:        sync.Mutex{},
		mHash:            sync.RWMutex{},
		blockHash:        nil,
		mVoteCnt:         sync.Mutex{},
		voteCnts:         make(map[int8]int),
	}

	p.keyFlag.Store(false)
	p.lockFlag.Store(false)
	p.finishFlag.Store(false)

	return p
}

func (p *Promote) ProcessProposal(proposal *CBCProposal) {
	if proposal.Author != p.Proposer {
		logger.Warn.Printf("promote error: the proposer of block is not match\n")
		return
	}
	if proposal.Phase != CBC_ONE_PHASE {
		p.mHash.RLock()
		if p.blockHash == nil {
			p.mHash.RUnlock()

			p.mUnhandle.Lock()
			p.unhandleProposal = append(p.unhandleProposal, proposal)
			p.mUnhandle.Unlock()

			return
		}
		p.mHash.RUnlock()
	}

	var vote *CBCVote

	switch proposal.Phase {
	case CBC_ONE_PHASE:
		{
			// if proposal.Author != p.Proposer {
			// 	logger.Warn.Printf("promote error: the proposer of block is not match\n")
			// 	return
			// }
			p.mHash.Lock()
			d := proposal.B.Hash()
			p.blockHash = &d
			p.mHash.Unlock()

			p.mUnhandle.Lock()
			for _, item := range p.unhandleProposal {
				go p.ProcessProposal(item)
			}

			for _, vote := range p.unhandleVote {
				go p.ProcessVote(vote)
			}
			p.mUnhandle.Unlock()

			vote, _ = NewCBCVote(p.C.Name, p.Proposer, d, p.Epoch, CBC_ONE_PHASE, p.C.SigService)
		}
	case CBC_TWO_PHASE:
		{
			p.keyFlag.Store(true)
			vote, _ = NewCBCVote(p.C.Name, p.Proposer, *p.blockHash, p.Epoch, CBC_TWO_PHASE, p.C.SigService)
		}
	case CBC_THREE_PHASE:
		{
			p.lockFlag.Store(true)
			vote, _ = NewCBCVote(p.C.Name, p.Proposer, *p.blockHash, p.Epoch, CBC_THREE_PHASE, p.C.SigService)
		}
	case LAST:
		{
			logger.Warn.Printf("enter the handleproposal with LAST epoch is %d\n", p.Epoch)
			if !p.IsFinish() { //只发送一次electshare
				if p.C.AchieveFinish(p.Epoch) {
					logger.Debug.Printf("finish the three-phase broadcast and beigin to achieve finish in epoch %d\n", p.Epoch)
					p.finishFlag.Store(true)
					share, _ := NewElectShare(p.C.Name, p.Epoch, p.C.SigService)
					p.C.Transimtor.Send(p.C.Name, core.NONE, share)
					p.C.Transimtor.RecvChannel() <- share
				}
			}
		}
	}
	if proposal.Phase != LAST {
		if p.C.Name != p.Proposer {
			p.C.Transimtor.Send(p.C.Name, p.Proposer, vote)
		} else {
			p.C.Transimtor.RecvChannel() <- vote
		}
	}
}

func (p *Promote) ProcessVote(vote *CBCVote) {
	if p.Proposer != vote.Proposer {
		logger.Warn.Printf("promote error: the vote of block is not match\n")
		return
	}
	p.mHash.RLock()
	if p.blockHash == nil {
		p.mHash.RUnlock()

		p.mUnhandle.Lock()
		p.unhandleVote = append(p.unhandleVote, vote)
		p.mUnhandle.Unlock()

		return
	} else if *p.blockHash != vote.BlockHash {
		p.mHash.RUnlock()
		logger.Warn.Printf("promote error: the block hash in vote is invaild\n")
		return
	}
	p.mHash.RUnlock()

	p.mVoteCnt.Lock()
	//p.voteCnts[vote.Phase]++
	//nums := p.voteCnts[vote.Phase]
	finish, qcvalue, _ := p.C.Aggreator.addVote(vote)
	p.mVoteCnt.Unlock()
	if finish {
		if vote.Phase < CBC_THREE_PHASE {
			QC := QuorumCert{vote.Epoch, -1, core.NONE}
			proposal, _ := NewCBCProposal(p.Proposer, p.Epoch, vote.Phase+1, nil, QC, qcvalue, p.C.SigService)
			//proposal, _ := NewCBCProposal(p.Proposer, p.Epoch, vote.Phase+1, nil, QC, p.C.SigService)
			p.C.Transimtor.Send(p.Proposer, core.NONE, proposal)
			p.C.Transimtor.RecvChannel() <- proposal
		} else {
			QC := QuorumCert{vote.Epoch, -1, core.NONE}
			proposal, _ := NewCBCProposal(p.Proposer, p.Epoch, LAST, nil, QC, qcvalue, p.C.SigService)
			///proposal, _ := NewCBCProposal(p.Proposer, p.Epoch, vote.Phase+1, nil, QC, p.C.SigService)
			p.C.Transimtor.Send(p.Proposer, core.NONE, proposal)
			p.C.Transimtor.RecvChannel() <- proposal
		}
	}
	// if nums == p.C.Committee.HightThreshold() {
	// 	if vote.Phase < CBC_THREE_PHASE {
	// 		QC := QuorumCert{vote.Epoch, -1, core.NONE}
	// 		proposal, _ := NewCBCProposal(p.Proposer, p.Epoch, vote.Phase+1, nil, QC, p.C.SigService)
	// 		p.C.Transimtor.Send(p.Proposer, core.NONE, proposal)
	// 		p.C.Transimtor.RecvChannel() <- proposal
	// 	} else {
	// 		QC := QuorumCert{vote.Epoch, -1, core.NONE}
	// 		proposal, _ := NewCBCProposal(p.Proposer, p.Epoch, LAST, nil, QC, p.C.SigService)
	// 		p.C.Transimtor.Send(p.Proposer, core.NONE, proposal)
	// 		p.C.Transimtor.RecvChannel() <- proposal
	// 	}
	// }
}

func (p *Promote) BlockHash() *crypto.Digest {
	return p.blockHash
}

func (p *Promote) IsKey() bool {
	return p.keyFlag.Load()
}

func (p *Promote) IsLock() bool {
	return p.lockFlag.Load()
}

func (p *Promote) IsFinish() bool {
	return p.finishFlag.Load()
}
