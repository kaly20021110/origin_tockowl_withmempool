package consensus

import (
	"bft/mvba/core"
	"bft/mvba/crypto"
	"bft/mvba/logger"
	"bft/mvba/pool"
	"bft/mvba/store"
)

type Core struct {
	Name       core.NodeID
	Committee  core.Committee
	Parameters core.Parameters
	SigService *crypto.SigService
	Store      *store.Store
	TxPool     *pool.Pool
	Transimtor *core.Transmitor
	Aggreator  *Aggreator
	Elector    *Elector
	Commitor   *Committor

	CBCInstances map[int64]map[core.NodeID]*Promote //map[epoch][core.NodeID]
	VSet         map[int64]map[core.NodeID]bool     //index standfor the QC and proposal
	Q1Set        map[int64]map[core.NodeID]bool
	Q2Set        map[int64]map[core.NodeID]bool
	Q3Set        map[int64]map[core.NodeID]bool
	// mV           sync.RWMutex
	// mQ1          sync.RWMutex
	// mQ2          sync.RWMutex
	// mQ3          sync.RWMutex
	ParentQ1    map[int64]QuorumCert //save the index of best priority
	ParentQ2    map[int64]QuorumCert
	CommitEpoch int64
	Epoch       int64
}

func NewCore(
	Name core.NodeID,
	Committee core.Committee,
	Parameters core.Parameters,
	SigService *crypto.SigService,
	Store *store.Store,
	TxPool *pool.Pool,
	Transimtor *core.Transmitor,
	callBack chan<- struct{},
) *Core {

	c := &Core{
		Name:         Name,
		Committee:    Committee,
		Parameters:   Parameters,
		SigService:   SigService,
		Store:        Store,
		TxPool:       TxPool,
		Transimtor:   Transimtor,
		Epoch:        0,
		CommitEpoch:  0,
		Aggreator:    NewAggreator(Committee),
		Elector:      NewElector(SigService, Committee),
		Commitor:     NewCommittor(callBack),
		CBCInstances: make(map[int64]map[core.NodeID]*Promote),
		VSet:         make(map[int64]map[core.NodeID]bool),
		Q1Set:        make(map[int64]map[core.NodeID]bool),
		Q2Set:        make(map[int64]map[core.NodeID]bool),
		Q3Set:        make(map[int64]map[core.NodeID]bool),
		ParentQ1:     make(map[int64]QuorumCert),
		ParentQ2:     make(map[int64]QuorumCert),
	}
	return c
}

func (c *Core) AddSet(settype uint8, epoch int64, node core.NodeID) {
	switch settype {
	case 0:
		{
			if _, ok := c.VSet[epoch]; !ok {
				c.VSet[epoch] = make(map[core.NodeID]bool)
			}
			c.VSet[epoch][node] = true
		}
	case 1:
		{
			if _, ok := c.Q1Set[epoch]; !ok {
				c.Q1Set[epoch] = make(map[core.NodeID]bool)
			}
			c.Q1Set[epoch][node] = true

		}
	case 2:
		{
			if _, ok := c.Q2Set[epoch]; !ok {
				c.Q2Set[epoch] = make(map[core.NodeID]bool)
			}
			c.Q2Set[epoch][node] = true
		}
	case 3:
		{
			if _, ok := c.Q3Set[epoch]; !ok {
				c.Q3Set[epoch] = make(map[core.NodeID]bool)
			}
			c.Q3Set[epoch][node] = true
		}
	}
}

func (c *Core) NewSet(epoch int64) {
	if _, ok := c.VSet[epoch]; !ok {
		c.VSet[epoch] = make(map[core.NodeID]bool)
	}
	if _, ok := c.Q1Set[epoch]; ok {
		c.Q1Set[epoch] = make(map[core.NodeID]bool)
	}
	if _, ok := c.Q2Set[epoch]; ok {
		c.Q2Set[epoch] = make(map[core.NodeID]bool)
	}
	if _, ok := c.Q3Set[epoch]; ok {
		c.Q3Set[epoch] = make(map[core.NodeID]bool)
	}
}

// 有可能先收到了bestexchange，然后更新了自己的相关内容，导致出现问题
func (c *Core) AchieveFinish(epoch int64) bool {
	if len(c.VSet[epoch]) >= c.Committee.HightThreshold() && len(c.Q1Set[epoch]) >= c.Committee.HightThreshold() && len(c.Q2Set[epoch]) >= c.Committee.HightThreshold() && len(c.Q3Set[epoch]) >= c.Committee.HightThreshold() {
		logger.Warn.Printf("AchieveFinish is right epoch %d\n", epoch)
		return true
	} else {
		logger.Warn.Printf("AchieveFinish is wrong epoch %d\n", epoch)
		return false
	}
}

func (c *Core) messageFilter(epoch int64) bool {
	return epoch < c.Epoch
}

func (c *Core) storeBlock(block *Block) error {
	key := block.Hash()
	value, err := block.Encode()
	if err != nil {
		return err
	}
	return c.Store.Write(key[:], value)
}

func (c *Core) getBlock(digest crypto.Digest) (*Block, error) {
	value, err := c.Store.Read(digest[:])

	if err == store.ErrNotFoundKey {
		return nil, nil
	}

	if err != nil {
		return nil, err
	}

	b := &Block{}
	if err := b.Decode(value); err != nil {
		return nil, err
	}
	return b, err
}

func (c *Core) getCBCInstance(epoch int64, node core.NodeID) *Promote {
	rItems, ok := c.CBCInstances[epoch]
	if !ok {
		rItems = make(map[core.NodeID]*Promote)
		c.CBCInstances[epoch] = rItems
	}
	instance, ok := rItems[node]
	if !ok {
		instance = NewPromote(c, epoch, node)
		rItems[node] = instance
	}
	return instance
}

func (c *Core) generatorBlock(epoch int64, prehash crypto.Digest) *Block {
	block := NewBlock(c.Name, c.TxPool.GetBatch(), epoch, prehash)
	logger.Info.Printf("create Block epoch %d node %d batch_id %d \n", block.Epoch, block.Proposer, block.Batch.ID)
	// if block.Batch.ID != -1 {
	// 	logger.Info.Printf("create Block epoch %d node %d batch_id %d \n", block.Epoch, block.Proposer, block.Batch.ID)
	// }
	return block
}

/*********************************** Protocol Start***************************************/
func (c *Core) handleProposal(p *CBCProposal) error {
	logger.Debug.Printf("processing proposal epoch %d phase %d proposer %d\n", p.Epoch, p.Phase, p.Author)
	if c.messageFilter(p.Epoch) {
		return nil
	}
	if p.Phase == CBC_ONE_PHASE {
		if err := c.storeBlock(p.B); err != nil {
			return err
		}
		//check priority  safeproposal index越小对应的值越大
		logger.Debug.Printf("p.ParentQC1.Priorityindex %d c.ParentQ2[p.Epoch-1].Priorityindex%d\n", p.ParentQC1.Priorityindex, c.ParentQ2[p.Epoch-1].Priorityindex)
		if p.Epoch == c.Epoch && p.ParentQC1.Priorityindex > c.ParentQ2[p.Epoch-1].Priorityindex && p.Epoch != 0 {
			return nil
		}
	}
	switch p.Phase {
	case CBC_ONE_PHASE:
		c.AddSet(0, p.Epoch, p.Author)
	case CBC_TWO_PHASE:
		c.AddSet(1, p.Epoch, p.Author)
	case CBC_THREE_PHASE:
		c.AddSet(2, p.Epoch, p.Author)
	case LAST:
		c.AddSet(3, p.Epoch, p.Author)
	}
	logger.Warn.Printf("epoch is %d len of V %d len of Q1 %d len of Q2 %d len 0f Q3 %d\n", p.Epoch, len(c.VSet[p.Epoch]), len(c.Q1Set[p.Epoch]), len(c.Q2Set[p.Epoch]), len(c.Q3Set[p.Epoch]))
	go c.getCBCInstance(p.Epoch, p.Author).ProcessProposal(p)
	return nil
}

func (c *Core) handleVote(v *CBCVote) error {
	logger.Debug.Printf("processing vote epoch %d phase %d proposer %d\n", v.Epoch, v.Phase, v.Proposer)
	if c.messageFilter(v.Epoch) {
		return nil
	}
	go c.getCBCInstance(v.Epoch, v.Proposer).ProcessVote(v)
	return nil
}

func (c *Core) handleElectShare(e *ElectShare) error {
	logger.Debug.Printf("processing electShare epoch %d from %d\n", e.Epoch, e.Author)
	if c.messageFilter(e.Epoch) {
		return nil
	}
	if leadermap, err := c.Elector.AddShareVote(e); err != nil {
		return err
	} else if leadermap[0] != -1 { //已经形成了优先级序列
		bestv := BestMessage{core.NONE, -1, nil}
		bestq1 := BestMessage{core.NONE, -1, nil}
		bestq2 := BestMessage{core.NONE, -1, nil}
		bestq3 := BestMessage{core.NONE, -1, nil}
		for i := 0; i < c.Committee.Size(); i++ {
			node := leadermap[i]
			if _, ok := c.VSet[e.Epoch][node]; ok {
				if bestv.BestNode == core.NONE {
					bestv.BestNode = node
					bestv.BestIndex = i
				}
			}
			if _, ok := c.Q1Set[e.Epoch][node]; ok {
				if bestq1.BestNode == core.NONE {
					bestq1.BestNode = node
					bestq1.BestIndex = i
				}
			}
			if _, ok := c.Q2Set[e.Epoch][node]; ok {
				if bestq2.BestNode == core.NONE {
					bestq2.BestNode = node
					bestq2.BestIndex = i
				}
			}
			if _, ok := c.Q3Set[e.Epoch][node]; ok {
				if bestq3.BestNode == core.NONE {
					bestq3.BestNode = node
					bestq3.BestIndex = i
				}
			}
			if bestv.BestNode != core.NONE && bestq1.BestNode != core.NONE && bestq2.BestNode != core.NONE && bestq3.BestNode != core.NONE {
				break
			}
		}
		msg, _ := NewBestMsg(c.Name, e.Epoch, bestv, bestq1, bestq2, bestq3, c.SigService)
		c.Transimtor.Send(c.Name, core.NONE, msg)
		c.Transimtor.RecvChannel() <- msg
	}
	return nil
}
func (c *Core) Best(epoch int64, set map[int64]map[core.NodeID]bool) (int, core.NodeID) {
	nodeset := c.Elector.GetPriority(epoch)
	for i := 0; i < c.Committee.Size(); i++ {
		node := nodeset[i]
		if _, ok := set[epoch][node]; ok {
			return i, node
		}
	}
	return -1, core.NONE
}

func (c *Core) handleBestMsg(m *BestMsg) error {
	logger.Debug.Printf("processing best message epoch %d author %d\n", m.Epoch, m.Author)
	if c.messageFilter(m.Epoch) {
		return nil
	}
	c.AddSet(0, m.Epoch, m.BestV.BestNode)
	c.AddSet(1, m.Epoch, m.BestQ1.BestNode)
	c.AddSet(2, m.Epoch, m.BestQ2.BestNode)
	c.AddSet(3, m.Epoch, m.BestQ3.BestNode)
	//聚合2f+1个bestexchange消息，如果收集到足够的消息
	if finish, err := c.Aggreator.addBestMessage(m); err != nil {
		return err
	} else if finish { //收集到了2f+1条消息
		logger.Debug.Printf("actually recieve 2f+1 best messages epoch %d \n", m.Epoch)
		//update parents q1 and q2
		_, bestvNode := c.Best(m.Epoch, c.VSet)
		bestq1Index, bestq1Node := c.Best(m.Epoch, c.Q1Set)
		bestq2Index, bestq2Node := c.Best(m.Epoch, c.Q2Set)
		bestq3Index, bestq3Node := c.Best(m.Epoch, c.Q3Set)
		c.ParentQ1[m.Epoch] = QuorumCert{m.Epoch, bestq1Index, bestq1Node}
		c.ParentQ2[m.Epoch] = QuorumCert{m.Epoch, bestq2Index, bestq2Node}
		//commit rule
		if bestvNode == bestq3Node || bestq3Index == 0 { //第二个条件下，有可能没有收到这个块
			//如何commit当前块以及它所有的祖先区块?  如何commit所有的祖先区块
			logger.Debug.Printf("actually commit blocks epoch %d \n", m.Epoch)
			_, node := c.Best(m.Epoch, c.Q3Set)
			blockHash := c.getCBCInstance(m.Epoch, node).BlockHash()
			if block, err := c.getBlock(*blockHash); err == nil {
				logger.Debug.Printf("success get the block and commit blocks epoch %d \n", m.Epoch)
				c.CommitAncestor(c.CommitEpoch, m.Epoch, block)
				c.Commitor.Commit(block)
				c.CommitEpoch = m.Epoch
			}
		} else {
			logger.Info.Printf("can not commit any blocks in this epoch %d \n", m.Epoch)
		}
		//进入下一个epoch
		prehash := c.getCBCInstance(m.Epoch, bestq1Node).blockHash
		c.advanceNextEpoch(m.Epoch+1, *prehash)
	}
	return nil
}

func (c *Core) CommitAncestor(lastepoch int64, nowepoch int64, block *Block) {
	blockmap := make(map[int64]crypto.Digest)
	for i := nowepoch - 1; i >= lastepoch; i-- {
		blockmap[i] = block.PreHash
	}
	for i := lastepoch + 1; i < nowepoch; i++ {
		if block, err := c.getBlock(blockmap[i]); err == nil {
			logger.Debug.Printf("success get the ancestors epoch %d \n", i)
			c.Commitor.Commit(block)
		} else {
			logger.Debug.Printf("error get the ancestors epoch %d \n", i)
			logger.Error.Printf("error get the ancestors epoch %d \n", i)
			//c.Commitor.Commit(block)
		}
	}
}

/*********************************** Protocol End***************************************/
func (c *Core) advanceNextEpoch(epoch int64, prehash crypto.Digest) {
	if epoch <= c.Epoch {
		return
	}
	logger.Debug.Println("advance next epoch", epoch)
	//Clear Something

	c.Epoch = epoch
	c.NewSet(c.Epoch)
	block := c.generatorBlock(epoch, prehash)
	proposal, _ := NewCBCProposal(c.Name, c.Epoch, CBC_ONE_PHASE, block, c.ParentQ1[epoch-1], c.SigService)
	c.Transimtor.Send(c.Name, core.NONE, proposal)
	c.Transimtor.RecvChannel() <- proposal
}

func (c *Core) Run() {
	if c.Name < core.NodeID(c.Parameters.Faults) {
		logger.Debug.Printf("Node %d is faulty\n", c.Name)
		return
	}
	//first proposal
	block := c.generatorBlock(c.Epoch, crypto.Digest{})
	c.NewSet(c.Epoch)
	proposal, _ := NewCBCProposal(c.Name, c.Epoch, CBC_ONE_PHASE, block, QuorumCert{0, -1, core.NONE}, c.SigService)
	if err := c.Transimtor.Send(c.Name, core.NONE, proposal); err != nil {
		panic(err)
	}
	c.Transimtor.RecvChannel() <- proposal

	recvChannal := c.Transimtor.RecvChannel()
	for {
		var err error
		select {
		case msg := <-recvChannal:
			{
				if validator, ok := msg.(Validator); ok {
					if !validator.Verify(c.Committee) {
						err = core.ErrSignature(msg.MsgType())
						break
					}
				}

				switch msg.MsgType() {

				case CBCProposalType:
					err = c.handleProposal(msg.(*CBCProposal))
				case CBCVoteType:
					err = c.handleVote(msg.(*CBCVote))
				case ElectShareType:
					err = c.handleElectShare(msg.(*ElectShare))
				case BestMsgType:
					err = c.handleBestMsg(msg.(*BestMsg))

				}
			}
		default:
		}
		if err != nil {
			logger.Warn.Println(err)
		}
	}
}
