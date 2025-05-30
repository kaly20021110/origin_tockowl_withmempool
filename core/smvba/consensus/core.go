package consensus

import (
	"bft/mvba/core"
	"bft/mvba/crypto"
	"bft/mvba/logger"
	"bft/mvba/mempool"
	"bft/mvba/pool"
	"bft/mvba/store"
	"math/rand"
	"sync"
)

type Core struct {
	Name                 core.NodeID
	Committee            core.Committee
	Parameters           core.Parameters
	SigService           *crypto.SigService
	Store                *store.Store
	TxPool               *pool.Pool
	Transimtor           *core.Transmitor
	Aggreator            *Aggreator
	Elector              *Elector
	Commitor             *Committor
	Retriever            *Retriever
	MemPool              *mempool.Mempool
	loopBackChannel      chan crypto.Digest //从retrieval部分获取到区块之后，直接commit
	BlockloopBackChannel chan *ConsensusBlock

	CBCInstances            map[int64]map[core.NodeID]*Promote      //map[epoch][core.NodeID]
	CBCInstancesBlockHash   map[int64]map[core.NodeID]crypto.Digest //存储每个块的哈希值
	BlocksWaitforCommit     map[int64]*ConsensusBlock               //等待提交区块
	BlocksWaitforCommitFlag map[int64]core.NodeID                   //用于标识这个epoch需要被commit
	VSet                    map[int64]map[core.NodeID]bool          //index standfor the QC and proposal
	Q1Set                   map[int64]map[core.NodeID]bool
	Q2Set                   map[int64]map[core.NodeID]bool
	Q3Set                   map[int64]map[core.NodeID]bool
	ParentQ1                map[int64]QuorumCert //save the index of best priority
	ParentQ2                map[int64]QuorumCert
	CommitEpoch             int64
	Epoch                   int64
	RandomPhase             map[int64]int8                     //每一轮停止阶段
	Stopstate               bool                               //用于判断当前状态是否是停止状态
	StopFlag                map[int64]map[core.NodeID]struct{} //用于停止前面的广播过程
	Stopmu                  sync.RWMutex

	mSet          sync.RWMutex
	blockhashlock sync.RWMutex
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
	loopBackchannel := make(chan crypto.Digest)
	BlockloopBackChannel := make(chan *ConsensusBlock)
	Sync := mempool.NewSynchronizer(Name, Transimtor, loopBackchannel, Store)
	c := &Core{
		Name:                    Name,
		Committee:               Committee,
		Parameters:              Parameters,
		SigService:              SigService,
		Store:                   Store,
		TxPool:                  TxPool,
		Transimtor:              Transimtor,
		Epoch:                   0,
		CommitEpoch:             0,
		Stopstate:               false,
		Aggreator:               NewAggreator(SigService, Committee),
		Elector:                 NewElector(SigService, Committee),
		Commitor:                NewCommittor(Store, callBack),
		Retriever:               NewRetriever(Name, Store, Transimtor, SigService, Parameters, BlockloopBackChannel), //这个地方应该也把consensu的区块检索得到
		MemPool:                 mempool.NewMempool(Name, Committee, Parameters, SigService, Store, TxPool, Transimtor, Sync),
		loopBackChannel:         loopBackchannel,
		BlockloopBackChannel:    BlockloopBackChannel,
		CBCInstances:            make(map[int64]map[core.NodeID]*Promote),
		CBCInstancesBlockHash:   make(map[int64]map[core.NodeID]crypto.Digest),
		BlocksWaitforCommit:     make(map[int64]*ConsensusBlock),
		BlocksWaitforCommitFlag: make(map[int64]core.NodeID),
		VSet:                    make(map[int64]map[core.NodeID]bool),
		Q1Set:                   make(map[int64]map[core.NodeID]bool),
		Q2Set:                   make(map[int64]map[core.NodeID]bool),
		Q3Set:                   make(map[int64]map[core.NodeID]bool),
		ParentQ1:                make(map[int64]QuorumCert),
		ParentQ2:                make(map[int64]QuorumCert),
		RandomPhase:             make(map[int64]int8),
		StopFlag:                make(map[int64]map[core.NodeID]struct{}),
	}
	return c
}

func (c *Core) AddCBCBlockHash(epoch int64, node core.NodeID, digest crypto.Digest) {
	c.blockhashlock.Lock()
	defer c.blockhashlock.Unlock()
	if _, ok := c.CBCInstancesBlockHash[epoch]; !ok {
		c.CBCInstancesBlockHash[epoch] = make(map[core.NodeID]crypto.Digest)
	}
	if _, exist := c.CBCInstancesBlockHash[epoch][node]; !exist {
		c.CBCInstancesBlockHash[epoch][node] = digest
	}
}

func (c *Core) GetCBCBlockHash(epoch int64, node core.NodeID) (crypto.Digest, bool) {
	c.blockhashlock.Lock()
	defer c.blockhashlock.Unlock()
	if _, ok := c.CBCInstancesBlockHash[epoch]; !ok {
		c.CBCInstancesBlockHash[epoch] = make(map[core.NodeID]crypto.Digest)
		return crypto.Digest{}, false
	}
	if value, exist := c.CBCInstancesBlockHash[epoch][node]; exist {
		return value, true
	} else {
		return crypto.Digest{}, false
	}
}

func (c *Core) AddSet(settype uint8, epoch int64, node core.NodeID) {
	c.mSet.Lock()
	defer c.mSet.Unlock()
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
	c.mSet.Lock()
	defer c.mSet.Unlock()
	if _, ok := c.VSet[epoch]; !ok {
		c.VSet[epoch] = make(map[core.NodeID]bool)
	}
	if _, ok := c.Q1Set[epoch]; !ok {
		c.Q1Set[epoch] = make(map[core.NodeID]bool)
	}
	if _, ok := c.Q2Set[epoch]; !ok {
		c.Q2Set[epoch] = make(map[core.NodeID]bool)
	}
	if _, ok := c.Q3Set[epoch]; !ok {
		c.Q3Set[epoch] = make(map[core.NodeID]bool)
	}
}

func (c *Core) AchieveFinish(epoch int64) bool {
	c.mSet.RLock()
	defer c.mSet.RUnlock()
	logger.Debug.Printf("len of len(c.VSet[epoch]) %d epoch %d\n", len(c.VSet[epoch]), epoch)
	if len(c.VSet[epoch]) >= c.Committee.HightThreshold() && len(c.Q1Set[epoch]) >= c.Committee.HightThreshold() && len(c.Q2Set[epoch]) >= c.Committee.HightThreshold() && len(c.Q3Set[epoch]) >= c.Committee.HightThreshold() {
		return true
	} else {
		return false
	}
}

func (c *Core) messageFilter(epoch int64) bool {
	return epoch < c.Epoch
}

func (c *Core) storeConsensusBlock(block *ConsensusBlock) error {
	key := block.Hash()
	value, err := block.Encode()
	if err != nil {
		return err
	}
	return c.Store.Write(key[:], value)
}

func (c *Core) getConsensusBlock(digest crypto.Digest) (*ConsensusBlock, error) {
	value, err := c.Store.Read(digest[:])

	if err == store.ErrNotFoundKey {
		return nil, nil
	}

	if err != nil {
		return nil, err
	}

	b := &ConsensusBlock{}
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

// 创建ConsensusBlock
func (c *Core) generatorConsensusBlock(epoch int64, prehash crypto.Digest, prenodeid core.NodeID) *ConsensusBlock {
	referencechan := make(chan []crypto.Digest)
	msg := &mempool.MakeConsensusBlockMsg{
		MaxBlockSize: c.Parameters.MaxPayloadSize, Blocks: referencechan,
	}
	c.Transimtor.ConnectRecvChannel() <- msg
	referrences := <-referencechan
	//这个地方一直没有消息了，导致无法产生共识块
	consensusblock := NewConsensusBlock(c.Name, epoch, prehash, prenodeid, referrences)
	logger.Info.Printf("create ConsensusBlock epoch %d proposer %d lens is %d\n", consensusblock.Epoch, consensusblock.Proposer, len(consensusblock.Referrence))
	logger.Info.Printf("create ConsensusBlock epoch %d proposer %d\n", consensusblock.Epoch, consensusblock.Proposer)
	return consensusblock
}

// 检查当前区块的所有payload是否都已经收到
func (c *Core) verifyConsensusBlock(block *ConsensusBlock) bool {
	verifychan := make(chan mempool.VerifyStatus)
	msg := &mempool.VerifyBlockMsg{
		Proposer:           block.Proposer, //提块的人
		Epoch:              block.Epoch,
		Payloads:           block.Referrence,
		ConsensusBlockHash: block.Hash(),
		Sender:             verifychan,
	}
	c.Transimtor.ConnectRecvChannel() <- msg
	//获取当前区块的状态
	verifystatus := <-verifychan
	if verifystatus == mempool.OK {
		return true
	} else {
		return false
	}
}

// 收到区块之后把区块加入到待commit数组中
func (c *Core) CheckBlockToCommit(block *ConsensusBlock) error {
	commitEpoch := c.CommitEpoch
	for i := commitEpoch; i <= c.Epoch; i++ {
		if existblock, ok := c.BlocksWaitforCommit[i]; ok {
			if i == commitEpoch { //不用检查直接提交即可
				return nil
			} else {
				if block.Hash() == existblock.PreHash { //如果等于父亲区块
					c.CommitAncestor(commitEpoch, block.Epoch, block)
				}
			}
		}
	}
	return nil
}

func (c *Core) CommitAllBlocks() {
	commitEpoch := c.CommitEpoch
	for i := commitEpoch; i <= c.Epoch; i++ {
		if block, ok := c.BlocksWaitforCommit[i]; ok {
			flag := c.TrytoCommit(block)
			if flag {
				delete(c.BlocksWaitforCommit, i)
				c.CommitEpoch++
			} else {
				break
			}
		} else {
			break
		}
	}
}

// 提交所有的祖先区块 从commitepoch to the actual can commit block
func (c *Core) CommitAncestor(lastepoch int64, nowepoch int64, block *ConsensusBlock) {
	blockmap := make(map[int64]crypto.Digest)
	for i := nowepoch - 1; i >= lastepoch; i-- { //获取所有的祖先
		blockmap[i] = block.PreHash
		if preblock, err := c.getConsensusBlock(blockmap[i]); err == nil { //获取得到
			block = preblock
			c.BlocksWaitforCommit[block.Epoch] = block
		} else { //没有获取得到 逻辑设计如下：
			//找生成这个区块的人要 找block的作者要 设置一个flag，如果commit还没结束，但是可以进入下一轮，不能堵塞进入下一轮
			//如果通过检索要到了某个区块，那就继续执行等待commit的函数
			//如果从检索处要到了这个区块，如果区块的epoch大于commitEpoch，就继续检查直到检查到commitEpoch为止
			//retrieve miss block
			c.Retriever.requestBlocks(block.PreHash, block.Proposer, block.Hash())
			logger.Error.Printf("Error Reference Author %d Epoch %d\n", block.Proposer, block.Epoch)
			break
		}
	}
}

// 提交某个共识块
func (c *Core) TrytoCommit(consensusblock *ConsensusBlock) bool {
	if ok := c.verifyConsensusBlock(consensusblock); !ok {
		logger.Debug.Printf("len of the paylads %d\n", len(consensusblock.Referrence))
		return false
	} else {

		for _, smallblockhash := range consensusblock.Referrence {
			if smallblock, err := c.MemPool.GetBlock(smallblockhash); err != nil {
				logger.Error.Printf("get payload error\n")
			} else {
				c.Commitor.Commit(smallblock)
			}
		}
		logger.Info.Printf("commit ConsensusBlock epoch %d proposer %d\n", consensusblock.Epoch, consensusblock.Proposer)
		msg := &mempool.CleanBlockMsg{
			Digests: consensusblock.Referrence,
			Epoch:   consensusblock.Epoch,
		}
		c.Transimtor.ConnectRecvChannel() <- msg

		return true
	}
}

// 检查父亲区块区块是否收到
func (c *Core) CheckReference(block *ConsensusBlock) (crypto.Digest, bool) {
	if block.Epoch == 0 { //创世纪块不用检查
		return crypto.Digest{}, true
	}
	if _, err := c.getConsensusBlock(block.PreHash); err != nil {
		return crypto.Digest{}, false
	} else {
		return block.PreHash, true
	}
}

/*********************************** Protocol Start***************************************/
func (c *Core) handleProposal(p *CBCProposal) error {
	logger.Debug.Printf("processing proposal epoch %d phase %d proposer %d\n", p.Epoch, p.Phase, p.Author)

	//ensure all block is received and commit 在这一步把所有的待提交的区块提交

	//01 checkEpoch
	if c.messageFilter(p.Epoch) {
		return nil
	}
	//02 checkCrash
	if c.restartProtocol(p.Epoch) {
		c.advanceNextEpoch(p.Epoch, crypto.Digest{}, core.NONE)
	}
	//03 handlefistproposal
	if p.Phase == CBC_ONE_PHASE {
		if err := c.storeConsensusBlock(p.B); err != nil {
			return err
		}
		if p.Epoch == c.Epoch && p.ParentQC1.Priorityindex > c.ParentQ2[p.Epoch-1].Priorityindex && p.Epoch != 0 {
			return nil
		}

		if ok := c.verifyConsensusBlock(p.B); !ok {
			logger.Info.Printf("payloads missing and ask mempool to get in Epoch %d\n", p.Epoch)
			logger.Debug.Printf("checkreferrence error and try to retriver Author %d Epoch %d lenof Reference %d\n", p.Author, p.Epoch, len(p.B.Referrence))
			return nil
		}
		if miss, ok := c.CheckReference(p.B); !ok {
			//retrieve miss block
			c.Retriever.requestBlocks(miss, p.Author, p.B.Hash())
			logger.Error.Printf("Error Reference Author %d Epoch %d\n", p.Author, p.Epoch)
			return nil
		}

	}
	//更新集合
	if c.stopProtocol(p.Epoch, int8(2*p.Phase+1)) {
		logger.Debug.Printf("c.stopProtocol(m.Epoch, STOP%d\n", int8(2*p.Phase+1))
		return nil
	}
	c.AddSet(uint8(p.Phase), p.Epoch, p.Author)
	if p.Phase == CBC_ONE_PHASE {
		c.AddCBCBlockHash(p.Epoch, p.Author, p.B.Hash())
	}
	//不能对这个值进行投票了，更新完set之后不对相应的题案进行投票了
	c.Stopmu.RLock()
	if _, oks := c.StopFlag[p.Epoch]; oks {
		if _, ok := c.StopFlag[p.Epoch][c.Name]; ok {
			c.Stopmu.RUnlock()
			return nil
		}
	}
	c.Stopmu.RUnlock()
	go c.getCBCInstance(p.Epoch, p.Author).ProcessProposal(p)
	return nil
}

func (c *Core) handleVote(v *CBCVote) error {
	logger.Debug.Printf("processing vote Author %d epoch %d phase %d proposer %d\n", v.Author, v.Epoch, v.Phase, v.Proposer)
	if c.messageFilter(v.Epoch) {
		return nil
	}
	if c.restartProtocol(v.Epoch) {
		c.advanceNextEpoch(v.Epoch, crypto.Digest{}, core.NONE)
	}
	tmpphase := 2
	switch v.Phase {
	case CBC_ONE_PHASE:
		tmpphase = 2
	case CBC_TWO_PHASE:
		tmpphase = 4
	case CBC_THREE_PHASE:
		tmpphase = 6
	}

	if c.stopProtocol(v.Epoch, int8(tmpphase)) {
		logger.Debug.Printf("c.stopProtocol(m.Epoch, STOP 2 4 6\n")
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
	if c.restartProtocol(e.Epoch) {
		c.advanceNextEpoch(e.Epoch, crypto.Digest{}, core.NONE)
	}
	if c.stopProtocol(e.Epoch, STOP8) {
		logger.Debug.Printf("c.stopProtocol(m.Epoch, STOP8\n")
		return nil
	}
	if leadermap, err := c.Elector.AddShareVote(e); err != nil {
		return err
	} else if leadermap[0] != -1 { //已经形成了优先级序列
		//logger.Warn.Printf("handle electshare ready in epoch %d author epoch %d\n", e.Epoch, c.Epoch)
		c.Stopmu.Lock()
		_, ok := c.StopFlag[e.Epoch]
		if !ok {
			c.StopFlag[e.Epoch] = make(map[core.NodeID]struct{})
		}
		c.StopFlag[e.Epoch][c.Name] = struct{}{} //更新不能投票了
		c.Stopmu.Unlock()
		bestv := BestMessage{core.NONE, -1, crypto.Digest{}}
		bestq1 := BestMessage{core.NONE, -1, crypto.Digest{}}
		bestq2 := BestMessage{core.NONE, -1, crypto.Digest{}}
		bestq3 := BestMessage{core.NONE, -1, crypto.Digest{}}
		for i := 0; i < c.Committee.Size(); i++ {
			node := leadermap[i]
			c.mSet.RLock()
			if _, ok := c.VSet[e.Epoch][node]; ok {
				if bestv.BestNode == core.NONE {
					bestv.BestNode = node
					bestv.BestIndex = i
					blockhash, exist := c.GetCBCBlockHash(e.Epoch, node)
					if exist {
						bestv.BestQC = blockhash
					} else {
						//must exist
					}
				}
			}
			if _, ok := c.Q1Set[e.Epoch][node]; ok {
				if bestq1.BestNode == core.NONE {
					bestq1.BestNode = node
					bestq1.BestIndex = i
					blockhash, exist := c.GetCBCBlockHash(e.Epoch, node)
					if exist {
						bestq1.BestQC = blockhash
					} else {
						//must exist
					}
				}
			}
			if _, ok := c.Q2Set[e.Epoch][node]; ok {
				if bestq2.BestNode == core.NONE {
					bestq2.BestNode = node
					bestq2.BestIndex = i
					blockhash, exist := c.GetCBCBlockHash(e.Epoch, node)
					if exist {
						bestq2.BestQC = blockhash
					} else {
						//must exist
					}
				}
			}
			if _, ok := c.Q3Set[e.Epoch][node]; ok {
				if bestq3.BestNode == core.NONE {
					bestq3.BestNode = node
					bestq3.BestIndex = i
					blockhash, exist := c.GetCBCBlockHash(e.Epoch, node)
					if exist {
						bestq3.BestQC = blockhash
					} else {
						//must exist
					}
				}
			}
			c.mSet.RUnlock()
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
	if c.messageFilter(m.Epoch) {
		return nil
	}
	logger.Debug.Printf("processing best message epoch %d author %d\n", m.Epoch, m.Author)
	if c.restartProtocol(m.Epoch) {
		c.advanceNextEpoch(m.Epoch, crypto.Digest{}, core.NONE)
	}
	if c.stopProtocol(m.Epoch, STOP9) {
		logger.Debug.Printf("c.stopProtocol(m.Epoch, STOP9\n")
		return nil
	}
	c.AddSet(0, m.Epoch, m.BestV.BestNode)
	c.AddCBCBlockHash(m.Epoch, m.BestV.BestNode, m.BestV.BestQC)
	c.AddSet(1, m.Epoch, m.BestQ1.BestNode)
	c.AddSet(2, m.Epoch, m.BestQ2.BestNode)
	c.AddSet(3, m.Epoch, m.BestQ3.BestNode)
	//聚合2f+1个bestexchange消息，如果收集到足够的消息
	if finish, err := c.Aggreator.addBestMessage(m); err != nil {
		return err
	} else if finish { //收集到了2f+1条消息
		//logger.Warn.Printf("handle e2f+1 best messages in epoch %d  author epoch %d\n", m.Epoch, c.Epoch)
		logger.Debug.Printf("actually recieve 2f+1 best messages epoch %d \n", m.Epoch)
		//update parents q1 and q2
		c.mSet.RLock()
		_, bestvNode := c.Best(m.Epoch, c.VSet)
		bestq1Index, bestq1Node := c.Best(m.Epoch, c.Q1Set)
		bestq2Index, bestq2Node := c.Best(m.Epoch, c.Q2Set)
		bestq3Index, bestq3Node := c.Best(m.Epoch, c.Q3Set)
		c.mSet.RUnlock()
		c.ParentQ1[m.Epoch] = QuorumCert{m.Epoch, bestq1Index, bestq1Node}
		c.ParentQ2[m.Epoch] = QuorumCert{m.Epoch, bestq2Index, bestq2Node}
		//commit rule
		if bestvNode == bestq3Node || bestq3Index == 0 {
			// if bestvNode == bestq1Node && bestq1Node == bestq2Node && bestq2Node == bestq3Node {
			logger.Debug.Printf("actually commit blocks epoch %d bestq3index is %d\n", m.Epoch, bestq3Index)
			blockHash, exist := c.GetCBCBlockHash(m.Epoch, bestq3Node)
			//blockHash := c.getCBCInstance(m.Epoch, bestq3Node).BlockHash()
			if !exist {
				//Impossible
				logger.Info.Printf("can commit but has not receive the first proposal\n")
			} else {
				if block, err := c.getConsensusBlock(blockHash); err == nil {
					logger.Info.Printf("can commit and actually commit the blocks\n")
					c.BlocksWaitforCommit[block.Epoch] = block
					c.CommitAncestor(c.CommitEpoch, block.Epoch, block)
					c.CommitAllBlocks()
				}
			}
		} else { //没有达到commit条件，不能更新c.commitEpoch
			logger.Info.Printf("can not commit any blocks in this epoch %d\n", m.Epoch)
		}
		//获取q1所在块的哈希值
		//prehash := c.getCBCInstance(m.Epoch, bestq1Node).BlockHash()
		prehash, preexist := c.GetCBCBlockHash(m.Epoch, bestq1Node)
		if preexist {
			logger.Error.Printf("advance next epoch %d with prehash of block %d", m.Epoch+1, bestq1Node)
			c.advanceNextEpoch(m.Epoch+1, prehash, bestq1Node)
		} else {
			//根据ID去找别人要区块  ERROR impossible must have
			logger.Error.Printf("the block need to reference has not received\n")
		}
	}
	return nil
}

//retriever handle process

func (c *Core) handleRequestBlock(request *RequestBlockMsg) error {
	logger.Debug.Println("procesing block request")

	//Step 1: verify signature
	if !request.Verify(c.Committee) {
		return core.ErrSignature(request.MsgType())
	}

	go c.Retriever.processRequest(request)
	return nil
}

func (c *Core) handleReplyBlock(reply *ReplyBlockMsg) error {
	logger.Debug.Println("procesing block reply")

	//Step 1: verify signature
	if !reply.Verify(c.Committee) {
		return core.ErrSignature(reply.MsgType())
	}
	c.storeConsensusBlock(reply.Blocks)
	//收到这个块之后需要做些什么工作呢？

	go c.Retriever.processReply(reply)

	return nil
}

// 传递回来的是consensusBlock的哈希值
func (c *Core) handleLoopBack(blockhash crypto.Digest) error {
	if block, err := c.getConsensusBlock(blockhash); err != nil {
		logger.Error.Printf("loopback error\n")
		return err
	} else {
		logger.Error.Printf("successfully get the block author %d epoch %d\n", block.Proposer, block.Epoch)
		if block.Epoch == c.Epoch {
			logger.Debug.Printf("procesing block loop back Epoch %d node %d \n", block.Epoch, block.Proposer)
			proposal, _ := NewCBCProposal(block.Proposer, block.Epoch, CBC_ONE_PHASE, block, c.ParentQ1[block.Epoch-1], nil, c.SigService)
			c.AddSet(0, proposal.Epoch, proposal.Author)
			c.AddCBCBlockHash(proposal.Epoch, proposal.Author, blockhash)
			go c.getCBCInstance(proposal.Epoch, proposal.Author).ProcessProposal(proposal)
		} else {
			c.CheckBlockToCommit(block)
			c.CommitAllBlocks()
		}

	}
	return nil
}

// 处理共识区块的回调函数,这里有两个问题，不检查本地是否有，只在提交的时候commit所有块
func (c *Core) handleBlockLoopBack(block *ConsensusBlock) error {
	//先把这个块存起来
	c.storeConsensusBlock(block)

	if c.Epoch == block.Epoch { //如果epoch如此
		logger.Debug.Printf("procesing block loop back round %d node %d \n", block.Epoch, block.Proposer)
		proposal, _ := NewCBCProposal(block.Proposer, block.Epoch, CBC_ONE_PHASE, block, c.ParentQ1[block.Epoch-1], nil, c.SigService)
		c.AddSet(0, proposal.Epoch, proposal.Author)
		c.AddCBCBlockHash(proposal.Epoch, proposal.Author, block.Hash())
		go c.getCBCInstance(proposal.Epoch, proposal.Author).ProcessProposal(proposal)
	} else {

		//尝试提交所有的区块,先更新待提交数组，再提交
		c.CheckBlockToCommit(block)
		c.CommitAllBlocks()
	}

	return nil
}

/*********************************** Protocol End***************************************/

func (c *Core) advanceNextEpoch(epoch int64, prehash crypto.Digest, prenodeid core.NodeID) {
	// if epoch > 300 {
	// 	return
	// }
	if epoch <= c.Epoch {
		return
	}
	logger.Debug.Printf("advance next epoch %d\n", epoch)
	logger.Info.Printf("advance next epoch %d\n", epoch)
	//Clear Something
	c.Stopstate = false
	c.Epoch = epoch
	c.NewSet(c.Epoch)
	if c.Name < core.NodeID(c.Parameters.Faults) {
		c.initStopCore(c.Epoch)
		if c.stopProtocol(c.Epoch, STOP0) {
			logger.Debug.Printf("c.stopProtocol(m.Epoch, STOP0\n")
			c.Stopstate = true
			//c.advanceNextEpoch(epoch+1, crypto.Digest{})
			return
		}
	}
	block := c.generatorConsensusBlock(epoch, prehash, prenodeid)
	proposal, _ := NewCBCProposal(c.Name, c.Epoch, CBC_ONE_PHASE, block, c.ParentQ1[epoch-1], nil, c.SigService)
	c.Transimtor.Send(c.Name, core.NONE, proposal)
	c.Transimtor.RecvChannel() <- proposal
}

func (c *Core) restartProtocol(epoch int64) bool {
	if c.Name < core.NodeID(c.Parameters.Faults) {
		if epoch > c.Epoch && c.Stopstate {
			return true
		}
	}
	return false
}

func (c *Core) initStopCore(epoch int64) {
	r := rand.New(rand.NewSource(epoch))
	randnum := int8(r.Int() % 10)
	c.RandomPhase[epoch] = randnum
	logger.Debug.Printf("initStopCore in epoch %d and the stopphase is %d\n", epoch, randnum)
}

func (c *Core) stopProtocol(epoch int64, phase int8) bool {
	if c.Name < core.NodeID(c.Parameters.Faults) {
		if c.RandomPhase[epoch] <= phase {
			c.Stopstate = true //如果满足这种情况，那么就说明目前core的状态就是已经宕机的状态
			logger.Debug.Printf("stopProtocol is begining epoch is %d phase is %d", epoch, c.RandomPhase[epoch])
			return true
		} else {
			return false
		}
	}
	return false
}

func (c *Core) Run() {
	if c.Name < core.NodeID(c.Parameters.Faults) {
		logger.Debug.Printf("Node %d is faulty\n", c.Name)
		return
		// c.initStopCore(c.Epoch)
		// if c.stopProtocol(c.Epoch, STOP0) {
		// 	logger.Debug.Printf("c.stopProtocol(m.Epoch, STOP0\n")
		// 	c.advanceNextEpoch(c.Epoch+1, crypto.Digest{}, core.NONE)
		// 	return
		// }
	}

	go c.MemPool.Run()
	//first proposal
	c.NewSet(c.Epoch)
	block := c.generatorConsensusBlock(c.Epoch, crypto.Digest{}, core.NONE)
	// if _, ok := c.CBCInstancesBlockHash[c.Epoch]; !ok {
	// 	c.CBCInstancesBlockHash[c.Epoch] = make(map[core.NodeID]crypto.Digest)
	// }
	// c.CBCInstancesBlockHash[c.Epoch][c.Name] = block.Hash()
	for _, d := range block.Referrence {
		logger.Warn.Printf("epoch %d author %d generatorConsensusBlock %x\n", c.Epoch, c.Name, d)
	}

	proposal, _ := NewCBCProposal(c.Name, c.Epoch, CBC_ONE_PHASE, block, QuorumCert{0, -1, core.NONE}, nil, c.SigService)
	c.Transimtor.Send(c.Name, core.NONE, proposal)
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
				case RequestBlockType:
					err = c.handleRequestBlock(msg.(*RequestBlockMsg))
				case ReplyBlockType:
					err = c.handleReplyBlock(msg.(*ReplyBlockMsg))
				default:
				}
			}
		case block := <-c.loopBackChannel: //向mempool请求检查完毕引用完毕的通道
			{
				err = c.handleLoopBack(block)
			}
		case consensusblock := <-c.BlockloopBackChannel: //和retriver的回调接口
			{
				err = c.handleBlockLoopBack(consensusblock)
			}
		}
		if err != nil {
			logger.Warn.Println(err)
		}
	}
}
