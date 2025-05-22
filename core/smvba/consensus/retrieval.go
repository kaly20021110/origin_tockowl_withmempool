package consensus

import (
	"bft/mvba/core"
	"bft/mvba/crypto"
	"bft/mvba/logger"
	"bft/mvba/store"
	"time"
)

//检索共识区块

const (
	ReqType = iota
	ReplyType
)

type reqRetrieve struct {
	typ       int
	reqID     int
	digest    crypto.Digest
	nodeID    core.NodeID
	backBlock crypto.Digest
}

type Retriever struct {
	nodeID          core.NodeID
	transmitor      *core.Transmitor
	cnt             int
	pendding        map[crypto.Digest]struct{} //dealing request
	requests        map[int]*RequestBlockMsg   //Request
	loopBackBlocks  map[int]crypto.Digest      // loopback deal block
	loopBackCnts    map[int]int
	miss2Blocks     map[crypto.Digest][]int //
	reqChannel      chan *reqRetrieve
	sigService      *crypto.SigService
	store           *store.Store
	parameters      core.Parameters
	loopBackChannel chan<- *ConsensusBlock
}

func NewRetriever(
	nodeID core.NodeID,
	store *store.Store,
	transmitor *core.Transmitor,
	sigService *crypto.SigService,
	parameters core.Parameters,
	loopBackChannel chan<- *ConsensusBlock,
) *Retriever {

	r := &Retriever{
		nodeID:          nodeID,
		cnt:             0,
		pendding:        make(map[crypto.Digest]struct{}),
		requests:        make(map[int]*RequestBlockMsg),
		loopBackBlocks:  make(map[int]crypto.Digest),
		loopBackCnts:    make(map[int]int),
		reqChannel:      make(chan *reqRetrieve, 1_00),
		miss2Blocks:     make(map[crypto.Digest][]int),
		store:           store,
		sigService:      sigService,
		transmitor:      transmitor,
		parameters:      parameters,
		loopBackChannel: loopBackChannel,
	}
	go r.run()

	return r
}

func (r *Retriever) run() {
	ticker := time.NewTicker(time.Duration(r.parameters.RetryDelay)) //定时进行请求区块
	for {
		select {
		case req := <-r.reqChannel:
			switch req.typ {
			case ReqType: //request Block
				{
					r.loopBackBlocks[r.cnt] = req.backBlock //请求第x个共识块
					r.loopBackCnts[r.cnt] = len(req.digest) //请求里面包含了多个摘要的数量
					var missBlocks []crypto.Digest
					r.miss2Blocks[req.digest] = append(r.miss2Blocks[req.digest], r.cnt)
					if _, ok := r.pendding[req.digest]; ok {
						continue
					}
					missBlocks = append(missBlocks, req.digest)
					r.pendding[req.digest] = struct{}{}

					if len(missBlocks) > 0 { //如果本地发现还有区块没收到，就继续去找别人要
						request, _ := NewRequestBlock(r.nodeID, missBlocks[0], r.cnt, time.Now().UnixMilli(), r.sigService)
						logger.Debug.Printf("sending request for miss block reqID %d \n", r.cnt)
						_ = r.transmitor.Send(request.Author, req.nodeID, request)
						r.requests[request.ReqID] = request
					}

					r.cnt++
				}
			case ReplyType: //request finish
				{
					logger.Debug.Printf("receive reply for miss block reqID %d \n", req.reqID)
					if _, ok := r.requests[req.reqID]; ok {
						_req := r.requests[req.reqID]
						d := _req.MissBlock
						for _, id := range r.miss2Blocks[d] {
							r.loopBackCnts[id]--
							if r.loopBackCnts[id] == 0 {
								go r.loopBack(r.loopBackBlocks[id])
							}
						}
						delete(r.pendding, d)          // delete
						delete(r.requests, _req.ReqID) //delete request that finished
					}
				}
			}
		case <-ticker.C: // recycle request
			{
				now := time.Now().UnixMilli()
				for _, req := range r.requests {
					if now-req.Ts >= int64(r.parameters.RetryDelay) {
						request, _ := NewRequestBlock(req.Author, req.MissBlock, req.ReqID, now, r.sigService)
						r.requests[req.ReqID] = request
						//BroadCast to all node
						r.transmitor.Send(r.nodeID, core.NONE, request)
					}
				}
			}
		}
	}
}

func (r *Retriever) requestBlocks(digest crypto.Digest, nodeid core.NodeID, backBlock crypto.Digest) {
	logger.Info.Printf("begin to go retriever\n")
	req := &reqRetrieve{
		typ:       ReqType,
		digest:    digest,
		nodeID:    nodeid,
		backBlock: backBlock,
	}
	r.reqChannel <- req
}

func (r *Retriever) processRequest(request *RequestBlockMsg) {
	var block *ConsensusBlock
	missBlock := request.MissBlock
	if val, err := r.store.Read(missBlock[:]); err != nil {
		logger.Warn.Println(err)
	} else {
		block := &ConsensusBlock{}
		if err := block.Decode(val); err != nil {
			logger.Warn.Println(err)
			return
		}
	}
	//reply
	reply, _ := NewReplyBlockMsg(r.nodeID, block, request.ReqID, r.sigService)
	r.transmitor.Send(r.nodeID, request.Author, reply)
}

func (r *Retriever) processReply(reply *ReplyBlockMsg) {
	req := &reqRetrieve{
		typ:   ReplyType,
		reqID: reply.ReqID,
	}
	r.reqChannel <- req
}

func (r *Retriever) loopBack(blockHash crypto.Digest) {
	// logger.Debug.Printf("processing loopback")
	if val, err := r.store.Read(blockHash[:]); err != nil {
		//must be  received
		logger.Error.Println(err)
		panic(err)
	} else {
		block := &ConsensusBlock{}
		if err := block.Decode(val); err != nil {
			logger.Warn.Println(err)
			panic(err)
		} else {
			r.loopBackChannel <- block
		}
	}
}
