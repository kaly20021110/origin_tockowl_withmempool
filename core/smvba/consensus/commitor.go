package consensus

import (
	"bft/mvba/logger"
	"bft/mvba/mempool"
	"bft/mvba/store"
)

type Committor struct {
	Index    int64
	Blocks   map[int64]*mempool.Block
	commitCh chan *mempool.Block
	callBack chan<- struct{}
	Store    *store.Store
}

func NewCommittor(store *store.Store, callBack chan<- struct{}) *Committor {
	c := &Committor{
		Index:    0,
		Blocks:   map[int64]*mempool.Block{},
		commitCh: make(chan *mempool.Block, 1000),
		callBack: callBack,
		Store:    store,
	}
	go c.run()
	return c
}

func (c *Committor) Commit(block *mempool.Block) {
	c.commitCh <- block
	// if block.Epoch < c.Index {
	// 	return
	// }
	// c.Blocks[block.Epoch] = block
	// for {
	// 	if b, ok := c.Blocks[c.Index]; ok {
	// 		c.commitCh <- b
	// 		delete(c.Blocks, c.Index)
	// 		c.Index++
	// 	} else {
	// 		break
	// 	}
	// }
}

func (c *Committor) run() {
	for b := range c.commitCh {
		if b.Batch.ID != -1 {
			logger.Info.Printf("commit Block node %d batch_id %d\n", b.Proposer, b.Batch.ID)
		} else {
			logger.Info.Printf("commit null Block node %d batch_id %d\n", b.Proposer, b.Batch.ID)
		}
		c.callBack <- struct{}{}
	}
}

//关于实际的commit逻辑

// func (h *Handler) Commit(block *Block) error {
// 	h.mu.Lock()
// 	if h.lastCommittedRound >= block.Round {
// 		h.mu.Unlock()
// 		return nil
// 	}
// 	h.mu.Unlock()

// 	toCommit := list.New()
// 	toCommit.PushBack(block.Clone())

// 	// 获取祖先直到 lastCommittedRound + 1
// 	parent := block.Clone()
// 	for {
// 		h.mu.Lock()
// 		needAncestor := h.lastCommittedRound+1 < parent.Round
// 		h.mu.Unlock()
// 		if !needAncestor {
// 			break
// 		}

// 		ancestor, err := h.synchronizer.GetParentBlock(parent)
// 		if err != nil {
// 			return fmt.Errorf("failed to get parent: %w", err)
// 		}
// 		if ancestor == nil {
// 			return fmt.Errorf("missing ancestor for block %v", parent.Digest())
// 		}

// 		toCommit.PushFront(ancestor.Clone())
// 		parent = ancestor
// 	}

// 	// 更新最后提交 round
// 	h.mu.Lock()
// 	h.lastCommittedRound = block.Round
// 	h.mu.Unlock()

// 	// 提交所有 block 到应用层
// 	for e := toCommit.Front(); e != nil; e = e.Next() {
// 		blk := e.Value.(*Block)

// 		if len(blk.Payload) > 0 {
// 			log.Printf("Committed %v", blk)

// 			// 如果是 benchmark 模式
// 			if h.benchmark {
// 				for _, item := range blk.Payload {
// 					encoded := base64.StdEncoding.EncodeToString(item)
// 					log.Printf("Committed B%d(%s)", blk.Round, encoded)
// 				}
// 			}
// 		}

// 		log.Printf("Committed block: %+v", blk)

// 		// 发送到 commit channel
// 		select {
// 		case h.commitCh <- blk:
// 		default:
// 			log.Printf("Warning: Commit channel full or closed for block %v", blk.Digest())
// 		}
// 	}

// 	return nil
// }
