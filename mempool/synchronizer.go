package mempool

import (
	"bft/mvba/core"
	"bft/mvba/crypto"
	"bft/mvba/logger"
	"bft/mvba/store"
)

type Synchronizer struct {
	Name         core.NodeID
	Store        *store.Store
	Transimtor   *core.Transmitor
	LoopBackChan chan crypto.Digest
	//consensusCoreChan chan<- core.Messgae //只接收消息
	interChan chan core.Messgae
}

func NewSynchronizer(
	Name core.NodeID,
	Transimtor *core.Transmitor,
	LoopBackChan chan crypto.Digest,
	//consensusCoreChan chan<- core.Messgae,
	store *store.Store,
) *Synchronizer {
	return &Synchronizer{
		Name:         Name,
		Store:        store,
		Transimtor:   Transimtor,
		LoopBackChan: LoopBackChan,
		//consensusCoreChan: consensusCoreChan,
		interChan: make(chan core.Messgae, 1000),
	}
}

func (sync *Synchronizer) Cleanup(epoch uint64) {
	message := &SyncCleanUpBlockMsg{
		epoch,
	}
	sync.interChan <- message
}

func (sync *Synchronizer) Verify(proposer core.NodeID, Epoch int64, digests []crypto.Digest, consensusblockhash crypto.Digest) VerifyStatus {
	logger.Debug.Printf("sync *Synchronizer verify all small block\n")
	var missing []crypto.Digest
	for _, digest := range digests {
		if _, err := sync.Store.Read(digest[:]); err != nil {
			missing = append(missing, digest)
		}
	}
	if len(missing) == 0 {
		return OK
	}
	message := &SyncBlockMsg{
		missing, proposer, Epoch, consensusblockhash,
	}
	sync.interChan <- message
	logger.Debug.Printf("sync.interChan <- message\n")
	return Wait
}

func (sync *Synchronizer) Run() {
	pending := make(map[crypto.Digest]struct {
		Epoch  uint64
		Notify chan<- struct{}
	})
	waiting := make(chan crypto.Digest, 1_000)
	for {
		select {
		case reqMsg := <-sync.interChan:
			{
				switch reqMsg.MsgType() {
				case SyncBlockType:
					req, _ := reqMsg.(*SyncBlockMsg)
					digest := req.ConsensusBlockHash
					if _, ok := pending[digest]; ok {
						continue
					}
					notify := make(chan struct{})
					go func() {
						waiting <- waiter(req.Missing, req.ConsensusBlockHash, *sync.Store, notify)
					}()
					pending[digest] = struct {
						Epoch  uint64
						Notify chan<- struct{}
					}{uint64(req.Epoch), notify}
					message := &RequestBlockMsg{
						Digests: req.Missing,
						Author:  sync.Name,
					}
					//找作者要相关的区块
					sync.Transimtor.Send(sync.Name, req.Author, message)

					//找所有人要
					//sync.Transimtor.Send(sync.Name, core.NONE, message)
				case SyncCleanUpBlockType:
					req, _ := reqMsg.(*SyncCleanUpBlockMsg)
					var keys []crypto.Digest
					for key, val := range pending {
						if val.Epoch <= req.Epoch {
							close(val.Notify)
							keys = append(keys, key)
						}
					}
					for _, key := range keys {
						delete(pending, key)
					}
				}
			}
		case block := <-waiting:
			{
				if block != (crypto.Digest{}) {
					logger.Error.Printf("successfully get the ask block\n")
					delete(pending, block)
					//LoopBack
					// msg := &LoopBackMsg{
					// 	BlockHash: block,
					// }
					sync.LoopBackChan <- block
					//sync.Transimtor.RecvChannel() <- msg
				}
			}

		}
	}
}

func waiter(missing []crypto.Digest, blockhash crypto.Digest, store store.Store, notify <-chan struct{}) crypto.Digest {
	finish := make(chan struct{})
	go func() {
		for _, digest := range missing {
			store.NotifyRead(digest[:])
		}
		close(finish)
	}()

	select {
	case <-finish:
	case <-notify:
		return crypto.Digest{}
	}
	return blockhash
}
