package consensus

import (
	"bft/mvba/core"
	"bft/mvba/crypto"
	"crypto/sha256"
	"fmt"
	"math/big"
	"sort"
)

type Elector struct {
	nodePriority    map[int64]map[int]core.NodeID //epoch - node队列
	electAggreators map[int64]*ElectAggreator
	sigService      *crypto.SigService
	committee       core.Committee
}

func NewElector(sigService *crypto.SigService, committee core.Committee) *Elector {
	return &Elector{
		nodePriority:    make(map[int64]map[int]core.NodeID),
		electAggreators: make(map[int64]*ElectAggreator),
		sigService:      sigService,
		committee:       committee,
	}
}

func (e *Elector) GetPriority(epoch int64) map[int]core.NodeID {
	var noReady map[int]core.NodeID = make(map[int]core.NodeID)
	noReady[0] = -1
	if leader, ok := e.nodePriority[epoch]; !ok {
		return noReady
	} else {
		return leader
	}
}

func (e *Elector) SetPriority(epoch int64, index map[int]core.NodeID) {
	if _, ok := e.nodePriority[epoch]; !ok {
		e.nodePriority[epoch] = make(map[int]core.NodeID)
	}
	e.nodePriority[epoch] = index
}

func (e *Elector) AddShareVote(share *ElectShare) (map[int]core.NodeID, error) { //给出了最高优先级的内容，但是实际使用的时候应该不是这么用的
	var noReady map[int]core.NodeID = make(map[int]core.NodeID)
	noReady[0] = -1
	item, ok := e.electAggreators[share.Epoch]
	if !ok {
		item = NewElectAggreator()
		e.electAggreators[share.Epoch] = item
	}
	node, err := item.Append(e.committee, e.sigService, share)
	if err != nil {
		return noReady, nil
	}
	if node[0] != -1 {
		e.SetPriority(share.Epoch, node)
	}
	return node, nil
}

const RANDOM_LEN = 3

type ElectAggreator struct {
	shares  []crypto.SignatureShare
	authors map[core.NodeID]struct{}
}

func NewElectAggreator() *ElectAggreator {
	return &ElectAggreator{
		shares:  make([]crypto.SignatureShare, 0),
		authors: make(map[core.NodeID]struct{}),
	}
}

func hash(rand, index int) uint64 {
	h := sha256.Sum256([]byte(fmt.Sprintf("%d-%d", rand, index)))
	return new(big.Int).SetBytes(h[:]).Uint64() // 转换整个哈希值为整数
}

func (e *ElectAggreator) Append(committee core.Committee, sigService *crypto.SigService, elect *ElectShare) (map[int]core.NodeID, error) {
	var noReady map[int]core.NodeID = make(map[int]core.NodeID)
	noReady[0] = -1
	if _, ok := e.authors[elect.Author]; ok {
		return noReady, core.ErrOneMoreMessage(elect.MsgType(), elect.Epoch, 0, elect.Author)
	}
	e.authors[elect.Author] = struct{}{}
	e.shares = append(e.shares, elect.SigShare)
	if len(e.shares) == committee.HightThreshold() {
		coin, err := crypto.CombineIntactTSPartial(e.shares, sigService.ShareKey, elect.Hash())
		if err != nil {
			return noReady, err
		}
		var rand int
		for i := 0; i < RANDOM_LEN; i++ {
			if coin[i] > 0 {
				rand = rand<<8 + int(coin[i])
			} else {
				rand = rand<<8 + int(-coin[i])
			}
		}
		index := make([]int, committee.Size()+1)
		for i := 0; i < committee.Size(); i++ {
			index[i] = i
		}
		sort.Slice(index, func(i, j int) bool {
			return hash(rand, index[i]) > hash(rand, index[j])
		})
		var prioritymap map[int]core.NodeID = make(map[int]core.NodeID)
		for i := 0; i < committee.Size(); i++ {
			prioritymap[i] = core.NodeID(index[i])
		}
		return prioritymap, nil
	}
	return noReady, nil
}
