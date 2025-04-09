package consensus

import (
	"bft/mvba/core"
)

type Aggreator struct {
	committee            core.Committee
	bestmessageAggreator map[int64]*normalAggreator
}

func NewAggreator(committee core.Committee) *Aggreator {
	return &Aggreator{
		committee:            committee,
		bestmessageAggreator: make(map[int64]*normalAggreator),
	}
}

func (a *Aggreator) addBestMessage(m *BestMsg) (bool, error) {
	item, ok := a.bestmessageAggreator[m.Epoch]
	if !ok {
		item = newNormalAggreator()
		a.bestmessageAggreator[m.Epoch] = item
	}
	return item.Append(m.Author, a.committee, m.MsgType(), m.Epoch)
}

type normalAggreator struct {
	Authors map[core.NodeID]struct{}
}

func newNormalAggreator() *normalAggreator {
	return &normalAggreator{
		Authors: make(map[core.NodeID]struct{}),
	}
}

func (a *normalAggreator) Append(node core.NodeID, committee core.Committee, mType int, epoch int64) (bool, error) {
	if _, ok := a.Authors[node]; ok {
		return false, core.ErrOneMoreMessage(mType, epoch, 0, node)
	}
	a.Authors[node] = struct{}{}
	if len(a.Authors) == committee.HightThreshold() {
		return true, nil
	}
	return false, nil
}
