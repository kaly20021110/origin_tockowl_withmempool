package consensus

import (
	"bft/mvba/core"
	"bft/mvba/crypto"
)

type Aggreator struct {
	committee            core.Committee
	sigService           *crypto.SigService
	bestmessageAggreator map[int64]*normalAggreator
	QCAggreator          map[int64]map[int8]*qcAggreator //epoch phase
}

func NewAggreator(sigService *crypto.SigService, committee core.Committee) *Aggreator {
	return &Aggreator{
		committee:            committee,
		sigService:           sigService,
		bestmessageAggreator: make(map[int64]*normalAggreator),
		QCAggreator:          make(map[int64]map[int8]*qcAggreator),
	}
}
func (a *Aggreator) addVote(v *CBCVote) (bool, []byte, error) {
	items, ok := a.QCAggreator[v.Epoch]
	if !ok {
		items = make(map[int8]*qcAggreator)
		a.QCAggreator[v.Epoch] = items
	}
	if item, ok := items[v.Phase]; ok {
		return item.Append(a.committee, a.sigService, v)
	} else {
		item = newqcAggreator()
		items[v.Phase] = item
		return item.Append(a.committee, a.sigService, v)
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

type qcAggreator struct {
	shares  []crypto.SignatureShare
	Authors map[core.NodeID]struct{}
}

func newqcAggreator() *qcAggreator {
	return &qcAggreator{
		shares:  make([]crypto.SignatureShare, 0),
		Authors: make(map[core.NodeID]struct{}),
	}
}
func (e *qcAggreator) Append(committee core.Committee, sigService *crypto.SigService, v *CBCVote) (bool, []byte, error) {
	if _, ok := e.Authors[v.Author]; ok {
		return false, nil, core.ErrOneMoreMessage(v.MsgType(), v.Epoch, int64(v.Phase), v.Author)
	}
	e.Authors[v.Author] = struct{}{}
	e.shares = append(e.shares, v.Signature)
	if len(e.shares) == committee.HightThreshold() {
		//qcvalue, err := crypto.CombineIntactTSPartial(e.shares, sigService.ShareKey, v.Hash())
		var result []byte
		for _, item := range e.shares {
			result = append(result, item.PartialSig...)
		}
		//return true, qcvalue, err
		return true, result, nil
	}
	return false, nil, nil

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
