package consensus

import (
	"bft/mvba/core"
	"bft/mvba/crypto"
	"bft/mvba/logger"
	"bft/mvba/mempool"
	"bft/mvba/network"
	"bft/mvba/pool"
	"bft/mvba/store"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"
)

func Consensus(
	id core.NodeID,
	committee core.Committee,
	parameters core.Parameters,
	txpool *pool.Pool,
	store *store.Store,
	sigService *crypto.SigService,
	callBack chan<- struct{},
	loopbackchannel chan crypto.Digest,
	connectChannel chan core.Messgae,
	pool *mempool.Mempool,
) error {
	logger.Info.Printf(
		"Consensus Node ID: %d\n",
		id,
	)
	logger.Info.Printf(
		"Consensus DDos: %v, Faults: %v \n",
		parameters.DDos, parameters.Faults,
	)
	logger.Info.Println("Protocol: SMVBA")
	if id < core.NodeID(parameters.Faults) {
		logger.Info.Println("Byzantine Node")
	} else {
		logger.Info.Println("Honest Node")
	}

	//step1 .Invoke networl
	addr := fmt.Sprintf(":%s", strings.Split(committee.Address(id), ":")[1])
	cc := network.NewCodec(DefaultMessageTypeMap) //消息类型注册表
	sender1 := network.NewSender(cc)
	go sender1.Run()
	receiver1 := network.NewReceiver(addr, cc)
	go receiver1.Run()
	transimtor := core.NewTransmitor(sender1, receiver1, parameters, committee)
	//Step 2: Waiting for all nodes to be online
	logger.Info.Println("Waiting for all nodes to be online...")
	time.Sleep(time.Millisecond * time.Duration(parameters.SyncTimeout))
	addrs := committee.BroadCast(id)
	wg := sync.WaitGroup{}
	for _, addr := range addrs {
		wg.Add(1)
		go func(address string) {
			defer wg.Done()
			for {
				conn, err := net.Dial("tcp", address)
				if err != nil {
					time.Sleep(time.Microsecond * 200)
					continue
				}
				conn.Close()
				break
			}
		}(addr)
	}
	wg.Wait()

	txpool.Run()

	//Step 3: start protocol
	core := NewCore(id, committee, parameters, sigService, store, txpool, transimtor, callBack, loopbackchannel, connectChannel, pool)
	go pool.Run()
	go core.Run()

	return nil
}
