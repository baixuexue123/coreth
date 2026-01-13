// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

// TODO: move to network

package evm

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/ava-labs/avalanchego/ids"
	"github.com/ava-labs/avalanchego/network/p2p/gossip"
	"github.com/ava-labs/libevm/core/types"
	"github.com/ava-labs/libevm/log"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/ava-labs/coreth/core"
	"github.com/ava-labs/coreth/core/txpool"
	"github.com/ava-labs/coreth/eth"
	"github.com/ava-labs/coreth/plugin/evm/config"

	ethcommon "github.com/ava-labs/libevm/common"
)

const pendingTxsBuffer = 10

var (
	_ gossip.Gossipable               = (*GossipEthTx)(nil)
	_ gossip.Marshaller[*GossipEthTx] = (*GossipEthTxMarshaller)(nil)
	_ gossip.Set[*GossipEthTx]        = (*GossipEthTxPool)(nil)

	_ eth.PushGossiper = (*EthPushGossiper)(nil)
)

func NewGossipEthTxPool(mempool *txpool.TxPool, registerer prometheus.Registerer) (*GossipEthTxPool, error) {
	bloom, err := gossip.NewBloomFilter(
		registerer,
		"eth_tx_bloom_filter",
		config.TxGossipBloomMinTargetElements,
		config.TxGossipBloomTargetFalsePositiveRate,
		config.TxGossipBloomResetFalsePositiveRate,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize bloom filter: %w", err)
	}

	return &GossipEthTxPool{
		mempool:    mempool,
		pendingTxs: make(chan core.NewTxsEvent, pendingTxsBuffer),
		bloom:      bloom,
	}, nil
}

type GossipEthTxPool struct {
	mempool    *txpool.TxPool
	pendingTxs chan core.NewTxsEvent

	bloom *gossip.BloomFilter
	lock  sync.RWMutex

	// subscribed is set to true when the gossip subscription is active
	// mostly used for testing
	subscribed atomic.Bool
}

// IsSubscribed returns whether or not the gossip subscription is active.
func (g *GossipEthTxPool) IsSubscribed() bool {
	return g.subscribed.Load()
}

func (g *GossipEthTxPool) Subscribe(ctx context.Context) {
	sub := g.mempool.SubscribeTransactions(g.pendingTxs, false)
	if sub == nil {
		log.Warn("failed to subscribe to new txs event")
		return
	}
	g.subscribed.CompareAndSwap(false, true)
	defer func() {
		sub.Unsubscribe()
		g.subscribed.CompareAndSwap(true, false)
	}()

	for {
		select {
		case <-ctx.Done():
			log.Debug("shutting down subscription")
			return
		case pendingTxs := <-g.pendingTxs:
			g.lock.Lock()
			optimalElements := (g.mempool.PendingSize(txpool.PendingFilter{}) + len(pendingTxs.Txs)) * config.TxGossipBloomChurnMultiplier
			for _, pendingTx := range pendingTxs.Txs {
				tx := &GossipEthTx{Tx: pendingTx}
				g.bloom.Add(tx)
				reset, err := gossip.ResetBloomFilterIfNeeded(g.bloom, optimalElements)
				if err != nil {
					log.Error("failed to reset bloom filter", "err", err)
					continue
				}

				if reset {
					log.Debug("resetting bloom filter", "reason", "reached max filled ratio")

					g.mempool.IteratePending(func(tx *types.Transaction) bool {
						g.bloom.Add(&GossipEthTx{Tx: tx})
						return true
					})
				}
			}
			g.lock.Unlock()
		}
	}
}

// Add enqueues the transaction to the mempool. Subscribe should be called
// to receive an event if tx is actually added to the mempool or not.
func (g *GossipEthTxPool) Add(tx *GossipEthTx) error {
	return g.mempool.Add([]*types.Transaction{tx.Tx}, false, false)[0]
}

// Has should just return whether or not the [txID] is still in the mempool,
// not whether it is in the mempool AND pending.
func (g *GossipEthTxPool) Has(txID ids.ID) bool {
	return g.mempool.Has(ethcommon.Hash(txID))
}

func (g *GossipEthTxPool) Iterate(f func(tx *GossipEthTx) bool) {
	g.mempool.IteratePending(func(tx *types.Transaction) bool {
		return f(&GossipEthTx{Tx: tx})
	})
}

func (g *GossipEthTxPool) GetFilter() ([]byte, []byte) {
	g.lock.RLock()
	defer g.lock.RUnlock()

	return g.bloom.Marshal()
}

type GossipEthTxMarshaller struct{}

func (GossipEthTxMarshaller) MarshalGossip(tx *GossipEthTx) ([]byte, error) {
	return tx.Tx.MarshalBinary()
}

func (GossipEthTxMarshaller) UnmarshalGossip(bytes []byte) (*GossipEthTx, error) {
	tx := &GossipEthTx{
		Tx: &types.Transaction{},
	}

	return tx, tx.Tx.UnmarshalBinary(bytes)
}

type GossipEthTx struct {
	Tx *types.Transaction
}

func (tx *GossipEthTx) GossipID() ids.ID {
	return ids.ID(tx.Tx.Hash())
}

// EthPushGossiper is used by the ETH backend to push transactions issued over
// the RPC and added to the mempool to peers.
type EthPushGossiper struct {
	vm *VM
	// priorityAddresses is a set of addresses that trigger immediate gossip
	// when a transaction with a matching "to" address is added.
	priorityAddresses map[ethcommon.Address]struct{}
}

// NewEthPushGossiper creates a new EthPushGossiper with the given priority addresses.
func NewEthPushGossiper(vm *VM, priorityAddresses []ethcommon.Address) *EthPushGossiper {
	addrSet := make(map[ethcommon.Address]struct{}, len(priorityAddresses))
	for _, addr := range priorityAddresses {
		addrSet[addr] = struct{}{}
	}
	return &EthPushGossiper{
		vm:                vm,
		priorityAddresses: addrSet,
	}
}

func (e *EthPushGossiper) Add(tx *types.Transaction) {
	// eth.Backend is initialized before the [ethTxPushGossiper] is created, so
	// we just ignore any gossip requests until it is set.
	ethTxPushGossiper := e.vm.ethTxPushGossiper.Get()
	if ethTxPushGossiper == nil {
		return
	}
	ethTxPushGossiper.Add(&GossipEthTx{tx})

	// If the transaction's "to" address is in the priority list, trigger immediate gossip
	if e.isPriorityTx(tx) {
		log.Info("triggering immediate gossip for priority transaction", "txHash", tx.Hash(), "to", tx.To())
		if err := ethTxPushGossiper.Gossip(context.Background()); err != nil {
			log.Warn("failed to trigger immediate gossip", "err", err)
		}
	}
}

// isPriorityTx checks if the transaction's "to" address is in the priority list.
func (e *EthPushGossiper) isPriorityTx(tx *types.Transaction) bool {
	if len(e.priorityAddresses) == 0 {
		return false
	}
	to := tx.To()
	if to == nil {
		return false
	}
	_, exists := e.priorityAddresses[*to]
	return exists
}
