package pubsub

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/dTelecom/p2p-database/common"
	"github.com/libp2p/go-libp2p-kad-dht/dual"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/p2p/discovery/routing"
	"github.com/libp2p/go-libp2p/p2p/discovery/util"
	"github.com/multiformats/go-multiaddr"
)

// DiscoveryService provides peer discovery functionality
type DiscoveryService struct {
	host      host.Host
	dht       *dual.DHT
	discovery *routing.RoutingDiscovery
	logger    common.Logger

	// Topic-specific discovery tracking
	topicDiscoveryMutex  sync.RWMutex
	activeTopicDiscovery map[string]context.CancelFunc // topic -> cancel function
}

// NewDiscoveryService creates a new discovery service
func NewDiscoveryService(host host.Host, dht *dual.DHT, logger common.Logger) *DiscoveryService {
	return &DiscoveryService{
		host:                 host,
		dht:                  dht,
		discovery:            routing.NewRoutingDiscovery(dht),
		logger:               logger,
		activeTopicDiscovery: make(map[string]context.CancelFunc),
	}
}

// StartDiscovery starts the discovery process for a database
func (ds *DiscoveryService) StartDiscovery(ctx context.Context, databaseName string) error {
	// Create rendezvous point for this database
	rendezvous := fmt.Sprintf("p2p-database-discovery_%s", databaseName)

	ds.logger.Info("Starting peer discovery",
		"database", databaseName,
		"rendezvous", rendezvous)

	// Start advertising ourselves
	go ds.advertiseLoop(ctx, rendezvous)

	// Start finding peers
	go ds.findPeersLoop(ctx, rendezvous)

	return nil
}

// advertiseLoop starts persistent advertisement for this node
func (ds *DiscoveryService) advertiseLoop(ctx context.Context, rendezvous string) {
	ds.logger.Debug("Starting persistent advertisement", "rendezvous", rendezvous)

	// Call util.Advertise ONLY ONCE - it creates its own goroutine with internal loop
	// that handles periodic re-advertisement based on TTL
	util.Advertise(ctx, ds.discovery, rendezvous)

	ds.logger.Debug("Advertisement started for", "rendezvous", rendezvous)

	// Wait for context cancellation - the util.Advertise goroutine will handle everything else
	<-ctx.Done()

	ds.logger.Debug("Advertisement stopped for", "rendezvous", rendezvous, "reason", ctx.Err())
}

// findPeersLoop periodically searches for peers
func (ds *DiscoveryService) findPeersLoop(ctx context.Context, rendezvous string) {
	// Immediate peer discovery on startup
	peers, err := util.FindPeers(ctx, ds.discovery, rendezvous)
	if err != nil {
		ds.logger.Debug("Initial peer discovery failed", "rendezvous", rendezvous, "error", err.Error())
	} else {
		go ds.processPeers(ctx, peers, rendezvous)
	}

	// Continue with periodic peer discovery (fast interval for quick connectivity)
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// Find peers
			peers, err := util.FindPeers(ctx, ds.discovery, rendezvous)
			if err != nil {
				ds.logger.Warn("Failed to find peers",
					"rendezvous", rendezvous,
					"error", err.Error())
				continue
			}

			// Process found peers
			go ds.processPeers(ctx, peers, rendezvous)
		}
	}
}

// processPeers processes peers found through discovery
func (ds *DiscoveryService) processPeers(ctx context.Context, peers []peer.AddrInfo, rendezvous string) {
	connectedCount := 0

	for _, peerInfo := range peers {
		// Skip if it's ourselves
		if peerInfo.ID == ds.host.ID() {
			continue
		}

		// Skip if already connected
		if ds.host.Network().Connectedness(peerInfo.ID) == network.Connected {
			continue
		}

		// Try to connect
		connectCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		if err := ds.host.Connect(connectCtx, peerInfo); err != nil {
			ds.logger.Debug("Failed to connect to discovered peer",
				"peer_id", peerInfo.ID.String(),
				"rendezvous", rendezvous,
				"error", err.Error())
			cancel()
			continue
		}
		cancel()

		connectedCount++
		ds.logger.Info("Connected to discovered peer",
			"peer_id", peerInfo.ID.String(),
			"rendezvous", rendezvous,
			"addrs", peerInfo.Addrs)

		// Limit connections per discovery round
		if connectedCount >= 5 {
			break
		}
	}

	if connectedCount > 0 {
		ds.logger.Info("Discovery round completed",
			"rendezvous", rendezvous,
			"new_connections", connectedCount)
	}
}

// GetConnectedPeers returns information about currently connected peers
func (ds *DiscoveryService) GetConnectedPeers() []peer.AddrInfo {
	peers := ds.host.Network().Peers()
	var peerInfos []peer.AddrInfo

	for _, peerID := range peers {
		conns := ds.host.Network().ConnsToPeer(peerID)
		if len(conns) > 0 {
			peerInfo := peer.AddrInfo{
				ID:    peerID,
				Addrs: make([]multiaddr.Multiaddr, 0, len(conns)),
			}
			for _, conn := range conns {
				peerInfo.Addrs = append(peerInfo.Addrs, conn.RemoteMultiaddr())
			}
			peerInfos = append(peerInfos, peerInfo)
		}
	}

	return peerInfos
}

// GetDHTStats returns DHT statistics
func (ds *DiscoveryService) GetDHTStats() map[string]interface{} {
	return map[string]interface{}{
		"connected_peers": len(ds.host.Network().Peers()),
	}
}

// StartTopicDiscovery starts topic-specific peer discovery for GossipSub mesh formation
func (ds *DiscoveryService) StartTopicDiscovery(ctx context.Context, databaseName, topic string) error {
	// Create topic-specific rendezvous point
	topicRendezvous := fmt.Sprintf("p2p-database-topic_%s_%s", databaseName, topic)

	ds.topicDiscoveryMutex.Lock()
	defer ds.topicDiscoveryMutex.Unlock()

	// Check if already running discovery for this topic
	if cancelFunc, exists := ds.activeTopicDiscovery[topic]; exists {
		ds.logger.Debug("Topic discovery already running", "topic", topic)
		cancelFunc() // Cancel existing discovery
	}

	// Create cancellable context for this topic discovery
	topicCtx, cancel := context.WithCancel(ctx)
	ds.activeTopicDiscovery[topic] = cancel

	ds.logger.Info("Starting topic-specific peer discovery",
		"database", databaseName,
		"topic", topic,
		"rendezvous", topicRendezvous)

	// Start topic advertisement
	go ds.advertiseLoop(topicCtx, topicRendezvous)

	// Start topic peer finding with more aggressive search
	go ds.findTopicPeersLoop(topicCtx, topicRendezvous, topic)

	return nil
}

// StopTopicDiscovery stops topic-specific peer discovery
func (ds *DiscoveryService) StopTopicDiscovery(topic string) {
	ds.topicDiscoveryMutex.Lock()
	defer ds.topicDiscoveryMutex.Unlock()

	if cancelFunc, exists := ds.activeTopicDiscovery[topic]; exists {
		ds.logger.Info("Stopping topic discovery", "topic", topic)
		cancelFunc()
		delete(ds.activeTopicDiscovery, topic)
	}
}

// findTopicPeersLoop is more aggressive peer discovery for topic-specific needs
func (ds *DiscoveryService) findTopicPeersLoop(ctx context.Context, rendezvous, topic string) {
	// Immediate peer discovery on startup
	peers, err := util.FindPeers(ctx, ds.discovery, rendezvous)
	if err != nil {
		ds.logger.Debug("Initial topic peer discovery failed", "rendezvous", rendezvous, "topic", topic, "error", err.Error())
	} else {
		go ds.processTopicPeers(ctx, peers, rendezvous, topic)
	}

	// More aggressive discovery for topics (every 1 second initially, then every 5 seconds)
	initialTicker := time.NewTicker(1 * time.Second)

	// Run aggressive discovery for 30 seconds
	aggressiveTimer := time.NewTimer(30 * time.Second)

	for {
		select {
		case <-ctx.Done():
			initialTicker.Stop()
			aggressiveTimer.Stop()
			return
		case <-aggressiveTimer.C:
			// Switch to slower discovery after 30 seconds
			initialTicker.Stop()
			slowTicker := time.NewTicker(5 * time.Second)
			defer slowTicker.Stop()

			for {
				select {
				case <-ctx.Done():
					return
				case <-slowTicker.C:
					peers, err := util.FindPeers(ctx, ds.discovery, rendezvous)
					if err != nil {
						ds.logger.Debug("Topic peer discovery failed",
							"rendezvous", rendezvous,
							"topic", topic,
							"error", err.Error())
						continue
					}
					go ds.processTopicPeers(ctx, peers, rendezvous, topic)
				}
			}
		case <-initialTicker.C:
			// Aggressive discovery phase
			peers, err := util.FindPeers(ctx, ds.discovery, rendezvous)
			if err != nil {
				ds.logger.Debug("Topic peer discovery failed",
					"rendezvous", rendezvous,
					"topic", topic,
					"error", err.Error())
				continue
			}
			go ds.processTopicPeers(ctx, peers, rendezvous, topic)
		}
	}
}

// processTopicPeers processes peers found through topic discovery
func (ds *DiscoveryService) processTopicPeers(ctx context.Context, peers []peer.AddrInfo, rendezvous, topic string) {
	connectedCount := 0

	for _, peerInfo := range peers {
		// Skip if it's ourselves
		if peerInfo.ID == ds.host.ID() {
			continue
		}

		// Skip if already connected
		if ds.host.Network().Connectedness(peerInfo.ID) == network.Connected {
			continue
		}

		// Try to connect
		connectCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
		if err := ds.host.Connect(connectCtx, peerInfo); err != nil {
			ds.logger.Debug("Failed to connect to topic peer",
				"peer_id", peerInfo.ID.String(),
				"topic", topic,
				"rendezvous", rendezvous,
				"error", err.Error())
			cancel()
			continue
		}
		cancel()

		connectedCount++
		ds.logger.Info("Connected to topic peer",
			"peer_id", peerInfo.ID.String(),
			"topic", topic,
			"rendezvous", rendezvous)

		// Connect to more topic peers than general peers
		if connectedCount >= 10 {
			break
		}
	}

	if connectedCount > 0 {
		ds.logger.Info("Topic discovery round completed",
			"topic", topic,
			"rendezvous", rendezvous,
			"new_connections", connectedCount)
	}
}
