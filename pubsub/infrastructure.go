package pubsub

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/dTelecom/p2p-database/common"
	logging "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p-kad-dht/dual"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/routing"
	"github.com/libp2p/go-libp2p/p2p/discovery/mdns"
	"github.com/libp2p/go-libp2p/p2p/host/autorelay"
	"github.com/libp2p/go-libp2p/p2p/net/connmgr"
	libp2pquic "github.com/libp2p/go-libp2p/p2p/transport/quic"
	"github.com/libp2p/go-libp2p/p2p/transport/tcp"
	"github.com/libp2p/go-libp2p/p2p/transport/websocket"
	"github.com/multiformats/go-multiaddr"
)

// notifee receives notifications of local peer discoveries via mDNS
type notifee struct {
	host   host.Host
	logger common.Logger
}

func (n *notifee) HandlePeerFound(pi peer.AddrInfo) {
	n.logger.Debug("mDNS discovered local peer", "peer_id", pi.ID.String(), "addrs", pi.Addrs)

	// Skip if it's ourselves
	if pi.ID == n.host.ID() {
		return
	}

	// Skip if already connected
	if n.host.Network().Connectedness(pi.ID) == network.Connected {
		n.logger.Debug("Already connected to mDNS discovered peer", "peer_id", pi.ID.String())
		return
	}

	// Try to connect to the discovered peer
	connectCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	if err := n.host.Connect(connectCtx, pi); err != nil {
		n.logger.Debug("Failed to connect to mDNS discovered peer",
			"peer_id", pi.ID.String(),
			"error", err.Error())
		return
	}

	n.logger.Info("Connected to mDNS discovered local peer",
		"peer_id", pi.ID.String(),
		"addrs", pi.Addrs)
}

// P2PInfrastructure manages the libp2p resources for a single database connection
type P2PInfrastructure struct {
	host      host.Host             // Single libp2p host
	dht       *dual.DHT             // Kademlia DHT for peer discovery
	gossipSub *pubsub.PubSub        // GossipSub instance for pub/sub messaging
	connMgr   *connmgr.BasicConnMgr // Connection manager
	discovery *DiscoveryService     // Peer discovery service
	mdns      mdns.Service          // mDNS service for local discovery

	// Configuration
	logger common.Logger

	// Readiness and bootstrap retry state
	isReady           bool                         // True when node has at least 1 peer connected
	readinessMutex    sync.RWMutex                 // Protects isReady
	getBootstrapNodes common.GetBootstrapNodesFunc // Bootstrap function for retry attempts
	retryCancel       context.CancelFunc           // Cancel function for bootstrap retry goroutine
	retryMutex        sync.Mutex                   // Protects retryCancel

	// Shutdown handling
	shutdownCtx    context.Context    // Context that gets cancelled during shutdown
	shutdownCancel context.CancelFunc // Cancel function for shutdown
}

// DatabaseInstance represents a database instance with isolated topics
type DatabaseInstance struct {
	name          string                        // Database name
	gater         *common.SolanaRegistryGater   // Registry-based connection gater
	topics        map[string]*pubsub.Topic      // Joined topics
	subscriptions map[string]*TopicSubscription // Active subscriptions
	mutex         sync.RWMutex                  // Protects topics/subscriptions
}

// TopicSubscription manages a topic subscription
type TopicSubscription struct {
	subscription *pubsub.Subscription // libp2p subscription
	topic        *pubsub.Topic        // Topic handle
	handler      common.PubSubHandler // User callback function
	cancel       context.CancelFunc   // Cancel function for the listener goroutine
}

// DB represents the main database connection
type DB struct {
	infrastructure *P2PInfrastructure
	instance       *DatabaseInstance
}

// IsReady returns true if the node has at least 1 peer connected
func (infra *P2PInfrastructure) IsReady() bool {
	infra.readinessMutex.RLock()
	defer infra.readinessMutex.RUnlock()
	return infra.isReady
}

// updateReadiness updates the readiness state based on peer count
func (infra *P2PInfrastructure) updateReadiness() {
	peerCount := len(infra.host.Network().Peers())

	infra.readinessMutex.Lock()
	defer infra.readinessMutex.Unlock()

	wasReady := infra.isReady
	infra.isReady = peerCount > 0

	// Log readiness state changes
	if !wasReady && infra.isReady {
		infra.logger.Info("Node became ready", "peer_count", peerCount)
	} else if wasReady && !infra.isReady {
		infra.logger.Info("Node no longer ready", "peer_count", peerCount)
		// Start bootstrap retry when node becomes not ready
		go infra.startBootstrapRetryAfterDelay()
	}
}

// startBootstrapRetryAfterDelay starts bootstrap retry after a short delay
// This is used when a node loses all peers and becomes not ready
func (infra *P2PInfrastructure) startBootstrapRetryAfterDelay() {
	// Check if retry is already running (avoid starting multiple goroutines)
	infra.retryMutex.Lock()
	if infra.retryCancel != nil {
		infra.retryMutex.Unlock()
		infra.logger.Debug("Bootstrap retry already running, skipping delayed start")
		return
	}
	infra.retryMutex.Unlock()

	// Wait a bit before starting retry to avoid immediate reconnection attempts
	time.Sleep(2 * time.Second)

	infra.readinessMutex.RLock()
	stillNotReady := !infra.isReady
	infra.readinessMutex.RUnlock()

	if stillNotReady {
		infra.logger.Info("Node still not ready after delay, starting bootstrap retry")
		infra.startBootstrapRetry(infra.shutdownCtx)
	} else {
		infra.logger.Debug("Node became ready during delay, skipping bootstrap retry")
	}
}

// enableLibp2pDebugLogging enables debug logging for key libp2p subsystems
func enableLibp2pDebugLogging() {
	// Set log levels for libp2p subsystems to debug level
	// Ignore errors as these are non-critical debug setup calls
	_ = logging.SetLogLevel("swarm2", "DEBUG")       // Connection management
	_ = logging.SetLogLevel("dht", "DEBUG")          // DHT operations
	_ = logging.SetLogLevel("pubsub", "DEBUG")       // GossipSub
	_ = logging.SetLogLevel("net/identify", "DEBUG") // Peer identification
	_ = logging.SetLogLevel("basichost", "DEBUG")    // Basic host operations
	_ = logging.SetLogLevel("autonat", "DEBUG")      // NAT detection
	_ = logging.SetLogLevel("connmgr", "DEBUG")      // Connection manager
	_ = logging.SetLogLevel("transport", "DEBUG")    // Transport layer
}

// initializeP2PInfrastructure creates the process-level infrastructure (once per process)
func initializeP2PInfrastructure(config common.Config) (*P2PInfrastructure, error) {
	// Enable debug logging for libp2p components if debug flag is set
	if config.Debug {
		enableLibp2pDebugLogging()
		config.Logger.Debug("Enabled libp2p debug logging")
	}

	// Create identity from Solana private key
	privateKey, peerID, err := common.CreateIdentityFromSolanaKey(config.WalletPrivateKey)
	if err != nil {
		return nil, fmt.Errorf("failed to create identity: %w", err)
	}

	config.Logger.Info("Created libp2p identity from Solana wallet",
		"peer_id", peerID.String())

	// Create connection manager
	connManager, err := connmgr.NewConnManager(
		100, // Low water mark
		400, // High water mark
		connmgr.WithGracePeriod(time.Minute),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create connection manager: %w", err)
	}

	// Create registry-based connection gater
	refreshInterval := config.RefreshInterval
	if refreshInterval == 0 {
		refreshInterval = 30 * time.Second // Default to 30 seconds
	}
	gater := common.NewSolanaRegistryGater(config.GetAuthorizedWallets, config.Logger, refreshInterval)

	config.Logger.Info("Creating libp2p host",
		"quic_port", config.ListenPorts.QUIC,
		"tcp_port", config.ListenPorts.TCP,
		"peer_id", peerID.String())

	// Variable to capture DHT created in routing function
	var dht *dual.DHT

	// Wait for bootstrap nodes to become available before creating libp2p host
	// This ensures AutoRelay can be properly configured with static relays
	var bootstrapNodes []common.BootstrapNode

	if !config.SkipBootstrapWait {
		maxRetries := 10
		retryDelay := 2 * time.Second
		maxWaitTime := 30 * time.Second

		config.Logger.Info("Waiting for bootstrap nodes to become available...",
			"max_wait_time", maxWaitTime, "max_retries", maxRetries)

		ctx, cancel := context.WithTimeout(context.Background(), maxWaitTime)
		defer cancel()

		for i := 0; i < maxRetries; i++ {
			select {
			case <-ctx.Done():
				config.Logger.Warn("Timeout waiting for bootstrap nodes, continuing without AutoRelay")
				goto createHost
			default:
			}

			if nodes, err := config.GetBootstrapNodes(context.Background()); err == nil && len(nodes) > 0 {
				bootstrapNodes = nodes
				config.Logger.Info("Bootstrap nodes available", "count", len(nodes), "attempt", i+1)
				break
			}

			if i < maxRetries-1 {
				config.Logger.Debug("Bootstrap nodes not available, retrying...",
					"attempt", i+1, "max_retries", maxRetries, "delay", retryDelay)
				time.Sleep(retryDelay)
			} else {
				config.Logger.Warn("No bootstrap nodes available after retries, continuing without AutoRelay")
			}
		}
	} else {
		// Skip bootstrap wait - try once and continue immediately
		config.Logger.Debug("Skipping bootstrap wait (SkipBootstrapWait=true)")
		if nodes, err := config.GetBootstrapNodes(context.Background()); err == nil && len(nodes) > 0 {
			bootstrapNodes = nodes
			config.Logger.Info("Bootstrap nodes available immediately", "count", len(nodes))
		}
	}

createHost:

	// Configure AutoRelay with bootstrap nodes as static relays
	var autoRelayOpts []autorelay.Option
	var relayPeers []peer.AddrInfo
	if len(bootstrapNodes) > 0 {
		// Convert bootstrap nodes to peer.AddrInfo for relay usage
		for _, node := range bootstrapNodes {
			// Create peer ID from public key
			peerID, err := peer.Decode(node.PublicKey.String())
			if err != nil {
				continue
			}

			// Create multiaddresses from bootstrap node info
			var addrs []multiaddr.Multiaddr
			quicAddr, err := multiaddr.NewMultiaddr(fmt.Sprintf("/ip4/%s/udp/%d/quic-v1",
				node.IP, node.QUICPort))
			if err == nil {
				addrs = append(addrs, quicAddr)
			}

			tcpAddr, err := multiaddr.NewMultiaddr(fmt.Sprintf("/ip4/%s/tcp/%d",
				node.IP, node.TCPPort))
			if err == nil {
				addrs = append(addrs, tcpAddr)
			}

			if len(addrs) > 0 {
				relayPeers = append(relayPeers, peer.AddrInfo{
					ID:    peerID,
					Addrs: addrs,
				})
			}
		}

		if len(relayPeers) > 0 {
			autoRelayOpts = append(autoRelayOpts, autorelay.WithStaticRelays(relayPeers))
			config.Logger.Info("Configured AutoRelay with bootstrap nodes as static relays",
				"relay_count", len(relayPeers))
		}
	}

	// Create libp2p host with integrated routing
	host, err := libp2p.New(
		libp2p.Identity(privateKey),
		libp2p.ListenAddrStrings(
			fmt.Sprintf("/ip4/0.0.0.0/udp/%d/quic-v1", config.ListenPorts.QUIC),
			fmt.Sprintf("/ip4/0.0.0.0/tcp/%d", config.ListenPorts.TCP),
		),
		libp2p.Transport(libp2pquic.NewTransport),
		libp2p.Transport(tcp.NewTCPTransport),
		libp2p.Transport(websocket.New), // WebSocket transport for better NAT traversal
		libp2p.EnableRelay(),
		// Conditionally enable AutoRelay if we have static relays configured
		func() libp2p.Option {
			if len(autoRelayOpts) > 0 {
				return libp2p.EnableAutoRelayWithStaticRelays(relayPeers)
			}
			return libp2p.Option(func(*libp2p.Config) error { return nil }) // No-op if no relays
		}(),
		libp2p.EnableHolePunching(), // NAT hole punching for direct connections
		libp2p.ConnectionManager(connManager),
		libp2p.ConnectionGater(gater),
		libp2p.NATPortMap(),
		libp2p.EnableNATService(),
		libp2p.Routing(func(h host.Host) (routing.PeerRouting, error) {
			// Create DHT and capture it for later use
			// This integrates our DHT directly into the libp2p host
			var err error
			dht, err = dual.New(context.Background(), h)
			return dht, err
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create libp2p host (ports QUIC:%d TCP:%d may be in use): %w",
			config.ListenPorts.QUIC, config.ListenPorts.TCP, err)
	}

	config.Logger.Info("Created libp2p host",
		"addresses", host.Addrs(),
		"peer_id", host.ID().String())

	// DHT was created and captured in the routing function above
	if dht == nil {
		if closeErr := host.Close(); closeErr != nil {
			config.Logger.Warn("Failed to close host during cleanup", "error", closeErr.Error())
		}
		return nil, fmt.Errorf("failed to initialize DHT through routing")
	}

	config.Logger.Info("Initialized Kademlia DHT with integrated routing")

	// Initialize GossipSub for pub/sub messaging
	gossipSub, err := pubsub.NewGossipSub(context.Background(), host)
	if err != nil {
		if closeErr := host.Close(); closeErr != nil {
			config.Logger.Warn("Failed to close host during cleanup", "error", closeErr.Error())
		}
		return nil, fmt.Errorf("failed to create GossipSub: %w", err)
	}

	config.Logger.Info("Initialized GossipSub")

	// Create discovery service
	discoveryService := NewDiscoveryService(host, dht, config.Logger)

	// Initialize mDNS for local discovery
	mdnsService := mdns.NewMdnsService(host, fmt.Sprintf("p2p-database-mdns_%s", config.DatabaseName), &notifee{host: host, logger: config.Logger})
	if err := mdnsService.Start(); err != nil {
		config.Logger.Warn("Failed to start mDNS service", "error", err.Error())
	} else {
		config.Logger.Info("Started mDNS service for local discovery", "service_name", fmt.Sprintf("p2p-database-mdns_%s", config.DatabaseName))
	}

	// Create shutdown context for proper cleanup
	shutdownCtx, shutdownCancel := context.WithCancel(context.Background())

	// Create infrastructure with readiness tracking
	infra := &P2PInfrastructure{
		host:              host,
		dht:               dht,
		gossipSub:         gossipSub,
		connMgr:           connManager,
		discovery:         discoveryService,
		mdns:              mdnsService,
		logger:            config.Logger,
		isReady:           false, // Initially not ready
		getBootstrapNodes: config.GetBootstrapNodes,
		shutdownCtx:       shutdownCtx,
		shutdownCancel:    shutdownCancel,
	}

	// Set up connection event monitoring to track readiness
	host.Network().Notify(&network.NotifyBundle{
		ConnectedF: func(n network.Network, c network.Conn) {
			config.Logger.Debug("Peer connected",
				"peer_id", c.RemotePeer().String(),
				"local_addr", c.LocalMultiaddr().String(),
				"remote_addr", c.RemoteMultiaddr().String())
			infra.updateReadiness()
		},
		DisconnectedF: func(n network.Network, c network.Conn) {
			config.Logger.Debug("Peer disconnected",
				"peer_id", c.RemotePeer().String())
			infra.updateReadiness()
		},
	})

	// Bootstrap connection to network with retry support
	if err := bootstrapNetworkWithRetry(context.Background(), infra); err != nil {
		config.Logger.Warn("Failed to start bootstrap process", "error", err.Error())
		// Don't fail completely, continue without bootstrap
	}

	return infra, nil
}

// bootstrapNetwork connects to bootstrap nodes and starts DHT
func bootstrapNetwork(ctx context.Context, host host.Host, dht *dual.DHT, getBootstrapNodes common.GetBootstrapNodesFunc, logger common.Logger) error {
	// Bootstrap DHT
	if err := dht.Bootstrap(ctx); err != nil {
		return fmt.Errorf("failed to bootstrap DHT: %w", err)
	}

	// Get bootstrap nodes
	bootstrapNodes, err := getBootstrapNodes(ctx)
	if err != nil {
		return fmt.Errorf("failed to get bootstrap nodes: %w", err)
	}

	if len(bootstrapNodes) == 0 {
		logger.Info("No bootstrap nodes provided, skipping network bootstrap")
		return nil
	}

	// Connect to bootstrap nodes
	var connectedCount int
	for _, node := range bootstrapNodes {
		// Create multiaddrs for the bootstrap node
		quicAddr, err := multiaddr.NewMultiaddr(fmt.Sprintf("/ip4/%s/udp/%d/quic-v1", node.IP, node.QUICPort))
		if err != nil {
			logger.Warn("Failed to create QUIC multiaddr for bootstrap node",
				"ip", node.IP,
				"port", node.QUICPort,
				"error", err.Error())
			continue
		}

		tcpAddr, err := multiaddr.NewMultiaddr(fmt.Sprintf("/ip4/%s/tcp/%d", node.IP, node.TCPPort))
		if err != nil {
			logger.Warn("Failed to create TCP multiaddr for bootstrap node",
				"ip", node.IP,
				"port", node.TCPPort,
				"error", err.Error())
			continue
		}

		// Create peer ID from Solana public key
		peerID, err := common.CreatePeerIDFromSolanaPublicKey(node.PublicKey)
		if err != nil {
			logger.Warn("Failed to create peer ID from Solana public key",
				"public_key", node.PublicKey.String(),
				"error", err.Error())
			continue
		}

		// Create peer info
		peerInfo := peer.AddrInfo{
			ID:    peerID,
			Addrs: []multiaddr.Multiaddr{quicAddr, tcpAddr},
		}

		// Try to connect
		connectCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		if err := host.Connect(connectCtx, peerInfo); err != nil {
			logger.Warn("Failed to connect to bootstrap node",
				"peer_id", peerID.String(),
				"addrs", peerInfo.Addrs,
				"error", err.Error())
			cancel()
			continue
		}
		cancel()

		connectedCount++
		logger.Info("Connected to bootstrap node",
			"peer_id", peerID.String(),
			"addrs", peerInfo.Addrs)
	}

	if connectedCount == 0 {
		return fmt.Errorf("failed to connect to any bootstrap nodes")
	}

	logger.Info("Successfully bootstrapped network",
		"connected_nodes", connectedCount,
		"total_bootstrap_nodes", len(bootstrapNodes))

	return nil
}

// bootstrapNetworkWithRetry starts bootstrap process with retry support for empty bootstrap node lists and bootstrap failures
func bootstrapNetworkWithRetry(ctx context.Context, infra *P2PInfrastructure) error {
	// Check bootstrap nodes availability first
	bootstrapNodes, err := infra.getBootstrapNodes(ctx)
	if err != nil {
		return fmt.Errorf("failed to get bootstrap nodes: %w", err)
	}

	if len(bootstrapNodes) == 0 {
		// No bootstrap nodes available, start retry process
		infra.logger.Info("No bootstrap nodes available, starting retry process")
		infra.startBootstrapRetry(infra.shutdownCtx)
		return nil // Don't return error, we'll retry
	}

	// Bootstrap nodes are available, try normal bootstrap
	err = bootstrapNetwork(ctx, infra.host, infra.dht, infra.getBootstrapNodes, infra.logger)
	if err != nil {
		// Bootstrap failed, start retry process to keep trying
		infra.logger.Warn("Initial bootstrap failed, starting retry process", "error", err.Error())
		infra.startBootstrapRetry(infra.shutdownCtx)
		return nil // Don't return error, we'll retry
	}

	// Bootstrap succeeded
	infra.logger.Info("Initial bootstrap successful")
	time.Sleep(1 * time.Second) // Give DHT a moment to settle
	return nil
}

// startBootstrapRetry starts a goroutine that retries bootstrap every 5 seconds until ready
func (infra *P2PInfrastructure) startBootstrapRetry(parentCtx context.Context) {
	// Check if shutdown is in progress
	select {
	case <-infra.shutdownCtx.Done():
		infra.logger.Debug("Shutdown in progress, not starting bootstrap retry")
		return
	default:
	}

	// Check if retry is already running (with proper synchronization)
	infra.retryMutex.Lock()
	if infra.retryCancel != nil {
		infra.retryMutex.Unlock()
		infra.logger.Debug("Bootstrap retry already running, not starting another")
		return
	}

	// Create cancelable context for the retry goroutine - combine parent and shutdown contexts
	retryCtx, cancel := context.WithCancel(parentCtx)
	infra.retryCancel = cancel
	infra.retryMutex.Unlock()

	go func() {
		defer func() {
			cancel()
			// Clear the cancel function when goroutine exits (with proper synchronization)
			infra.retryMutex.Lock()
			infra.retryCancel = nil
			infra.retryMutex.Unlock()
		}()

		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()

		infra.logger.Info("Starting bootstrap retry loop (every 5 seconds)")

		for {
			select {
			case <-retryCtx.Done():
				infra.logger.Debug("Bootstrap retry loop cancelled")
				return
			case <-infra.shutdownCtx.Done():
				infra.logger.Debug("Bootstrap retry loop cancelled due to shutdown")
				return
			case <-ticker.C:
				// Check if node is already ready (has peers)
				if infra.IsReady() {
					infra.logger.Info("Node is ready, stopping bootstrap retry")
					return
				}

				// Try to get bootstrap nodes
				bootstrapNodes, err := infra.getBootstrapNodes(retryCtx)
				if err != nil {
					infra.logger.Warn("Failed to get bootstrap nodes during retry", "error", err.Error())
					continue
				}

				if len(bootstrapNodes) == 0 {
					infra.logger.Debug("Still no bootstrap nodes available, will retry")
					continue
				}

				// Found bootstrap nodes, try to connect
				infra.logger.Info("Found bootstrap nodes during retry", "count", len(bootstrapNodes))
				if err := bootstrapNetwork(retryCtx, infra.host, infra.dht, infra.getBootstrapNodes, infra.logger); err != nil {
					infra.logger.Warn("Bootstrap retry failed", "error", err.Error())
					continue
				}

				// Bootstrap succeeded
				infra.logger.Info("Bootstrap retry successful")
				time.Sleep(1 * time.Second) // Give DHT a moment to settle
				return
			}
		}
	}()
}

// stopBootstrapRetry stops the bootstrap retry goroutine if it's running
func (infra *P2PInfrastructure) stopBootstrapRetry() {
	infra.retryMutex.Lock()
	defer infra.retryMutex.Unlock()

	if infra.retryCancel != nil {
		infra.logger.Debug("Stopping bootstrap retry goroutine")
		infra.retryCancel()
		infra.retryCancel = nil
	}
}

// createDatabaseInstance creates a new database instance with connection gating
func createDatabaseInstance(infra *P2PInfrastructure, config common.Config) (*DB, error) {
	// Create database instance
	dbInstance := &DatabaseInstance{
		name:          config.DatabaseName,
		gater:         nil, // Gater is set at host level, not per database
		topics:        make(map[string]*pubsub.Topic),
		subscriptions: make(map[string]*TopicSubscription),
	}

	// This globalInfraMutex and globalInfrastructures are removed, so this line is removed.
	// The infrastructure is now directly managed by the caller.
	// infra.mutex.Lock()
	// infra.databases[config.DatabaseName] = dbInstance
	// infra.mutex.Unlock()

	config.Logger.Info("Created database instance",
		"database_name", config.DatabaseName,
		"peer_id", infra.host.ID().String())

	// Start discovery immediately for this database
	ctx := context.Background()
	if err := infra.discovery.StartDiscovery(ctx, config.DatabaseName); err != nil {
		config.Logger.Warn("Failed to start discovery", "error", err.Error())
	} else {
		config.Logger.Info("Started peer discovery for database", "database", config.DatabaseName)
	}

	return &DB{
		infrastructure: infra,
		instance:       dbInstance,
	}, nil
}

// Connect is the main entry point for connecting to the P2P pubsub network
func Connect(ctx context.Context, config common.Config) (*DB, error) {
	// Validate configuration
	if config.WalletPrivateKey == "" {
		return nil, fmt.Errorf("wallet private key is required")
	}
	if config.DatabaseName == "" {
		return nil, fmt.Errorf("database name is required")
	}
	if config.GetAuthorizedWallets == nil {
		return nil, fmt.Errorf("GetAuthorizedWallets function is required")
	}
	if config.GetBootstrapNodes == nil {
		return nil, fmt.Errorf("GetBootstrapNodes function is required")
	}
	if config.Logger == nil {
		return nil, fmt.Errorf("logger is required")
	}

	// Set default ports if not provided
	if config.ListenPorts.QUIC == 0 {
		config.ListenPorts.QUIC = 4001
	}
	if config.ListenPorts.TCP == 0 {
		config.ListenPorts.TCP = 4002
	}

	// Initialize new infrastructure for this connection
	infrastructure, err := initializeP2PInfrastructure(config)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize P2P infrastructure: %w", err)
	}

	// Create database instance
	return createDatabaseInstance(infrastructure, config)
}
