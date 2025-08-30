package test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/dTelecom/p2p-database/common"
	"github.com/gagliardetto/solana-go"
)

// TestTopicSpecificDiscovery tests the new topic-specific peer discovery feature
// that should solve the issue where local nodes don't discover each other
// when only connected through bootstrap nodes that aren't subscribed to their topics.
func TestTopicSpecificDiscovery(t *testing.T) {
	t.Log("=== Testing Topic-Specific Peer Discovery Enhancement ===")
	t.Log("This test verifies the fix for local nodes not discovering each other")
	t.Log("when communicating via topics that bootstrap nodes don't subscribe to.")

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	logger := &TestLogger{}
	databaseName := "test_topic_discovery"

	// === Setup Phase ===
	t.Log("\n=== Phase 1: Setting up nodes ===")

	// Mock authorized wallets function
	mockGetAuthorizedWallets := func(ctx context.Context) ([]solana.PublicKey, error) {
		return []solana.PublicKey{
			solana.MustPublicKeyFromBase58(node0PublicKey),
			solana.MustPublicKeyFromBase58(node1PublicKey),
			solana.MustPublicKeyFromBase58(node2PublicKey),
		}, nil
	}

	// Create bootstrap function that returns Node1 as bootstrap
	bootstrapFunc := func(ctx context.Context) ([]common.BootstrapNode, error) {
		return []common.BootstrapNode{
			{
				PublicKey: solana.MustPublicKeyFromBase58(node0PublicKey),
				IP:        "127.0.0.1",
				QUICPort:  36001,
				TCPPort:   36002,
			},
		}, nil
	}

	// Node1: Bootstrap node (NOT subscribed to test topic)
	node1DB := setupNode(t, ctx, logger, node0PrivateKey, databaseName, 36001, 36002,
		mockGetAuthorizedWallets, func(ctx context.Context) ([]common.BootstrapNode, error) {
			return []common.BootstrapNode{}, nil // Bootstrap doesn't need other bootstraps
		})
	defer func() { _ = node1DB.Disconnect(ctx) }()

	// Node2: Local node 1 (subscribes to test topic)
	node2DB := setupNode(t, ctx, logger, node1PrivateKey, databaseName, 36003, 36004,
		mockGetAuthorizedWallets, bootstrapFunc)
	defer func() { _ = node2DB.Disconnect(ctx) }()

	// Node3: Local node 2 (subscribes to test topic)
	node3DB := setupNode(t, ctx, logger, node2PrivateKey, databaseName, 36005, 36006,
		mockGetAuthorizedWallets, bootstrapFunc)
	defer func() { _ = node3DB.Disconnect(ctx) }()

	// === Wait for Initial Connections ===
	t.Log("\n=== Phase 2: Waiting for bootstrap connections ===")
	time.Sleep(5 * time.Second)

	// Check initial topology
	node1Peers := node1DB.GetHost().Network().Peers()
	node2Peers := node2DB.GetHost().Network().Peers()
	node3Peers := node3DB.GetHost().Network().Peers()

	t.Logf("Initial connections:")
	t.Logf("  Node1 (bootstrap): %d peers", len(node1Peers))
	t.Logf("  Node2 (local): %d peers", len(node2Peers))
	t.Logf("  Node3 (local): %d peers", len(node3Peers))

	// Track whether Node2 and Node3 are directly connected initially
	node2ConnectedToNode3 := false
	node3ConnectedToNode2 := false

	for _, peerID := range node2Peers {
		if peerID == node3DB.GetHost().ID() {
			node2ConnectedToNode3 = true
			break
		}
	}

	for _, peerID := range node3Peers {
		if peerID == node2DB.GetHost().ID() {
			node3ConnectedToNode2 = true
			break
		}
	}

	t.Logf("Initial direct connection Node2↔Node3: %v", node2ConnectedToNode3 && node3ConnectedToNode2)

	// === Subscribe to Topic (This should trigger topic discovery) ===
	t.Log("\n=== Phase 3: Subscribing to topic (triggers topic discovery) ===")

	testTopic := "enhanced_discovery_test"
	var messagesMutex sync.RWMutex
	var node2ReceivedMessages []string
	var node3ReceivedMessages []string

	// Node2 subscribes (should start topic discovery)
	node2Handler := func(event common.Event) {
		messagesMutex.Lock()
		defer messagesMutex.Unlock()
		if msgMap, ok := event.Message.(map[string]interface{}); ok {
			if content, exists := msgMap["content"]; exists {
				if contentStr, ok := content.(string); ok {
					node2ReceivedMessages = append(node2ReceivedMessages, contentStr)
					t.Logf("Node2 received: %s", contentStr)
				}
			}
		}
	}

	err := node2DB.Subscribe(ctx, testTopic, node2Handler)
	if err != nil {
		t.Fatalf("Failed to subscribe Node2: %v", err)
	}

	// Node3 subscribes (should start topic discovery)
	node3Handler := func(event common.Event) {
		messagesMutex.Lock()
		defer messagesMutex.Unlock()
		if msgMap, ok := event.Message.(map[string]interface{}); ok {
			if content, exists := msgMap["content"]; exists {
				if contentStr, ok := content.(string); ok {
					node3ReceivedMessages = append(node3ReceivedMessages, contentStr)
					t.Logf("Node3 received: %s", contentStr)
				}
			}
		}
	}

	err = node3DB.Subscribe(ctx, testTopic, node3Handler)
	if err != nil {
		t.Fatalf("Failed to subscribe Node3: %v", err)
	}

	t.Log("Both nodes subscribed - topic discovery should be active")

	// === Wait for Topic Discovery ===
	t.Log("\n=== Phase 4: Waiting for topic discovery to connect peers ===")

	// Wait for topic discovery to work
	discoveryWaitTime := 20 * time.Second
	t.Logf("Waiting %v for topic discovery...", discoveryWaitTime)
	time.Sleep(discoveryWaitTime)

	// Check post-discovery topology
	node1PeersAfter := node1DB.GetHost().Network().Peers()
	node2PeersAfter := node2DB.GetHost().Network().Peers()
	node3PeersAfter := node3DB.GetHost().Network().Peers()

	t.Logf("Post-discovery connections:")
	t.Logf("  Node1 (bootstrap): %d peers", len(node1PeersAfter))
	t.Logf("  Node2 (local): %d peers", len(node2PeersAfter))
	t.Logf("  Node3 (local): %d peers", len(node3PeersAfter))

	// Check if Node2 and Node3 are now connected
	node2ConnectedToNode3After := false
	node3ConnectedToNode2After := false

	for _, peerID := range node2PeersAfter {
		if peerID == node3DB.GetHost().ID() {
			node2ConnectedToNode3After = true
			break
		}
	}

	for _, peerID := range node3PeersAfter {
		if peerID == node2DB.GetHost().ID() {
			node3ConnectedToNode2After = true
			break
		}
	}

	directConnectionEstablished := node2ConnectedToNode3After && node3ConnectedToNode2After
	t.Logf("Direct connection Node2↔Node3 after topic discovery: %v", directConnectionEstablished)

	// === Test Message Exchange ===
	t.Log("\n=== Phase 5: Testing message exchange ===")

	// Node2 publishes a message
	_, err = node2DB.Publish(ctx, testTopic, map[string]interface{}{
		"content": "Hello from Node2 via topic discovery",
		"from":    "node2",
	})
	if err != nil {
		t.Fatalf("Failed to publish from Node2: %v", err)
	}

	// Node3 publishes a message
	_, err = node3DB.Publish(ctx, testTopic, map[string]interface{}{
		"content": "Hello from Node3 via topic discovery",
		"from":    "node3",
	})
	if err != nil {
		t.Fatalf("Failed to publish from Node3: %v", err)
	}

	// Wait for message propagation
	t.Log("Waiting for message propagation...")
	time.Sleep(5 * time.Second)

	// === Check Results ===
	messagesMutex.RLock()
	node2MessageCount := len(node2ReceivedMessages)
	node3MessageCount := len(node3ReceivedMessages)
	messagesMutex.RUnlock()

	t.Logf("\n=== Results ===")
	t.Logf("Node2 received %d messages", node2MessageCount)
	t.Logf("Node3 received %d messages", node3MessageCount)

	// Success criteria
	topicDiscoveryWorked := directConnectionEstablished
	messagesExchanged := node2MessageCount > 0 && node3MessageCount > 0

	if topicDiscoveryWorked {
		t.Log("✅ SUCCESS: Topic discovery established direct connection between local nodes")
	} else {
		t.Log("❌ ISSUE: Topic discovery did not establish direct connection")
	}

	if messagesExchanged {
		t.Log("✅ SUCCESS: Messages successfully exchanged between local nodes")
	} else {
		t.Log("❌ ISSUE: Message exchange failed")
	}

	// Overall assessment
	if topicDiscoveryWorked && messagesExchanged {
		t.Log("\n🎉 TOPIC DISCOVERY ENHANCEMENT SUCCESSFUL!")
		t.Log("Local nodes can now discover each other via topic-specific discovery")
		t.Log("This solves the original issue where nodes couldn't communicate")
		t.Log("when bootstrap nodes weren't subscribed to their topics.")
	} else {
		t.Errorf("\n❌ Topic discovery enhancement needs more work")
		if !topicDiscoveryWorked {
			t.Error("- Direct connection not established via topic discovery")
		}
		if !messagesExchanged {
			t.Error("- Message exchange failed")
		}
	}
}
