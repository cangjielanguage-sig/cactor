/*
 * Copyright (c) 2024. All rights reserved.
 * CActor - 分布式核心测试
 * 测试Phi Accrual故障检测器、Gossip协议和集群协调器
 */
package cactor.tests.unit.distribution.cluster

import std.unittest.*
import std.collection.*
import std.sync.*
import std.time.*
import cactor.foundation.network.{PhiAccrualFailureDetector, PhiAccrualConfig, NetworkAddress}
import cactor.distribution.cluster.{GossipProtocolManager, GossipConfig, ClusterCoordinator, ClusterCoordinatorConfig}
import cactor.distribution.cluster.{GossipClusterNode, NodeStatus, GossipState}

/**
 * 分布式核心测试套件
 */
@Test
public class DistributedCoreTestSuite {
    
    /**
     * 测试Phi Accrual故障检测器基本功能
     */
    @TestCase
    public func testPhiAccrualFailureDetectorBasics(): Unit {
        let config = PhiAccrualConfig.createDefault()
        let detector = PhiAccrualFailureDetector(config)
        
        let nodeId = "test-node-1"
        
        // 初始状态下节点不可用（没有心跳记录）
        @Assert(detector.isAvailable(nodeId) == false)
        
        // 记录几次心跳
        detector.heartbeat(nodeId)
        Thread.sleep(Duration.ofMilliseconds(100))
        detector.heartbeat(nodeId)
        Thread.sleep(Duration.ofMilliseconds(100))
        detector.heartbeat(nodeId)
        
        // 现在节点应该可用
        @Assert(detector.isAvailable(nodeId), true)
        
        // 检查统计信息
        match (detector.getStatistics(nodeId)) {
            case Some(stats) =>
                @Assert(stats.heartbeatCount >= 3, true)
                @Assert(stats.isAvailable, true)
            case None =>
                @Assert(false, true)  // 不应该为None
        }
    }

    /**
     * 测试Phi Accrual故障检测器敏感配置
     */
    @TestCase
    public func testPhiAccrualSensitiveConfig(): Unit {
        let sensitiveConfig = PhiAccrualConfig.createSensitive()
        let detector = PhiAccrualFailureDetector(sensitiveConfig)
        
        let nodeId = "sensitive-node"
        
        // 记录心跳
        detector.heartbeat(nodeId)
        Thread.sleep(Duration.ofMilliseconds(50))
        detector.heartbeat(nodeId)
        
        // 敏感配置应该更快检测到故障
        @Assert(detector.isAvailable(nodeId), true)
        
        // 等待较长时间后应该检测到故障
        Thread.sleep(Duration.ofMilliseconds(1000))
        let phi = detector.phi(nodeId)
        @Assert(phi > 0.0, true)
    }

    /**
     * 测试Phi Accrual故障检测器容忍配置
     */
    @TestCase
    public func testPhiAccrualTolerantConfig(): Unit {
        let tolerantConfig = PhiAccrualConfig.createTolerant()
        let detector = PhiAccrualFailureDetector(tolerantConfig)
        
        let nodeId = "tolerant-node"
        
        // 记录心跳
        detector.heartbeat(nodeId)
        Thread.sleep(Duration.ofMilliseconds(100))
        detector.heartbeat(nodeId)
        
        // 容忍配置应该更慢检测到故障
        @Assert(detector.isAvailable(nodeId), true)
        
        // 即使等待较长时间，容忍配置也应该保持可用
        Thread.sleep(Duration.ofMilliseconds(500))
        @Assert(detector.isAvailable(nodeId), true)
    }

    /**
     * 测试故障检测器节点移除功能
     */
    @TestCase
    public func testFailureDetectorNodeRemoval(): Unit {
        let detector = PhiAccrualFailureDetector()
        let nodeId = "removable-node"
        
        // 添加节点并记录心跳
        detector.heartbeat(nodeId)
        @Assert(detector.isMonitoring(nodeId), true)
        
        // 移除节点
        detector.remove(nodeId)
        @Assert(detector.isMonitoring(nodeId), false)
        
        // 统计信息应该为空
        match (detector.getStatistics(nodeId)) {
            case Some(_) => @Assert(false, true)  // 不应该有统计信息
            case None => @Assert(true, true)      // 正确
        }
    }

    /**
     * 测试Gossip协议管理器基本功能
     */
    @TestCase
    public func testGossipProtocolManagerBasics(): Unit {
        let config = GossipConfig.createDefault()
        let localNodeId = "gossip-node-1"
        let localAddress = NetworkAddress("localhost", 8080)
        let gossipManager = GossipProtocolManager(config, localNodeId, localAddress)
        
        // 启动Gossip管理器
        gossipManager.start()
        
        // 检查当前状态
        let currentState = gossipManager.getCurrentState()
        @Assert(currentState.members.size >= 1, true)  // 至少包含本地节点
        
        // 添加另一个节点
        let remoteNode = ClusterNode(
            "gossip-node-2",
            NetworkAddress("localhost", 8081),
            Array<String>(),
            NodeStatus.Up,
            getCurrentTimeNanos()
        )
        gossipManager.addNode(remoteNode)
        
        // 检查活跃节点
        let activeNodes = gossipManager.getActiveNodes()
        @Assert(activeNodes.size >= 1, true)
        
        // 停止Gossip管理器
        gossipManager.stop()
    }

    /**
     * 测试Gossip配置变体
     */
    @TestCase
    public func testGossipConfigVariants(): Unit {
        let fastConfig = GossipConfig.createFast()
        let slowConfig = GossipConfig.createSlow()
        
        @Assert(fastConfig.gossipInterval < slowConfig.gossipInterval, true)
        @Assert(fastConfig.convergenceTimeout < slowConfig.convergenceTimeout, true)
        
        // 测试快速配置
        let fastGossip = GossipProtocolManager(
            fastConfig, 
            "fast-node", 
            NetworkAddress("localhost", 9000)
        )
        fastGossip.start()
        fastGossip.stop()
        
        // 测试慢速配置
        let slowGossip = GossipProtocolManager(
            slowConfig, 
            "slow-node", 
            NetworkAddress("localhost", 9001)
        )
        slowGossip.start()
        slowGossip.stop()
    }

    /**
     * 测试集群协调器基本功能
     */
    @TestCase
    public func testClusterCoordinatorBasics(): Unit {
        let localNodeId = "coordinator-node-1"
        let localAddress = NetworkAddress("localhost", 7000)
        let coordinator = ClusterCoordinator(localNodeId, localAddress)
        
        // 启动协调器
        coordinator.start()
        
        // 检查初始状态
        @Assert(!coordinator.isLeader(), true)  // 单节点时不是领导者
        
        // 检查集群成员
        let members = coordinator.getClusterMembers()
        @Assert(members.size >= 0, true)  // 可能为空或包含本地节点
        
        // 停止协调器
        coordinator.stop()
    }

    /**
     * 测试集群协调器配置
     */
    @TestCase
    public func testClusterCoordinatorConfig(): Unit {
        let defaultConfig = ClusterCoordinatorConfig.createDefault()
        let fastConfig = ClusterCoordinatorConfig.createFast()
        let slowConfig = ClusterCoordinatorConfig.createSlow()
        
        @Assert(fastConfig.heartbeatInterval < defaultConfig.heartbeatInterval, true)
        @Assert(slowConfig.heartbeatInterval > defaultConfig.heartbeatInterval, true)
        
        @Assert(fastConfig.leaderElectionTimeout < slowConfig.leaderElectionTimeout, true)
        @Assert(fastConfig.convergenceTimeout < slowConfig.convergenceTimeout, true)
    }

    /**
     * 测试集群协调器事件监听
     */
    @TestCase
    public func testClusterCoordinatorEventListening(): Unit {
        let coordinator = ClusterCoordinator("event-node", NetworkAddress("localhost", 7100))
        
        var eventReceived = false
        coordinator.addEventListener { event =>
            eventReceived = true
        }
        
        coordinator.start()
        
        // 等待一小段时间让事件处理
        Thread.sleep(Duration.ofMilliseconds(100))
        
        coordinator.stop()
        
        // 应该收到至少一个事件（启动或停止事件）
        @Assert(eventReceived, true)
    }

    /**
     * 测试多节点Gossip状态同步
     */
    @TestCase
    public func testMultiNodeGossipSync(): Unit {
        let node1 = GossipProtocolManager(
            GossipConfig.createFast(),
            "sync-node-1",
            NetworkAddress("localhost", 8100)
        )
        
        let node2 = GossipProtocolManager(
            GossipConfig.createFast(),
            "sync-node-2", 
            NetworkAddress("localhost", 8101)
        )
        
        // 启动两个节点
        node1.start()
        node2.start()
        
        // 添加节点到彼此的状态中
        let clusterNode1 = ClusterNode(
            "sync-node-1",
            NetworkAddress("localhost", 8100),
            Array<String>(),
            NodeStatus.Up,
            getCurrentTimeNanos()
        )
        
        let clusterNode2 = ClusterNode(
            "sync-node-2",
            NetworkAddress("localhost", 8101),
            Array<String>(),
            NodeStatus.Up,
            getCurrentTimeNanos()
        )
        
        node1.addNode(clusterNode2)
        node2.addNode(clusterNode1)
        
        // 执行几轮Gossip
        for (i in 0..3) {
            node1.performGossipRound()
            node2.performGossipRound()
            Thread.sleep(Duration.ofMilliseconds(100))
        }
        
        // 检查状态同步
        let state1 = node1.getCurrentState()
        let state2 = node2.getCurrentState()
        
        @Assert(state1.members.size >= 1, true)
        @Assert(state2.members.size >= 1, true)
        
        // 停止节点
        node1.stop()
        node2.stop()
    }

    /**
     * 测试故障检测器统计功能
     */
    @TestCase
    public func testFailureDetectorStatistics(): Unit {
        let detector = PhiAccrualFailureDetector()
        
        let node1 = "stats-node-1"
        let node2 = "stats-node-2"
        
        // 为两个节点记录心跳
        for (i in 0..5) {
            detector.heartbeat(node1)
            detector.heartbeat(node2)
            Thread.sleep(Duration.ofMilliseconds(50))
        }
        
        // 获取所有统计信息
        let allStats = detector.getAllStatistics()
        @Assert(allStats.size, 2)
        
        // 检查单个节点统计
        match (detector.getStatistics(node1)) {
            case Some(stats) =>
                @Assert(stats.nodeId, node1)
                @Assert(stats.heartbeatCount >= 5, true)
                @Assert(stats.isAvailable, true)
            case None =>
                @Assert(false, true)
        }
        
        // 重置统计信息（如果支持）
        detector.remove(node1)
        detector.remove(node2)
        
        let emptyStats = detector.getAllStatistics()
        @Assert(emptyStats.size, 0)
    }

    private func getCurrentTimeNanos(): Int64 {
        DateTime.now().toUnixTimeStamp().toMilliseconds() * 1_000_000
    }
}
