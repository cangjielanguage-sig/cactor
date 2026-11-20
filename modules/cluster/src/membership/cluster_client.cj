/*
 * CActor Cluster - 集群客户端（差距 4.3 / C2c：外部进程不加入集群也能访问服务）
 *
 * 与 Akka `ClusterClient` 对齐的最小可用形态：
 * - 外部进程持有若干**联系点**（种子/负载均衡器地址），向任一成员的 "cluster" 守护查询当前 Up 成员；
 * - 拿到成员后按 ActorSelection 的路径语义（成员地址 + Actor 名称）投递，无需成为集群成员；
 * - 查询失败明确报错，不静默丢弃；联系点可以轮换，任何一个可达即可。
 *
 * 线协议（复用远程层普通消息，不需要新版本）：
 *   → `$cc-members|<replyAddr>|<replyName>`   发给联系点的 "cluster" 演员
 *   ← `$cc-members|<replyName>|<count>|<a1,a2,...>`
 */
package cactor_cluster.membership

import std.collection.*
import std.sync.*
import std.time.*
import cactor_core.message.{Message, StringMessage}
import cactor_core.typed.TypedPromise
import cactor_foundation.network.{NetworkAddress}
import cactor_remote.{RemoteActorRef, SimpleRemoteManager}

let clusterClientReplySeq = AtomicInt64(0)

public class ClusterClient {
    private let remote: SimpleRemoteManager
    private let contacts: Array<NetworkAddress>
    private let timeout: Duration
    private let roundRobin = AtomicInt64(0)

    /**
     * @param contacts 联系点（任一可达即可）：通常是种子节点或前置负载均衡地址
     * @param timeout 单次成员查询的超时
     */
    public init(remote: SimpleRemoteManager, contacts: Array<NetworkAddress>,
        timeout!: Duration = Duration.second * 2) {
        if (contacts.isEmpty()) {
            throw IllegalArgumentException("集群客户端至少需要一个联系点")
        }
        this.remote = remote
        this.contacts = contacts
        this.timeout = timeout
    }

    /**
     * 查询当前 Up 成员（排序）。逐个尝试联系点；全部超时/不可达时抛 IllegalStateException。
     */
    public func members(): Array<String> {
        for (c in contacts) {
            match (query(c)) {
                case Some(list) => return list
                case None => ()
            }
        }
        throw IllegalStateException("没有可用的集群联系点（${contacts.size} 个均未响应，超时 ${timeout}）")
    }

    /** 在成员中轮询选一个并投递；没有成员时抛 IllegalStateException */
    public func tell(actorName: String, message: Message): Unit {
        ref(actorName).tell(message)
    }

    /** 在成员中轮询选一个并返回远程引用（每次新建，成员重启后旧引用会失效） */
    public func ref(actorName: String): RemoteActorRef {
        let list = members()
        if (list.isEmpty()) {
            throw IllegalStateException("集群当前没有 Up 成员")
        }
        let idx = roundRobin.fetchAdd(1) % Int64(list.size)
        remote.remoteActorOf(NetworkAddress.parse(list[idx]), actorName)
    }

    private func query(contact: NetworkAddress): Option<Array<String>> {
        let seq = clusterClientReplySeq.fetchAdd(1)
        let replyName = "$cc-reply-${seq}"
        let promise = TypedPromise<Array<String>>()
        let receiver = remote.getReceiver()
        receiver.registerHandler(replyName, { m: Message =>
            if (let Some(s) <- (m as StringMessage)) {
                let text = s.getContent()
                if (text.startsWith(CLUSTER_CLIENT_QUERY)) {
                    let rest = text[CLUSTER_CLIENT_QUERY.size..]
                    match (rest.indexOf("|")) {
                        case Some(i) =>
                            let payload = rest[(i + 1)..]
                            match (payload.indexOf("|")) {
                                case Some(j) =>
                                    let names = payload[(j + 1)..]
                                    let list = if (names.isEmpty()) {
                                        Array<String>()
                                    } else {
                                        let out = ArrayList<String>()
                                        for (n in names.split(",")) {
                                            out.add(n)
                                        }
                                        out.toArray()
                                    }
                                    promise.trySuccess(list)
                                case None => ()
                            }
                        case None => ()
                    }
                }
            }
        })
        try {
            let local = remote.getLocalAddress()
            remote.sendControl(contact, "cluster",
                StringMessage("${CLUSTER_CLIENT_QUERY}${local.toString()}|${replyName}"))
            Some(promise.get(timeout))
        } catch (_: Exception) {
            None<Array<String>>
        } finally {
            receiver.unregisterHandler(replyName)
        }
    }
}
