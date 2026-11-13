/*
 * CActor Cluster.StreamRef - 阶段 Actor 的运行环境：创建、暴露、寻址与对端监视
 *
 * 两个阶段 Actor 都挂在系统顶层（与 Akka 的物化器一样，流的寿命不跟随调用方 Actor 的子树），
 * 由 owner 监视把生命周期绑回属主。
 * 跨节点寻址只用远程层已有的两件事：exposeActor 暴露名 + "remote://host:port/name" 位置文本；
 * 监视用 watchRemote（节点级心跳 + 对端开启远程监视时的 Actor 级 DEATH）。
 */
package cactor_cluster.streamref

import std.sync.*
import std.random.*
import cactor.api.{CActorSystem}
import cactor_core.actor.{ActorRef}
import cactor_core.typed.*
import cactor_remote.{SimpleRemoteManager, RemoteActorRef, parseRemoteSender, encodeRemoteSender}
import cactor_foundation.logging.{Logger, Logging}

let streamRefLog: Logger = Logging.logger("cactor_cluster.streamref")

/** 解析后的对端：发送用的类型化引用 + 远程时的经典远程引用（用于 watch/unwatch） */
class StreamPeer {
    let ref: TypedActorRef<StreamRefMsg>
    let remoteRef: Option<RemoteActorRef>
    /** 用于识别"同一个订阅者重发 Subscribe"的稳定键 */
    let key: String

    init(ref: TypedActorRef<StreamRefMsg>, remoteRef: Option<RemoteActorRef>, key: String) {
        this.ref = ref
        this.remoteRef = remoteRef
        this.key = key
    }

    func isRemote(): Bool {
        remoteRef.isSome()
    }

    /** 发送失败（编码失败、发送器已停）只记日志：调用方已处在终止路径上，不能再抛 */
    func tellQuietly(m: StreamRefMsg): Unit {
        try {
            ref.tell(m)
        } catch (e: Exception) {
            streamRefLog.debug("StreamRef 向 ${key} 发送 ${m.streamIdOrEmpty()} 的终止消息失败：${e.message}")
        }
    }
}

class StreamRefEnv {
    let system: CActorSystem
    let remote: Option<SimpleRemoteManager>
    let seq = AtomicInt64(0)
    let nodeTag: String

    init(system: CActorSystem, remote: Option<SimpleRemoteManager>) {
        this.system = system
        this.remote = remote
        // 流标识要在同址重启后也不重复：带上本次启动的随机标签，旧消费方的迟到消息对不上新流
        let tag = match (remote) {
            case Some(r) => r.selfUid()
            case None => Random().nextUInt64()
        }
        this.nodeTag = "${tag}"
    }

    func nextStreamId(): String {
        "${nodeTag}-${seq.fetchAdd(1) + 1}-${Random().nextUInt32()}"
    }

    func spawnStage(behavior: TypedBehavior<StreamRefMsg>, name: String): TypedActorRef<StreamRefMsg> {
        system.typedActorOf<StreamRefMsg>(behavior, name)
    }

    /** 有远程层时把阶段暴露出去并返回位置文本；否则返回空串（只能本进程使用） */
    func expose(name: String, ref: TypedActorRef<StreamRefMsg>): String {
        match ((remote, ref.toClassic())) {
            case (Some(r), Some(classic)) =>
                r.exposeActor(name, classic)
                encodeRemoteSender(r.getLocalAddress(), name)
            case _ => ""
        }
    }

    func unexpose(name: String): Unit {
        if (let Some(r) <- remote) {
            try {
                r.unexposeActor(name)
            } catch (_: Exception) {}
        }
    }

    /** 位置文本 -> 对端（远程引用）；没有远程层或文本不合法时 None */
    func resolveRemote(location: String): Option<StreamPeer> {
        match ((remote, parseRemoteSender(location))) {
            case (Some(r), Some((addr, name))) =>
                let rr = r.remoteActorOf(addr, name)
                Some(StreamPeer(ClassicTypedRef<StreamRefMsg>(rr), Some(rr), location))
            case _ => None<StreamPeer>
        }
    }

    /** 监视对端：远程用 watchRemote（节点不可达 / 对端 DEATH 都会变成 Terminated 信号），本地用 ctx.watch */
    func watchPeer(ctx: TypedActorContext<StreamRefMsg>, peer: StreamPeer): Unit {
        try {
            match ((peer.remoteRef, remote, ctx.self.toClassic())) {
                case (Some(rr), Some(r), Some(me)) => r.watchRemote(rr, me)
                case (None, _, _) => ctx.watch<StreamRefMsg>(peer.ref)
                case _ => ()
            }
        } catch (e: Exception) {
            streamRefLog.warn("StreamRef 无法监视对端 ${peer.key}：${e.message}")
        }
    }

    func unwatchPeer(ctx: TypedActorContext<StreamRefMsg>, peer: StreamPeer): Unit {
        try {
            match ((peer.remoteRef, remote, ctx.self.toClassic())) {
                case (Some(rr), Some(r), Some(me)) => r.unwatchRemote(rr, me)
                case (None, _, _) => ctx.unwatch<StreamRefMsg>(peer.ref)
                case _ => ()
            }
        } catch (_: Exception) {}
    }
}

/** 信号里的经典引用是否就是这个对端 */
func isPeerRef(peer: Option<StreamPeer>, ref: ActorRef): Bool {
    match (peer) {
        case Some(p) =>
            match (p.remoteRef) {
                case Some(rr) => rr.path.toString() == ref.path.toString()
                case None =>
                    match (p.ref.toClassic()) {
                        case Some(c) => c.path.toString() == ref.path.toString()
                        case None => false
                    }
            }
        case None => false
    }
}
