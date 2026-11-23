/*
 * CActor Platform.Chat - 线程/历史/订阅/输入中/已读（对标 rivet-chat）
 *
 * 覆盖：直聊线程幂等且对称、历史游标分页、长轮询收到新消息、输入中 TTL、已读夹取、
 * 显式失败（未知线程/非成员/空消息/自己和自己），以及同一 store 下历史可被新服务实例读回。
 */
package cactor_tests.unit.platform.chat

import std.time.*
import std.unittest.*
import std.unittest.testmacro.*
import cactor_persistence.{MemoryDurableStateStore}
import cactor_platform.chat.*

@Test
public class ChatServiceTest {
    @TestCase
    public func directThreadIsSymmetricAndHistoryPaginates(): Unit {
        let chat = ChatService.create()
        let t1 = chat.directThread("alice", "bob", topic: "weekly sync")
        let t2 = chat.directThread("bob", "alice")
        @Expect(t2.id, t1.id)
        @Expect(t1.memberIds.size, 2)
        @Expect(chat.topic(t1.id), "weekly sync")

        @Expect(chat.send(t1.id, "alice", "hi").offset, 1)
        @Expect(chat.send(t1.id, "bob", "hello").offset, 2)
        @Expect(chat.send(t1.id, "alice", "how are you").offset, 3)

        let page = chat.history(t1.id, maxRows: 2)
        @Expect(page.messages.size, 2)
        @Expect(page.nextOffset, 3)
        @Expect(page.messages[0].senderId, "alice")
        @Expect(page.messages[0].text, "hi")
        @Expect(page.messages[1].senderId, "bob")

        let rest = chat.history(t1.id, fromOffset: page.nextOffset)
        @Expect(rest.messages.size, 1)
        @Expect(rest.messages[0].text, "how are you")
        @Expect(rest.messages[0].offset, 3)

        @Expect(chat.history(t1.id, fromOffset: rest.nextOffset).isEmpty(), true)
    }

    @TestCase
    public func watchReturnsOnNewMessage(): Unit {
        let chat = ChatService.create()
        let t = chat.directThread("a", "b")
        let cursor = chat.history(t.id).nextOffset
        let sender = spawn {
            sleep(Duration.millisecond * 150)
            chat.send(t.id, "a", "ping")
        }
        let got = chat.watch(t.id, fromOffset: cursor, timeout: Duration.second * 5)
        @Expect(got.isEmpty(), false)
        @Expect(got.messages[0].text, "ping")
        sender.get()
    }

    @TestCase
    public func typingStatusRespectsTtlAndClear(): Unit {
        let chat = ChatService.create(typingTtl: Duration.millisecond * 200)
        let t = chat.directThread("a", "b")

        chat.setTyping(t.id, "a")
        let active = chat.typingStatus(t.id)
        @Expect(active.size, 1)
        @Expect(active[0].playerId, "a")

        // 发送消息即清除发送者的输入中
        chat.send(t.id, "a", "done")
        @Expect(chat.typingStatus(t.id).size, 0)

        // 显式停止输入
        chat.setTyping(t.id, "a")
        chat.setTyping(t.id, "a", typing: false)
        @Expect(chat.typingStatus(t.id).size, 0)

        // TTL 过期后不再出现
        chat.setTyping(t.id, "b")
        sleep(Duration.millisecond * 300)
        @Expect(chat.typingStatus(t.id).size, 0)
    }

    @TestCase
    public func readStateTracksAndClamps(): Unit {
        let chat = ChatService.create()
        let t = chat.directThread("a", "b")
        chat.send(t.id, "a", "1")
        chat.send(t.id, "a", "2")
        chat.send(t.id, "a", "3")

        // 默认读到最新
        @Expect(chat.markRead(t.id, "b").lastReadOffset, 3)
        @Expect(chat.readState(t.id, "b").lastReadOffset, 3)

        // 显式回退
        @Expect(chat.markRead(t.id, "b", offset: 1).lastReadOffset, 1)
        @Expect(chat.readState(t.id, "b").lastReadOffset, 1)

        // 超出末尾夹到最新
        @Expect(chat.markRead(t.id, "b", offset: 99).lastReadOffset, 3)

        // 未标记过的成员为 0
        @Expect(chat.readState(t.id, "a").lastReadOffset, 0)
    }

    @TestCase
    public func rejectsUnknownThreadNonMemberAndEmptyText(): Unit {
        let chat = ChatService.create()
        let t = chat.directThread("a", "b")

        var unknown = false
        try {
            chat.history("missing-thread")
        } catch (e: IllegalArgumentException) {
            unknown = true
        }
        @Expect(unknown, true)

        var nonMember = false
        try {
            chat.send(t.id, "eve", "hi")
        } catch (e: IllegalArgumentException) {
            nonMember = true
        }
        @Expect(nonMember, true)

        var emptyText = false
        try {
            chat.send(t.id, "a", "")
        } catch (e: IllegalArgumentException) {
            emptyText = true
        }
        @Expect(emptyText, true)

        var selfThread = false
        try {
            chat.directThread("a", "a")
        } catch (e: IllegalArgumentException) {
            selfThread = true
        }
        @Expect(selfThread, true)

        @Expect(chat.threadInfo("missing-thread").isNone(), true)
        @Expect(chat.threadInfo(t.id).isSome(), true)
    }

    @TestCase
    public func historySurvivesNewServiceWithSameStore(): Unit {
        let store = MemoryDurableStateStore()
        let first = ChatService.create(store: store)
        let t = first.directThread("a", "b")
        first.send(t.id, "a", "persisted")

        // 新的服务实例（模拟重启）用同一 store：线程元信息重建，历史仍在
        let second = ChatService.create(store: store)
        let rebuilt = second.directThread("a", "b")
        @Expect(rebuilt.id, t.id)
        let history = second.history(rebuilt.id)
        @Expect(history.messages.size, 1)
        @Expect(history.messages[0].text, "persisted")
    }
}
