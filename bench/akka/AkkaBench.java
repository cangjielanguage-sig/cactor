import akka.actor.*;
import akka.pattern.Patterns;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 与 cactor_examples.real_benchmark 一一对应的 Akka 基准（经典 Actor，默认 dispatcher，未调参）。
 *
 * 运行：bench/akka/run.sh [轮数]
 * 场景、消息数、计时方式与 modules/examples/src/real_benchmark/main.cj 保持一致；修改其中一个时请同步修改另一个。
 */
public class AkkaBench {
    static final class Ping { final long id; Ping(long id) { this.id = id; } }
    static final class Pong { final long id; Pong(long id) { this.id = id; } }

    static class CountActor extends AbstractActor {
        final AtomicLong counter;
        CountActor(AtomicLong c) { counter = c; }
        public Receive createReceive() {
            return receiveBuilder().matchAny(m -> counter.incrementAndGet()).build();
        }
    }

    static class PongActor extends AbstractActor {
        public Receive createReceive() {
            return receiveBuilder().match(Ping.class, p -> getSender().tell(new Pong(p.id), getSelf())).build();
        }
    }

    static class PingActor extends AbstractActor {
        final AtomicLong done;
        PingActor(AtomicLong d) { done = d; }
        public Receive createReceive() {
            return receiveBuilder().match(Pong.class, p -> done.set(p.id)).build();
        }
    }

    static class EchoActor extends AbstractActor {
        public Receive createReceive() {
            return receiveBuilder().matchAny(m -> getSender().tell(m, getSelf())).build();
        }
    }

    static void waitFor(java.util.function.BooleanSupplier c) throws InterruptedException {
        while (!c.getAsBoolean()) { Thread.sleep(0, 50_000); }
    }

    static long perSecond(long n, long ns) { return n * 1_000_000_000L / ns; }

    static void report(String title, List<Long> samples) {
        long[] a = samples.stream().mapToLong(Long::longValue).toArray();
        Arrays.sort(a);
        double avg = Arrays.stream(a).average().orElse(0) / 1000.0;
        System.out.printf("  %s: n=%d 平均=%.3fµs P50=%.3fµs P99=%.3fµs P99.9=%.3fµs 最大=%.3fµs%n", title, a.length, avg,
                a[(int) ((a.length - 1) * 0.5)] / 1000.0, a[(int) ((a.length - 1) * 0.99)] / 1000.0,
                a[(int) ((a.length - 1) * 0.999)] / 1000.0, a[a.length - 1] / 1000.0);
    }

    static void single(ActorSystem sys, long n) throws Exception {
        AtomicLong c = new AtomicLong();
        ActorRef ref = sys.actorOf(Props.create(CountActor.class, () -> new CountActor(c)));
        String msg = "x";
        long t0 = System.nanoTime();
        for (long i = 0; i < n; i++) ref.tell(msg, ActorRef.noSender());
        long sent = System.nanoTime() - t0;
        waitFor(() -> c.get() >= n);
        long total = System.nanoTime() - t0;
        System.out.printf("[1] 单 Actor 吞吐：%d 条，发送耗时 %dms，处理完成 %dms → %d msg/s%n", n, sent / 1_000_000, total / 1_000_000, perSecond(n, total));
    }

    static void fanOut(ActorSystem sys, int senders, int actors, long perSender) throws Exception {
        AtomicLong[] cs = new AtomicLong[actors];
        ActorRef[] refs = new ActorRef[actors];
        for (int i = 0; i < actors; i++) {
            AtomicLong c = new AtomicLong();
            cs[i] = c;
            refs[i] = sys.actorOf(Props.create(CountActor.class, () -> new CountActor(c)));
        }
        long total = senders * perSender;
        String msg = "x";
        long t0 = System.nanoTime();
        List<Thread> ts = new ArrayList<>();
        for (int s = 0; s < senders; s++) {
            final int sf = s;
            Thread t = new Thread(() -> { for (long i = 0; i < perSender; i++) refs[(int) ((sf + i) % actors)].tell(msg, ActorRef.noSender()); });
            t.start();
            ts.add(t);
        }
        for (Thread t : ts) t.join();
        waitFor(() -> { long s = 0; for (AtomicLong c : cs) s += c.get(); return s >= total; });
        long el = System.nanoTime() - t0;
        System.out.printf("[2] 扇出吞吐：%d 发送方 × %d Actor，共 %d 条，%dms → %d msg/s%n", senders, actors, total, el / 1_000_000, perSecond(total, el));
    }

    static void pingPong(ActorSystem sys, int rounds) {
        AtomicLong done = new AtomicLong(-1);
        ActorRef pong = sys.actorOf(Props.create(PongActor.class, PongActor::new));
        ActorRef ping = sys.actorOf(Props.create(PingActor.class, () -> new PingActor(done)));
        List<Long> samples = new ArrayList<>();
        for (int i = 0; i < rounds; i++) {
            long t0 = System.nanoTime();
            pong.tell(new Ping(i), ping);
            while (done.get() != i) { Thread.onSpinWait(); }
            if (i >= rounds / 10) samples.add(System.nanoTime() - t0);
        }
        report("[3] ping-pong 往返延迟", samples);
    }

    static void ask(ActorSystem sys, int rounds) throws Exception {
        ActorRef echo = sys.actorOf(Props.create(EchoActor.class, EchoActor::new));
        Duration to = Duration.ofSeconds(5);
        List<Long> samples = new ArrayList<>();
        for (int i = 0; i < rounds; i++) {
            long t0 = System.nanoTime();
            Patterns.ask(echo, "q", to).toCompletableFuture().get();
            if (i >= rounds / 10) samples.add(System.nanoTime() - t0);
        }
        report("[4] Ask 往返延迟", samples);
    }

    static void remote(long n) throws Exception {
        Config cfg = ConfigFactory.parseString(
                "akka.loglevel=WARNING\n" +
                "akka.actor.provider=remote\n" +
                "akka.remote.artery.transport=tcp\n" +
                "akka.remote.artery.canonical.hostname=127.0.0.1\n" +
                "akka.remote.artery.canonical.port=0\n" +
                "akka.remote.artery.advanced.outbound-message-queue-size=300000\n").withFallback(ConfigFactory.load());
        ActorSystem a = ActorSystem.create("a", cfg);
        ActorSystem b = ActorSystem.create("b", cfg);
        AtomicLong c = new AtomicLong();
        b.actorOf(Props.create(CountActor.class, () -> new CountActor(c)), "sink");
        Address addr = ((ExtendedActorSystem) b).provider().getDefaultAddress();
        ActorRef remote = a.actorSelection(addr + "/user/sink").resolveOne(Duration.ofSeconds(5)).toCompletableFuture().get();
        String msg = "payload-0123456789";
        // 预热连接
        remote.tell(msg, ActorRef.noSender());
        waitFor(() -> c.get() >= 1);
        c.set(0);
        long t0 = System.nanoTime();
        for (long i = 0; i < n; i++) remote.tell(msg, ActorRef.noSender());
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
        waitFor(() -> c.get() >= n || System.nanoTime() > deadline);
        long el = System.nanoTime() - t0;
        System.out.printf("[5] 远程 TCP 吞吐（回环，Artery TCP）：%d 条（收到 %d），%dms → %d msg/s%n", n, c.get(), el / 1_000_000, perSecond(c.get(), el));
        a.terminate();
        b.terminate();
    }

    public static void main(String[] args) throws Exception {
        System.out.println("=== Akka 2.6.21 基准 ===");
        Config cfg = ConfigFactory.parseString("akka.loglevel=WARNING").withFallback(ConfigFactory.load());
        ActorSystem sys = ActorSystem.create("bench", cfg);
        // 参数：轮数（默认 3）。JIT 需要预热，取最后一轮作为结果
        int rounds = args.length > 0 ? Integer.parseInt(args[0]) : 3;
        for (int r = 0; r < rounds; r++) {
            System.out.println("--- 第 " + (r + 1) + " 轮 ---");
            single(sys, 1_000_000);
            fanOut(sys, 8, 100, 250_000);
            pingPong(sys, 20_000);
            ask(sys, 20_000);
        }
        sys.terminate();
        for (int r = 0; r < rounds; r++) remote(200_000);
        System.exit(0);
    }
}
