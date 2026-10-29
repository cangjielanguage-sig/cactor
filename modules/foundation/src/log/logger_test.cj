package cactor_foundation.log

import std.sync.*
import std.unittest.*
import std.unittest.testmacro.*

@Test
public class LoggerTest {
    @TestCase
    func levelFilteringAndLazyMessage(): Unit {
        let mem = MemoryLogBackend()
        LoggerFactory.setBackend(mem)
        LoggerFactory.setLevel(LogLevel.Info)
        let log = LoggerFactory.getLogger("t")
        // lambda 不能捕获可变变量，用 AtomicBool 记录是否被求值
        let evaluated = AtomicBool(false)
        log.debug({ => evaluated.store(true); "hidden" })
        log.info("shown")
        log.warn("warned")
        @Expect(evaluated.load(), false)
        let lines = mem.entries()
        @Expect(lines.size, 2)
        @Expect(lines[0], "INFO [t] shown")
        LoggerFactory.setLevel(LogLevel.Off)
        log.error("nothing")
        @Expect(mem.entries().size, 2)
        LoggerFactory.setLevel(LogLevel.Info)
        LoggerFactory.setBackend(StderrLogBackend())
    }
}
