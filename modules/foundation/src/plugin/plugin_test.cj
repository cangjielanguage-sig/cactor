package cactor_foundation.plugin

import std.collection.*
import std.unittest.*
import std.unittest.testmacro.*

@Test
public class PluginKernelTest {
    @TestCase
    func duplicateInstallRejected(): Unit {
        let r = PluginRegistry()
        r.register("A")
        var thrown = false
        try { r.register("A") } catch (e: DuplicatePluginException) { thrown = true }
        @Expect(thrown, true)
        @Expect(r.contains("A"), true)
    }

    @TestCase
    func frozenRegistryRejectsInstall(): Unit {
        let r = PluginRegistry()
        r.freeze()
        var thrown = false
        try { r.register("B") } catch (e: PluginFrozenException) { thrown = true }
        @Expect(thrown, true)
    }

    @TestCase
    func interceptorOrderFirstIsOutermost(): Unit {
        let trace = ArrayList<String>()
        let chain = InterceptorChain<ArrayList<String>>()
        chain.add({ t, next => t.add("a>"); next(); t.add("<a") })
        chain.add({ t, next => t.add("b>"); next(); t.add("<b") })
        let f = chain.build({ t => t.add("core") })
        f(trace)
        @Expect(String.join(trace.toArray(), delimiter: ","), "a>,b>,core,<b,<a")
    }

    @TestCase
    func interceptorCanShortCircuit(): Unit {
        let trace = ArrayList<String>()
        let chain = InterceptorChain<ArrayList<String>>()
        chain.add({ t, next => t.add("stop") })
        chain.build({ t => t.add("core") })(trace)
        @Expect(trace.size, 1)
    }

    @TestCase
    func hookListRunsInOrder(): Unit {
        let trace = ArrayList<Int64>()
        let hooks = HookList<ArrayList<Int64>>()
        hooks.add({ t => t.add(1) })
        hooks.add({ t => t.add(2) })
        hooks.freeze()
        hooks.run(trace)
        @Expect(trace[0], 1)
        @Expect(trace[1], 2)
    }
}
