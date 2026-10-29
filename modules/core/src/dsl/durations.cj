/*
 * CActor Core.DSL - 时间字面量
 *
 *   import cactor_core.dsl.*
 *   timers.startSingleTimer("tick", Tick(), 100.millis)
 *   ref.ask(..., 3.seconds)
 *
 * 仓颉中对外部类型的直接扩展不会导出，因此以接口扩展的形式提供（导入本包即可使用）。
 */
package cactor_core.dsl

import std.time.*

public interface DurationLiterals {
    prop nanos: Duration
    prop micros: Duration
    prop millis: Duration
    prop seconds: Duration
    prop minutes: Duration
    prop hours: Duration
}

extend Int64 <: DurationLiterals {
    public prop nanos: Duration {
        get() { Duration.nanosecond * this }
    }

    public prop micros: Duration {
        get() { Duration.microsecond * this }
    }

    public prop millis: Duration {
        get() { Duration.millisecond * this }
    }

    public prop seconds: Duration {
        get() { Duration.second * this }
    }

    public prop minutes: Duration {
        get() { Duration.minute * this }
    }

    public prop hours: Duration {
        get() { Duration.hour * this }
    }
}
