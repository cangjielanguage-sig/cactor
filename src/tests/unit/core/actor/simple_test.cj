/*
 * Copyright (c) 2024. All rights reserved.
 * CActor 简单测试验证
 * Plan 0.20 Phase 1.1: 标准化测试框架 - 验证@Test
 */
package cactor.tests.unit.core.actor

import std.unittest.*
import std.unittest.testmacro.*

/**
 * 简单测试套件 - 验证@Test注解是否工作
 */
@Test
public class SimpleTestSuite {
    
    /**
     * 测试基本断言
     */
    @TestCase
    public func testBasicAssertion(): Unit {
        @Assert(true, true)
        @Assert(1 + 1 == 2, true)
        @Assert("hello".size == 5, true)
    }
    
    /**
     * 测试字符串操作
     */
    @TestCase
    public func testStringOperations(): Unit {
        let str = "Hello World"
        @Assert(str.contains("World"), true)
        @Assert(str.startsWith("Hello"), true)
        @Assert(str.size == 11, true)
    }
    
    /**
     * 测试数组操作
     */
    @TestCase
    public func testArrayOperations(): Unit {
        let arr = [1, 2, 3, 4, 5]
        @Assert(arr.size == 5, true)
        @Assert(arr[0] == 1, true)
        @Assert(arr[4] == 5, true)
    }
}
