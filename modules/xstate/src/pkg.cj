/*
 * Copyright (c) 2024. All rights reserved.
 * CActor XState - 基于仓颉语言和CActor框架的状态机系统
 * 顶级包文件
 */
package cactor_xstate

/**
 * XState包信息
 */
public struct XStatePackage {
    /**
     * 获取包信息
     */
    public static func getInfo(): String {
        "CActor XState - 基于仓颉语言和CActor框架的企业级状态机系统"
    }
    
    /**
     * 获取版本信息
     */
    public static func getVersion(): String {
        "0.1.0"
    }
}
