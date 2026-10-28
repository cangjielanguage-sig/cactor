/*
 * CActor - 控制消息识别器（默认实现）
 * 集中管理系统/控制消息的识别逻辑，避免在各处硬编码字符串包含判断
 */
package cactor_runtime.mailbox

import cactor_core.message.{Message}

public struct ControlClassifier {
  /**
   * 判断消息是否为控制/系统优先消息
   * 最小改动：沿用现有 contains 规则，后续可替换为类型或枚举判断
   */
  public static func isControl(msg: Message): Bool {
    let t = msg.messageType()
    t.contains("Stop") || t.contains("Restart") ||
    t.contains("Suspend") || t.contains("Resume") ||
    t.contains("Terminated") || t.contains("DeadLetter")
  }
}

