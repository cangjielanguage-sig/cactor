#!/bin/bash

# CActor 0.60.5 升级 - 修复最终错误脚本
echo "修复最终的语法错误..."

# 修复错误的Range语法 - 字符串类型的remove操作
echo "修复字符串类型的remove操作..."

# 修复 ask_pattern.cj
sed -i '' 's/requestId\.\.requestId+1/requestId/g' src/patterns/ask/ask_pattern.cj

# 修复 advanced_routing.cj 中的错误Range语法
sed -i '' 's/0\.\.1\.\.0\.\.1+1/0..1/g' src/patterns/routing/advanced/advanced_routing.cj
sed -i '' 's/actor\.path\.toString(\.\.actor\.path\.toString(+1)/actor.path.toString()/g' src/patterns/routing/advanced/advanced_routing.cj

# 修复 guardian 文件中的字符串+Int64问题
sed -i '' 's/name\.\.name+1/name/g' src/runtime/guardian/base_guardian_actor.cj
sed -i '' 's/childPath\.\.childPath+1/childPath/g' src/runtime/guardian/base_guardian_actor.cj
sed -i '' 's/name\.\.name+1/name/g' src/runtime/guardian/runtime_system_guardian.cj
sed -i '' 's/name\.\.name+1/name/g' src/runtime/guardian/runtime_user_guardian.cj

# 修复 distributed_tracing.cj
sed -i '' 's/span\.spanId\.value\.\.span\.spanId\.value+1/span.spanId.value/g' src/integration/monitoring/distributed_tracing.cj

# 修复剩余的append调用
echo "修复剩余的append调用..."

# 修复一些遗漏的文件
FILES_TO_FIX=(
    "src/distribution/persistence/persistent_actor_manager.cj"
    "src/distribution/persistence/snapshot_store.cj"
    "src/integration/logging/logger.cj"
    "src/integration/monitoring/metrics.cj"
    "src/integration/monitoring/performance_analyzer.cj"
    "src/integration/monitoring/visualization.cj"
    "src/runtime/lifecycle/actor_lifecycle_manager.cj"
    "src/runtime/supervision/supervision_manager.cj"
    "src/runtime/mailbox/advanced/foundation_based_mailbox.cj"
    "src/runtime/mailbox/batching/batching_mailbox.cj"
    "src/runtime/system/high_performance_actor_system.cj"
    "src/runtime/dispatcher/monitoring/dispatcher_metrics.cj"
    "src/runtime/dispatcher/monitoring/scheduler_monitor.cj"
)

for file in "${FILES_TO_FIX[@]}"; do
    if [ -f "$file" ]; then
        echo "修复文件: $file"
        # 修复 append -> add
        sed -i '' 's/\.append(/\.add(/g' "$file"
        # 修复 put -> add
        sed -i '' 's/\.put(/\.add(/g' "$file"
        # 修复 ReentrantMutex -> Mutex
        sed -i '' 's/ReentrantMutex/Mutex/g' "$file"
        # 修复 ReentrantReadWriteMutex -> ReadWriteLock
        sed -i '' 's/ReentrantReadWriteMutex/ReadWriteLock/g' "$file"
        # 修复错误的remove语法
        sed -i '' 's/\.remove(0)/\.remove(0..1)/g' "$file"
    fi
done

echo "修复完成！验证构建..."
