#!/bin/bash

# CActor 0.60.5 升级 - 批量修复剩余错误脚本
# 此脚本用于批量修复剩余的API兼容性问题

echo "开始批量修复CActor项目剩余的API兼容性问题..."

# 定义需要修复的文件列表
FILES=(
    "src/runtime/mailbox/mailbox_factory.cj"
    "src/runtime/registry/actor_registry.cj"
    "src/runtime/events/actor_event_bus.cj"
    "src/runtime/events/simple_actor_event_bus.cj"
    "src/runtime/guardian/base_guardian_actor.cj"
    "src/runtime/guardian/runtime_system_guardian.cj"
    "src/runtime/guardian/runtime_user_guardian.cj"
    "src/config/file/config_loader.cj"
    "src/distribution/persistence/event_store.cj"
    "src/distribution/persistence/persistence_plugin.cj"
    "src/distribution/cluster/cluster_support.cj"
    "src/distribution/cluster/failover.cj"
    "src/integration/logging/debug_tools.cj"
    "src/integration/monitoring/distributed_tracing.cj"
    "src/integration/monitoring/memory_monitor.cj"
    "src/patterns/ask/ask_pattern.cj"
    "src/patterns/routing/advanced/advanced_routing.cj"
    "src/core/supervision/circuit_breaker_supervision.cj"
)

# 修复函数
fix_file() {
    local file=$1
    echo "修复文件: $file"
    
    if [ -f "$file" ]; then
        # 修复 ArrayList.append() -> add()
        sed -i '' 's/\.append(/\.add(/g' "$file"
        
        # 修复 HashMap.put() -> add()
        sed -i '' 's/\.put(/\.add(/g' "$file"
        
        # 修复 ArrayList.remove(index) -> remove(index..index+1)
        sed -i '' 's/\.remove(0)/\.remove(0..1)/g' "$file"
        sed -i '' 's/\.remove(\([^)]*\))/\.remove(\1..\1+1)/g' "$file"
        
        # 修复 ReentrantMutex -> Mutex
        sed -i '' 's/ReentrantMutex/Mutex/g' "$file"
        
        echo "  ✅ 完成修复: $file"
    else
        echo "  ❌ 文件不存在: $file"
    fi
}

# 批量修复所有文件
for file in "${FILES[@]}"; do
    fix_file "$file"
done

echo ""
echo "批量修复完成！正在验证构建..."

# 验证构建
if cjpm build > /dev/null 2>&1; then
    echo "🎉 构建成功！所有错误已修复。"
else
    echo "⚠️  仍有错误需要手动修复，请检查构建输出。"
    cjpm build 2>&1 | head -20
fi

echo ""
echo "升级进度总结:"
echo "- ✅ 环境配置升级完成"
echo "- ✅ 核心语法修复完成"
echo "- ✅ 批量错误修复完成"
echo "- 🔄 正在进行最终验证..."
