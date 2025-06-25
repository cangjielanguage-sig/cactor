#!/bin/bash

# CActor 宏包自动编译脚本
# 用于自动化编译宏包和相关示例

set -e  # 遇到错误立即退出

echo "🔧 CActor宏包自动编译脚本"
echo "================================"

# 检查是否存在cjc命令
if ! command -v cjc &> /dev/null; then
    echo "❌ 错误: 找不到cjc命令，请确保仓颉工具链已正确安装"
    exit 1
fi

# 清理之前的编译产物
echo "🧹 清理之前的编译产物..."
rm -f *.cjo *.so *.dylib macro_test

# 编译宏包
echo "📦 编译宏包..."
echo "执行命令: cjc src/macros/actor_dsl_macros.cj --compile-macro"
cjc src/macros/actor_dsl_macros.cj --compile-macro

if [ $? -eq 0 ]; then
    echo "✅ 宏包编译成功"
else
    echo "❌ 宏包编译失败"
    exit 1
fi

# 编译宏测试示例
echo "🧪 编译宏测试示例..."
echo "执行命令: cjc src/examples/macro_test_demo/main.cj -o macro_test"
cjc src/examples/macro_test_demo/main.cj -o macro_test

if [ $? -eq 0 ]; then
    echo "✅ 宏测试示例编译成功"
else
    echo "❌ 宏测试示例编译失败"
    exit 1
fi

# 运行测试
echo "🚀 运行宏功能测试..."
echo "================================"
./macro_test

echo ""
echo "================================"
echo "✅ 宏包编译和测试完成！"
echo ""
echo "📝 使用说明:"
echo "1. 修改宏定义后，重新运行此脚本"
echo "2. 宏包文件: src/macros/actor_dsl_macros.cj"
echo "3. 测试文件: src/examples/macro_test_demo/main.cj"
echo ""
echo "🔍 调试宏展开结果:"
echo "   cjc --debug-macro src/examples/macro_test_demo/main.cj"
echo "   查看生成的 .macrocall 文件"
