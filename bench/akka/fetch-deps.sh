#!/usr/bin/env bash
# 下载 Akka 基准所需的 jar 到 bench/akka/jars/（不纳入版本库），并用 Maven Central 提供的 SHA-1 校验。
# Akka 2.6.21 是最后一个 Apache-2.0 许可的版本。
set -euo pipefail
cd "$(dirname "$0")"
REPO="${MAVEN_REPO:-https://repo1.maven.org/maven2}"
ARTIFACTS=(
  com/typesafe/akka/akka-actor_2.13/2.6.21/akka-actor_2.13-2.6.21.jar
  com/typesafe/akka/akka-remote_2.13/2.6.21/akka-remote_2.13-2.6.21.jar
  com/typesafe/akka/akka-stream_2.13/2.6.21/akka-stream_2.13-2.6.21.jar
  com/typesafe/akka/akka-protobuf-v3_2.13/2.6.21/akka-protobuf-v3_2.13-2.6.21.jar
  org/scala-lang/scala-library/2.13.12/scala-library-2.13.12.jar
  org/scala-lang/modules/scala-java8-compat_2.13/1.0.2/scala-java8-compat_2.13-1.0.2.jar
  com/typesafe/config/1.4.2/config-1.4.2.jar
  com/typesafe/ssl-config-core_2.13/0.6.1/ssl-config-core_2.13-0.6.1.jar
  org/reactivestreams/reactive-streams/1.0.4/reactive-streams-1.0.4.jar
  org/agrona/agrona/1.15.1/agrona-1.15.1.jar
)
mkdir -p jars
for a in "${ARTIFACTS[@]}"; do
  f="jars/$(basename "$a")"
  expected="$(curl -sSf "$REPO/$a.sha1" | cut -c1-40)"
  if [[ -f "$f" ]] && [[ "$(sha1sum "$f" | cut -c1-40)" == "$expected" ]]; then
    continue
  fi
  curl -sSf -o "$f.part" "$REPO/$a"
  actual="$(sha1sum "$f.part" | cut -c1-40)"
  if [[ "$actual" != "$expected" ]]; then
    rm -f "$f.part"
    echo "校验失败: $a (期望 $expected, 实际 $actual)" >&2
    exit 1
  fi
  mv "$f.part" "$f"
  echo "已下载 $(basename "$a")"
done
echo "依赖就绪: $(ls jars/*.jar | wc -l) 个 jar"
