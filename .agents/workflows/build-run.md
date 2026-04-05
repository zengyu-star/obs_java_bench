---
description: Build and Run OBS Java Bench
---

1. Build the project using Maven
// turbo
run_command(CommandLine: "mvn clean package", Cwd: "/home/zengyu/obs_java_bench", SafeToAutoRun: true, WaitMsBeforeAsync: 0)

2. Run the benchmark tool
// turbo
run_command(CommandLine: "java -jar target/obs_java_bench-1.0.0-SNAPSHOT.jar", Cwd: "/home/zengyu/obs_java_bench", SafeToAutoRun: true, WaitMsBeforeAsync: 0)
