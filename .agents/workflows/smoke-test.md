---
description: Run OBS Java Bench Compilation and Smoke Test
---

1. Execute the compilation and smoke test script
// turbo
run_command(CommandLine: "python3 compile_and_smoke_test.py", Cwd: "/home/zengyu/obs_java_bench", SafeToAutoRun: true, WaitMsBeforeAsync: 0)
