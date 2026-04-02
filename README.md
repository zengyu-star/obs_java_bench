# HUAWEI Cloud OBS Java Bench
![Java Standard](https://img.shields.io/badge/Java-17%2B-blue.svg)
![License](https://img.shields.io/badge/License-Apache_2.0-green.svg)
![Build](https://img.shields.io/badge/Build-Maven-orange.svg)

`obs_java_bench` 是专为 Huawei Cloud OBS (Object Storage Service) 打造的 **高性能 Java 基准压测工具**。本工具通过架构级的极限优化，消除了常规并发工具由于 GC 停顿或锁竞争带来的结果偏差，提供最精准、最纯粹的 OBS 服务端性能（如 TPS 和 长尾时延）评估。

---

## 🎯 核心特性 (Key Features)

* **零分配 (Zero-Allocation) & 零 GC (Zero-GC)**: 在压测的核心主循环发令模块内，完全屏蔽了 `new` 操作和短促生命周期对象，避免压测过程中产生因为 GC 引发的性能抖动。
* **零拷贝 (Zero-Copy) 内存缓冲**: 利用 `DirectByteBuffer`（堆外内存）在操作系统内核态提前打满测试载荷，测试发流阶段复用游标直接读区内存，避免业务数据的 Byte 复制。
* **发令枪并发模型 (Start-Gun Mechanism)**: 引入强一致性的线程等待 `CountDownLatch` 等发令机制构建屏障，确保瞬时高并发线程能在严格同步的一瞬间扣动“扳机”。
* **无锁监控采集**: 打破传统的互斥锁方案，全盘引入 `LongAdder` 及 `Atomic` 构建数据采集统计模块，支持千万级极低开销的数据累加。
* **高精度时延度量 (HdrHistogram)**: 内置 HdrHistogram 实现无偏差的、纳秒精度的时延数据统计。包含 P50、P90、P99 及最大分布时延报告。

---

## 🚀 极速构建与运行 (Quick Start)

### 1. 编译打包
请确保您的环境安装有 **Java 17+** 和 **Maven 3.x**。
```bash
mvn clean package -DskipTests
```
> 如果系统没有默认的 `mvn`，可以使用绝对路径，例如 `/usr/local/maven/bin/mvn clean package -DskipTests`

打包成功后，将在 `target/` 目录下生成包含所有依赖包（Log4j2 / OBS SDK / Disruptor 等）的 `obs_java_bench-1.0.0-SNAPSHOT.jar` 终态文件。

### 2. 执行压测
压测启动只需要输入配置文件的相对路径即可即可。
```bash
java -jar target/obs_java_bench-1.0.0-SNAPSHOT.jar conf/config.dat conf/users.dat
```

---

## ⚙️ 参数配置指南 (Configuration)

工具的所有环境依赖配置化。您主要需要修改 `conf/config.dat` (压测场景参数) 和 `conf/users.dat` (凭证参数) 两个文件。

### `conf/config.dat`

主要核心配置说明如下：

```properties
# ==================== 网络认证项 =====================
Endpoint=obs.cn-north-4.myhuaweicloud.com   # 目标区域地址
IsSecure=true                               # 是否启用 HTTPS，HTTPS 开销更大
IsMockMode=false                            # 是否离线模拟发流请求 (用于自测)

# ==================== 并发与控制 =====================
MaxConnections=2000                         # 底层连接池最大连接数(需大于UsersCount*ThreadsPerUser)
UsersCount=1                                # 并发用户数
ThreadsPerUser=50                           # 单一用户拥有的压测线程数
RunSeconds=60                               # 连续压压测试验时长 (秒)
TestCaseCode=201                            # 压测路由指令

# ==================== 存储与载荷 =====================
BucketName=my-bench-bucket                  # 压测打向的具体通名
KeyPrefix=bench_test_run_                   # 在该桶中生成的测试对象名前缀
ObjectSize=1048576                          # 上传/读取 基础请求单片 Payload 大小（字节）
```

**支持的常用 `TestCaseCode` 列表**：
- `201`：**上传对象** (`PutObject`)
- `202`：**下载对象** (`GetObject`) - 目前会做 Range / Drain InputStream 调优排空流控制。
- `204`：**删除对象** (`DeleteObject`)
- `216`：**分段上传** (`MultipartUpload`)
- `230`：**断点续传** (`ResumableUploadFile`)

### `conf/users.dat`

在这里配置您用于登录华为云的凭证信息。CSV 格式文本，每一行对应 `UsersCount` 的一个用户：
```csv
# username, ak, sk
hw_user_a, 真实AK1, 真实SK1
hw_user_b, 真实AK2, 真实SK2
```

> **提示**：如果遇到安全问题，您也可以通过添加一列配置 `[sts_token]` 来传递 STS 临时凭证。

---

## 📈 压测报告解读 (Benchmarking Report)

工具将在运行时为您以 1 秒为固定帧率输出压测快照：
```text
=========================================================================================
Time(s)    | TPS        | Bandwidth(MB/s) | Total Succ   | Errors(403/404/5xx/Client)
=========================================================================================
```

压测在 `RunSeconds` 时限结束后，将抛出权威的基准测试总结：
```text
================= 🏆 BENCHMARK FINAL SUMMARY 🏆 =================
Total Time Elapsed: 60 seconds
Total Requests:     31109 (Success)
Average TPS:        518.48 req/s
Average Bandwidth:  518.48 MB/s
-------------------------------------------------------------------
[Latency Distribution]
  P50 (Median):  1.52 ms
  P90 Latency:   2.89 ms
  P99 Latency:   8.41 ms
  P99.9 Latency: 12.05 ms
  Max Latency:   30.22 ms
===================================================================
```
*注：由于工具自身消耗近乎为 0，所报告的纳秒级 P99 数据，可视为对 OBS 服务端最公正、最直观的响应延时性能体现。*
