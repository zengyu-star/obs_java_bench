# HUAWEI Cloud OBS Java Bench
![Java Standard](https://img.shields.io/badge/Java-17%2B-blue.svg)
![License](https://img.shields.io/badge/License-Apache_2.0-green.svg)
![Build](https://img.shields.io/badge/Build-Maven-orange.svg)

`obs_java_bench` 是专为 Huawei Cloud OBS (Object Storage Service) 打造的 **高性能 Java 基准压测工具**。本工具通过架构级的极限优化，消除了常规并发工具由于 GC 停顿或锁竞争带来的结果偏差，提供最精准、最纯粹的 OBS 服务端性能（如 TPS 和 长尾时延）评估。

---

## 🎯 核心特性 (Key Features)

* **零分配 (Zero-Allocation) & 零 GC (Zero-GC)**: 在压测核心主循环内屏蔽 `new` 操作，避免 GC 引发的性能抖动。
* **零拷贝 (Zero-Copy) 内存缓冲**: 利用 `DirectByteBuffer` 提前加载测试载荷，避免业务数据在堆内外的重复复制。
* **发令枪并发模型 (Start-Gun Mechanism)**: 使用 `CountDownLatch` 确保所有线程在同一瞬间开始请求。
* **无锁监控采集**: 引入 `LongAdder` 和 `HdrHistogram` 实现极低开销的数据统计。
* **智能桶名生成**: 支持多租户下的 `{ak}.{prefix}` 动态桶名自动生成及 DNS 规范化校验。
* **内置高性能数据生成器**: 集成基于 SplitMix64 算法的确定性数据生成器，确保测试数据与校验逻辑位流一致。

---

## 🚀 极速构建与运行 (Quick Start)

### 1. 编译打包
请确保您的环境安装有 **Java 17+** (推荐 OpenJDK) 和 **Maven 3.x**。
```bash
mvn clean package -DskipTests
```

### 2. 生成测试数据 (可选)
对于分段上传 (216) 或断点续传 (230) 场景，需要本地测试文件。您可以使用内置工具快速生成：
```bash
# 生成 100MB 的测试文件 (默认文件名 test_data.bin)
java -jar target/obs_java_bench-1.0.0-SNAPSHOT.jar gen 100
```
> [!TIP]
> 如果运行 216/230 Case 时未指定 `UploadFilePath` 且文件不存在，工具会尝试自动触发生成（默认 100MB）。

### 3. 执行压测
工具支持智能参数解析，会自动寻找目录下的 `.dat` 配置文件：
```bash
# 默认使用当前目录下的 config.dat 和 users.dat
java -jar target/obs_java_bench-1.0.0-SNAPSHOT.jar 

# 也可以显式指定路径或 TestCaseCode 覆盖配置
java -jar target/obs_java_bench-1.0.0-SNAPSHOT.jar config.dat users.dat 230
```

---

## ⚙️ 参数配置指南 (Configuration)

### `config.dat` (压测场景参数)

```properties
# ==================== 网络与认证 =====================
Endpoint=obs.cn-north-4.myhuaweicloud.com   # 目标区域地址
Protocol=http                               # 协议选择 (http/https)
IsTemporaryToken=false                      # 是否启用 STS 临时凭证

# ==================== 存储与桶策略 =====================
BucketNameFixed=zengyu-test-0405            # 固定桶名 (最高优先级)
BucketNamePrefix=bench-bucket               # 动态前缀 (若 Fixed 为空，格式为 {ak}.prefix)
BucketLocation=cn-north-4                   # 桶所在区域 (Case 101/104 必填)

# ==================== 对象属性 =====================
KeyPrefix=java-bench-test                   # 对象名前缀
ObjectSize=1048576                          # 单个对象大小 (字节)
UploadFilePath=test_data.bin                # 本地上传文件路径 (Case 216/230 必填)
EnableDetailLog=true                        # 是否开启详细请求日志记录 (detail.csv)

# ==================== 并发控制 =====================
UsersCount=1                                # 并发用户数 (对应 users.dat 行数)
ThreadsPerUser=50                           # 单一用户压测线程数
ResumableThreads=4                          # 断点续传全局共享线程池大小 (Case 230 专用，控制总并发)
RunSeconds=60                               # 压测总时长 (秒)
TestCaseCode=201                            # 压测路由指令 (详见下方列表)
```

**支持的 `TestCaseCode` 列表**：
- `101`: **创建桶** (`CreateBucket`)
- `104`: **删除桶** (`DeleteBucket`)
- `201`: **上传对象** (`PutObject`)
- `202`: **下载对象** (`GetObject`)
- `204`: **删除对象** (`DeleteObject`)
- `216`: **分段上传** (`MultipartUpload`)
- `230`: **断点续传** (`ResumableUploadFile`)
- `900`: **混合模式** (`MixMode`)

### `users.dat` (多租户凭证)

⚠️ **安全警告**：由于此处包含真实 AK/SK，该文件已被加入 `.gitignore`。**严禁提交此文件到任何公共代码仓库！**
```csv
# username, ak, sk, [sts_token]
hw_user_a, YOUR_AK_1, YOUR_SK_1
hw_user_b, YOUR_AK_2, YOUR_SK_2
```

---

## 📈 压测报告与日志 (Reporting)

每次压测都会在 `logs/task_YYYYMMDD_HHMMSS/` 目录下生成独立的任务文件夹。

**1. 简要报告 (`brief.txt`)**:
包含平均 TPS、带宽、成功率及 P99/P99.9 时延快照。

**2. 详细请求追踪 (`detail.csv`)**:
开启 `EnableDetailLog=true` 后生成，包含每个请求的：
- `BucketName/ObjectKey`: 实际操作资源。
- `LatencyMs`: 毫秒级精确耗时。
- `StatusCode`: HTTP 状态码（如 200, 204, 403, 404 等）。
- `ObsRequestId`: 用于云端故障隔离的唯一请求 ID。
