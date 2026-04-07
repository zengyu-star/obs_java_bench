#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import subprocess
import os
import shutil
import time
import sys
import re

# ================= 配置区域 =================
CONFIG_FILE = 'config.dat'
CONFIG_BAK = 'config.dat.bak'
USERS_FILE = 'users.dat'
USERS_BAK = 'users.dat.bak'
CACHE_DIR = './test_bin_cache'  # 存放编译产物的临时目录
TEST_DATA_FILE = 'test_data.bin' # 本地测试数据文件
TEST_DATA_SIZE_MB = 10           # 测试数据大小 (MB)

# 测试用例 ID (基于 README.md 支持列表)
TEST_CASES = [101, 201, 202, 204, 216, 230, 900]
TEST_DURATION = 3 # 冒烟测试运行时间 (秒)

# 编译与测试任务: (显示名称, 构建命令, 产物路径, 是否 Mock 模式, 是否 STS 模式, 用户文件)
# Java 只有一个产物，但我们通过配置切换模式路径进行验证
TASKS = [
    ("Mock",     "mvn clean package -DskipTests", "target/obs_java_bench-1.0.0-SNAPSHOT.jar", True,  False, USERS_FILE),
    ("Standard", "%SAME%",                         "target/obs_java_bench-1.0.0-SNAPSHOT.jar", False, False, USERS_FILE),
    ("STS",      "%SAME%",                         "target/obs_java_bench-1.0.0-SNAPSHOT.jar", False, True,  "users.dat_tmp")
]
# ===========================================

class JavaBenchmarkTester:
    def __init__(self):
        self.results = []
        self.work_dir = os.path.dirname(os.path.abspath(__file__))
        os.chdir(self.work_dir)
        self.env = os.environ.copy()

    def run_cmd(self, cmd, capture=True):
        """执行命令并返回 (returncode, stdout+stderr)"""
        try:
            if capture:
                result = subprocess.run(
                    cmd, shell=True, capture_output=True, text=True, env=self.env
                )
                return result.returncode, result.stdout + result.stderr
            else:
                result = subprocess.run(cmd, shell=True, env=self.env)
                return result.returncode, ""
        except Exception as e:
            return -1, str(e)

    def prepare_env(self):
        print("[Init] Preparing environment...")
        
        # 1. 创建缓存目录
        if os.path.exists(CACHE_DIR):
            shutil.rmtree(CACHE_DIR)
        os.makedirs(CACHE_DIR)

        # 2. 备份 Config 和 Users
        has_config = os.path.exists(CONFIG_FILE)
        if has_config:
            shutil.copy(CONFIG_FILE, CONFIG_BAK)
        
        # 3. 如果没有配置，创建一个基础模板
        if not has_config:
            print("[Init] No config.dat found, creating dummy base.")
            with open(CONFIG_FILE, 'w') as f:
                f.write("Endpoint=obs.example.com\n")
                f.write("Protocol=http\n")
                f.write("IsTemporaryToken=false\n")
                f.write("BucketLocation=cn-north-4\n")
                f.write("BucketNamePrefix=bench-smoke\n")
                f.write("KeyPrefix=smoke-test-object\n")
                f.write("ObjectSize=1048576\n")
                f.write("PartSize=5242880\n")
                f.write("PartsForEachUploadID=5\n")
                f.write("MixOperation=201,202,204\n")
                f.write("MixLoopCount=10\n")
                f.write("LogLevel=INFO\n")

        # 4. 强制填充一些冒烟测试必选但用户可能没写的字段 (使用 sed 幂等处理)
        mandatory_fields = {
            "UsersCount": "1",
            "ThreadsPerUser": "1",
            "RequestsPerThread": "1",
            "EnableDetailLog": "false",
            "EnableDataValidation": "true",
            "ObjNamePatternHash": "false",
            "IsTemporaryToken": "false",
            "MockErrorRate": "0"
        }
        for key, val in mandatory_fields.items():
            # 如果不存在该配置项，则追加；如果存在，后续 run_tests 会用 sed 修改
            ret, _ = self.run_cmd(f"grep -q '^{key}=' {CONFIG_FILE}")
            if ret != 0:
                with open(CONFIG_FILE, 'a') as f:
                    f.write(f"\n{key}={val}\n")
        
        if os.path.exists(USERS_FILE):
            shutil.copy(USERS_FILE, USERS_BAK)
        else:
            # 创建 Dummy Users
            print("[Init] No users.dat found, creating dummy user.")
            with open(USERS_FILE, 'w') as f:
                f.write("test_user,AK_TEST,SK_TEST\n")

    def restore_env(self):
        print("[Cleanup] Restoring environment...")
        if os.path.exists(CONFIG_BAK):
            shutil.move(CONFIG_BAK, CONFIG_FILE)
        if os.path.exists(USERS_BAK):
            shutil.move(USERS_BAK, USERS_FILE)
        if os.path.exists(CACHE_DIR):
            shutil.rmtree(CACHE_DIR)

    def parse_stats(self, output):
        """解析 Java 工具输出"""
        stats = {"success": 0, "failed": 0}
        m_success = re.search(r"Success:\s+(\d+)", output)
        if m_success: stats["success"] = int(m_success.group(1))
        m_failed = re.search(r"Failed:\s+(\d+)", output)
        if m_failed: stats["failed"] = int(m_failed.group(1))
        return stats

    def compile(self):
        """编译单一 JAR 产物 (只执行一次即可)"""
        name, cmd, jar_path, *others = TASKS[0]
        print(f"\n>>> Compiling Build: {name} ...", end='', flush=True)
        start_t = time.time()
        ret, output = self.run_cmd(cmd)
        duration = time.time() - start_t
        
        if ret != 0:
            print(f" FAIL! ({duration:.1f}s)")
            print(f"Error Log:\n{output[-1000:]}") 
            return False
        
        if not os.path.exists(jar_path):
            print(f" FAIL! (JAR {jar_path} not found)")
            return False
        
        # 缓存 JAR
        dst_jar = os.path.join(CACHE_DIR, "obs_java_bench.jar")
        shutil.copy(jar_path, dst_jar)
        print(f" PASS ({duration:.1f}s) -> JAR Cached")

        # [新增] 运行一次 gen 生成测试数据文件
        print(f"[Init] Generating {TEST_DATA_SIZE_MB}MB test data via tool...")
        ret, _ = self.run_cmd(f"java -jar {dst_jar} gen {TEST_DATA_SIZE_MB}")
        if ret != 0 or not os.path.exists(TEST_DATA_FILE):
             print(f"[Warn] Failed to generate {TEST_DATA_FILE} via tool, fallback to dd...")
             self.run_cmd(f"dd if=/dev/urandom of={TEST_DATA_FILE} bs=1M count={TEST_DATA_SIZE_MB} status=none")

        return True

    def run_tests(self):
        print("\n" + "=" * 60)
        print(">>> Smoke Testing (Mock vs Standard)")
        print("=" * 60)
        
        jar_path = os.path.join(CACHE_DIR, "obs_java_bench.jar")
        
        # 101 (Create) -> Others -> 104 (Delete)
        CORE_TESTS = [201, 202, 204, 216, 230, 900]
        
        for name, _, _, is_mock, is_sts, user_file in TASKS:
            print(f"\n--- Testing Scenario: {name} (IsMock={is_mock}, IsSTS={is_sts}, UserFile={user_file}) ---")
            
            # 生成带时间戳的固定桶名
            timestamp = int(time.time())
            fixed_bucket = f"bench-smoke-{timestamp}"
            
            # 动态修改配置文件
            mock_val = "true" if is_mock else "false"
            sts_val = "true" if is_sts else "false"
            sed_cmds = [
                f"sed -i 's/^IsMockMode=.*/IsMockMode={mock_val}/g' {CONFIG_FILE}",
                f"sed -i 's/^IsTemporaryToken=.*/IsTemporaryToken={sts_val}/g' {CONFIG_FILE}",
                f"sed -i 's/^RunSeconds=.*/RunSeconds={TEST_DURATION}/g' {CONFIG_FILE}",
                f"sed -i 's/^MockErrorRate=.*/MockErrorRate=0/g' {CONFIG_FILE}",
                f"sed -i 's/^BucketNameFixed=.*/BucketNameFixed={fixed_bucket}/g' {CONFIG_FILE}",
                f"sed -i 's/^MixLoopCount=.*/MixLoopCount=10/g' {CONFIG_FILE}",
                f"sed -i 's/^MixOperation=.*/MixOperation=201,202,204/g' {CONFIG_FILE}",
                f"sed -i 's|^UploadFilePath=.*|UploadFilePath={TEST_DATA_FILE}|g' {CONFIG_FILE}",
                f"sed -i 's/^EnableDetailLog=.*/EnableDetailLog=false/g' {CONFIG_FILE}"
            ]
            for cmd in sed_cmds:
                self.run_cmd(cmd)

            # 严格执行逻辑: 101 -> CORE -> 104
            order = [101] + CORE_TESTS + [104]
            
            for case in order:
                print(f"  Case {case:<3} ... ", end='', flush=True)
                
                start_t = time.time()
                # 命令格式: java -jar app.jar [config] [users] [TestCaseCode]
                ret, output = self.run_cmd(f"java -jar {jar_path} {CONFIG_FILE} {user_file} {case}")
                duration = time.time() - start_t
                
                stats = self.parse_stats(output)
                status = "PASS"
                detail = ""

                if ret != 0:
                    status = "FAIL"
                    detail = f"Crash(Exit {ret})"
                elif stats['failed'] > 0:
                    status = "FAIL"
                    detail = f"Business Fail ({stats['failed']} errs)"
                elif stats['success'] == 0:
                    status = "FAIL"
                    detail = "0 Success"

                print(f"{status} (Succ:{stats['success']}, Fail:{stats['failed']}, {duration:.1f}s)")
                
                self.results.append({
                    "Build": name,
                    "Case": case,
                    "Status": status,
                    "Detail": detail
                })

    def print_summary(self):
        print("\n" + "=" * 60)
        print(f"{'SCENARIO':<12} | {'CASE':<6} | {'STATUS':<10} | {'DETAIL'}")
        print("-" * 60)
        
        pass_count = 0
        total_count = 0
        failed_tests = []

        for r in self.results:
            total_count += 1
            if r['Status'] == 'PASS':
                pass_count += 1
            else:
                failed_tests.append(r)
            
            print(f"{r['Build']:<12} | {r['Case']:<6} | {r['Status']:<10} | {r['Detail']}")
            
        print("-" * 60)
        print(f"Summary: {pass_count}/{total_count} Passed")
        
        if failed_tests:
            print("\nFAILED TESTS FOUND!")
            sys.exit(1)
        else:
            print("\nALL SMOKE TESTS COMPLETED SUCCESSFULLY.")
            sys.exit(0)

    def run(self):
        try:
            self.prepare_env()
            if not self.compile():
                sys.exit(1)
            self.run_tests()
            self.print_summary()
        except KeyboardInterrupt:
            print("\nInterrupted.")
        finally:
            self.restore_env()

if __name__ == "__main__":
    JavaBenchmarkTester().run()
