#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import shutil
import time
import subprocess
import re

# ===========================================
# Configuration
# ===========================================
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
CONFIG_FILE = os.path.join(PROJECT_ROOT, "config.dat")
USERS_FILE = os.path.join(PROJECT_ROOT, "users.dat")
CONFIG_BAK = os.path.join(PROJECT_ROOT, "config.dat.bak")
USERS_BAK = os.path.join(PROJECT_ROOT, "users.dat.bak")
TEST_DATA_FILE = os.path.join(PROJECT_ROOT, "test_data.bin")
TEST_DATA_SIZE_MB = 10 # 10MB test data for smoke test
CACHE_DIR = os.path.join(PROJECT_ROOT, ".smoke_test_cache")

# Test scenarios (Mock, Standard, etc.)
TEST_CASES = [101, 201, 202, 204, 216, 230, 900]
TEST_DURATION = 3 # Duration per case (seconds)

# Build & Test Tasks: (Name, Command, JAR Path, IsMock, IsSTS, UserFile)
TASKS = [
    ("Mock",     "mvn clean package -DskipTests", "target/obs_java_bench-1.0.0-SNAPSHOT.jar", True,  False, USERS_FILE),
    ("Standard", "%SAME%",                         "target/obs_java_bench-1.0.0-SNAPSHOT.jar", False, False, USERS_FILE),
    ("STS",      "%SAME%",                         "target/obs_java_bench-1.0.0-SNAPSHOT.jar", False, True,  "users.dat_tmp")
]
# ===========================================

class JavaBenchmarkTester:
    def __init__(self):
        self.results = []

    def run_cmd(self, command):
        """Execute shell command and return (exit_code, output)"""
        try:
            # Handle %SAME% macro
            if "%SAME%" in command:
                for i in range(len(TASKS)):
                    if TASKS[i][1] != "%SAME%":
                        command = command.replace("%SAME%", TASKS[i][1])
                        break
            
            result = subprocess.run(
                command,
                shell=True,
                capture_output=True,
                text=True,
                cwd=PROJECT_ROOT
            )
            if result.returncode == 0:
                return 0, result.stdout
            else:
                return result.returncode, result.stderr
        except Exception as e:
            return -1, str(e)

    def prepare_env(self):
        print("[Init] Preparing environment...")
        
        # 1. Create cache directory
        if os.path.exists(CACHE_DIR):
            shutil.rmtree(CACHE_DIR)
        os.makedirs(CACHE_DIR)

        # 2. Backup Config and Users
        has_config = os.path.exists(CONFIG_FILE)
        if has_config:
            shutil.copy(CONFIG_FILE, CONFIG_BAK)
        
        # 3. Create dummy base if no config found
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

        # 4. Inject mandatory fields to pass ConfigValidator
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
            ret, _ = self.run_cmd(f"grep -q '^{key}=' {CONFIG_FILE}")
            if ret != 0:
                with open(CONFIG_FILE, 'a') as f:
                    f.write(f"\n{key}={val}\n")
        
        if os.path.exists(USERS_FILE):
            shutil.copy(USERS_FILE, USERS_BAK)
        else:
            # Create Dummy Users
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
        """Parse Success/Failed counts from Java tool output"""
        stats = {"success": 0, "failed": 0}
        m_success = re.search(r"Success:\s+(\d+)", output)
        if m_success: stats["success"] = int(m_success.group(1))
        m_failed = re.search(r"Failed:\s+(\d+)", output)
        if m_failed: stats["failed"] = int(m_failed.group(1))
        return stats

    def compile(self):
        """Build JAR artifact (Cached after first build)"""
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
        
        # Cache JAR artifact
        dst_jar = os.path.join(CACHE_DIR, "obs_java_bench.jar")
        shutil.copy(jar_path, dst_jar)
        print(f" PASS ({duration:.1f}s) -> JAR Cached")

        # Generate test data via tool 'gen' command
        print(f"[Init] Generating {TEST_DATA_SIZE_MB}MB test data via tool...")
        ret, _ = self.run_cmd(f"java -jar {dst_jar} gen {TEST_DATA_SIZE_MB}")
        if ret != 0 or not os.path.exists(TEST_DATA_FILE):
             print(f"[Warn] Failed to generate {TEST_DATA_FILE} via tool, fallback to dd...")
             self.run_cmd(f"dd if=/dev/urandom of={TEST_DATA_FILE} bs=1M count={TEST_DATA_SIZE_MB} status=none")

        return True

    def run_tests(self):
        print("\n" + "=" * 60)
        print(">>> Smoke Testing (Mock vs Standard vs STS)")
        print("=" * 60)
        
        jar_path = os.path.join(CACHE_DIR, "obs_java_bench.jar")
        
        # 101 (Create) -> Core Ops -> 104 (Delete)
        CORE_TESTS = [201, 202, 204, 216, 230, 900]
        
        for name, _, _, is_mock, is_sts, user_file in TASKS:
            print(f"\n--- Testing Scenario: {name} (IsMock={is_mock}, IsSTS={is_sts}, UserFile={user_file}) ---")
            
            # Use timestamped bucket name for isolation
            timestamp = int(time.time())
            fixed_bucket = f"bench-smoke-{timestamp}"
            
            # Dynamically modify config for the current scenario
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

            # Strict order: 101 -> CORE -> 104
            order = [101] + CORE_TESTS + [104]
            
            for case in order:
                print(f"  Case {case:<3} ... ", end='', flush=True)
                
                start_t = time.time()
                # Execution command: java -jar app.jar [config] [users] [TestCaseCode]
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
