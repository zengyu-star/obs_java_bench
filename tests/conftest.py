import pytest
import os
import subprocess
import shutil
import uuid
import time

@pytest.fixture(scope="session")
def project_root():
    return os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

@pytest.fixture(scope="function")
def test_id():
    return str(uuid.uuid4())[:8]

@pytest.fixture(scope="function")
def run_dir(project_root, test_id):
    path = os.path.join(project_root, "tests", "run_" + test_id)
    os.makedirs(path, exist_ok=True)
    yield path
    # Teardown: Delete local files
    print(f"[DEBUG] Test run dir: {path}")
    # if os.path.exists(path):
    #     shutil.rmtree(path)

@pytest.fixture(scope="function")
def cleanup_registry():
    """Registry to track objects/buckets created during tests for remote cleanup."""
    registry = {"buckets": [], "objects": []}
    yield registry
    # Remote Teardown should be handled by specific tests or a global purge
    # For black-box testing, we will mostly rely on the tool's 104/204 codes in the test body.

def run_bench(project_root, config_path, users_path):
    """Helper to run the benchmark tool via shaded JAR."""
    with open(config_path, "r") as f:
        print(f"[DEBUG] Content of {config_path}:\n{f.read()}")
    jar_path = os.path.join(project_root, "target", "obs_java_bench-1.0.0-SNAPSHOT.jar")
    cmd = [
        "java", "-jar", jar_path,
        config_path, users_path
    ]
    return subprocess.run(cmd, cwd=project_root, capture_output=True, text=True)

@pytest.fixture(scope="function")
def base_config_params():
    return {
        "ObjNamePatternHash": "false",
        "EnableDataValidation": "false",
        "Endpoint": "obs.cn-north-4.myhuaweicloud.com",
        "Protocol": "https",
        "IsTemporaryToken": "false",
        "MaxConnections": "100",
        "UsersCount": "2",
        "ThreadsPerUser": "1",
        "RunSeconds": "5",
        "RequestsPerThread": "1",
        "TestCaseCode": "201",
        "BucketNamePrefix": "pytest-bench-",
        "BucketLocation": "cn-north-4",
        "KeyPrefix": "test-obj-",
        "ObjectSize": "1024",
        "LogLevel": "INFO"
    }

def write_config(path, params):
    with open(path, "w") as f:
        for k, v in params.items():
            f.write(f"{k}={v}\n")

def write_users(path, users):
    with open(path, "w") as f:
        for u in users:
            f.write(",".join(u) + "\n")

def load_fixed_users():
    users = []
    # Try to find users.dat_fix_ak_sk in the project root
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    path = os.path.join(project_root, "users.dat_fix_ak_sk")
    if os.path.exists(path):
        with open(path, "r") as f:
            for line in f:
                line = line.strip()
                if line and "," in line and not line.startswith("#"):
                    users.append(line.split(","))
    return users

USER_FIX = load_fixed_users()
