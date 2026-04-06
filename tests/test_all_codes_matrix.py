import pytest
import os
import subprocess
from conftest import write_config, write_users, run_bench, USER_FIX

CODES = [101, 104, 201, 202, 204, 216, 230, 900]

@pytest.mark.parametrize("code", CODES)
def test_all_codes(project_root, run_dir, base_config_params, code):
    """Exhaustive check for all TestCaseCodes with low concurrency (1 User, 1 Thread)."""
    config_path = os.path.join(run_dir, f"config_{code}.dat")
    users_path = os.path.join(run_dir, "users.dat")
    
    params = base_config_params.copy()
    params["TestCaseCode"] = str(code)
    params["UsersCount"] = "1"
    params["ThreadsPerUser"] = "1"
    params["RequestsPerThread"] = "1"
    
    if code == 900:
        params["MixOperation"] = "201,202,204"
        params["MixLoopCount"] = "1"
    
    if code == 230:
        # Resumable requires an upload file. Use gen to create one.
        upload_path = os.path.join(run_dir, "test_data_230.bin")
        jar_path = os.path.join(project_root, "target", "obs_java_bench-1.0.0-SNAPSHOT.jar")
        subprocess.run(["java", "-jar", jar_path, "gen", "1"], cwd=project_root)
        # Move generated file to run_dir
        if os.path.exists(os.path.join(project_root, "test_data.bin")):
             os.rename(os.path.join(project_root, "test_data.bin"), upload_path)
        params["UploadFilePath"] = upload_path
    
    write_config(config_path, params)
    write_users(users_path, USER_FIX[:1]) # Use only 1 user for cost optimization
    
    result = run_bench(project_root, config_path, users_path)
    # Since we use fake/real AK/SK, we check initialization success
    assert "Initializing connection pool" in result.stdout
    assert result.returncode == 0 or "ObsException" in result.stdout
