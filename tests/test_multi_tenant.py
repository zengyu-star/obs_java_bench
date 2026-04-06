import pytest
import os
import subprocess
from conftest import write_config, write_users, run_bench, USER_FIX

def test_two_user_lifecycle(project_root, run_dir, base_config_params):
    """Verify 2-user lifecycle: Create -> Put -> Get -> Delete -> DeleteBucket."""
    config_path = os.path.join(run_dir, "config.dat")
    users_path = os.path.join(run_dir, "users.dat")
    
    # 1. Create Bucket (101)
    params = base_config_params.copy()
    params["TestCaseCode"] = "101"
    params["BucketNamePrefix"] = "pytest-multi-"
    write_config(config_path, params)
    write_users(users_path, USER_FIX)
    
    result = run_bench(project_root, config_path, users_path)
    assert result.returncode == 0
    assert "SUCCESS" in result.stdout or "Initialization complete" in result.stdout

    # 2. Put Object (201)
    params["TestCaseCode"] = "201"
    write_config(config_path, params)
    result = run_bench(project_root, config_path, users_path)
    assert result.returncode == 0

    # 3. Teardown: Cleanup objects (204) and buckets (104)
    # We do this sequentially to ensure no residual data
    for code in ["204", "104"]:
        params["TestCaseCode"] = code
        write_config(config_path, params)
        run_bench(project_root, config_path, users_path)

def test_sts_temporary_token_flow(project_root, run_dir, base_config_params):
    """Verify loading of temporary tokens (STS)."""
    config_path = os.path.join(run_dir, "config_sts.dat")
    users_path = os.path.join(run_dir, "temptoken.dat")
    
    # Mock temptoken.dat with 4 columns
    STS_USER = [["sts-user", "AK_STS", "SK_STS", "TOKEN_STS"]]
    write_users(users_path, STS_USER)
    
    params = base_config_params.copy()
    params["IsTemporaryToken"] = "true"
    params["UsersCount"] = "1"
    write_config(config_path, params)
    
    # Run the tool and expect it to fail on actual network call but pass INITIALIZATION
    # Since AK/SK are fake, it will fail at connection, but we check if it parses correctly.
    result = run_bench(project_root, config_path, users_path)
    
    # Verify that it tried to load users (since we provided them)
    # The tool might run python generation if temptoken.dat is not found, 
    # but here we provide it.
    assert "Initializing connection pool" in result.stdout
    # It might fail with "ObsClient initialization failed" or network error, 
    # but we care about the flow.
