# %% import packages

from onepassword.client import Client
from dotenv import dotenv_values
import subprocess

# %%


import subprocess
import os


def establish_service_acct_token(
    acct_token_name: str = "OP_SERVICE_ACCOUNT_TOKEN",
    env_file: str = "./config/dev.env",
) -> str:
    """
    Loads a secret reference from a .env file, uses 1Password CLI to fetch the actual service
    account token, and runs `op run` to retrieve the value of the token environment variable.

    Args:
        acct_token_name (str): The name of the env var in the .env file that holds the 1Password secret reference.
        env_file (str): Path to the .env file containing the secret reference.

    Returns:
        str: The resolved token value as returned by `printenv` within `op run`.

    Raises:
        Exception: If the secret reference cannot be resolved or the token cannot be retrieved.
    """
    # Step 1: Load the .env file into a dictionary
    env_vars = dotenv_values(env_file)

    if acct_token_name not in env_vars:
        raise Exception(f"{acct_token_name} not found in {env_file}")

    secret_reference = env_vars[acct_token_name]

    # Step 2: Use `op read` to resolve the secret reference into a raw token
    read_result = subprocess.run(
        ["op", "read", secret_reference],
        capture_output=True,
        text=True,
    )

    if read_result.returncode != 0:
        raise Exception(f"Failed to read token from 1Password: {read_result.stderr}")

    raw_token = read_result.stdout.strip()

    # Step 3: Use `op run` to extract the environment variable using the resolved token
    env = os.environ.copy()
    env["OP_SERVICE_ACCOUNT_TOKEN"] = raw_token

    run_result = subprocess.run(
        [
            "op",
            "run",
            "--no-masking",
            "--",
            "printenv",
            acct_token_name,
        ],
        capture_output=True,
        text=True,
        env=env,
    )

    if run_result.returncode != 0:
        raise Exception(f"Failed to retrieve env variable via op run: {run_result.stderr}")

    return run_result.stdout.strip()
