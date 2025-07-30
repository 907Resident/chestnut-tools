# %% import packages

from onepassword.client import Client

import subprocess

# %%


import subprocess
import os


def establish_service_acct_token(
    secret_reference: str = "op://MyVault/Service Account Token/token",
    acct_token_name: str = "OP_SERVICE_ACCOUNT_TOKEN",
):
    """
    Fetches the service account token from a 1Password secret reference using `op read`,
    then uses `op run` with the raw token set in memory to fetch the target environment variable.

    Args:
        secret_reference (str): The 1Password secret reference to the service account token.
        acct_token_name (str): The name of the environment variable to retrieve from `op run`.

    Returns:
        str: The resolved value of the requested environment variable.

    Raises:
        Exception: If the token cannot be fetched or `op run` fails.
    """

    # Step 1: Use `op read` to fetch the raw service account token
    token_result = subprocess.run(
        ["op", "read", secret_reference],
        capture_output=True,
        text=True,
    )

    if token_result.returncode != 0:
        raise Exception(f"Failed to read service account token: {token_result.stderr}")

    raw_token = token_result.stdout.strip()

    # Step 2: Use `op run` with the token injected into the environment
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
        raise Exception(f"Failed to get env variable from op run: {run_result.stderr}")

    return run_result.stdout.strip()

