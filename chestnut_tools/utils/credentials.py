# %% import packages

from onepassword.client import Client

import subprocess

# %%


def establish_service_acct_token(
    acct_token_name: str = "OP_SERVICE_ACCOUNT_TOKEN",
    env_file: str = "./config/dev.env",
):
    """
    Fetches the service account token from the 1Password vault using the `op` command.

    This function executes a subprocess command to interact with 1Password's command-line tool (`op`).
    It uses the environment variable file specified by `env_file` to look for the service account token.
    If the process fails, it raises an exception with the error message returned by the subprocess.

    Args:
        acct_token_name (str, optional): The name of the environment variable containing the service account token.
            Default is "OP_SERVICE_ACCOUNT_TOKEN".
        env_file (str, optional): The path to the environment variable file. Default is "../../config/dev.env".

    Returns:
        str: The service account token as a string.

    Raises:
        Exception: If the subprocess fails, an exception is raised with the error message returned by the subprocess.

    Example:
        >>> token = establish_service_acct_token(acct_token_name="OP_SERVICE_ACCOUNT_TOKEN", env_file="../../config/dev.env")
        >>> print(token)
    """

    result = subprocess.run(
        [
            "op",
            "run",
            "--no-masking",
            f"--env-file={env_file}",
            "--",
            "printenv",
            acct_token_name,
        ],
        capture_output=True,
        text=True,
    )

    if result.returncode != 0:
        raise Exception(f"Failed to get password: {result.stderr}")

    return result.stdout.strip()
