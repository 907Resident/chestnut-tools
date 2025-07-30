# %% import packages

from onepassword.client import Client
from dotenv import load_dotenv
from pathlib import Path
import subprocess
import os

# %%


def establish_service_acct_token_from_env(env_path: str, env_var_name: str) -> str:
    """
    Loads a service account token from 1Password using a secret reference defined in a .env file.

    If the environment variable starts with 'ops', it is used directly as the token.
    If it starts with 'op:', it is treated as a 1Password secret reference and resolved using the `op` CLI.

    Args:
        env_path (str): Path to the .env file.
        env_var_name (str): Name of the environment variable containing either the token or a 1Password reference.

    Returns:
        str: The service account token.

    Raises:
        RuntimeError: If sign-in or secret retrieval fails.
    """
    env_path = Path(env_path)
    if env_path.exists():
        load_dotenv(dotenv_path=env_path)

    # Check if OP CLI is signed in
    is_signed_in = "OP_SESSION_my" in os.environ  # adjust 'my' if needed
    if not is_signed_in:
        print("Not signed into 1Password CLI. Launching sign-in flow...")
        try:
            subprocess.run(["op", "signin"], check=True)
        except subprocess.CalledProcessError:
            raise RuntimeError("1Password sign-in failed. Please authenticate manually and try again.")

    # Fetch secret reference or token
    value = os.environ.get(env_var_name)
    if not value:
        raise RuntimeError(f"Environment variable '{env_var_name}' not found.")

    # If it’s already a token
    if value.startswith("ops"):
        token = value
        print("Service account token retrieved directly from environment variable.")
    # If it's a 1Password reference
    elif value.startswith("op:"):
        try:
            token = subprocess.check_output(["op", "read", value], text=True).strip()
            print("Service account token retrieved from 1Password.")
        except subprocess.CalledProcessError as e:
            raise RuntimeError(f"Failed to read 1Password secret: {e}")
    else:
        raise RuntimeError("Unexpected format for secret reference or token.")

    # Update the environment with the resolved token
    os.environ[f"{env_var_name}"] = token
    return token
