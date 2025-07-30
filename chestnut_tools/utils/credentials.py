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
    """
    # Load .env if it exists
    env_path = Path(env_path)
    if env_path.exists():
        load_dotenv(dotenv_path=env_path)

    # Check if OP_SESSION is active
    is_signed_in = any(var.startswith("OP_SESSION_") for var in os.environ)

    if not is_signed_in:
        print("Not signed into 1Password CLI. Launching sign-in flow...")
        try:
            subprocess.run(["op", "signin"], check=True)
        except subprocess.CalledProcessError:
            raise RuntimeError("1Password sign-in failed. Please authenticate manually and try again.")

    # Get secret reference from env
    secret_reference = os.environ.get(env_var_name)
    if not secret_reference:
        raise RuntimeError(f"Environment variable {env_var_name} not found or is empty.")

    # Read secret from 1Password
    try:
        result = subprocess.run(
            ["op", "read", secret_reference],
            capture_output=True,
            text=True,
            check=True
        )
        token = result.stdout.strip()
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"Failed to read secret from 1Password: {e.stderr}")

    print("Service account token successfully retrieved.")
    return token