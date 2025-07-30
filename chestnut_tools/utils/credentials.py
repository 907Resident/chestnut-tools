# %% import packages

from onepassword.client import Client
from dotenv import dotenv_values
from pathlib import Path
import subprocess
import os

# %%


def establish_service_acct_token_from_env(env_path: str, secret_name: str) -> str:
    # Load .env if it exists
    env_path = Path(env_path)
    if env_path.exists():
        load_dotenv(dotenv_path=env_path)

    # Check if OP_SESSION environment variable is present
    is_signed_in = "OP_SESSION_my" in os.environ  # replace 'my' with your actual 1P shorthand if needed

    if not is_signed_in:
        print("Not signed into 1Password CLI. Launching sign-in flow...")
        try:
            subprocess.run(["op", "signin"], check=True)
        except subprocess.CalledProcessError:
            raise RuntimeError("1Password sign-in failed. Please authenticate manually and try again.")

    # Now build and resolve secret reference
    reference = f"op://MyVault/{secret_name}/token"  # you can also make vault name a parameter
    try:
        token = subprocess.check_output(["op", "read", reference], text=True).strip()
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"Failed to read 1Password secret: {e}")
    
    os.environ["SERVICE_ACCOUNT_TOKEN"] = token
    print("Service account token successfully loaded into environment.")
    return token
