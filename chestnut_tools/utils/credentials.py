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

    This function loads a `.env` file from the given path, ensures that the user is signed in to the 
    1Password CLI (`op`), retrieves the secret reference from the specified environment variable, and 
    reads the actual secret using `op read`.

    Args:
        env_path (str): Path to the .env file containing the 1Password secret reference.
        env_var_name (str): The name of the environment variable that contains the 1Password reference 
                            (e.g., "OP_SERVICE_ACCOUNT_TOKEN" with value like "op://Vault/Item/field").

    Returns:
        str: The service account token value retrieved from 1Password.

    Raises:
        RuntimeError: If the 1Password sign-in fails or if reading the secret reference fails.
    """
    
    # load .env if it exists
    env_path = Path(env_path)
    if env_path.exists():
        load_dotenv(dotenv_path=env_path)

    # check if OP_SESSION environment variable is present
    is_signed_in = "OP_SESSION_my" in os.environ  # replace 'my' with your actual 1P shorthand if needed

    if not is_signed_in:
        print("Not signed into 1Password CLI. Launching sign-in flow...")
        try:
            subprocess.run(["op", "signin"], check=True)
        except subprocess.CalledProcessError:
            raise RuntimeError("1Password sign-in failed. Please authenticate manually and try again.")

    # build and resolve secret reference
    secret_name_reference = os.environ.get(env_var_name)
    if not secret_name_reference:
        raise RuntimeError(f"Environment variable '{env_var_name}' is not set or empty.")

    # read the token from 1Password
    try:
        token = subprocess.check_output(["op", "read", secret_name_reference], text=True).strip()
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"Failed to read 1Password secret: {e}")

    # store token in environment
    os.environ[f"{env_var_name}"] = token
    print("Service account token successfully loaded into environment.")

    return token