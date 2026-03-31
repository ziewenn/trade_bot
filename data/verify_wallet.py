"""Quick script to verify wallet setup for Polymarket."""
from eth_account import Account

# Load from .env
import sys
sys.path.insert(0, ".")
from config.settings import Settings

settings = Settings()
private_key = settings.polymarket_private_key.get_secret_value()
funder_address = settings.polymarket_funder_address

# Derive the public address from the private key
derived_address = Account.from_key(private_key).address

print(f"Private key derives address: {derived_address}")
print(f"Funder address in .env:      {funder_address}")
print(f"Addresses match:             {derived_address.lower() == funder_address.lower()}")
print()
print("NOTE: On Polymarket, your 'funder' address should be your PROXY wallet,")
print("not necessarily the same as your EOA address derived from your private key.")
print()
print("To find your proxy wallet address:")
print("1. Go to https://polymarket.com")
print("2. Log in with this wallet")
print("3. Click your profile icon -> Settings -> Your wallet address there is your proxy")
