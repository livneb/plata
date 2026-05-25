"""Generate a VAPID (Web Push) key pair for Plata.

    python scripts/generate_vapid.py

Prints the pair in the URL-safe base64 form pywebpush expects, plus a ready-to-paste
Railway CLI command. The output is plain text — copy + run; the keys never leave your machine.
"""
from __future__ import annotations

import argparse
import base64

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ec


def _urlb64(b: bytes) -> str:
    return base64.urlsafe_b64encode(b).rstrip(b"=").decode()


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate a VAPID key pair for Plata.")
    parser.add_argument("--subject", default="mailto:admin@example.com",
                        help="VAPID 'sub' claim (mailto: or https:// URL identifying the app contact).")
    parser.add_argument("--service", default="ingestion_hub",
                        help="Railway service name to set the variables on.")
    args = parser.parse_args()

    priv = ec.generate_private_key(ec.SECP256R1())
    priv_b = priv.private_numbers().private_value.to_bytes(32, "big")
    pub_b = priv.public_key().public_bytes(
        encoding=serialization.Encoding.X962,
        format=serialization.PublicFormat.UncompressedPoint,
    )
    priv_b64 = _urlb64(priv_b)
    pub_b64 = _urlb64(pub_b)

    print()
    print("=== VAPID key pair ===")
    print(f"VAPID_PUBLIC_KEY={pub_b64}")
    print(f"VAPID_PRIVATE_KEY={priv_b64}")
    print(f"VAPID_SUBJECT={args.subject}")
    print()
    print("=== Railway CLI command (run on your machine) ===")
    print(f"railway service {args.service}")
    print(
        f'railway variables --set "VAPID_PUBLIC_KEY={pub_b64}" '
        f'--set "VAPID_PRIVATE_KEY={priv_b64}" '
        f'--set "VAPID_SUBJECT={args.subject}"'
    )
    print()


if __name__ == "__main__":
    main()
