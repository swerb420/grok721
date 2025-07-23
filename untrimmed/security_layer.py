"""Security utility helpers (simplified)."""

from __future__ import annotations

import hashlib
import secrets
from dataclasses import dataclass


@dataclass
class EncryptedPackage:
    encrypted_data: bytes
    salt: bytes


class SecurityLayer:
    def __init__(self) -> None:
        self.master_key = secrets.token_bytes(32)

    def encrypt(self, data: str) -> EncryptedPackage:
        salt = secrets.token_bytes(16)
        digest = hashlib.pbkdf2_hmac("sha256", data.encode(), salt, 100000)
        return EncryptedPackage(digest, salt)

    def verify(self, data: str, pkg: EncryptedPackage) -> bool:
        digest = hashlib.pbkdf2_hmac("sha256", data.encode(), pkg.salt, 100000)
        return secrets.compare_digest(digest, pkg.encrypted_data)
