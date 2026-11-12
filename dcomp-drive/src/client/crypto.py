import os
import hashlib
from cryptography.hazmat.primitives.ciphers.aead import AESGCM


class CryptoManager:
    def __init__(self, key_hex: str | None = None):
        """
        Recebe uma chave hexadecimal de 64 caracteres (32 bytes = 256 bits).
        Se for None, opera sem criptografia (apenas calcula/verifica o SHA-256).
        """
        self.enabled = key_hex is not None
        if self.enabled:
            key_bytes = bytes.fromhex(key_hex)
            if len(key_bytes) != 32:
                raise ValueError("A chave AES-256 deve ter exatamente 32 bytes (64 caracteres hex).")
            self.aesgcm = AESGCM(key_bytes)

    def process_outgoing_block(self, raw_data: bytes) -> tuple[bytes, str]:
        """Cifra o bloco (se ativo) e calcula o hash SHA-256 para envio."""
        if self.enabled:
            nonce = os.urandom(12)
            ciphertext = self.aesgcm.encrypt(nonce, raw_data, None)
            payload = nonce + ciphertext
        else:
            payload = raw_data

        sha256_hash = hashlib.sha256(payload).hexdigest()
        return payload, sha256_hash

    def process_incoming_block(self, payload: bytes, expected_hash: str) -> bytes:
        """Verifica o SHA-256 do bloco recebido da rede e o decifra."""
        calculated_hash = hashlib.sha256(payload).hexdigest()
        if calculated_hash != expected_hash:
            raise ValueError(
                f"Integridade do bloco falhou! Esperado: {expected_hash}, Recebido: {calculated_hash}"
            )

        if self.enabled:
            if len(payload) < 28:
                raise ValueError("Payload cifrado inválido ou truncado.")
            nonce = payload[:12]
            ciphertext = payload[12:]
            return self.aesgcm.decrypt(nonce, ciphertext, None)

        return payload
