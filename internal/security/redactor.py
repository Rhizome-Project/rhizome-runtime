"""
Secret Redaction Middleware for Rhizome.
Filters sensitive values from stdout, stderr, and structured logs.

Implements SEC-003, SEC-005, NFR-004 from TZ:
- Secrets are removed from runtime output.
- Secret patterns are matched by denylist.
- No false positives on normal text.
"""
import re
from typing import Optional

# Env var name patterns that indicate secrets
SECRET_KEY_PATTERNS = [
    re.compile(r".*_API_KEY$", re.IGNORECASE),
    re.compile(r".*_SECRET$", re.IGNORECASE),
    re.compile(r".*_TOKEN$", re.IGNORECASE),
    re.compile(r".*_PASSWORD$", re.IGNORECASE),
    re.compile(r".*_CREDENTIAL$", re.IGNORECASE),
    re.compile(r"^SECRET_.*", re.IGNORECASE),
    re.compile(r"^TOKEN_.*", re.IGNORECASE),
]

# Value patterns that look like secrets (prefix-based)
SECRET_VALUE_PREFIXES = [
    "sk-",          # OpenAI
    "sk-ant-",      # Anthropic
    "ghp_",         # GitHub PAT
    "ghu_",         # GitHub user token
    "glpat-",       # GitLab PAT
    "xoxb-",        # Slack bot
    "xoxp-",        # Slack user
    "Bearer ",      # OAuth bearer
]

REDACTED = "[REDACTED]"


class Redactor:
    """Middleware for redacting secrets from text output.

    Usage:
        redactor = Redactor()
        clean = redactor.redact(raw_stdout, secrets=["sk-abc123"])
        clean_env = redactor.redact_env({"OPENAI_API_KEY": "sk-abc123"})
    """

    def __init__(self, extra_patterns: Optional[list[str]] = None):
        """Initialize redactor.

        Args:
            extra_patterns: Additional regex patterns for secret values.
        """
        self._extra_value_patterns = []
        if extra_patterns:
            self._extra_value_patterns = [re.compile(p) for p in extra_patterns]

    def redact(self, text: str, secrets: Optional[list[str]] = None) -> str:
        """Redact known secret values from text.

        Args:
            text: Raw text (stdout, stderr, log line).
            secrets: Explicit list of secret values to redact.

        Returns:
            Text with all secret values replaced by [REDACTED].
        """
        if not text:
            return text

        result = text

        # 1. Redact explicitly provided secrets
        if secrets:
            for secret in secrets:
                if secret and len(secret) >= 4:  # Skip very short values
                    result = result.replace(secret, REDACTED)

        # 2. Redact values matching known prefixes
        for prefix in SECRET_VALUE_PREFIXES:
            # Match prefix followed by alphanumeric chars (typical token format)
            pattern = re.compile(
                re.escape(prefix) + r"[A-Za-z0-9_\-]{8,}",
                re.IGNORECASE,
            )
            result = pattern.sub(REDACTED, result)

        # 3. Apply extra patterns
        for pattern in self._extra_value_patterns:
            result = pattern.sub(REDACTED, result)

        return result

    def redact_env(self, env: dict[str, str]) -> dict[str, str]:
        """Redact secret values from an environment dict (for logging).

        Args:
            env: Environment variable dict.

        Returns:
            Copy with secret values replaced by [REDACTED].
        """
        redacted = {}
        for key, value in env.items():
            if self.is_secret_key(key):
                redacted[key] = REDACTED
            else:
                redacted[key] = value
        return redacted

    def extract_secrets_from_env(self, env: dict[str, str]) -> list[str]:
        """Extract secret values from env dict for use in text redaction.

        Args:
            env: Environment variable dict.

        Returns:
            List of secret values that should be redacted from output.
        """
        secrets = []
        for key, value in env.items():
            if self.is_secret_key(key) and value:
                secrets.append(value)
        return secrets

    @staticmethod
    def is_secret_key(key: str) -> bool:
        """Check if an env var name matches secret patterns.

        Args:
            key: Environment variable name.

        Returns:
            True if the key matches any secret pattern.
        """
        for pattern in SECRET_KEY_PATTERNS:
            if pattern.match(key):
                return True
        return False

    def redact_json(self, data: dict, keys_to_redact: Optional[set[str]] = None) -> dict:
        """Deep-redact sensitive keys in a JSON-like dict.

        Args:
            data: Dict to process.
            keys_to_redact: Set of key names to redact. If None, uses SECRET_KEY_PATTERNS.

        Returns:
            Copy with sensitive values replaced.
        """
        return self._deep_redact(data, keys_to_redact)

    def _deep_redact(self, obj, keys_to_redact: Optional[set[str]] = None):
        if isinstance(obj, dict):
            result = {}
            for k, v in obj.items():
                if keys_to_redact and k in keys_to_redact:
                    result[k] = REDACTED
                elif self.is_secret_key(k):
                    result[k] = REDACTED
                else:
                    result[k] = self._deep_redact(v, keys_to_redact)
            return result
        elif isinstance(obj, list):
            return [self._deep_redact(item, keys_to_redact) for item in obj]
        elif isinstance(obj, str):
            return self.redact(obj)
        else:
            return obj
