"""
Tests for Secret Redaction (SEC-003, NFR-004, AT-008).
Validates that secrets are properly removed from output.
"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from internal.security.redactor import Redactor, REDACTED


def test_redact_explicit_secrets():
    """Inject known secret -> verify replaced."""
    r = Redactor()
    text = "Using API key: sk-abc123456789012345 for request"
    result = r.redact(text, secrets=["sk-abc123456789012345"])
    assert "sk-abc123456789012345" not in result
    assert REDACTED in result
    print("[PASS] Explicit secret redacted")


def test_redact_openai_prefix():
    """OpenAI-style key detected by prefix pattern."""
    r = Redactor()
    text = "Token: sk-proj-abcdef123456789 end"
    result = r.redact(text)
    assert "sk-proj-abcdef123456789" not in result
    assert REDACTED in result
    print("[PASS] OpenAI prefix pattern redacted")


def test_redact_anthropic_prefix():
    """Anthropic-style key detected by prefix pattern."""
    r = Redactor()
    text = "Authorization: sk-ant-api03-xyzABC123 done"
    result = r.redact(text)
    assert "sk-ant-api03-xyzABC123" not in result
    assert REDACTED in result
    print("[PASS] Anthropic prefix pattern redacted")


def test_redact_github_pat():
    """GitHub PAT detected by prefix pattern."""
    r = Redactor()
    text = "git clone https://ghp_1234567890abcdef1234567890abcdef12345678@github.com/repo"
    result = r.redact(text)
    assert "ghp_1234567890abcdef1234567890abcdef12345678" not in result
    assert REDACTED in result
    print("[PASS] GitHub PAT redacted")


def test_no_false_positives():
    """Normal text is not redacted."""
    r = Redactor()
    text = "This is a normal log line with no secrets. Counter: 42. Status: OK."
    result = r.redact(text)
    assert result == text
    print("[PASS] No false positives")


def test_redact_env():
    """Environment dict redaction."""
    r = Redactor()
    env = {
        "OPENAI_API_KEY": "sk-real-key-here123456",
        "PATH": "/usr/bin:/usr/local/bin",
        "MY_SECRET": "super-secret-value",
        "DATABASE_PASSWORD": "db-pass-123",
        "NORMAL_VAR": "hello",
    }
    redacted = r.redact_env(env)
    assert redacted["OPENAI_API_KEY"] == REDACTED
    assert redacted["MY_SECRET"] == REDACTED
    assert redacted["DATABASE_PASSWORD"] == REDACTED
    assert redacted["PATH"] == "/usr/bin:/usr/local/bin"
    assert redacted["NORMAL_VAR"] == "hello"
    print("[PASS] Env dict redacted correctly")


def test_extract_secrets_from_env():
    """Extract secret values from env for text redaction."""
    r = Redactor()
    env = {
        "OPENAI_API_KEY": "sk-real-key-12345678",
        "PATH": "/usr/bin",
        "ANTHROPIC_API_KEY": "sk-ant-key-abc123",
    }
    secrets = r.extract_secrets_from_env(env)
    assert "sk-real-key-12345678" in secrets
    assert "sk-ant-key-abc123" in secrets
    assert "/usr/bin" not in secrets
    print("[PASS] Secrets extracted from env")


def test_is_secret_key():
    """Validate secret key detection."""
    assert Redactor.is_secret_key("OPENAI_API_KEY") is True
    assert Redactor.is_secret_key("MY_SECRET") is True
    assert Redactor.is_secret_key("AUTH_TOKEN") is True
    assert Redactor.is_secret_key("DATABASE_PASSWORD") is True
    assert Redactor.is_secret_key("SECRET_SAUCE") is True
    assert Redactor.is_secret_key("PATH") is False
    assert Redactor.is_secret_key("HOME") is False
    assert Redactor.is_secret_key("LOG_LEVEL") is False
    print("[PASS] Secret key detection works")


def test_redact_json_deep():
    """Deep redaction in nested JSON structure."""
    r = Redactor()
    data = {
        "config": {
            "OPENAI_API_KEY": "sk-nested-key-12345678",
            "timeout": 30,
        },
        "list_val": ["normal", "sk-in-list-12345678abcd"],
        "flat_token": "Bearer ghp_tokenvalue123456789012345678901234",
    }
    result = r.redact_json(data)
    assert result["config"]["OPENAI_API_KEY"] == REDACTED
    assert result["config"]["timeout"] == 30
    assert "sk-nested-key-12345678" not in str(result)
    assert "ghp_tokenvalue123456789012345678901234" not in str(result)
    print("[PASS] Deep JSON redaction works")


def test_partial_match_in_string():
    """Secret embedded in a larger string is still caught."""
    r = Redactor()
    secret = "unit-test-redaction-secret-abc123"
    text = f'Error connecting with key="{secret}" to API'
    result = r.redact(text, secrets=[secret])
    assert secret not in result
    print("[PASS] Partial match in string redacted")


def main():
    print("\n  Redactor Test Suite\n")
    tests = [
        test_redact_explicit_secrets,
        test_redact_openai_prefix,
        test_redact_anthropic_prefix,
        test_redact_github_pat,
        test_no_false_positives,
        test_redact_env,
        test_extract_secrets_from_env,
        test_is_secret_key,
        test_redact_json_deep,
        test_partial_match_in_string,
    ]

    passed = 0
    failed = 0
    for test in tests:
        try:
            test()
            passed += 1
        except AssertionError as e:
            print(f"[FAIL] {test.__name__}: {e}")
            failed += 1
        except Exception as e:
            print(f"[ERROR] {test.__name__}: {e}")
            failed += 1

    print(f"\n  {passed}/{passed + failed} tests passed")
    sys.exit(0 if failed == 0 else 1)


if __name__ == "__main__":
    main()
