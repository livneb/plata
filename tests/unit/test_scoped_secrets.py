import pytest
from pydantic import SecretStr

from plata.config.secrets import ScopedSecrets, SecretAccessError
from plata.config.settings import Settings


def _settings() -> Settings:
    return Settings(
        bybit_api_key=SecretStr("KEY"),
        bybit_api_secret=SecretStr("SECRET"),
        openrouter_api_key=SecretStr("OR"),
    )


def test_executor_can_read_bybit():
    s = ScopedSecrets(agent="executor", _settings=_settings())
    assert s.reveal("bybit_api_key") == "KEY"


def test_strategist_cannot_read_bybit():
    s = ScopedSecrets(agent="strategist", _settings=_settings())
    with pytest.raises(SecretAccessError):
        s.reveal("bybit_api_key")


def test_unknown_agent_denied():
    s = ScopedSecrets(agent="totally_made_up", _settings=_settings())
    with pytest.raises(SecretAccessError):
        s.reveal("openrouter_api_key")


def test_unknown_key_denied_even_for_allowed_agent():
    s = ScopedSecrets(agent="executor", _settings=_settings())
    with pytest.raises(SecretAccessError):
        s.reveal("telegram_bot_token")
