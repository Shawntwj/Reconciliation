import pytest
import pandas as pd
import logging
import os
from unittest.mock import MagicMock, patch
from src.alerts import AlertManager
from src.email_alerts import EmailAlertSender

# --- FIXTURES ---

@pytest.fixture
def sample_critical_df():
    """Mock data representing critical breaks"""
    return pd.DataFrame([
        {
            'contract_id': 'T001',
            'counterparty': 'BP',
            'status': 'MISSING IN BANK',
            'amount_diff': 500.00
        },
        {
            'contract_id': 'T002',
            'counterparty': 'SHELL',
            'status': 'DISCREPANCY',
            'amount_diff': 150.00
        }
    ])

# --- ALERT MANAGER TESTS ---

class TestAlertManager:
    def test_alert_manager_empty_df(self, caplog):
        """Verify behavior when no alerts exist"""
        manager = AlertManager()
        with caplog.at_level(logging.INFO):
            manager.send_alerts(pd.DataFrame())
        assert "No critical alerts" in caplog.text

    def test_business_context_logic(self, caplog):
        """Verify the 'Risk' labels match the status"""
        manager = AlertManager()
        
        # Test Leakage
        row_leak = pd.Series({'status': 'MISSING IN BANK', 'amount_diff': 100})
        manager._add_business_context(row_leak)
        assert "Revenue leakage" in caplog.text
        
        # Test Overpayment
        row_over = pd.Series({'status': 'MISSING IN EXCHANGE', 'amount_diff': 100})
        manager._add_business_context(row_over)
        assert "Overpayment" in caplog.text

    def test_print_summary_formatting(self, caplog):
        """Verify summary table prints correctly"""
        manager = AlertManager()
        summary = {'total_contracts': 10, 'critical_alerts': 2}
        with caplog.at_level(logging.INFO):
            manager.print_summary(summary)
        assert "RECONCILIATION SUMMARY" in caplog.text
        assert "Total Contracts" in caplog.text

# --- EMAIL SENDER TESTS ---

class TestEmailSender:
    def test_email_disabled_by_default(self):
        """Ensure email doesn't attempt to send if disabled in .env"""
        with patch.dict(os.environ, {"EMAIL_ENABLED": "false"}):
            sender = EmailAlertSender()
            result = sender.send_alerts(pd.DataFrame([{'data': 1}]), {})
            assert result is False

    @patch("smtplib.SMTP")
    def test_email_smtp_flow(self, mock_smtp):
        """Mock SMTP to verify the email sending sequence"""
        mock_server = MagicMock()
        mock_smtp.return_value.__enter__.return_value = mock_server
        
        env_vars = {
            "EMAIL_ENABLED": "true",
            "SMTP_HOST": "smtp.gmail.com",
            "SMTP_PORT": "587",
            "SMTP_USER": "test@user.com",
            "SMTP_PASSWORD": "password123",
            "EMAIL_FROM": "test@user.com",
            "EMAIL_TO": "boss@user.com"
        }
        
        with patch.dict(os.environ, env_vars):
            sender = EmailAlertSender()
            df = pd.DataFrame([{'contract_id': 'T1', 'amount_diff': 100, 'status': 'ERR', 'counterparty': 'X'}])
            
            result = sender.send_alerts(df, {'total_discrepancy_amount': 100})
            
            assert result is True
            # Verify SMTP methods were called
            mock_server.starttls.assert_called_once()
            mock_server.login.assert_called_with("test@user.com", "password123")
            mock_server.send_message.assert_called_once()