import pytest
import pandas as pd
import os
from unittest.mock import patch, MagicMock
from src.reconcile import run_reconciliation

class TestReconciliationLogic:
    """
    Tests the logic found in reconcile.py by mocking the SQL view results
    """

    @patch('src.reconcile.create_engine')
    @patch('pandas.read_sql')
    @patch('src.reconcile.AlertManager')
    def test_run_reconciliation_integration(self, mock_alert_manager, mock_read_sql, mock_engine):
        """
        Verify that run_reconciliation fetches data, maps columns, 
        and passes critical alerts to the AlertManager.
        """
        # 1. Setup Mock DB Data (matching your SQL View output)
        mock_data = pd.DataFrame([
            {
                'record_ref': 'T001-1',
                'product': 'GAS-UK',
                'counterparty': 'BP',
                'bank_value': 1000.0,
                'exchange_value': 1000.0,
                'value_diff': 0.0,
                'recon_status': 'MATCHED'
            },
            {
                'record_ref': 'T002-1',
                'product': 'PWR-GER',
                'counterparty': 'RWE',
                'bank_value': 500.0,
                'exchange_value': 200.0,
                'value_diff': 300.0,  # Critical discrepancy
                'recon_status': 'DISCREPANCY'
            }
        ])
        mock_read_sql.return_value = mock_data
        
        # 2. Setup AlertManager Mock
        mock_mgr_instance = mock_alert_manager.return_value
        
        # 3. Execute the function
        with patch.dict(os.environ, {'ALERT_THRESHOLD': '100.0'}):
            run_reconciliation()

        # 4. Assertions
        # Check that AlertManager was initialized with correct threshold
        mock_alert_manager.assert_called_with(alert_threshold=100.0)
        
        # Check that send_alerts was called with ONLY the critical discrepancy
        # (The first row was MATCHED, second was DISCREPANCY > 100)
        called_df = mock_mgr_instance.send_alerts.call_args[0][0]
        assert len(called_df) == 1
        assert called_df.iloc[0]['contract_id'] == 'T002-1'
        assert called_df.iloc[0]['amount_diff'] == 300.0

    @patch('src.reconcile.pd.read_sql')
    @patch('src.reconcile.create_engine')
    def test_reconciliation_error_handling(self, mock_engine, mock_read_sql, caplog):
        """Verify that an exception in DB connection is logged"""
        mock_read_sql.side_effect = Exception("DB Connection Failed")
        
        run_reconciliation()
        
        assert "Reconciliation failed: DB Connection Failed" in caplog.text