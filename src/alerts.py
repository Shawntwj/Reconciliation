import pandas as pd
import logging
from typing import Optional

logger = logging.getLogger(__name__)

class AlertManager:
    """Handles alerting for critical reconciliation discrepancies"""

    def __init__(self, alert_threshold: float = 100.0):
        self.alert_threshold = alert_threshold

    def send_alerts(self, alerts_df: pd.DataFrame, summary: Optional[dict] = None, enable_email: bool = False) -> None:
        if alerts_df.empty:
            logger.info("✅ No critical alerts - all discrepancies below threshold")
            return

        self._log_console_alerts(alerts_df)

        if enable_email and summary:
            try:
                from src.email_alerts import send_email_alerts
                send_email_alerts(alerts_df, summary)
            except Exception as e:
                logger.warning(f"Email alerts failed: {e}")

    def _log_console_alerts(self, alerts_df: pd.DataFrame) -> None:
        logger.warning("=" * 80)
        logger.warning(f"🚨 CRITICAL ALERTS: {len(alerts_df)} items require attention")
        logger.warning("=" * 80)

        for _, row in alerts_df.iterrows():
            logger.warning(f"Contract: {row['product']} | Counterparty: {row['counterparty']}")
            logger.warning(f"Status:   {row['status']}")
            logger.warning(f"Diff:     ${row['amount_diff']:,.2f}")
            self._add_business_context(row)
            logger.warning("-" * 80)

    def _add_business_context(self, row: pd.Series) -> None:
        status = row['status']
        if 'MISSING' in status and 'BANK' in status:
            logger.warning("⚠️  RISK: Revenue leakage - trade exists but no bank record.")
        elif 'MISSING' in status and 'EXCHANGE' in status:
            logger.warning("⚠️  RISK: Overpayment - bank record exists without matching trade.")
        elif status == 'DISCREPANCY':
            logger.warning(f"⚠️  VALUE MISMATCH: Financial gap of ${row['amount_diff']:,.2f}")

    def print_summary(self, summary_stats: dict) -> None:
        logger.info("=" * 80)
        logger.info("RECONCILIATION SUMMARY")
        logger.info("=" * 80)
        for key, val in summary_stats.items():
            logger.info(f"{key.replace('_', ' ').title():<25}: {val}")
        logger.info("=" * 80)