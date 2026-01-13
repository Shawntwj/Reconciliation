import pandas as pd
import logging
import os
import argparse
from sqlalchemy import create_engine
from src.alerts import AlertManager
from src.email_alerts import send_email_alerts

# Configure logging to match your style
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("ReconEngine")

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:password@db:5432/in_commodities")

def run_reconciliation(is_test_mode=False):
    """
    Main reconciliation process:
    1. Query the SQL View
    2. Analyze discrepancies
    3. Trigger AlertManager
    """
    logger.info("📊 Starting Reconciliation Analysis...")
    engine = create_engine(DATABASE_URL)
    
    try:
        # 1. Fetch the data from the SQL view we created
        df = pd.read_sql("SELECT * FROM stg.reconciliation_report;", engine)

        # 2. Map SQL columns to your AlertManager expectations
        df_mapped = df.rename(columns={
            'bank_refs': 'product', 
            'bank_value': 'trade_total', 
            'exchange_value': 'invoice_total', 
            'value_diff': 'amount_diff',
            'recon_status': 'status'  
        })

        # 3. Calculate Summary Stats for Monika
        summary_stats = {
            'total_contracts': len(df_mapped),
            'matched': len(df_mapped[df_mapped['status'] == 'MATCHED']),
            'discrepancies': len(df_mapped[df_mapped['status'] == 'DISCREPANCY']),
            'missing_trades': len(df_mapped[df_mapped['status'] == 'MISSING IN BANK']),
            'missing_invoices': len(df_mapped[df_mapped['status'] == 'MISSING IN EXCHANGE']),
            'critical_alerts': 0, # Will update below
            'total_discrepancy_amount': df_mapped['amount_diff'].abs().sum()
        }

        # 4. Filter for Critical Alerts (Using your $100 threshold)
        threshold = float(os.getenv('ALERT_THRESHOLD', 100.0))
        critical_alerts = df_mapped[
            (df_mapped['amount_diff'].abs() >= threshold) | 
            (df_mapped['status'].str.contains('MISSING'))
        ].copy()
        
        summary_stats['critical_alerts'] = len(critical_alerts)

        # 5. Initialize your AlertManager and trigger notifications
        alert_mgr = AlertManager(alert_threshold=threshold)
        
        # This will print the detailed logs you wrote in AlertManager
        alert_mgr.send_alerts(critical_alerts, summary=summary_stats, enable_email=True)
        
        # This will print the summary table
        alert_mgr.print_summary(summary_stats)

    except Exception as e:
        logger.error(f"Reconciliation failed: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Reconciliation Engine')
    parser.add_argument('--test-email', action='store_true', help='Run with mock data and trigger email')
    args = parser.parse_args()

    if args.test_email:
        # Override env for local test convenience
        os.environ['EMAIL_ENABLED'] = 'true'
        run_reconciliation(is_test_mode=True)
    else:
        run_reconciliation(is_test_mode=False)    