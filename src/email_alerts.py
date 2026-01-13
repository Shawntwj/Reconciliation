import os
import logging
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import pandas as pd
from datetime import datetime

logger = logging.getLogger(__name__)

class EmailAlertSender:
    def __init__(self):
        self.enabled = os.getenv('EMAIL_ENABLED', 'false').lower() == 'true'
        self.from_email = os.getenv('EMAIL_FROM')
        self.to_emails = [e.strip() for e in os.getenv('EMAIL_TO', '').split(',') if e.strip()]

    def send_alerts(self, alerts_df: pd.DataFrame, summary: dict) -> bool:
        if not self.enabled or alerts_df.empty:
            return False

        try:
            subject = f"🚨 RECON ALERT: {len(alerts_df)} Discrepancies Found"
            body = f"Summary:\nTotal Discrepancy: ${summary.get('total_discrepancy_amount', 0):,.2f}\n\nCheck logs for details."
            
            msg = MIMEMultipart()
            msg['Subject'] = subject
            msg['From'] = self.from_email
            msg['To'] = ', '.join(self.to_emails)
            msg.attach(MIMEText(body, 'plain'))

            with smtplib.SMTP(os.getenv('SMTP_HOST'), int(os.getenv('SMTP_PORT', 587))) as server:
                server.starttls()
                server.login(os.getenv('SMTP_USER'), os.getenv('SMTP_PASSWORD'))
                server.send_message(msg)
            
            logger.info("✅ Email alert sent successfully.")
            return True
        except Exception as e:
            logger.error(f"Email failed: {e}")
            return False

def send_email_alerts(alerts_df, summary):
    sender = EmailAlertSender()
    return sender.send_alerts(alerts_df, summary)