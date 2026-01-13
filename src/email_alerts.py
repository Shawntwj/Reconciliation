"""
Email alerting for reconciliation discrepancies

Clean, minimalistic email templates for easy readability.
Supports SMTP for email delivery.
"""

import os
import logging
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import pandas as pd
from datetime import datetime
from typing import Dict

logger = logging.getLogger(__name__)


class EmailAlertSender:
    """
    Send email alerts for reconciliation discrepancies

    Configuration via environment variables:
    - EMAIL_ENABLED: 'true' to enable, 'false' to disable
    - EMAIL_FROM: Sender email address
    - EMAIL_TO: Comma-separated recipient emails
    - SMTP_HOST: SMTP server (e.g., smtp.gmail.com)
    - SMTP_PORT: SMTP port (587 for TLS)
    - SMTP_USER: SMTP username
    - SMTP_PASSWORD: SMTP password or app password
    """

    def __init__(self):
        """Initialize email sender with configuration from environment"""
        self.enabled = os.getenv('EMAIL_ENABLED', 'false').lower() == 'true'
        self.from_email = os.getenv('EMAIL_FROM', 'reconciliation@company.com')
        self.to_emails = [e.strip() for e in os.getenv('EMAIL_TO', '').split(',') if e.strip()]

        if self.enabled and not self.to_emails:
            logger.warning("EMAIL_ENABLED=true but EMAIL_TO is not set. Disabling email alerts.")
            self.enabled = False

    def send_alerts(self, alerts_df: pd.DataFrame, summary: Dict) -> bool:
        """
        Send email alerts for discrepancies

        Args:
            alerts_df: DataFrame with alert records
            summary: Summary statistics dictionary

        Returns:
            True if email sent successfully, False otherwise
        """
        if not self.enabled:
            logger.info("Email alerts disabled (EMAIL_ENABLED=false)")
            return False

        if alerts_df.empty:
            logger.info("No alerts to send via email")
            return False

        try:
            subject = self._create_subject(len(alerts_df), summary)
            html_body = self._create_html_body(alerts_df, summary)
            text_body = self._create_text_body(alerts_df, summary)

            msg = MIMEMultipart('alternative')
            msg['Subject'] = subject
            msg['From'] = self.from_email
            msg['To'] = ', '.join(self.to_emails)
            msg.attach(MIMEText(text_body, 'plain'))
            msg.attach(MIMEText(html_body, 'html'))

            smtp_host = os.getenv('SMTP_HOST', 'smtp.gmail.com')
            smtp_port = int(os.getenv('SMTP_PORT', '587'))
            smtp_user = os.getenv('SMTP_USER')
            smtp_password = os.getenv('SMTP_PASSWORD')

            if not smtp_user or not smtp_password:
                logger.error("SMTP_USER and SMTP_PASSWORD must be set")
                return False

            with smtplib.SMTP(smtp_host, smtp_port) as server:
                server.starttls()
                server.login(smtp_user, smtp_password)
                server.send_message(msg)

            logger.info(f"✅ Email sent successfully to {', '.join(self.to_emails)}")
            return True

        except Exception as e:
            logger.error(f"Email failed: {e}")
            return False

    def _create_subject(self, alert_count: int, summary: Dict) -> str:
        """Create email subject line"""
        total_amount = summary.get('total_discrepancy_amount', 0)
        return f"Reconciliation Alert: {alert_count} issue{'s' if alert_count != 1 else ''} found (${total_amount:,.2f})"

    def _create_html_body(self, alerts_df: pd.DataFrame, summary: Dict) -> str:
        """Create minimalistic HTML email body"""
        # Build alert rows
        alerts_rows = ""
        for _, row in alerts_df.iterrows():
            trade_val = '${:,.2f}'.format(row['trade_total']) if pd.notna(row.get('trade_total')) else '—'
            invoice_val = '${:,.2f}'.format(row['invoice_total']) if pd.notna(row.get('invoice_total')) else '—'
            product = row.get('product', row.get('product', 'N/A'))

            alerts_rows += f"""
                <tr style="border-bottom: 1px solid #e5e7eb;">
                    <td style="padding: 12px 8px; font-weight: 500;">{product}</td>
                    <td style="padding: 12px 8px;">{row['counterparty']}</td>
                    <td style="padding: 12px 8px; text-align: right;">{trade_val}</td>
                    <td style="padding: 12px 8px; text-align: right;">{invoice_val}</td>
                    <td style="padding: 12px 8px; text-align: right; font-weight: 600; color: #dc2626;">${row['amount_diff']:,.2f}</td>
                </tr>
            """

        html = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
        </head>
        <body style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; line-height: 1.5; color: #1f2937; background: #f9fafb; margin: 0; padding: 20px;">
            <div style="max-width: 700px; margin: 0 auto; background: white; border-radius: 8px; overflow: hidden; box-shadow: 0 1px 3px rgba(0,0,0,0.1);">

                <!-- Header -->
                <div style="background: #1f2937; color: white; padding: 24px; border-bottom: 3px solid #dc2626;">
                    <h1 style="margin: 0; font-size: 20px; font-weight: 600;">Reconciliation Alert</h1>
                    <p style="margin: 4px 0 0 0; font-size: 14px; opacity: 0.8;">{datetime.now().strftime('%B %d, %Y at %H:%M')}</p>
                </div>

                <!-- Summary -->
                <div style="padding: 24px; background: #fef2f2; border-bottom: 1px solid #fee2e2;">
                    <table style="width: 100%; border-collapse: collapse;">
                        <tr>
                            <td style="padding: 6px 0; font-size: 14px;">Total Contracts</td>
                            <td style="padding: 6px 0; text-align: right; font-weight: 600;">{summary.get('total_contracts', 'N/A')}</td>
                        </tr>
                        <tr>
                            <td style="padding: 6px 0; font-size: 14px;">Alerts Found</td>
                            <td style="padding: 6px 0; text-align: right; font-weight: 600;">{summary.get('critical_alerts', len(alerts_df))}</td>
                        </tr>
                        <tr>
                            <td style="padding: 6px 0; font-size: 14px; font-weight: 600;">Total Discrepancy</td>
                            <td style="padding: 6px 0; text-align: right; font-weight: 700; color: #dc2626; font-size: 16px;">${summary.get('total_discrepancy_amount', 0):,.2f}</td>
                        </tr>
                    </table>
                </div>

                <!-- Alert Details -->
                <div style="padding: 24px;">
                    <h2 style="margin: 0 0 16px 0; font-size: 16px; font-weight: 600; color: #374151;">Details</h2>
                    <table style="width: 100%; border-collapse: collapse; font-size: 14px;">
                        <thead>
                            <tr style="border-bottom: 2px solid #e5e7eb; background: #f9fafb;">
                                <th style="padding: 10px 8px; text-align: left; font-weight: 600; color: #6b7280;">Contract</th>
                                <th style="padding: 10px 8px; text-align: left; font-weight: 600; color: #6b7280;">Counterparty</th>
                                <th style="padding: 10px 8px; text-align: right; font-weight: 600; color: #6b7280;">Trade</th>
                                <th style="padding: 10px 8px; text-align: right; font-weight: 600; color: #6b7280;">Invoice</th>
                                <th style="padding: 10px 8px; text-align: right; font-weight: 600; color: #6b7280;">Diff</th>
                            </tr>
                        </thead>
                        <tbody>
                            {alerts_rows}
                        </tbody>
                    </table>
                </div>

                <!-- Footer -->
                <div style="padding: 16px 24px; background: #f9fafb; border-top: 1px solid #e5e7eb; text-align: center; font-size: 12px; color: #6b7280;">
                    <p style="margin: 0;">Automated Reconciliation Pipeline</p>
                </div>
            </div>
        </body>
        </html>
        """
        return html

    def _create_text_body(self, alerts_df: pd.DataFrame, summary: Dict) -> str:
        """Create plain text email body (fallback)"""
        text = f"""RECONCILIATION ALERT
{datetime.now().strftime('%B %d, %Y at %H:%M')}
{'=' * 70}

SUMMARY
Total Contracts:     {summary.get('total_contracts', 'N/A')}
Alerts Found:        {summary.get('critical_alerts', len(alerts_df))}
Total Discrepancy:   ${summary.get('total_discrepancy_amount', 0):,.2f}

DETAILS
{'-' * 70}
"""
        for _, row in alerts_df.iterrows():
            trade_str = '${:,.2f}'.format(row['trade_total']) if pd.notna(row.get('trade_total')) else '—'
            invoice_str = '${:,.2f}'.format(row['invoice_total']) if pd.notna(row.get('invoice_total')) else '—'
            product = row.get('product', row.get('product', 'N/A'))

            text += f"""
{product} | {row['counterparty']}
Trade: {trade_str:<15} Invoice: {invoice_str:<15} Diff: ${row['amount_diff']:,.2f}
{'-' * 70}
"""

        text += """
---
Automated Reconciliation Pipeline
"""
        return text


def send_email_alerts(alerts_df: pd.DataFrame, summary: Dict) -> bool:
    """
    Quick function to send email alerts

    Usage:
        from src.email_alerts import send_email_alerts
        send_email_alerts(alerts_df, summary_stats)
    """
    sender = EmailAlertSender()
    return sender.send_alerts(alerts_df, summary)