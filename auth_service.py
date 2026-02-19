import smtplib
import os
import random
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

class AuthService:
    def __init__(self):
        self.email = os.environ.get("TS_EMAIL")
        self.password = os.environ.get("TS_PASSWORD")
        self.smtp_server = "smtp.gmail.com"
        self.smtp_port = 465

    def send_verification_code(self, recipient, code):
        """Sends a 10-character code for password recovery."""
        subject = "Taremwa Studios - Account Recovery"
        body = (
            f"Your recovery code is: {code}\n\n"
            "Enter this code in the game to reset your password. "
            "If this wasn't you trying to reset your password, you can file a spam report at "
            "taremwastudios@gmail.com to resolve the issue or just ignore it."
        )
        return self._send_email(recipient, subject, body)

    def send_notification(self, recipient, subject_suffix, content):
        """Sends a general branded notification (tickets, alerts, etc)."""
        subject = f"Taremwa Studios - {subject_suffix}"
        body = (
            f"{content}\n\n"
            "If this wasn't you or you have questions about this notification, please contact "
            "taremwastudios@gmail.com. If you did not expect this message, you can safely ignore it."
        )
        return self._send_email(recipient, subject, body)

    def _send_email(self, recipient, subject, body):
        if not self.email or not self.password:
            print("[AuthService] Error: Credentials not set in environment.")
            return False

        msg = MIMEText(body)
        msg['Subject'] = subject
        msg['From'] = f"Taremwa Studios <{self.email}>"
        msg['To'] = recipient

        try:
            with smtplib.SMTP_SSL(self.smtp_server, self.smtp_port) as server:
                server.login(self.email, self.password)
                server.send_message(msg)
            return True
        except Exception as e:
            print(f"[AuthService Error] {e}")
            return False