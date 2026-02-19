import sqlite3
import datetime
import secrets
import json
import hashlib
import random
from auth_service import AuthService

class BillboardManager:
    def __init__(self, db_path):
        self.db_path = db_path
        self.auth_service = AuthService()
        self.init_db()

    def hash_password(self, password):
        return hashlib.sha256(password.encode()).hexdigest()

    def init_db(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute('''CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT UNIQUE,
                password TEXT,
                email TEXT UNIQUE,
                phone TEXT,
                full_names TEXT,
                home_address TEXT,
                bio TEXT,
                avatar_url TEXT,
                badge_type TEXT DEFAULT 'none',
                current_theme_id TEXT DEFAULT 'retro',
                is_muted INTEGER DEFAULT 0,
                is_email_verified INTEGER DEFAULT 0,
                verification_code TEXT,
                subscription_expiry TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )''')
            conn.execute('''CREATE TABLE IF NOT EXISTS posts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                content TEXT,
                post_type TEXT,
                channel_id INTEGER,
                media_url TEXT,
                media_type TEXT,
                is_deleted INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(user_id) REFERENCES users(id)
            )''')
            conn.execute('''CREATE TABLE IF NOT EXISTS channels (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                owner_id INTEGER,
                name TEXT,
                description TEXT,
                access_price INTEGER,
                channel_type TEXT DEFAULT 'private',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(owner_id) REFERENCES users(id)
            )''')
            conn.execute('''CREATE TABLE IF NOT EXISTS channel_memberships (
                user_id INTEGER,
                channel_id INTEGER,
                joined_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY(user_id, channel_id)
            )''')
            conn.execute('''CREATE TABLE IF NOT EXISTS chat_messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                sender_id INTEGER,
                receiver_id INTEGER,
                content TEXT,
                is_read INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )''')
            conn.execute('''CREATE TABLE IF NOT EXISTS reports (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                post_id INTEGER,
                user_id INTEGER,
                reason TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )''')
            conn.execute('''CREATE TABLE IF NOT EXISTS last_read_markers (
                user_id INTEGER,
                category TEXT,
                last_post_id INTEGER,
                PRIMARY KEY(user_id, category)
            )''')
            conn.execute('''CREATE TABLE IF NOT EXISTS pending_payments (
                id TEXT PRIMARY KEY,
                user_id INTEGER,
                item_id TEXT,
                amount INTEGER,
                status TEXT DEFAULT 'pending',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )''')
            conn.execute('''CREATE TABLE IF NOT EXISTS dev_applications (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                details TEXT,
                cert_pdf_url TEXT,
                status TEXT DEFAULT 'pending',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )''')

    # --- USER LOGIC ---
    def get_user_by_username(self, username):
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            res = conn.execute("SELECT * FROM users WHERE username = ?", (username,)).fetchone()
            return dict(res) if res else None

    def get_user_by_email(self, email):
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            res = conn.execute("SELECT * FROM users WHERE email = ?", (email,)).fetchone()
            return dict(res) if res else None

    def create_user(self, username, password, email, phone, full_names, home_address):
        code = str(random.randint(1000000, 9999999))
        hashed_pw = self.hash_password(password)
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute('''
                INSERT INTO users (username, password, email, phone, full_names, home_address, verification_code)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (username, hashed_pw, email, phone, full_names, home_address, code))
            user_id = cursor.lastrowid
            self.auth_service.send_notification(email, "Verification Code", f"Your Campus Billboard verification code is: {code}")
            return user_id

    def verify_email(self, email, code):
        with sqlite3.connect(self.db_path) as conn:
            res = conn.execute("SELECT id FROM users WHERE email = ? AND verification_code = ?", (email, code)).fetchone()
            if res:
                conn.execute("UPDATE users SET is_email_verified = 1 WHERE id = ?", (res[0],))
                return True
        return False

    def is_email_verified(self, user_id):
        with sqlite3.connect(self.db_path) as conn:
            res = conn.execute("SELECT is_email_verified FROM users WHERE id = ?", (user_id,)).fetchone()
            return res[0] == 1 if res else False

    def update_user_profile(self, user_id, bio=None, avatar_url=None):
        with sqlite3.connect(self.db_path) as conn:
            if bio: conn.execute("UPDATE users SET bio = ? WHERE id = ?", (bio, user_id))
            if avatar_url: conn.execute("UPDATE users SET avatar_url = ? WHERE id = ?", (avatar_url, user_id))
            return True

    def upgrade_user_badge(self, user_id, badge_type):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("UPDATE users SET badge_type = ? WHERE id = ?", (badge_type, user_id))
            return True

    # --- FEED LOGIC ---
    def create_post(self, user_id, content, post_type, channel_id=None, media_url=None, media_type=None):
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute('''
                INSERT INTO posts (user_id, content, post_type, channel_id, media_url, media_type)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (user_id, content, post_type, channel_id, media_url, media_type))
            return cursor.lastrowid

    def get_feed(self, limit=100, after_id=0):
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            query = '''
                SELECT p.*, u.username, u.badge_type, u.current_theme_id, u.avatar_url as user_avatar, u.is_muted
                FROM posts p 
                JOIN users u ON p.user_id = u.id 
                WHERE p.is_deleted = 0 
                AND p.post_type != 'news' 
                AND p.channel_id IS NULL
                AND p.id > ? 
                ORDER BY p.created_at DESC
                LIMIT ?
            '''
            cursor = conn.execute(query, (after_id, limit))
            return [dict(row) for row in cursor.fetchall()]

    def get_news(self, limit=100, after_id=0):
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            query = '''
                SELECT p.*, u.username, u.badge_type, u.current_theme_id, u.avatar_url as user_avatar, u.is_muted
                FROM posts p 
                JOIN users u ON p.user_id = u.id 
                WHERE p.is_deleted = 0 
                AND p.post_type = 'news'
                AND p.id > ? 
                ORDER BY p.created_at DESC
                LIMIT ?
            '''
            cursor = conn.execute(query, (after_id, limit))
            return [dict(row) for row in cursor.fetchall()]

    # --- CHANNEL LOGIC ---
    def create_channel(self, owner_id, name, description, price):
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute('''
                INSERT INTO channels (owner_id, name, description, access_price)
                VALUES (?, ?, ?, ?)
            ''', (owner_id, name, description, price))
            channel_id = cursor.lastrowid
            conn.execute("INSERT INTO channel_memberships (user_id, channel_id) VALUES (?, ?)", (owner_id, channel_id))
            return channel_id

    def get_all_channels(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute('''
                SELECT c.*, u.username as owner_name 
                FROM channels c 
                JOIN users u ON c.owner_id = u.id
            ''')
            return [dict(row) for row in cursor.fetchall()]

    def is_channel_member(self, user_id, channel_id):
        with sqlite3.connect(self.db_path) as conn:
            res = conn.execute("SELECT 1 FROM channel_memberships WHERE user_id = ? AND channel_id = ?", (user_id, channel_id)).fetchone()
            return res is not None

    def add_channel_member(self, channel_id, user_id):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("INSERT OR IGNORE INTO channel_memberships (user_id, channel_id) VALUES (?, ?)", (user_id, channel_id))
            return True

    def get_channel_feed(self, channel_id, limit=100):
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute('''
                SELECT p.*, u.username, u.badge_type, u.avatar_url as user_avatar
                FROM posts p 
                JOIN users u ON p.user_id = u.id 
                WHERE p.channel_id = ? AND p.is_deleted = 0
                ORDER BY p.created_at DESC
                LIMIT ?
            ''', (channel_id, limit))
            return [dict(row) for row in cursor.fetchall()]

    # --- CHAT LOGIC ---
    def send_chat_message(self, sender_id, receiver_id, content):
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("INSERT INTO chat_messages (sender_id, receiver_id, content) VALUES (?, ?, ?)", (sender_id, receiver_id, content))
            return cursor.lastrowid

    def get_chat_messages(self, user_id, other_id):
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute('''
                SELECT * FROM chat_messages 
                WHERE (sender_id = ? AND receiver_id = ?) 
                OR (sender_id = ? AND receiver_id = ?)
                ORDER BY created_at ASC
            ''', (user_id, other_id, other_id, user_id))
            return [dict(row) for row in cursor.fetchall()]

    def get_user_chat_list(self, user_id):
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute('''
                SELECT DISTINCT u.id, u.username, u.avatar_url, u.badge_type 
                FROM users u 
                JOIN chat_messages m ON (u.id = m.sender_id OR u.id = m.receiver_id)
                WHERE (m.sender_id = ? OR m.receiver_id = ?) AND u.id != ?
            ''', (user_id, user_id, user_id))
            return [dict(row) for row in cursor.fetchall()]

    # --- PAYMENT LOGIC ---
    def initiate_simulated_payment(self, user_id, item_id, amount):
        pay_id = secrets.token_hex(8)
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("INSERT INTO pending_payments (id, user_id, item_id, amount) VALUES (?, ?, ?, ?)", (pay_id, user_id, item_id, amount))
            return pay_id

    def get_simulated_payment(self, pay_id):
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            res = conn.execute("SELECT * FROM pending_payments WHERE id = ?", (pay_id,)).fetchone()
            return dict(res) if res else None

    def complete_simulated_payment(self, pay_id):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("UPDATE pending_payments SET status = 'completed' WHERE id = ?", (pay_id,))
            return True

    def get_pending_simulated_payments(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute("SELECT p.*, u.username FROM pending_payments p JOIN users u ON p.user_id = u.id WHERE p.status = 'pending'")
            return [dict(row) for row in cursor.fetchall()]

    # --- ADMIN & MOD LOGIC ---
    def report_post(self, post_id, user_id):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("INSERT INTO reports (post_id, user_id) VALUES (?, ?)", (post_id, user_id))
            return True

    def get_all_reports(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute('''
                SELECT r.*, p.content as post_content, u.username as reported_user 
                FROM reports r 
                JOIN posts p ON r.post_id = p.id 
                JOIN users u ON p.user_id = u.id
            ''')
            return [dict(row) for row in cursor.fetchall()]

    def delete_post(self, post_id):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("UPDATE posts SET is_deleted = 1 WHERE id = ?", (post_id,))
            return True

    def mute_user(self, username):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("UPDATE users SET is_muted = 1 WHERE username = ?", (username,))
            return True

    def create_dev_application(self, user_id, details, cert_pdf_url):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("INSERT INTO dev_applications (user_id, details, cert_pdf_url) VALUES (?, ?, ?)", (user_id, details, cert_pdf_url))
            return True

    def get_pending_dev_applications(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute("SELECT d.*, u.username FROM dev_applications d JOIN users u ON d.user_id = u.id WHERE d.status = 'pending'")
            return [dict(row) for row in cursor.fetchall()]

    def approve_dev_application(self, app_id):
        with sqlite3.connect(self.db_path) as conn:
            res = conn.execute("SELECT user_id FROM dev_applications WHERE id = ?", (app_id,)).fetchone()
            if res:
                conn.execute("UPDATE users SET badge_type = 'dev' WHERE id = ?", (res[0],))
                conn.execute("UPDATE dev_applications SET status = 'approved' WHERE id = ?", (app_id,))
                return True
        return False

    def get_online_users(self): return 12 # Mock
    def get_total_users(self):
        with sqlite3.connect(self.db_path) as conn: return conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]
    def get_total_posts(self):
        with sqlite3.connect(self.db_path) as conn: return conn.execute("SELECT COUNT(*) FROM posts WHERE is_deleted = 0").fetchone()[0]
    def get_total_channels(self):
        with sqlite3.connect(self.db_path) as conn: return conn.execute("SELECT COUNT(*) FROM channels").fetchone()[0]

import random