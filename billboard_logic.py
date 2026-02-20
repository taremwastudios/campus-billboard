import sqlite3
import psycopg2
import psycopg2.extras
import datetime
import secrets
import json
import hashlib
import random
import os
from auth_service import AuthService

class BillboardManager:
    def __init__(self, db_path):
        self.db_path = db_path
        self.database_url = os.environ.get("DATABASE_URL")
        self.auth_service = AuthService()
        self.init_db()

    def get_connection(self):
        try:
            if self.database_url:
                # Use PostgreSQL if URL is provided (Production)
                conn = psycopg2.connect(self.database_url)
                return conn
            else:
                # Use SQLite (Local Development)
                conn = sqlite3.connect(self.db_path)
                conn.row_factory = sqlite3.Row
                return conn
        except Exception as e:
            print(f"[Database] Connection Error: {e}")
            return None

    def hash_password(self, password):
        return hashlib.sha256(password.encode()).hexdigest()

    def init_db(self):
        conn = self.get_connection()
        cur = conn.cursor()
        
        # SQL for User Table (Postgres compatible)
        users_sql = '''CREATE TABLE IF NOT EXISTS users (
            id SERIAL PRIMARY KEY,
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
        )'''
        
        posts_sql = '''CREATE TABLE IF NOT EXISTS posts (
            id SERIAL PRIMARY KEY,
            user_id INTEGER,
            content TEXT,
            post_type TEXT,
            channel_id INTEGER,
            media_url TEXT,
            media_type TEXT,
            is_deleted INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )'''

        channels_sql = '''CREATE TABLE IF NOT EXISTS channels (
            id SERIAL PRIMARY KEY,
            owner_id INTEGER,
            name TEXT,
            description TEXT,
            access_price INTEGER,
            channel_type TEXT DEFAULT 'private',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )'''

        memberships_sql = '''CREATE TABLE IF NOT EXISTS channel_memberships (
            user_id INTEGER,
            channel_id INTEGER,
            joined_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY(user_id, channel_id)
        )'''

        markers_sql = '''CREATE TABLE IF NOT EXISTS last_read_markers (
            user_id INTEGER,
            category TEXT,
            last_post_id INTEGER,
            PRIMARY KEY(user_id, category)
        )'''

        cur.execute(users_sql)
        cur.execute(posts_sql)
        cur.execute(channels_sql)
        cur.execute(memberships_sql)
        cur.execute(markers_sql)
        
        conn.commit()
        cur.close()
        conn.close()

    def get_user_by_username(self, username):
        conn = self.get_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) if self.database_url else conn.cursor()
        cur.execute("SELECT * FROM users WHERE username = %s" if self.database_url else "SELECT * FROM users WHERE username = ?", (username,))
        res = cur.fetchone()
        cur.close()
        conn.close()
        return dict(res) if res else None

    def get_user_by_id(self, user_id):
        conn = self.get_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) if self.database_url else conn.cursor()
        cur.execute("SELECT * FROM users WHERE id = %s" if self.database_url else "SELECT * FROM users WHERE id = ?", (user_id,))
        res = cur.fetchone()
        cur.close()
        conn.close()
        return dict(res) if res else None

    def get_user_by_email(self, email):
        conn = self.get_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) if self.database_url else conn.cursor()
        cur.execute("SELECT * FROM users WHERE email = %s" if self.database_url else "SELECT * FROM users WHERE email = ?", (email,))
        res = cur.fetchone()
        cur.close()
        conn.close()
        return dict(res) if res else None

    def get_user_by_email(self, email):
        conn = self.get_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) if self.database_url else conn.cursor()
        cur.execute("SELECT * FROM users WHERE email = %s" if self.database_url else "SELECT * FROM users WHERE email = ?", (email,))
        res = cur.fetchone()
        cur.close()
        conn.close()
        return dict(res) if res else None

    def create_user(self, username, password, email, phone, full_names, home_address):
        code = str(random.randint(1000000, 9999999))
        hashed_pw = self.hash_password(password)
        conn = self.get_connection()
        cur = conn.cursor()
        sql = '''
            INSERT INTO users (username, password, email, phone, full_names, home_address, verification_code)
            VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING id
        ''' if self.database_url else '''
            INSERT INTO users (username, password, email, phone, full_names, home_address, verification_code)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        '''
        cur.execute(sql, (username, hashed_pw, email, phone, full_names, home_address, code))
        user_id = cur.fetchone()[0] if self.database_url else cur.lastrowid
        conn.commit()
        cur.close()
        conn.close()
        self.auth_service.send_notification(email, "Verification Code", f"Your verification code is: {code}")
        return user_id

    def verify_email(self, email, code):
        conn = self.get_connection()
        cur = conn.cursor()
        sql = "SELECT id FROM users WHERE email = %s AND verification_code = %s" if self.database_url else "SELECT id FROM users WHERE email = ? AND verification_code = ?"
        cur.execute(sql, (email, code))
        res = cur.fetchone()
        if res:
            upd = "UPDATE users SET is_email_verified = 1 WHERE id = %s" if self.database_url else "UPDATE users SET is_email_verified = 1 WHERE id = ?"
            cur.execute(upd, (res[0],))
            conn.commit()
            cur.close()
            conn.close()
            return True
        cur.close()
        conn.close()
        return False

    def is_email_verified(self, user_id):
        conn = self.get_connection()
        cur = conn.cursor()
        sql = "SELECT is_email_verified FROM users WHERE id = %s" if self.database_url else "SELECT is_email_verified FROM users WHERE id = ?"
        cur.execute(sql, (user_id,))
        res = cur.fetchone()
        cur.close()
        conn.close()
        return res[0] == 1 if res else False

    def create_post(self, user_id, content, post_type, channel_id=None, media_url=None, media_type=None):
        conn = self.get_connection()
        cur = conn.cursor()
        sql = '''
            INSERT INTO posts (user_id, content, post_type, channel_id, media_url, media_type)
            VALUES (%s, %s, %s, %s, %s, %s) RETURNING id
        ''' if self.database_url else '''
            INSERT INTO posts (user_id, content, post_type, channel_id, media_url, media_type)
            VALUES (?, ?, ?, ?, ?, ?)
        '''
        cur.execute(sql, (user_id, content, post_type, channel_id, media_url, media_type))
        post_id = cur.fetchone()[0] if self.database_url else cur.lastrowid
        conn.commit()
        cur.close()
        conn.close()
        return post_id

    def get_feed(self, limit=100, after_id=0):
        conn = self.get_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) if self.database_url else conn.cursor()
        sql = '''
            SELECT p.*, u.username, u.badge_type, u.avatar_url as user_avatar 
            FROM posts p 
            JOIN users u ON p.user_id = u.id 
            WHERE p.is_deleted = 0 AND p.post_type != 'news' AND p.channel_id IS NULL AND p.id > %s
            ORDER BY p.created_at DESC LIMIT %s
        ''' if self.database_url else '''
            SELECT p.*, u.username, u.badge_type, u.avatar_url as user_avatar 
            FROM posts p 
            JOIN users u ON p.user_id = u.id 
            WHERE p.is_deleted = 0 AND p.post_type != 'news' AND p.channel_id IS NULL AND p.id > ?
            ORDER BY p.created_at DESC LIMIT ?
        '''
        cur.execute(sql, (after_id, limit))
        res = [dict(r) for r in cur.fetchall()]
        cur.close()
        conn.close()
        return res

    def get_news(self, limit=100, after_id=0):
        conn = self.get_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) if self.database_url else conn.cursor()
        sql = '''
            SELECT p.*, u.username, u.badge_type, u.avatar_url as user_avatar 
            FROM posts p 
            JOIN users u ON p.user_id = u.id 
            WHERE p.is_deleted = 0 AND p.post_type = 'news' AND p.id > %s
            ORDER BY p.created_at DESC LIMIT %s
        ''' if self.database_url else '''
            SELECT p.*, u.username, u.badge_type, u.avatar_url as user_avatar 
            FROM posts p 
            JOIN users u ON p.user_id = u.id 
            WHERE p.is_deleted = 0 AND p.post_type = 'news' AND p.id > ?
            ORDER BY p.created_at DESC LIMIT ?
        '''
        cur.execute(sql, (after_id, limit))
        res = [dict(r) for r in cur.fetchall()]
        cur.close()
        conn.close()
        return res

    def update_user_profile(self, user_id, bio=None, avatar_url=None):
        conn = self.get_connection()
        cur = conn.cursor()
        if bio:
            sql = "UPDATE users SET bio = %s WHERE id = %s" if self.database_url else "UPDATE users SET bio = ? WHERE id = ?"
            cur.execute(sql, (bio, user_id))
        if avatar_url:
            sql = "UPDATE users SET avatar_url = %s WHERE id = %s" if self.database_url else "UPDATE users SET avatar_url = ? WHERE id = ?"
            cur.execute(sql, (avatar_url, user_id))
        conn.commit()
        cur.close()
        conn.close()
        return True

    # --- CHANNELS & NODES ---
    def create_channel(self, owner_id, name, description, price, channel_type='private'):
        conn = self.get_connection()
        cur = conn.cursor()
        sql = '''
            INSERT INTO channels (owner_id, name, description, access_price, channel_type)
            VALUES (%s, %s, %s, %s, %s) RETURNING id
        ''' if self.database_url else '''
            INSERT INTO channels (owner_id, name, description, access_price, channel_type)
            VALUES (?, ?, ?, ?, ?)
        '''
        cur.execute(sql, (owner_id, name, description, price, channel_type))
        channel_id = cur.fetchone()[0] if self.database_url else cur.lastrowid
        
        # Owner automatically becomes a member
        mem_sql = "INSERT INTO channel_memberships (user_id, channel_id) VALUES (%s, %s)" if self.database_url else "INSERT INTO channel_memberships (user_id, channel_id) VALUES (?, ?)"
        cur.execute(mem_sql, (owner_id, channel_id))
        
        conn.commit()
        cur.close()
        conn.close()
        return channel_id

    def get_channels(self):
        conn = self.get_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) if self.database_url else conn.cursor()
        cur.execute("SELECT * FROM channels ORDER BY created_at DESC")
        res = [dict(r) for r in cur.fetchall()]
        cur.close()
        conn.close()
        return res

    # --- CHATS & MESSAGING ---
    def send_message(self, sender_id, receiver_id, content):
        conn = self.get_connection()
        cur = conn.cursor()
        # Re-using posts table for messages to keep it simple, or we can create a messages table.
        # For MVP, let's use a dedicated messages table for better structure.
        msg_table_sql = "CREATE TABLE IF NOT EXISTS messages (id SERIAL PRIMARY KEY, sender_id INTEGER, receiver_id INTEGER, content TEXT, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        cur.execute(msg_table_sql)
        
        sql = "INSERT INTO messages (sender_id, receiver_id, content) VALUES (%s, %s, %s)" if self.database_url else "INSERT INTO messages (sender_id, receiver_id, content) VALUES (?, ?, ?)"
        cur.execute(sql, (sender_id, receiver_id, content))
        conn.commit()
        cur.close()
        conn.close()
        return True

    def get_messages(self, user1, user2):
        conn = self.get_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) if self.database_url else conn.cursor()
        sql = '''
            SELECT * FROM messages 
            WHERE (sender_id = %s AND receiver_id = %s) OR (sender_id = %s AND receiver_id = %s)
            ORDER BY created_at ASC
        ''' if self.database_url else '''
            SELECT * FROM messages 
            WHERE (sender_id = ? AND receiver_id = ?) OR (sender_id = ? AND receiver_id = ?)
            ORDER BY created_at ASC
        '''
        cur.execute(sql, (user1, user2, user2, user1))
        res = [dict(r) for r in cur.fetchall()]
        cur.close()
        conn.close()
        return res

    def get_chats(self, user_id):
        conn = self.get_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) if self.database_url else conn.cursor()
        # Find all users this person has messaged
        sql = '''
            SELECT DISTINCT u.id, u.username, u.badge_type, u.avatar_url 
            FROM users u
            JOIN messages m ON (u.id = m.sender_id OR u.id = m.receiver_id)
            WHERE (m.sender_id = %s OR m.receiver_id = %s) AND u.id != %s
        ''' if self.database_url else '''
            SELECT DISTINCT u.id, u.username, u.badge_type, u.avatar_url 
            FROM users u
            JOIN messages m ON (u.id = m.sender_id OR u.id = m.receiver_id)
            WHERE (m.sender_id = ? OR m.receiver_id = ?) AND u.id != ?
        '''
        cur.execute(sql, (user_id, user_id, user_id))
        res = [dict(r) for r in cur.fetchall()]
        cur.close()
        conn.close()
        return res

    def update_user_profile(self, user_id, bio=None, avatar_url=None):
        conn = self.get_connection()
        cur = conn.cursor()
        if bio:
            sql = "UPDATE users SET bio = %s WHERE id = %s" if self.database_url else "UPDATE users SET bio = ? WHERE id = ?"
            cur.execute(sql, (bio, user_id))
        if avatar_url:
            sql = "UPDATE users SET avatar_url = %s WHERE id = %s" if self.database_url else "UPDATE users SET avatar_url = ? WHERE id = ?"
            cur.execute(sql, (avatar_url, user_id))
        conn.commit()
        cur.close()
        conn.close()
        return True

    # --- PAYMENTS & PROMOTIONS ---
    def initiate_simulated_payment(self, user_id, item_id, amount):
        pid = secrets.token_hex(8)
        conn = self.get_connection()
        cur = conn.cursor()
        sql = "CREATE TABLE IF NOT EXISTS payments (id TEXT PRIMARY KEY, user_id INTEGER, item_id TEXT, amount INTEGER, status TEXT DEFAULT 'pending')"
        cur.execute(sql)
        ins = "INSERT INTO payments (id, user_id, item_id, amount) VALUES (%s, %s, %s, %s)" if self.database_url else "INSERT INTO payments (id, user_id, item_id, amount) VALUES (?, ?, ?, ?)"
        cur.execute(ins, (pid, user_id, item_id, amount))
        conn.commit()
        cur.close()
        conn.close()
        return pid

    def get_simulated_payment(self, pid):
        conn = self.get_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) if self.database_url else conn.cursor()
        sql = "SELECT * FROM payments WHERE id = %s" if self.database_url else "SELECT * FROM payments WHERE id = ?"
        cur.execute(sql, (pid,))
        res = cur.fetchone()
        cur.close()
        conn.close()
        return dict(res) if res else None

    def complete_simulated_payment(self, pid):
        conn = self.get_connection()
        cur = conn.cursor()
        sql = "UPDATE payments SET status = 'completed' WHERE id = %s" if self.database_url else "UPDATE payments SET status = 'completed' WHERE id = ?"
        cur.execute(sql, (pid,))
        conn.commit()
        cur.close()
        conn.close()
        return True

    def upgrade_user_badge(self, user_id, item_id):
        badge = 'none'
        if 'Verified' in item_id: badge = 'verified'
        elif 'Gold' in item_id: badge = 'gold'
        elif 'Dev' in item_id: badge = 'dev'
        
        conn = self.get_connection()
        cur = conn.cursor()
        sql = "UPDATE users SET badge_type = %s WHERE id = %s" if self.database_url else "UPDATE users SET badge_type = ? WHERE id = ?"
        cur.execute(sql, (badge, user_id))
        conn.commit()
        cur.close()
        conn.close()
        return True

    # Mock stats for Dashboard
    def get_online_users(self): return random.randint(5, 15)
    def get_total_users(self): return 100 
    def get_total_posts(self): return 500 
    def get_total_channels(self): return 10 
