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

    # Mock stats for Dashboard
    def get_online_users(self): return random.randint(5, 15)
    def get_total_users(self): return 100 # Mock
    def get_total_posts(self): return 500 # Mock
    def get_total_channels(self): return 10 # Mock
