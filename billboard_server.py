from fastapi import FastAPI, Request, HTTPException, Form, File, UploadFile, Depends, BackgroundTasks, status, Query
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional, Dict
import os
import secrets
import datetime
import mimetypes
import logging
import json
import uvicorn
from billboard_logic import BillboardManager
from pydantic import BaseModel

class VerifyEmailBody(BaseModel):
    email: str
    code: str
    user_data: dict

# Initialize FastAPI
app = FastAPI()

# --- SELF-HEALING DIRECTORIES ---
for d in ["static", "uploads", "uploads/dev_certs"]:
    if not os.path.exists(d):
        os.makedirs(d)
        print(f"[System] Created missing directory: {d}")

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Static files
app.mount("/static", StaticFiles(directory="static"), name="static")
app.mount("/uploads", StaticFiles(directory="uploads"), name="uploads")

# Database Manager
db = BillboardManager("studio_billboard.db")

# Configure logging
logging.basicConfig(level=logging.INFO, filename='billboard.log',
                    format='%(asctime)s - %(levelname)s - %(message)s')

class MessageCreate(BaseModel):
    sender_id: int
    receiver_id: int
    content: str

class ChannelCreate(BaseModel):
    owner_id: int
    name: str
    description: str
    price: int

class RegisterData(BaseModel):
    username: str
    password: str
    email: str
    phone: str
    full_names: str
    home_address: str

class LoginData(BaseModel):
    username: str
    password: str

# --- ROUTES ---

@app.get("/", response_class=FileResponse)
async def read_root():
    return FileResponse("billboard.html")

@app.get("/status")
async def get_status():
    return {
        "status": "ok", 
        "users_online": db.get_online_users(), 
        "total_users": db.get_total_users(), 
        "total_posts": db.get_total_posts(), 
        "total_channels": db.get_total_channels()
    }

@app.post("/register")
async def register(data: RegisterData):
    if db.get_user_by_username(data.username) or db.get_user_by_email(data.email):
        raise HTTPException(status_code=400, detail="Username or Email already registered.")
    
    user_id = db.create_user(data.username, data.password, data.email, data.phone, data.full_names, data.home_address)
    if user_id:
        return {"message": "Verification email sent.", "user_id": user_id}
    raise HTTPException(status_code=500, detail="User registration failed.")

@app.post("/verify-email")
async def verify_email(body: VerifyEmailBody):
    if db.verify_email(body.email, body.code):
        user = db.get_user_by_email(body.email)
        return {"message": "Email verified successfully.", "user_id": user['id']}
    raise HTTPException(status_code=400, detail="Invalid verification code.")

@app.post("/login")
async def login(data: LoginData):
    user = db.get_user_by_username(data.username)
    hashed_pw = db.hash_password(data.password)
    if user and user['password'] == hashed_pw:
        return user
    raise HTTPException(status_code=401, detail="Invalid credentials.")

@app.get("/get-user/{username}")
async def get_user_profile(username: str):
    user = db.get_user_by_username(username)
    if user: return user
    raise HTTPException(status_code=404, detail="User not found.")

@app.post("/update-profile")
async def update_profile(user_id: int = Form(...), bio: Optional[str] = Form(None), avatar: Optional[UploadFile] = File(None)):
    avatar_url = None
    if avatar:
        upload_dir = "uploads"
        os.makedirs(upload_dir, exist_ok=True)
        ext = mimetypes.guess_extension(avatar.content_type) or ".jpg"
        filename = f"{user_id}_av_{secrets.token_hex(4)}{ext}"
        file_path = os.path.join(upload_dir, filename)
        with open(file_path, "wb") as f: f.write(await avatar.read())
        avatar_url = f"uploads/{filename}"

    if db.update_user_profile(user_id, bio, avatar_url):
        return {"message": "Profile updated", "avatar_url": avatar_url}
    raise HTTPException(status_code=500, detail="Update failed.")

@app.post("/post")
async def create_post(user_id: int = Form(...), content: str = Form(...), post_type: str = Form(...), channel_id: Optional[int] = Form(None), media: Optional[UploadFile] = File(None)):
    if not db.is_email_verified(user_id):
        raise HTTPException(status_code=403, detail="Email not verified.")

    media_url = None
    media_type = None
    if media:
        upload_dir = "uploads"
        os.makedirs(upload_dir, exist_ok=True)
        ext = mimetypes.guess_extension(media.content_type) or ".bin"
        filename = f"{user_id}_post_{secrets.token_hex(4)}{ext}"
        file_path = os.path.join(upload_dir, filename)
        with open(file_path, "wb") as f: f.write(await media.read())
        media_url = f"uploads/{filename}"
        media_type = media.content_type.split('/')[0]

    post_id = db.create_post(user_id, content, post_type, channel_id, media_url, media_type)
    return {"message": "Post created", "post_id": post_id}

@app.get("/feed")
async def get_feed_api(limit: int = 100, after_id: int = Query(0)):
    return db.get_feed(limit=limit, after_id=after_id)

@app.get("/news")
async def get_news_api(limit: int = 100, after_id: int = Query(0)):
    return db.get_news(limit=limit, after_id=after_id)

@app.get("/messages/{user_id}/{other_id}")
async def get_messages(user_id: int, other_id: int):
    return db.get_chat_messages(user_id, other_id)

@app.post("/send-message")
async def send_message(msg: MessageCreate):
    if not db.is_email_verified(msg.sender_id):
        raise HTTPException(status_code=403, detail="Email not verified.")
    mid = db.send_chat_message(msg.sender_id, msg.receiver_id, msg.content)
    return {"message_id": mid}

@app.get("/chat-list/{user_id}")
async def get_chat_list(user_id: int):
    return db.get_user_chat_list(user_id)

@app.get("/channels")
async def get_channels():
    return db.get_all_channels()

@app.post("/create-channel")
async def create_channel(c: ChannelCreate):
    cid = db.create_channel(c.owner_id, c.name, c.description, c.price)
    return {"channel_id": cid}

@app.get("/channel-feed/{channel_id}")
async def get_channel_feed_api(channel_id: int, user_id: int):
    if not db.is_channel_member(user_id, channel_id):
        raise HTTPException(status_code=403, detail="Access Denied")
    return db.get_channel_feed(channel_id)

@app.post("/join-channel/{channel_id}")
async def join_channel(channel_id: int, user_id: int = Form(...)):
    db.add_channel_member(channel_id, user_id)
    return {"message": "Joined"}

class PaymentData(BaseModel):
    user_id: int
    item_id: str
    amount: int

class PaymentConfirmData(BaseModel):
    payment_id: str
    test_code: str

@app.post("/initiate-payment")
async def initiate_payment(data: PaymentData):
    pid = db.initiate_simulated_payment(data.user_id, data.item_id, data.amount)
    return {"payment_id": pid}

@app.post("/simulated-payment/confirm")
async def confirm_payment(data: PaymentConfirmData):
    if data.test_code == "0000":
        pay_info = db.get_simulated_payment(data.payment_id)
        if pay_info:
            db.complete_simulated_payment(data.payment_id)
            db.upgrade_user_badge(pay_info['user_id'], pay_info['item_id'])
            return {"message": "Success"}
    raise HTTPException(status_code=400, detail="Failed")

@app.get("/admin/pending-payments")
async def get_pending_payments(): return db.get_pending_simulated_payments()

@app.get("/admin/pending-devs")
async def get_pending_devs(): return db.get_pending_dev_applications()

@app.get("/admin/reports")
async def get_reports(): return db.get_all_reports()

@app.post("/admin/approve-payment/{pid}")
async def approve_payment_admin(pid: str):
    pay_info = db.get_simulated_payment(pid)
    if pay_info:
        db.complete_simulated_payment(pid)
        db.upgrade_user_badge(pay_info['user_id'], pay_info['item_id'])
    return {"message": "Approved"}

@app.post("/apply-dev")
async def apply_dev(user_id: int = Form(...), details: str = Form(...), cert_pdf: Optional[UploadFile] = File(None)):
    pdf_url = None
    if cert_pdf:
        os.makedirs("uploads/dev_certs", exist_ok=True)
        filename = f"{user_id}_dev_{secrets.token_hex(4)}.pdf"
        pdf_url = f"uploads/dev_certs/{filename}"
        with open(pdf_url, "wb") as f: f.write(await cert_pdf.read())
    db.create_dev_application(user_id, details, pdf_url)
    return {"message": "Submitted"}

@app.post("/admin/approve-dev/{app_id}")
async def approve_dev_admin(app_id: int):
    db.approve_dev_application(app_id)
    return {"message": "Approved"}

@app.post("/admin/delete-post/{pid}")
async def delete_post_admin(pid: int):
    db.delete_post(pid)
    return {"message": "Deleted"}

@app.post("/admin/mute-user/{username}")
async def mute_user_admin(username: str):
    db.mute_user(username)
    return {"message": "Muted"}

@app.post("/report-post/{pid}")
async def report_post_api(pid: int, user_id: int = Form(...)):
    db.report_post(pid, user_id)
    return {"message": "Reported"}

@app.get("/unreads/{user_id}")
async def get_unreads(user_id: int):
    return {"wall": 0, "pulse": 0, "nodes": 0, "chats": 0}

@app.post("/mark-read/{user_id}/{cat}")
async def mark_read(user_id: int, cat: str):
    return {"status": "ok"}

@app.exception_handler(404)
async def custom_404(request: Request, exc: HTTPException):
    if request.url.path.startswith("/api") or request.url.path.startswith("/uploads"):
        return JSONResponse(status_code=404, content={"detail": "Not Found"})
    return FileResponse("billboard.html")

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8006)
