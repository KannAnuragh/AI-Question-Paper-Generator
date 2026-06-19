from sqlalchemy.orm import Session
from fastapi import HTTPException
from datetime import datetime, timezone
from app.auth.models import User, RefreshToken
from app.auth.schemas import RegisterRequest
from app.core.security import hash_password, verify_password, create_access_token
from app.core.config import settings
import uuid
from datetime import timedelta

def register_user(db: Session, body: RegisterRequest) -> User:
    if db.query(User).filter(User.email == body.email).first():
        raise HTTPException(status_code=409, detail="Email already registered")
    user = User(
        email=body.email,
        name=body.name,
        hashed_password=hash_password(body.password),
        institution=body.institution,
    )
    db.add(user)
    db.commit()
    db.refresh(user)
    return user

def authenticate_user(db: Session, email: str, password: str) -> User:
    user = db.query(User).filter(User.email == email, User.is_active == True).first()
    if not user or not verify_password(password, user.hashed_password):
        raise HTTPException(status_code=401, detail="Invalid email or password")
    return user

def create_refresh_token(db: Session, user_id: str) -> str:
    token = str(uuid.uuid4())
    expires = datetime.now(timezone.utc) + timedelta(days=settings.REFRESH_TOKEN_EXPIRE_DAYS)
    db.add(RefreshToken(token=token, user_id=user_id, expires_at=expires))
    db.commit()
    return token

def refresh_access_token(db: Session, refresh_token: str) -> str:
    record = db.query(RefreshToken).filter(
        RefreshToken.token == refresh_token,
        RefreshToken.revoked == False,
    ).first()
    if not record:
        raise HTTPException(status_code=401, detail="Invalid or expired refresh token")
    if record.expires_at.replace(tzinfo=timezone.utc) < datetime.now(timezone.utc):
        raise HTTPException(status_code=401, detail="Refresh token expired")
    record.revoked = True
    db.commit()
    
    create_refresh_token(db, record.user_id) # Optionally create a new refresh token, but here we just return the new access token, wait the original did create_refresh_token too, but didn't return it in TokenResponse. Let's just create access token.
    return create_access_token(record.user_id)

def logout_user(db: Session, refresh_token: str):
    record = db.query(RefreshToken).filter(RefreshToken.token == refresh_token).first()
    if record:
        record.revoked = True
        db.commit()
