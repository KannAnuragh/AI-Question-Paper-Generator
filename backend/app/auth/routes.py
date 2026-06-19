from fastapi import APIRouter, Depends
from fastapi.security import OAuth2PasswordRequestForm
from sqlalchemy.orm import Session
from app.core.database import get_db
from app.core.security import create_access_token
from app.auth import schemas, service
from app.auth.dependencies import get_current_user
from app.auth.models import User

router = APIRouter(prefix="/auth", tags=["Auth"])

@router.post("/register", response_model=schemas.LoginResponse, status_code=201)
def register(body: schemas.RegisterRequest, db: Session = Depends(get_db)):
    user = service.register_user(db, body)
    return schemas.LoginResponse(
        access_token=create_access_token(user.id),
        refresh_token=service.create_refresh_token(db, user.id),
        user_id=user.id,
        name=user.name,
        email=user.email,
        role=user.role,
    )

@router.post("/token", response_model=schemas.LoginResponse)
def login(form: OAuth2PasswordRequestForm = Depends(), db: Session = Depends(get_db)):
    user = service.authenticate_user(db, form.username, form.password)
    return schemas.LoginResponse(
        access_token=create_access_token(user.id),
        refresh_token=service.create_refresh_token(db, user.id),
        user_id=user.id,
        name=user.name,
        email=user.email,
        role=user.role,
    )

@router.post("/refresh", response_model=schemas.TokenResponse)
def refresh_token(body: schemas.RefreshRequest, db: Session = Depends(get_db)):
    access_token = service.refresh_access_token(db, body.refresh_token)
    return schemas.TokenResponse(access_token=access_token)

@router.post("/logout")
def logout(body: schemas.RefreshRequest, db: Session = Depends(get_db)):
    service.logout_user(db, body.refresh_token)
    return {"message": "Logged out"}

@router.get("/me", response_model=schemas.UserOut)
def me(current_user: User = Depends(get_current_user)):
    return current_user
