import os
from fastapi import FastAPI, Request, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session

from .core.config import get_settings
from .models.base import Base, engine, get_db
from .api.api_v1.api import api_router
from .core.security import get_current_active_user, create_first_superuser

# Create database tables
Base.metadata.create_all(bind=engine)

app = FastAPI(title="SQLite Web Viewer")

# Set up CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include API routes
app.include_router(api_router, prefix="/api/v1")

# Create static and templates directories if they don't exist
os.makedirs("app/static", exist_ok=True)
os.makedirs("app/templates", exist_ok=True)

# Mount static files
app.mount("/static", StaticFiles(directory="app/static"), name="static")
templates = Jinja2Templates(directory="app/templates")

# Create first superuser on startup
@app.on_event("startup")
def startup_event():
    db = next(get_db())
    try:
        create_first_superuser(db)
    finally:
        db.close()

# Root endpoint
@app.get("/")
async def root():
    return {"message": "Welcome to SQLite Web Viewer API. Use /docs for API documentation."}

# Health check endpoint
@app.get("/health")
async def health_check():
    return {"status": "ok"}
