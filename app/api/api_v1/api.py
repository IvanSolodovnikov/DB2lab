from fastapi import APIRouter

from app.api.endpoints import auth, tables

api_router = APIRouter()

api_router.include_router(auth.router, prefix="/auth", tags=["authentication"])
api_router.include_router(tables.router, prefix="/api/v1", tags=["tables"])

