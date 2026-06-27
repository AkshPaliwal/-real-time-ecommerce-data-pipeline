from fastapi import FastAPI
from sqlalchemy import text
from .database import engine

app = FastAPI(
    title="E-Commerce Analytics API",
    version="1.0.0"
)

@app.get("/")
def root():
    return {"message": "FastAPI is running 🚀"}
@app.get("/health/database")
def database_health():
    try:
        with engine.connect() as connection:
            connection.execute(text("SELECT 1"))

        return {
            "status": "healthy",
            "database": "connected"
        }

    except Exception as e:
        return {
            "status": "failed",
            "error": str(e)
        }