from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
import redis
import json
import os
from typing import Dict, Any
from utils.redis_utils import connect_Redis

app = FastAPI(
    title="Qyrus Assignment API",
    description="FastAPI application for message processing with Redis and SQS",
    version="1.0.0"
)

# Redis connection
redis_client = connect_Redis(host="redis", port=6379, db=0, password=None)

@app.get("/")
async def root():
    """Root endpoint with API information"""
    return {
        "message": "Welcome to Qyrus Assignment API",
        "version": "1.0.0",
        "docs": "/docs",
        "health": "/health"
    }

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    try:
        # Test Redis connection
        redis_client.ping()
        redis_status = "connected"
    except Exception as e:
        redis_status = f"error: {str(e)}"
    
    return {
        "status": "healthy",
        "services": {
            "redis": redis_status,
            "api": "running"
        }
    }

@app.get("/stats/global")
async def get_global_stats():
    """Get global statistics"""
    try:
        total_orders = redis_client.hget('global:stats', 'total_orders') or 0
        total_revenue = redis_client.hget('global:stats', 'total_revenue') or 0
        return {
            "total_orders": int(total_orders),
            "total_revenue": float(total_revenue)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving global stats: {str(e)}")

@app.get("/users/{user_id}/stats")
async def get_user_stats(user_id: str):
    """Get statistics for a specific user"""
    try:
        user_stats = redis_client.hgetall(f"user:{user_id}")
        if not user_stats:
            return {"order_count": 0, "total_spend": 0.0}
        return {
            "user_id": user_id,
            "order_count": int(user_stats.get('order_count', 0)),
            "total_spend": float(user_stats.get('total_spend', 0.0))
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving user stats: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
