from fastapi import FastAPI, HTTPException, Query
from typing import List
from utils.redis_utils import connect_Redis
import datetime

def get_dates_for_period(period: str, date_str: str) -> List[str]:
    """Get all dates for the given period"""
    if period == 'd':
        return [date_str]
    elif period == 'w':
        year, week = map(int, date_str.split('-'))
        # Get Monday of that ISO week
        first_day = datetime.date.fromisocalendar(year, week, 1)  # 1 = Monday
        return [(first_day + datetime.timedelta(days=i)).strftime('%Y-%m-%d') for i in range(7)]
    elif period == 'm':
        # date_str format: YYYY-MM
        year, month = map(int, date_str.split('-'))
        first_day = datetime.date(year, month, 1)
        next_month = datetime.date(year + (month // 12), (month % 12) + 1, 1)
        days = (next_month - first_day).days
        return [(first_day + datetime.timedelta(days=i)).strftime('%Y-%m-%d') for i in range(days)]
    elif period == 'y':
        # date_str format: YYYY
        year = int(date_str)
        first_day = datetime.date(year, 1, 1)
        next_year = datetime.date(year + 1, 1, 1)
        days = (next_year - first_day).days
        return [(first_day + datetime.timedelta(days=i)).strftime('%Y-%m-%d') for i in range(days)]
    return []

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

@app.get("/top-users")
async def get_top_users(
    period: str = Query(..., description="Period: 'd' for daily, 'w' for weekly, 'm' for monthly, 'y' for yearly"),
    date: str = Query(..., description="Date in format: YYYY-MM-DD for daily, YYYY-WW for weekly, YYYY-MM for monthly, YYYY for yearly"),
    n: int = Query(10, description="Number of top users to return")
):
    """Get top N users by total spend for the specified period and date"""
    try:
        dates = get_dates_for_period(period, date)
        if not dates:
            raise HTTPException(status_code=400, detail="Invalid date format for period")
        
        if period == 'd':
            # Direct from daily sorted set
            key = f"daily:{date}"
            top_users_with_scores = redis_client.zrevrange(key, 0, n-1, withscores=True)
            hash_prefix = f"user:{{}}:{date}"
        else:
            daily_keys = [f"daily:{d}" for d in dates]
            temp_key = f"temp:top:{period}:{date}"
            
            redis_client.zunionstore(temp_key, daily_keys, aggregate='SUM')
            
            # Get top N from aggregated set
            top_users_with_scores = redis_client.zrevrange(temp_key, 0, n-1, withscores=True)
            
            hash_prefix = None  
            redis_client.delete(temp_key)
        
        # Format response
        top_users = []
        for user_id_bytes, score in top_users_with_scores:
            user_id = user_id_bytes.decode('utf-8') if isinstance(user_id_bytes, bytes) else user_id_bytes
            
            if period == 'd':
                hash_key = hash_prefix.format(user_id)
                order_count = redis_client.hget(hash_key, 'order_count')
                order_count = int(order_count) if order_count else 0
            else:
                # Sum order_count across all dates for this user
                order_count = 0
                for d in dates:
                    hash_key = f"user:{user_id}:{d}"
                    count = redis_client.hget(hash_key, 'order_count')
                    if count:
                        order_count += int(count)
            
            top_users.append({
                "user_id": user_id,
                "order_count": order_count,
                "total_spend": float(score)
            })
        
        return {
            "period": period,
            "date": date,
            "top_users": top_users
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving top users: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
