from fastapi import APIRouter, Query
from sqlalchemy import text
from ..database import engine
from typing import Optional
from collections import defaultdict
from datetime import date, timedelta

router = APIRouter(prefix="/dashboard", tags=["Dashboard"])


def build_where(city=None, category=None, date_from=None, date_to=None):
    conditions = []
    params = {}
    if city:
        conditions.append("city = :city")
        params["city"] = city
    if category:
        conditions.append("category = :category")
        params["category"] = category
    if date_from:
        conditions.append("timestamp >= :date_from")
        params["date_from"] = date_from
    if date_to:
        conditions.append("timestamp <= :date_to")
        params["date_to"] = date_to
    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""
    return where, params


@router.get("/summary")
def dashboard_summary(
    city: Optional[str] = Query(None),
    category: Optional[str] = Query(None),
    date_from: Optional[str] = Query(None),
    date_to: Optional[str] = Query(None),
):
    where, params = build_where(city, category, date_from, date_to)
    with engine.connect() as conn:
        result = conn.execute(text(f"""
            SELECT
                COUNT(*) AS total_orders,
                SUM(total_amount) AS total_revenue,
                COUNT(DISTINCT customer_id) AS total_customers,
                AVG(total_amount) AS average_order_value
            FROM orders {where};
        """), params).fetchone()
        return {
            "total_orders": result.total_orders,
            "total_revenue": float(result.total_revenue or 0),
            "total_customers": result.total_customers,
            "average_order_value": float(result.average_order_value or 0)
        }


@router.get("/revenue-by-city")
def revenue_by_city(
    city: Optional[str] = Query(None),
    category: Optional[str] = Query(None),
    date_from: Optional[str] = Query(None),
    date_to: Optional[str] = Query(None),
):
    where, params = build_where(city, category, date_from, date_to)
    with engine.connect() as conn:
        result = conn.execute(text(f"""
            SELECT city, SUM(total_amount) AS revenue
            FROM orders {where}
            GROUP BY city ORDER BY revenue DESC;
        """), params)
        return [{"city": r.city, "revenue": float(r.revenue)} for r in result]


@router.get("/category-sales")
def category_sales(
    city: Optional[str] = Query(None),
    category: Optional[str] = Query(None),
    date_from: Optional[str] = Query(None),
    date_to: Optional[str] = Query(None),
):
    where, params = build_where(city, category, date_from, date_to)
    with engine.connect() as conn:
        result = conn.execute(text(f"""
            SELECT category, SUM(total_amount) AS sales
            FROM orders {where}
            GROUP BY category ORDER BY sales DESC;
        """), params)
        return [{"category": r.category, "sales": float(r.sales)} for r in result]


@router.get("/order-status")
def order_status(
    city: Optional[str] = Query(None),
    category: Optional[str] = Query(None),
    date_from: Optional[str] = Query(None),
    date_to: Optional[str] = Query(None),
):
    where, params = build_where(city, category, date_from, date_to)
    with engine.connect() as conn:
        result = conn.execute(text(f"""
            SELECT status, COUNT(*) AS total
            FROM orders {where}
            GROUP BY status;
        """), params)
        return [{"status": r.status, "count": r.total} for r in result]


@router.get("/monthly-revenue")
def monthly_revenue(
    city: Optional[str] = Query(None),
    category: Optional[str] = Query(None),
    date_from: Optional[str] = Query(None),
    date_to: Optional[str] = Query(None),
):
    where, params = build_where(city, category, date_from, date_to)
    with engine.connect() as conn:
        result = conn.execute(text(f"""
            SELECT TO_CHAR(timestamp, 'YYYY-MM') AS month,
                SUM(total_amount) AS revenue
            FROM orders {where}
            GROUP BY month ORDER BY month;
        """), params)
        return [{"month": r.month, "revenue": float(r.revenue)} for r in result]


@router.get("/top-products")
def top_products(
    city: Optional[str] = Query(None),
    category: Optional[str] = Query(None),
    date_from: Optional[str] = Query(None),
    date_to: Optional[str] = Query(None),
):
    where, params = build_where(city, category, date_from, date_to)
    with engine.connect() as conn:
        result = conn.execute(text(f"""
            SELECT product_name,
                SUM(quantity) AS total_quantity,
                SUM(total_amount) AS total_revenue
            FROM orders {where}
            GROUP BY product_name
            ORDER BY total_revenue DESC LIMIT 10;
        """), params)
        return [{"product_name": r.product_name, "total_quantity": r.total_quantity, "total_revenue": float(r.total_revenue)} for r in result]


@router.get("/sales-prediction")
def sales_prediction(
    city: Optional[str] = Query(None),
    category: Optional[str] = Query(None),
    date_from: Optional[str] = Query(None),
    date_to: Optional[str] = Query(None),
):
    conditions = ["timestamp >= NOW() - INTERVAL '90 days'"]
    params = {}
    if city:
        conditions.append("city = :city")
        params["city"] = city
    if category:
        conditions.append("category = :category")
        params["category"] = category
    if date_from:
        conditions.append("timestamp >= :date_from")
        params["date_from"] = date_from
    if date_to:
        conditions.append("timestamp <= :date_to")
        params["date_to"] = date_to
    where = "WHERE " + " AND ".join(conditions)

    with engine.connect() as conn:
        rows = conn.execute(text(f"""
            SELECT DATE(timestamp) AS day, SUM(total_amount) AS revenue
            FROM orders {where}
            GROUP BY day ORDER BY day;
        """), params).fetchall()

        actual = [{"day": str(r[0]), "revenue": float(r[1])} for r in rows]
        if len(actual) < 3:
            return {"actual": actual, "predicted": [], "note": "Not enough data"}

        revenues = [r["revenue"] for r in actual]
        n = len(revenues)
        first_half  = sum(revenues[:n//2]) / (n//2)
        second_half = sum(revenues[n//2:]) / (n - n//2)
        daily_trend = (second_half - first_half) / (n//2)

        last_day = date.fromisoformat(actual[-1]["day"])
        last_rev = revenues[-1]

        predicted = []
        for i in range(1, 8):
            pred_day = last_day + timedelta(days=i)
            pred_rev = max(0, last_rev + daily_trend * i)
            predicted.append({"day": str(pred_day), "predicted": round(pred_rev, 2)})

        return {"actual": actual, "predicted": predicted}


@router.get("/category-forecast")
def category_forecast(
    city: Optional[str] = Query(None),
    category: Optional[str] = Query(None),
    date_from: Optional[str] = Query(None),
    date_to: Optional[str] = Query(None),
):
    conditions = ["timestamp >= NOW() - INTERVAL '90 days'"]
    params = {}
    if city:
        conditions.append("city = :city")
        params["city"] = city
    if category:
        conditions.append("category = :category")
        params["category"] = category
    if date_from:
        conditions.append("timestamp >= :date_from")
        params["date_from"] = date_from
    if date_to:
        conditions.append("timestamp <= :date_to")
        params["date_to"] = date_to
    where = "WHERE " + " AND ".join(conditions)

    with engine.connect() as conn:
        rows = conn.execute(text(f"""
            SELECT category, DATE_TRUNC('week', timestamp) AS week,
                SUM(total_amount) AS revenue, COUNT(*) AS orders
            FROM orders {where}
            GROUP BY category, week ORDER BY category, week;
        """), params).fetchall()

        cat_data = defaultdict(list)
        for r in rows:
            cat_data[r[0]].append({"week": str(r[1])[:10], "revenue": float(r[2]), "orders": r[3]})

        result = []
        for cat, weeks in cat_data.items():
            if len(weeks) >= 2:
                first_rev = weeks[0]["revenue"]
                last_rev  = weeks[-1]["revenue"]
                growth = ((last_rev - first_rev) / first_rev * 100) if first_rev > 0 else 0
                total_rev = sum(w["revenue"] for w in weeks)
                result.append({
                    "category": cat,
                    "total_revenue": round(total_rev, 2),
                    "growth_pct": round(growth, 1),
                    "trend": "rising" if growth > 5 else "falling" if growth < -5 else "stable",
                    "weekly_data": weeks
                })

        result.sort(key=lambda x: x["growth_pct"], reverse=True)
        return result


@router.get("/inventory-alert")
def inventory_alert(
    city: Optional[str] = Query(None),
    category: Optional[str] = Query(None),
    date_from: Optional[str] = Query(None),
    date_to: Optional[str] = Query(None),
):
    conditions = ["timestamp >= NOW() - INTERVAL '90 days'"]
    params = {}
    if city:
        conditions.append("city = :city")
        params["city"] = city
    if category:
        conditions.append("category = :category")
        params["category"] = category
    if date_from:
        conditions.append("timestamp >= :date_from")
        params["date_from"] = date_from
    if date_to:
        conditions.append("timestamp <= :date_to")
        params["date_to"] = date_to
    where = "WHERE " + " AND ".join(conditions)

    with engine.connect() as conn:
        rows = conn.execute(text(f"""
            SELECT product_name, category,
                SUM(quantity) AS total_sold, COUNT(*) AS order_count,
                AVG(quantity) AS avg_qty, MAX(timestamp) AS last_ordered,
                SUM(total_amount) AS total_revenue
            FROM orders {where}
            GROUP BY product_name, category
            ORDER BY total_sold DESC LIMIT 15;
        """), params).fetchall()

        result = []
        for r in rows:
            velocity = round((r[3] or 0) / 90, 2)
            risk = "critical" if velocity > 2 else "high" if velocity > 1 else "medium" if velocity > 0.5 else "low"
            result.append({
                "product_name": r[0], "category": r[1],
                "total_sold": int(r[2] or 0), "order_count": int(r[3] or 0),
                "avg_qty_per_order": round(float(r[4] or 0), 1),
                "last_ordered": str(r[5])[:10] if r[5] else None,
                "total_revenue": round(float(r[6] or 0), 2),
                "velocity_per_day": velocity, "risk_level": risk
            })
        return result


@router.get("/customer-churn")
def customer_churn(
    city: Optional[str] = Query(None),
    category: Optional[str] = Query(None),
    date_from: Optional[str] = Query(None),
    date_to: Optional[str] = Query(None),
):
    where, params = build_where(city, category, date_from, date_to)
    with engine.connect() as conn:
        rows = conn.execute(text(f"""
            SELECT customer_id, customer_name,
                COUNT(*) AS total_orders,
                MAX(timestamp) AS last_order,
                SUM(total_amount) AS total_spent,
                NOW() - MAX(timestamp) AS days_since_last
            FROM orders {where}
            GROUP BY customer_id, customer_name
            ORDER BY last_order ASC;
        """), params).fetchall()

        churned = 0
        at_risk = 0
        active  = 0
        high_value_churn = []

        for r in rows:
            days_since  = r[5].days if r[5] else 999
            total_spent = float(r[4] or 0)

            if days_since > 30:
                churned += 1
                if total_spent > 50000:
                    high_value_churn.append({
                        "customer_id": r[0], "customer_name": r[1],
                        "total_orders": r[2], "days_since_last": days_since,
                        "total_spent": round(total_spent, 2), "risk": "high"
                    })
            elif days_since > 10:
                at_risk += 1
            else:
                active += 1

        return {
            "summary": {"active": active, "at_risk": at_risk, "churned": churned, "total": active + at_risk + churned},
            "high_value_at_risk": sorted(high_value_churn, key=lambda x: x["total_spent"], reverse=True)[:10]
        }
