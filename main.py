import os
import psycopg2
from psycopg2.extras import RealDictCursor
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pathlib import Path
import json
from fastapi import HTTPException
from datetime import datetime, timedelta
from pydantic import BaseModel
from typing import Optional,List
import difflib
from routers.chat import router as chat_router
from routers.analytics import router as analytics_router
from services.llm_service import get_llm_provider, MFBESTIE_SYSTEM_PROMPT
from google.cloud import storage

app = FastAPI(title="MF Advisor API", version="1.0")

app.include_router(chat_router)
app.include_router(analytics_router)

# CORS Configuration
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------------------------------------------
# Load Data
# ---------------------------------------------------


# Smart path that works everywhere
def get_data_file_path():
    # Option 1: Same level as main.py
    option1 = Path(__file__).resolve().parent / "data" / "scheme_metrics_merged.json"
    if option1.exists():
        return option1
    
    # Option 2: One level up
    option2 = Path(__file__).resolve().parent.parent / "data" / "scheme_metrics_merged.json"
    if option2.exists():
        return option2
    
    # Option 3: Two levels up
    option3 = Path(__file__).resolve().parent.parent.parent / "data" / "scheme_metrics_merged.json"
    if option3.exists():
        return option3
    
    # Default to option 1
    return option1

DATA_FILE = get_data_file_path()

# Load NAV Data

def get_nav_data_file_path():
    # Option 1: Same level as main.py
    option1 = Path(__file__).resolve().parent / "data" / "parent_scheme_nav.json"
    if option1.exists():
        return option1
    
    # Option 2: One level up
    option2 = Path(__file__).resolve().parent.parent / "data" / "parent_scheme_nav.json"
    if option2.exists():
        return option2
    
    # Option 3: Two levels up
    option3 = Path(__file__).resolve().parent.parent.parent / "data" / "parent_scheme_nav.json"
    if option3.exists():
        return option3
    
    # Default to option 1
    return option1

NAV_DATA_FILE = get_nav_data_file_path()


FUNDS_DATA = {}





# Database connection
def get_db_connection():
    database_url = os.getenv("DATABASE_URL")
    #database_url = "postgresql://postgres:admin@localhost:5432/mf_advisor"
    if not database_url:
        raise Exception("DATABASE_URL not set")
    conn = psycopg2.connect(database_url, cursor_factory=RealDictCursor)
    return conn


# ---------------------------------------------------
# NAV Data from PostgreSQL
# ---------------------------------------------------

def get_nav_from_db(scheme_code: int):
    """Get NAV data for a scheme from PostgreSQL"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT nav_date, nav_value, scheme_name, fund_house
            FROM scheme_nav 
            WHERE scheme_code = %s
            ORDER BY nav_date DESC
        """, (str(scheme_code),))
        
        results = cursor.fetchall()
        cursor.close()
        conn.close()
        
        if not results:
            return None
        
        # Convert to format matching old NAV_DATA_MAP
        return {
            'name': results[0]['scheme_name'],
            'fund_house': results[0]['fund_house'],
            'data': [
                {
                    'date': row['nav_date'].strftime('%d-%m-%Y'),
                    'nav': str(row['nav_value'])
                }
                for row in results
            ]
        }
    except Exception as e:
        print(f"❌ Error fetching NAV for scheme {scheme_code}: {e}")
        return None





# ---------------------------------------------------
# Category Display Helpers
# ---------------------------------------------------

def get_category_emoji(main_category):
    """Get emoji for category"""
    emojis = {
        "Equity": "📈",
        "Debt": "🏦",
        "Hybrid": "⚖️",
        "Income": "💰",
        "Solution Oriented": "🎯",
        "Other": "📊"
    }
    return emojis.get(main_category, "📊")

def load_data():
    global FUNDS_DATA
    
    # 1. Detect if we are running in Google Cloud Run
    is_cloud_run = os.environ.get('K_SERVICE') is not None
    
    # 2. If in the cloud, download the file from GCS first
    if is_cloud_run:
        print("☁️ Running in Cloud Run! Downloading data from GCS...")
        try:
            # Ensure the /app/data directory exists
            os.makedirs(os.path.dirname(DATA_FILE), exist_ok=True)
            
            # Connect to GCS and download the file
            storage_client = storage.Client()
            bucket = storage_client.bucket("run-sources-mf-advisor-487108-asia-south1")
            blob = bucket.blob("scheme_metrics_merged.json")
            
            blob.download_to_filename(DATA_FILE)
            print("✅ Successfully downloaded scheme_metrics_merged.json from GCS")
        except Exception as e:
            print(f"❌ Failed to download from GCS: {e}")
    else:
        print("💻 Running locally. Skipping GCS download.")

    # 3. Load the data into memory (Works for both Cloud and Local)
    try:
        with open(DATA_FILE, 'r', encoding='utf-8') as f:
            FUNDS_DATA = json.load(f)
        print(f"✅ Loaded {len(FUNDS_DATA)} funds from {DATA_FILE}")
    except Exception as e:
        print(f"❌ Error loading data: {e}")
        FUNDS_DATA = {}

load_data()

# ---------------------------------------------------
# Helper: Calculate Composite Score (for ranking)
# ---------------------------------------------------

def calculate_composite_score(metrics):
    """
    Calculate 0-100 composite score based on key metrics
    Used for ranking funds
    """
    if not metrics or not metrics.get('is_statistically_reliable'):
        return 0
    
    score = 0
    
    # RETURNS (40 points)
    cagr = metrics.get('cagr', 0)
    if cagr:
        if cagr > 0.15: score += 20
        elif cagr > 0.12: score += 15
        elif cagr > 0.10: score += 10
        elif cagr > 0.08: score += 5
    
    rolling_3y = metrics.get('rolling_3y')
    if rolling_3y:
        if rolling_3y > 0.15: score += 10
        elif rolling_3y > 0.12: score += 7
        elif rolling_3y > 0.10: score += 5
    
    consistency = metrics.get('consistency_score', 0)
    if consistency:
        if consistency > 70: score += 10
        elif consistency > 60: score += 7
        elif consistency > 50: score += 5
    
    # RISK (30 points)
    sharpe = metrics.get('sharpe', 0)
    if sharpe:
        if sharpe > 2: score += 15
        elif sharpe > 1: score += 10
        elif sharpe > 0.5: score += 5
    
    max_dd = metrics.get('max_drawdown', 0)
    if max_dd:
        if max_dd > -0.20: score += 10
        elif max_dd > -0.30: score += 5
    
    sortino = metrics.get('sortino', 0)
    if sortino:
        if sortino > 2: score += 5
        elif sortino > 1: score += 3
    
    # RISK-ADJUSTED (20 points)
    calmar = metrics.get('calmar_ratio')
    if calmar:
        if calmar > 2: score += 10
        elif calmar > 1: score += 7
        elif calmar > 0.5: score += 5
    
    gain_to_pain = metrics.get('gain_to_pain_ratio')
    if gain_to_pain:
        if gain_to_pain > 2: score += 10
        elif gain_to_pain > 1: score += 5
    
    # STABILITY (10 points)
    pos_months = metrics.get('positive_months_pct', 0)
    if pos_months:
        if pos_months > 65: score += 5
        elif pos_months > 55: score += 3
    
    ulcer = metrics.get('ulcer_index')
    if ulcer:
        if ulcer < 5: score += 5
        elif ulcer < 10: score += 3
    
    return min(score, 100)

# ---------------------------------------------------
# Helper: Generate AI Verdict (Enhanced)
# ---------------------------------------------------

def generate_verdict(metrics):
    """
    Generate AI verdict with enhanced logic
    """
    if not metrics or not metrics.get('is_statistically_reliable'):
        return {
            "verdict": "insufficient data 📊",
            "score": 0,
            "explanation": "Not enough historical data to generate reliable verdict",
            "pros": ["New fund - potential opportunity"],
            "cons": ["Limited track record", "Cannot assess consistency"]
        }
    
    score = calculate_composite_score(metrics)
    
    # Determine verdict
    if score >= 75:
        verdict = "absolute fire! 🔥🔥🔥"
    elif score >= 60:
        verdict = "fire! 🔥"
    elif score >= 40:
        verdict = "pretty good! ✨"
    elif score >= 25:
        verdict = "meh, could be better 😐"
    else:
        verdict = "nah, skip this one 🚫"
    
    # Generate pros
    pros = []
    cagr = metrics.get('cagr', 0)
    if cagr and cagr > 0.12:
        pros.append(f"Strong returns: {cagr*100:.1f}% CAGR")
    
    sharpe = metrics.get('sharpe', 0)
    if sharpe and sharpe > 1:
        pros.append(f"Good risk-adjusted returns (Sharpe: {sharpe:.2f})")
    
    consistency = metrics.get('consistency_score', 0)
    if consistency and consistency > 60:
        pros.append(f"Consistent performer ({consistency:.0f}% positive periods)")
    
    calmar = metrics.get('calmar_ratio')
    if calmar and calmar > 1:
        pros.append(f"Good downside protection (Calmar: {calmar:.2f})")
    
    if not pros:
        pros.append("Active management approach")
    
    # Generate cons
    cons = []
    if cagr and cagr < 0.08:
        cons.append(f"Below-average returns: {cagr*100:.1f}% CAGR")
    
    max_dd = metrics.get('max_drawdown', 0)
    if max_dd and max_dd < -0.30:
        cons.append(f"High drawdown: {abs(max_dd)*100:.1f}%")
    
    volatility = metrics.get('volatility', 0)
    if volatility and volatility > 0.25:
        cons.append(f"High volatility: {volatility*100:.1f}%")
    
    ulcer = metrics.get('ulcer_index')
    if ulcer and ulcer > 10:
        cons.append(f"Prolonged drawdowns (Ulcer Index: {ulcer:.1f})")
    
    if not cons:
        cons.append("Past performance doesn't guarantee future results")
    
    return {
        "verdict": verdict,
        "score": score,
        "explanation": f"Score: {score}/100 based on returns, risk, and consistency",
        "pros": pros[:4],  # Max 4 pros
        "cons": cons[:4]   # Max 4 cons
    }


# ---------------------------------------------------
# NAV & RETURNS CALCULATION HELPERS
# ---------------------------------------------------

def parse_date(date_str: str) -> datetime:
    """Convert DD-MM-YYYY string to datetime object"""
    try:
        day, month, year = date_str.split('-')
        return datetime(int(year), int(month), int(day))
    except:
        raise ValueError(f"Invalid date format: {date_str}. Use DD-MM-YYYY")

def format_date(dt: datetime) -> str:
    """Convert datetime to DD-MM-YYYY string"""
    return dt.strftime("%d-%m-%Y")

def get_nav_on_date(scheme_code: int, investment_date: str):
    """
    Get NAV for a scheme on a specific date (from PostgreSQL)
    Returns None if date is before fund started
    """
    nav_scheme = get_nav_from_db(scheme_code)
    
    if not nav_scheme:
        return None
    
    target_date = parse_date(investment_date)
    
    # Get fund start date (oldest NAV date)
    all_dates = sorted(
        [parse_date(entry['date']) for entry in nav_scheme['data']],
        reverse=False  # Oldest first
    )
    
    if not all_dates:
        return None
    
    fund_start_date = all_dates[0]
    
    # Check if investment date is before fund started
    if target_date < fund_start_date:
        return {
            'error': True,
            'error_type': 'BEFORE_FUND_START',
            'fund_start_date': format_date(fund_start_date),
            'message': f'Fund started on {format_date(fund_start_date)}, which is after the investment date'
        }
    
    # Find exact match first
    for entry in nav_scheme['data']:
        entry_date = parse_date(entry['date'])
        if entry_date.date() == target_date.date():
            return {
                'nav': float(entry['nav']),
                'date': entry['date'],
                'exact_match': True
            }
    
    # Find nearest previous date
    previous_navs = []
    for entry in nav_scheme['data']:
        entry_date = parse_date(entry['date'])
        if entry_date <= target_date:
            previous_navs.append({
                'nav': float(entry['nav']),
                'date': entry['date'],
                'entry_date': entry_date
            })
    
    if previous_navs:
        previous_navs.sort(key=lambda x: x['entry_date'], reverse=True)
        nearest = previous_navs[0]
        return {
            'nav': nearest['nav'],
            'date': nearest['date'],
            'exact_match': False
        }
    
    return None

def get_current_nav(scheme_code: int):
    """Get the most recent NAV for a scheme (from PostgreSQL)"""
    nav_scheme = get_nav_from_db(scheme_code)
    
    if not nav_scheme or not nav_scheme['data']:
        return None
    
    # Sort by date to get latest
    sorted_data = sorted(
        nav_scheme['data'],
        key=lambda x: parse_date(x['date']),
        reverse=True
    )
    
    return {
        'nav': float(sorted_data[0]['nav']),
        'date': sorted_data[0]['date']
    }


def calculate_returns(scheme_code: int, investment_amount: float, investment_date: str, investment_type: str = "Lumpsum"):
    """
    Calculate investment returns for a single fund.
    Handles both 'Lumpsum' and 'SIP' (monthly installments).
    """
    # 1. Fetch all NAVs from PostgreSQL
    nav_scheme = get_nav_from_db(scheme_code)
    
    if not nav_scheme or not nav_scheme.get('data'):
        return {'error': True, 'message': f'NAV data not available for scheme {scheme_code}'}
    
    # 2. Convert to datetime for easy sorting and comparison
    parsed_navs = []
    for entry in nav_scheme['data']:
        parsed_navs.append({
            'nav': float(entry['nav']),
            'date_str': entry['date'],
            'date': parse_date(entry['date'])
        })
        
    # Sort descending (newest first)
    parsed_navs.sort(key=lambda x: x['date'], reverse=True)
    
    current_nav_data = parsed_navs[0]
    target_date = parse_date(investment_date)
    fund_start_date = parsed_navs[-1]['date']
    
    # Validation: Cannot invest before fund started
    if target_date < fund_start_date:
        return {
            'error': True,
            'error_type': 'BEFORE_FUND_START',
            'fund_start_date': format_date(fund_start_date),
            'message': f'Fund started on {format_date(fund_start_date)}, which is after the investment date'
        }

    # Helper function to find NAV on or before a given date
    def get_nearest_nav(t_date):
        for entry in parsed_navs:
            if entry['date'] <= t_date:
                return entry
        return parsed_navs[-1]

    # ---------------------------------------------------------
    # SIP LOGIC (Monthly accumulation)
    # ---------------------------------------------------------
    if investment_type.upper().strip() == "SIP":
        total_units = 0.0
        total_invested = 0.0
        sip_date = target_date
        current_date = current_nav_data['date']
        
        while sip_date <= current_date:
            nav_entry = get_nearest_nav(sip_date)
            total_units += investment_amount / nav_entry['nav']
            total_invested += investment_amount
            
            # Move to next month safely (handles leap years & month ends)
            month = sip_date.month - 1 + 1
            year = sip_date.year + month // 12
            month = month % 12 + 1
            max_day = [31, 29 if year%4==0 and (year%100!=0 or year%400==0) else 28, 31,30,31,30,31,31,30,31,30,31][month-1]
            day = min(target_date.day, max_day) # Keeps your original SIP day
            
            sip_date = datetime(year, month, day)

        current_value = total_units * current_nav_data['nav']
        absolute_returns = current_value - total_invested
        return_percentage = (absolute_returns / total_invested) * 100 if total_invested > 0 else 0
        
        # Simple XIRR approximation for SIP
        days_invested = (current_date - target_date).days
        years_invested = days_invested / 365.25
        xirr = 0 
        if years_invested > 0:
            # SIP is distributed, so average time invested is roughly half
            xirr = ((current_value / total_invested) ** (1 / (years_invested / 2)) - 1) * 100

        return {
            'error': False,
            'scheme_code': scheme_code,
            'scheme_name': nav_scheme['name'],
            'investment': {
                'amount': round(total_invested, 2), # Correct total across all months!
                'monthly_sip': investment_amount,
                'date': investment_date,
            },
            'current': {
                'nav': current_nav_data['nav'],
                'date': current_nav_data['date_str'],
                'value': round(current_value, 2)
            },
            'returns': {
                'absolute': round(absolute_returns, 2),
                'percentage': round(return_percentage, 2),
                'xirr': round(xirr, 2) if isinstance(xirr, (int, float)) else 0
            }
        }
        
    # ---------------------------------------------------------
    # LUMPSUM LOGIC (One-time investment)
    # ---------------------------------------------------------
    else:
        purchase_nav_data = get_nearest_nav(target_date)
        purchase_nav = purchase_nav_data['nav']
        current_nav = current_nav_data['nav']
        
        units = investment_amount / purchase_nav
        current_value = units * current_nav
        
        absolute_returns = current_value - investment_amount
        return_percentage = (absolute_returns / investment_amount) * 100
        
        days = (current_nav_data['date'] - target_date).days
        years = days / 365.25
        xirr = (pow(current_value / investment_amount, 1 / years) - 1) * 100 if years > 0 else 0
        
        return {
            'error': False,
            'scheme_code': scheme_code,
            'scheme_name': nav_scheme['name'],
            'investment': {
                'amount': round(investment_amount, 2),
                'date': investment_date,
                'purchase_nav': round(purchase_nav, 4)
            },
            'current': {
                'nav': round(current_nav, 4),
                'date': current_nav_data['date_str'],
                'value': round(current_value, 2)
            },
            'returns': {
                'absolute': round(absolute_returns, 2),
                'percentage': round(return_percentage, 2),
                'xirr': round(xirr, 2)
            }
        }



# ===================================================
# PORTFOLIO VIBE CHECK MODELS & ENDPOINT
# ===================================================

class PortfolioItem(BaseModel):
    fund_name: str
    amfi_code: Optional[int] = None  # Made optional so users can just pass names
    investment_type: str  # "SIP" or "Lumpsum"
    invested_date: str    # DD-MM-YYYY
    invested_amount: float

class PortfolioAnalysisRequest(BaseModel):
    items: List[PortfolioItem]

def find_best_fund_match(query_name: str) -> Optional[int]:
    """
    Intelligently finds the best matching AMFI code for a given fund name.
    Uses Exact Match -> Substring Match -> Fuzzy Matching.
    """
    if not query_name:
        return None
        
    query = query_name.lower().strip()
    
    # 1. Exact Match
    for name, data in FUNDS_DATA.items():
        if name.lower() == query:
            return data.get("canonical_code")
            
    # 2. Substring Match (e.g., User types "HDFC Small Cap", we match "HDFC Small Cap Fund - Direct Plan")
    candidates = [name for name in FUNDS_DATA.keys() if query in name.lower()]
    if candidates:
        # Sort by length ascending so we get the cleanest match (avoids picking IDCW if Direct Growth exists)
        candidates.sort(key=len)
        return FUNDS_DATA[candidates[0]].get("canonical_code")
        
    # 3. Fuzzy Matching (Catches typos like "Parag Parik" instead of "Parag Parikh")
    all_names = list(FUNDS_DATA.keys())
    matches = difflib.get_close_matches(query_name, all_names, n=1, cutoff=0.4)
    if matches:
        return FUNDS_DATA[matches[0]].get("canonical_code")
        
    return None





# Request model for investment comparison
class InvestmentComparisonRequest(BaseModel):
    fund1_code: int
    fund2_code: int
    investment_date: str  # DD-MM-YYYY
    investment_amount: float
    investment_type: str = "LUMPSUM"  # 🟢 Added default value

# ---------------------------------------------------
# Endpoint 1: Health Check (Enhanced)
# ---------------------------------------------------

@app.get("/")
def root():
    reliable_count = sum(
        1 for data in FUNDS_DATA.values() 
        if data.get('metrics', {}).get('is_statistically_reliable', False)
    )
    
    return {
        "status": "ok",
        "total_funds": len(FUNDS_DATA),
        "reliable_funds": reliable_count,
        "insufficient_data_funds": len(FUNDS_DATA) - reliable_count,
        "message": "MF Advisor API - Enhanced with 33+ metrics"
    }

# ---------------------------------------------------
# Endpoint 2: Search Funds (Enhanced)
# ---------------------------------------------------

@app.get("/api/funds/search")
def search_funds(
    q: str = "",
    reliable_only: bool = False,
    min_age: float = 0,
    min_cagr: float = 0
):
    """
    Search funds with filters
    
    Parameters:
    - q: Search query (fund name)
    - reliable_only: Filter to only funds with sufficient data
    - min_age: Minimum fund age in years
    - min_cagr: Minimum CAGR (as decimal, e.g., 0.12 for 12%)
    """
    if len(q) < 2:
        return {"query": q, "results": []}
    
    query = q.lower()
    results = []
    
    for name, data in FUNDS_DATA.items():
        if query not in name.lower():
            continue
        
        metrics = data.get("metrics", {})
        
        # Apply filters
        is_reliable = metrics.get("is_statistically_reliable", False)
        fund_age = metrics.get("fund_age_years", 0)
        cagr = metrics.get("cagr", 0)
        
        if reliable_only and not is_reliable:
            continue
        
        if min_age > 0 and (not fund_age or fund_age < min_age):
            continue
        
        if min_cagr > 0 and (not cagr or cagr < min_cagr):
            continue
        
        # Calculate composite score
        composite_score = calculate_composite_score(metrics)
        
        # Get new score object from data
        score_obj = data.get("score")

        # Get category info
        main_cat = data.get("main_category", "Other")
        sub_cat = data.get("sub_category")
        cat_display = data.get("category_display", main_cat)
        cat_emoji = data.get("category_emoji", get_category_emoji(main_cat))
        
        results.append({
            "name": name,
            "code": data.get("canonical_code"),
            "type": data.get("fund_type"),
            "risk": data.get("riskometer"),
             #CATEGORY FIELDS (NEW)
            "category": cat_display,
            "category_emoji": cat_emoji,
            "main_category": main_cat,
            "sub_category": sub_cat,
            "cagr": round(cagr * 100, 2) if cagr else None,
            "fund_age": round(fund_age, 1) if fund_age else None,
            "is_reliable": is_reliable,
            "data_quality": metrics.get("data_quality", "unknown"),
            "composite_score": composite_score,
            "score": score_obj,  # ✅ NEW - Add score object
            "total_nav_records": data.get("total_nav_records", 0)
        })
    
    # Sort by new score if available, fallback to composite, then CAGR
    results.sort(
        key=lambda x: (
            x["score"]["total"] if x.get("score") else x["composite_score"],
            x["cagr"] or 0
        ), 
        reverse=True
    )
    
    return {
        "query": q,
        "filters": {
            "reliable_only": reliable_only,
            "min_age": min_age,
            "min_cagr": min_cagr
        },
        "results": results[:20]
    }


# ---------------------------------------------------
# Endpoint 4: Get All Funds Summary
# ---------------------------------------------------

@app.get("/api/funds/all")
def get_all_funds(reliable_only: bool = False):
    """
    Get summary of all funds
    """
    results = []
    
    for name, data in FUNDS_DATA.items():
        metrics = data.get("metrics", {})
        is_reliable = metrics.get("is_statistically_reliable", False)
        
        if reliable_only and not is_reliable:
            continue
        
        cagr = metrics.get("cagr", 0)
        
        results.append({
            "name": name,
            "code": data.get("canonical_code"),
            "type": data.get("fund_type"),
            "risk": data.get("riskometer"),
            "cagr": round(cagr * 100, 2) if cagr else None,
            "fund_age": round(metrics.get("fund_age_years", 0), 1),
            "is_reliable": is_reliable,
            "composite_score": calculate_composite_score(metrics)
        })
    
    # Sort by new score if available, fallback to composite
    results.sort(
        key=lambda x: x["score"]["total"] if x.get("score") else x["composite_score"],
        reverse=True
    )
    
    return {
        "total": len(results),
        "reliable_count": sum(1 for r in results if r["is_reliable"]),
        "funds": results
    }

# ---------------------------------------------------
# NEW Endpoint 5: Top Ranked Funds
# ---------------------------------------------------

@app.get("/api/funds/top")
def get_top_funds(
    limit: int = 10,
    category: str = None,
    risk: str = None
):
    """
    Get top-ranked funds by composite score
    """
    results = []
    
    for name, data in FUNDS_DATA.items():
        metrics = data.get("metrics", {})
        
        # Only include funds with reliable data
        if not metrics.get("is_statistically_reliable", False):
            continue
        
        # Get category info and risk
        fund_type = data.get("fund_type") or ""
        main_cat = data.get("main_category", "Other")
        sub_cat = data.get("sub_category")
        cat_display = data.get("category_display", main_cat)
        cat_emoji = data.get("category_emoji", get_category_emoji(main_cat))
        risk_level = data.get("riskometer") or ""

        # Apply category filter using main_category
        if category and category.lower() != main_cat.lower():
            continue
        
        if risk and risk.lower() not in risk_level.lower():
            continue
        
        cagr = metrics.get("cagr") or 0
        composite_score = calculate_composite_score(metrics)
        
        # Get new score object from data
        score_obj = data.get("score")


        
        results.append({
            "name": name,
            "code": data.get("canonical_code"),
            "type": fund_type,
            "risk": risk_level,
            # CATEGORY FIELDS (NEW)
            "category": cat_display,
            "category_emoji": cat_emoji,
            "main_category": main_cat,
            "sub_category": sub_cat,
            "cagr": round(cagr * 100, 2) if cagr else None,
            "sharpe": round(metrics.get("sharpe") or 0, 2),
            "composite_score": composite_score,
            "score": score_obj,  # ✅ NEW - Add score object
            "fund_age": round(metrics.get("fund_age_years") or 0, 1)
        })
    
    # Sort by new score if available, fallback to composite
    results.sort(
        key=lambda x: x["score"]["total"] if x.get("score") else x["composite_score"],
        reverse=True
    )
    
    return {
        "filters": {
            "category": category,
            "risk": risk,
            "limit": limit
        },
        "count": len(results),
        "results": results[:limit]
    }

# ---------------------------------------------------
# NEW Endpoint 6: Metrics Glossary
# ---------------------------------------------------

@app.get("/api/metrics/glossary")
def get_metrics_glossary():
    """
    Get explanations for all metrics
    """
    return {
        "returns": {
            "cagr": {
                "name": "CAGR",
                "full_name": "Compound Annual Growth Rate",
                "description": "Average yearly return over time",
                "good_value": "> 12% for equity funds",
                "unit": "percentage"
            },
            "rolling_returns": {
                "name": "Rolling Returns",
                "description": "Returns over specific time periods",
                "periods": ["1Y", "3Y", "5Y"]
            }
        },
        "risk": {
            "volatility": {
                "name": "Volatility",
                "description": "How much the NAV fluctuates",
                "good_value": "Lower is better",
                "unit": "percentage"
            },
            "max_drawdown": {
                "name": "Max Drawdown",
                "description": "Worst peak-to-trough decline",
                "good_value": "> -20% is acceptable",
                "unit": "percentage"
            },
            "ulcer_index": {
                "name": "Ulcer Index",
                "description": "Measures depth and duration of drawdowns",
                "good_value": "< 5 is low stress, > 10 is high stress"
            }
        },
        "risk_adjusted": {
            "sharpe": {
                "name": "Sharpe Ratio",
                "description": "Risk-adjusted returns",
                "good_value": "> 1 is good, > 2 is excellent"
            },
            "sortino": {
                "name": "Sortino Ratio",
                "description": "Like Sharpe but only penalizes downside",
                "good_value": "> 1 is good"
            },
            "calmar": {
                "name": "Calmar Ratio",
                "description": "Return per unit of worst-case risk",
                "good_value": "> 1 is good, > 3 is excellent"
            }
        },
        "consistency": {
            "consistency_score": {
                "name": "Consistency Score",
                "description": "% of periods with positive returns",
                "good_value": "> 60% is consistent"
            },
            "positive_months_pct": {
                "name": "Positive Months %",
                "description": "% of months with gains",
                "good_value": "> 55% is good"
            }
        }
    }


# ---------------------------------------------------
# Endpoint 3: Get Fund Details (Enhanced)
# ---------------------------------------------------

# In backend/main.py, update get_fund_details:

@app.get("/api/funds/{code}")
def get_fund_details(code: str):
    """
    Get complete fund details with all 33+ metrics
    """
    for name, data in FUNDS_DATA.items():
        if str(data.get("canonical_code")) == str(code):
            metrics = data.get("metrics", {})
            
            # FIX: Check is_statistically_reliable, not just data_quality
            is_reliable = metrics.get("is_statistically_reliable", False)
            fund_age = metrics.get("fund_age_years", 0)
            
            score_obj = data.get("score")

            # Generate AI verdict
            ai_verdict = generate_verdict(metrics)

            # Get category info
            main_cat = data.get("main_category", "Other")
            sub_cat = data.get("sub_category")
            cat_display = data.get("category_display", main_cat)
            cat_emoji = data.get("category_emoji", get_category_emoji(main_cat))
            
            return {
                "name": name,
                "code": code,
                "type": data.get("fund_type"),
                "risk": data.get("riskometer"),
                # CATEGORY FIELDS (NEW)
                "category": cat_display,
                "category_emoji": cat_emoji,
                "main_category": main_cat,
                "sub_category": sub_cat,
                "objective": data.get("investment_objective"),
                "benchmark": data.get("benchmark"),
                "managers": data.get("fund_managers"),
                "expense": data.get("annual_expense"),
                "exit_load": data.get("exit_load"),
                "fund_age": round(fund_age, 1) if fund_age else None,
                "fund_house": data.get("fund_house"),
                "asset_allocation": data.get("asset_allocation"),
                "variants": data.get("variants"),
                "total_nav_records": data.get("total_nav_records", 0),
                
                # FIX: Use is_statistically_reliable from metrics
                "is_reliable": is_reliable,
                "data_quality": "sufficient" if is_reliable else "insufficient",
                "data_quality_reason": metrics.get("data_quality_reason"),
                "score": score_obj,                
                # ALL METRICS
                "metrics": metrics,
                
                # AI verdict
                "ai_verdict": ai_verdict
            }
    
    raise HTTPException(404, "fund not found")



# ---------------------------------------------------
# Entry Point
# ---------------------------------------------------


@app.get("/api/recommendations/{fund_code}")
def get_recommendations(
    fund_code: str,
    limit: int = 5,
    min_score_diff: int = 5
):
    """
    Get fund recommendations for a given fund
    
    Logic:
    1. Find the user's fund
    2. Get same main_category funds
    3. Filter by higher score
    4. Sort by score difference
    5. Return top recommendations with comparison
    """
    # Find user's fund
    user_fund = None
    user_fund_name = None
    
    for name, data in FUNDS_DATA.items():
        if str(data.get("canonical_code")) == str(fund_code):
            user_fund = data
            user_fund_name = name
            break
    
    if not user_fund:
        raise HTTPException(404, "Fund not found")
    
    # Get user fund details
    user_metrics = user_fund.get("metrics", {})
    user_score_obj = user_fund.get("score")
    user_score = user_score_obj.get("total") if user_score_obj else calculate_composite_score(user_metrics)
    user_category = user_fund.get("main_category", "Other")
    user_expense = user_fund.get("annual_expense", {})
    user_expense_direct = float(user_expense.get("Direct", 1.5)) if isinstance(user_expense, dict) else 1.5
    
    # Find better alternatives
    recommendations = []
    
    for name, data in FUNDS_DATA.items():
        # Skip same fund
        if name == user_fund_name:
            continue
        
        metrics = data.get("metrics", {})
        
        # Only reliable funds
        if not metrics.get("is_statistically_reliable", False):
            continue
        
        # Same category only
        main_cat = data.get("main_category", "Other")
        if main_cat != user_category:
            continue
        
        # Get score
        score_obj = data.get("score")
        fund_score = score_obj.get("total") if score_obj else calculate_composite_score(metrics)
        
        # Must be better by at least min_score_diff
        score_diff = fund_score - user_score
        if score_diff < min_score_diff:
            continue
        
        # Get expense ratio
        expense = data.get("annual_expense", {})
        expense_direct = float(expense.get("Direct", 1.5)) if isinstance(expense, dict) else 1.5
        
        # Calculate switch potential
        if score_diff >= 15:
            switch_potential = "High"
        elif score_diff >= 10:
            switch_potential = "Moderate"
        else:
            switch_potential = "Low"
        
        # Get category display
        cat_display = data.get("category_display", main_cat)
        cat_emoji = data.get("category_emoji", get_category_emoji(main_cat))
        
        recommendations.append({
            "name": name,
            "code": data.get("canonical_code"),
            "category": cat_display,
            "category_emoji": cat_emoji,
            "main_category": main_cat,
            "score": score_obj or {"total": fund_score},
            "expense_ratio": expense_direct,
            "expense_difference": user_expense_direct - expense_direct,
            "score_difference": score_diff,
            "switch_potential": switch_potential,
            "risk": data.get("riskometer"),
            "fund_age": round(metrics.get("fund_age_years", 0), 1),
            "cagr": round((metrics.get("cagr", 0) * 100), 2),
            "sharpe": round(metrics.get("sharpe", 0), 2)
        })
    
    # Sort by score difference (highest first)
    recommendations.sort(key=lambda x: x["score_difference"], reverse=True)
    
    # Prepare user fund info
    user_fund_info = {
        "name": user_fund_name,
        "code": fund_code,
        "category": user_fund.get("category_display", user_category),
        "category_emoji": user_fund.get("category_emoji", get_category_emoji(user_category)),
        "score": user_score_obj or {"total": user_score},
        "expense_ratio": user_expense_direct,
        "risk": user_fund.get("riskometer"),
        "fund_age": round(user_metrics.get("fund_age_years", 0), 1),
        "cagr": round((user_metrics.get("cagr", 0) * 100), 2),
        "sharpe": round(user_metrics.get("sharpe", 0), 2)
    }
    
    return {
        "user_fund": user_fund_info,
        "recommendations_count": len(recommendations),
        "recommendations": recommendations[:limit]
    }



@app.get("/api/compare/{fund1_code}/{fund2_code}")
def compare_funds(fund1_code: str, fund2_code: str):
    """
    Compare two funds side-by-side
    """
    funds_data = []
    
    for code in [fund1_code, fund2_code]:
        fund_found = None
        fund_name = None
        
        for name, data in FUNDS_DATA.items():
            if str(data.get("canonical_code")) == str(code):
                fund_found = data
                fund_name = name
                break
        
        if not fund_found:
            raise HTTPException(404, f"Fund {code} not found")
        
        metrics = fund_found.get("metrics", {})
        score_obj = fund_found.get("score")
        
        funds_data.append({
            "name": fund_name,
            "code": code,
            "category": fund_found.get("category_display"),
            "category_emoji": fund_found.get("category_emoji"),
            "score": score_obj,
            "expense": fund_found.get("annual_expense"),
            "risk": fund_found.get("riskometer"),
            "fund_age": round(metrics.get("fund_age_years", 0), 1),
            "cagr": metrics.get("cagr"),
            "sharpe": metrics.get("sharpe"),
            "sortino": metrics.get("sortino"),
            "volatility": metrics.get("volatility"),
            "max_drawdown": metrics.get("max_drawdown"),
            "consistency_score": metrics.get("consistency_score"),
            "positive_months_pct": metrics.get("positive_months_pct")
        })
    
    return {
        "fund1": funds_data[0],
        "fund2": funds_data[1]
    }


# ---------------------------------------------------
# Endpoint: Investment Comparison
# ---------------------------------------------------

@app.post("/api/compare-investment")
def compare_investment(request: InvestmentComparisonRequest):
    """
    Compare investment returns between two funds (Handles SIP and Lumpsum)
    """
    try:
        fund1_code = request.fund1_code
        fund2_code = request.fund2_code
        investment_date = request.investment_date
        investment_amount = request.investment_amount
        investment_type = request.investment_type  # 🟢 Extract the type
        
        # Validation: Amount
        if investment_amount <= 0:
            raise HTTPException(400, "Investment amount must be positive")
        
        if investment_amount < 500:
            raise HTTPException(400, "Minimum investment amount is ₹500")
        
        if investment_amount > 100000000:  # 10 Crore
            raise HTTPException(400, "Maximum investment amount is ₹10 Crore")
        
        # Validation: Date format
        try:
            invest_date = parse_date(investment_date)
        except:
            raise HTTPException(400, "Invalid date format. Use DD-MM-YYYY")
        
        # Validation: Date not in future
        today = datetime.now()
        if invest_date.date() >= today.date():
            raise HTTPException(400, "Investment date cannot be today or in the future")
        
        # Validation: At least 1 month old
        one_month_ago = today - timedelta(days=30)
        if invest_date > one_month_ago:
            raise HTTPException(400, "Investment date must be at least 1 month old for accurate comparison")
        
        # Validation: Funds exist in NAV data
        if not get_nav_from_db(fund1_code):
            raise HTTPException(404, f"Fund 1 (code: {fund1_code}) not found in NAV database")

        if not get_nav_from_db(fund2_code):
            raise HTTPException(404, f"Fund 2 (code: {fund2_code}) not found in NAV database")
        
        # 🟢 Calculate returns for Fund 1 (Pass investment_type)
        fund1_returns = calculate_returns(fund1_code, investment_amount, investment_date, investment_type)

        if fund1_returns.get('error'):
            error_msg = fund1_returns.get('message', 'Invalid investment date')
            
            # If it's a "before fund start" error, provide clear guidance
            if fund1_returns.get('error_type') == 'BEFORE_FUND_START':
                fund_start = fund1_returns.get('fund_start_date', 'unknown')
                fund_name = get_nav_from_db(fund1_code)['name'] if get_nav_from_db(fund2_code) else 'Your fund'
                error_msg = f"{fund_name} started on {fund_start}. Please select a date after this."
    
            raise HTTPException(400, error_msg)
            
        # 🟢 Calculate returns for Fund 2 (Pass investment_type)
        fund2_returns = calculate_returns(fund2_code, investment_amount, investment_date, investment_type)

        # Handle Case 2: Fund 2 started after investment date
        adjusted_comparison = False
        original_investment_date = investment_date

        if fund2_returns.get('error') and fund2_returns.get('error_type') == 'BEFORE_FUND_START':
            fund2_start_date = fund2_returns.get('fund_start_date')
            
            # 🟢 Recalculate Fund 2 from its start date (Pass investment_type)
            fund2_returns = calculate_returns(fund2_code, investment_amount, fund2_start_date, investment_type)
            
            # 🟢 Recalculate Fund 1 from Fund 2's start date for fair comparison (Pass investment_type)
            fund1_adjusted = calculate_returns(fund1_code, investment_amount, fund2_start_date, investment_type)
            
            if fund1_adjusted.get('error'):
                raise HTTPException(400, {
                    "error": True,
                    "error_type": "CANNOT_COMPARE",
                    "message": f"Cannot compare: Recommended fund started on {fund2_start_date}, but your fund data is not available from that date.",
                    "fund2_start_date": fund2_start_date
                })
            
            adjusted_comparison = True
            adjustment_info = {
                "adjusted": True,
                "reason": "Recommended fund started later than your investment date",
                "original_date": original_investment_date,
                "adjusted_date": fund2_start_date,
                "fund2_start_date": fund2_start_date,
                "disclaimer": f"The recommended fund started on {fund2_start_date}. Comparison is from that date onwards for fair evaluation."
            }
            
            # Use adjusted Fund 1 returns for comparison
            fund1_returns = fund1_adjusted
            
        elif fund2_returns.get('error'):
            # Other error with Fund 2
            raise HTTPException(404, f"Recommended fund: {fund2_returns.get('message')}")
        else:
            adjustment_info = {
                "adjusted": False
            }
        
        # Calculate differences
        value_difference = fund2_returns['current']['value'] - fund1_returns['current']['value']
        percentage_difference = fund2_returns['returns']['percentage'] - fund1_returns['returns']['percentage']
        xirr_difference = fund2_returns['returns']['xirr'] - fund1_returns['returns']['xirr']
        
        return {
            "fund1": fund1_returns,
            "fund2": fund2_returns,
            "comparison": {
                "value_difference": round(value_difference, 2),
                "percentage_difference": round(percentage_difference, 2),
                "xirr_difference": round(xirr_difference, 2),
                "is_fund2_better": value_difference > 0,
                "improvement_text": (
                    f"Fund 2 would have given you ₹{abs(value_difference):,.0f} {'more' if value_difference > 0 else 'less'}"
                )
            },
            "adjustment": adjustment_info, 
            "meta": {
                "investment_type": investment_type,
                "investment_amount": investment_amount,
                "investment_date": investment_date,
                "calculation_date": format_date(datetime.now())
            }
        }
    
    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ Error in compare_investment: {e}")
        raise HTTPException(500, f"Internal server error: {str(e)}")



@app.get("/debug/scheme-codes")
async def debug_scheme_codes(limit: int = 20):
    """Debug endpoint to see what scheme codes are in PostgreSQL"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get sample scheme codes from database
        cursor.execute("""
            SELECT DISTINCT scheme_code, scheme_name, fund_house
            FROM scheme_nav 
            ORDER BY scheme_code
            LIMIT %s
        """, (limit,))
        
        results = cursor.fetchall()
        cursor.close()
        conn.close()
        
        return {
            "total_checked": len(results),
            "sample_codes": [
                {
                    "code": row['scheme_code'],
                    "name": row['scheme_name'],
                    "fund_house": row['fund_house']
                }
                for row in results
            ]
        }
    except Exception as e:
        return {"error": str(e)}





# ---------------------------------------------------
# DEBUG: Print all registered routes
# ---------------------------------------------------

@app.on_event("startup")
def debug_routes():
    print("\n" + "="*60)
    print("REGISTERED ROUTES:")
    print("="*60)
    for route in app.routes:
        if hasattr(route, 'methods'):
            print(f"{list(route.methods)[0]:6s} {route.path}")
    print("="*60 + "\n")

@app.get("/health")
async def health():
    return {"status": "ok"}


"""
EXPENSE IMPACT ENDPOINT - WITH VALIDATION
Requires actual expense ratio data from fund
Does NOT calculate if expense data is missing
"""

from pydantic import BaseModel
from typing import Optional
from fastapi import HTTPException

class ExpenseImpactRequest(BaseModel):
    scheme_code: int
    amount: float
    duration_years: int
    investment_type: str = "lumpsum"
    sip_amount: Optional[float] = None

@app.post("/api/expense-impact")
async def calculate_expense_impact(request: ExpenseImpactRequest):
    """Calculate impact of expense ratio: Direct vs Regular plan"""
    
    # Get fund data from FUNDS_DATA
    fund_data = None
    fund_name = None
    
    for name, data in FUNDS_DATA.items():
        if str(data.get("canonical_code")) == str(request.scheme_code):
            fund_data = data
            fund_name = name
            break
    
    if not fund_data:
        raise HTTPException(status_code=404, detail="Fund not found")
    
    # Get metrics
    metrics = fund_data.get("metrics", {})
    
    # ✅ VALIDATION: Check if expense ratio data exists
    annual_expense = fund_data.get("annual_expense")
    
    # Check if we have valid expense data
    has_expense_data = False
    direct_expense = None
    regular_expense = None
    
    if annual_expense and isinstance(annual_expense, dict):
        direct_str = annual_expense.get("Direct")
        regular_str = annual_expense.get("Regular")
        
        if direct_str and regular_str:
            try:
                direct_expense = float(direct_str)
                regular_expense = float(regular_str)
                has_expense_data = True
            except (ValueError, TypeError):
                has_expense_data = False
    
    # ❌ If no expense data, return error with helpful message
    if not has_expense_data:
        raise HTTPException(
            status_code=400,
            detail={
                "error": "Expense ratio data not available",
                "message": f"Sorry, we don't have expense ratio data for {fund_name}. Please try another fund.",
                "fund_name": fund_name,
                "suggestion": "Choose a fund with complete expense ratio information for accurate comparison."
            }
        )
    
    # Expected return (use fund's CAGR or default)
    expected_return = 12.0  # Default 12%
    if metrics and metrics.get('cagr'):
        expected_return = metrics['cagr'] * 100
    
    # ========== LUMPSUM CALCULATION ==========
    if request.investment_type == "lumpsum":
        amount = request.amount
        years = request.duration_years
        
        # Direct plan
        direct_rate = (expected_return - direct_expense) / 100
        direct_value = amount * ((1 + direct_rate) ** years)
        
        # Regular plan
        regular_rate = (expected_return - regular_expense) / 100
        regular_value = amount * ((1 + regular_rate) ** years)
        
        direct_returns = direct_value - amount
        regular_returns = regular_value - amount
        invested_amount = amount
        
    # ========== SIP CALCULATION ==========
    else:  # SIP
        monthly_sip = request.amount
        months = request.duration_years * 12
        invested_amount = monthly_sip * months
        
        # Direct plan SIP
        direct_monthly_rate = (expected_return - direct_expense) / 100 / 12
        if direct_monthly_rate > 0:
            direct_value = monthly_sip * (
                ((1 + direct_monthly_rate) ** months - 1) / direct_monthly_rate
            ) * (1 + direct_monthly_rate)
        else:
            direct_value = invested_amount
        
        # Regular plan SIP
        regular_monthly_rate = (expected_return - regular_expense) / 100 / 12
        if regular_monthly_rate > 0:
            regular_value = monthly_sip * (
                ((1 + regular_monthly_rate) ** months - 1) / regular_monthly_rate
            ) * (1 + regular_monthly_rate)
        else:
            regular_value = invested_amount
        
        direct_returns = direct_value - invested_amount
        regular_returns = regular_value - invested_amount
        amount = invested_amount
    
    # ========== CALCULATE SAVINGS ==========
    savings_amount = direct_value - regular_value
    savings_per_year = savings_amount / request.duration_years
    
    if regular_value > 0:
        savings_percentage = ((direct_value - regular_value) / regular_value) * 100
    else:
        savings_percentage = 0
    
    # Effective CAGR
    if amount > 0 and direct_value > 0 and regular_value > 0:
        direct_cagr = ((direct_value / amount) ** (1 / request.duration_years) - 1) * 100
        regular_cagr = ((regular_value / amount) ** (1 / request.duration_years) - 1) * 100
    else:
        direct_cagr = 0
        regular_cagr = 0
    
    # ========== RETURN RESPONSE ==========
    return {
        "fund_name": fund_name,
        "investment_type": request.investment_type,
        "duration_years": request.duration_years,
        "invested_amount": round(invested_amount, 2),
        "direct_plan": {
            "expense_ratio": round(direct_expense, 2),
            "final_value": round(direct_value, 2),
            "returns": round(direct_returns, 2),
            "effective_cagr": round(direct_cagr, 2)
        },
        "regular_plan": {
            "expense_ratio": round(regular_expense, 2),
            "final_value": round(regular_value, 2),
            "returns": round(regular_returns, 2),
            "effective_cagr": round(regular_cagr, 2)
        },
        "savings": {
            "amount": round(savings_amount, 2),
            "per_year": round(savings_per_year, 2),
            "percentage": round(savings_percentage, 2)
        },
        "verdict": f"Direct plan saves you ₹{round(savings_amount/100000, 2)} lakhs! Always choose Direct plans to avoid distributor commissions."
    }


# ===================================================
# PORTFOLIO VIBE CHECK MODELS & ENDPOINT
# ===================================================


@app.post("/api/portfolio/analyze")
async def analyze_portfolio(request: PortfolioAnalysisRequest):
    """
    Gen-Z Portfolio Vibe Check ⚡
    Relies on Frontend Mapping. Safely calculates returns and AI Vibes.
    """
    results = []
    total_invested = 0
    total_current_value = 0
    category_breakdown = {}

    for item in request.items:
        try:
            # 1. Use the AMFI code provided by the React Native mapping
            resolved_code = item.amfi_code
            
            if not resolved_code:
                results.append({
                    "fund_name": item.fund_name, 
                    "error": True, 
                    "message": "Missing AMFI code. Please map the fund correctly."
                })
                continue

            # 2. Calculate Returns
            analysis = calculate_returns(
                scheme_code=resolved_code, 
                investment_amount=item.invested_amount, 
                investment_date=item.invested_date, 
                investment_type=item.investment_type
            )
            
            if analysis.get('error'):
                results.append({"fund_name": item.fund_name, "error": True, "message": analysis['message']})
                continue

            # 3. Safely fetch details and recommendations
            fund_details = get_fund_details(str(resolved_code)) or {}
            recs_data = get_recommendations(str(resolved_code), limit=1) or {}
            
            best_alt = recs_data.get('recommendations', [None])[0] if recs_data.get('recommendations') else None
            
            # Safely extract score in case it's null in the database
            score_obj = fund_details.get('score') or {}
            score = score_obj.get('total', 0)
            
            should_rebalance = best_alt and best_alt.get('score_difference', 0) > 10
            verdict = "Rebalance" if should_rebalance else "Keep"

            true_invested = analysis['investment']['amount'] 
            total_invested += true_invested
            total_current_value += analysis['current']['value']
            
            cat = fund_details.get('main_category', 'Other')
            category_breakdown[cat] = category_breakdown.get(cat, 0) + analysis['current']['value']

            results.append({
                "fund_name": item.fund_name, # Mapped name from frontend
                "amfi_code": resolved_code,
                "current_value": round(analysis['current']['value'], 2),
                "returns": analysis['returns'],
                "score": score,
                "verdict": verdict,
                "status": "W" if analysis['returns']['absolute'] > 0 else "L",
                "recommendation": best_alt,
                "category": cat,
                "category_emoji": fund_details.get('category_emoji', '📊')
            })
            
        except Exception as e:
            print(f"❌ Error processing {item.fund_name}: {str(e)}")
            results.append({
                "fund_name": item.fund_name,
                "error": True,
                "message": "Backend calculation failed for this fund."
            })

    net_aura = round(((total_current_value - total_invested) / total_invested * 100), 2) if total_invested > 0 else 0

    # ---------------------------------------------------------
    # CRASH-PROOF LLM CALL FOR VIBE CHECK
    # ---------------------------------------------------------
    ai_message = ""
    try:
        llm = get_llm_provider()
        prompt = f"""
        Analyze this user's mutual fund portfolio:
        - Total Invested: ₹{total_invested:,.2f}
        - Current Value: ₹{total_current_value:,.2f}
        - Net Return (Aura): {net_aura}%
        - Category Breakdown: {category_breakdown}
        
        Give a 1-2 sentence Gen-Z style "Toast" or "Roast" about this portfolio. 
        Use terms like 'Aura', 'W', 'L', 'Bestie', 'Main Character' if appropriate.
        Be funny, supportive, and helpful. Do not use markdown bolding or asterisks.
        """
        # Inline system prompt to avoid import errors
        system_prompt = "You are a Gen-Z financial advisor bestie who loves mutual funds and speaks entirely in internet slang."
        
        ai_message = llm.generate(prompt, system_prompt=system_prompt, max_tokens=100)
        ai_message = ai_message.strip().strip('"').strip("'")
        
    except Exception as e:
        print(f"⚠️ LLM Error generating vibe check: {e}")
        # Safe Fallbacks
        if net_aura > 15:
            ai_message = "Your portfolio is giving main character energy! 🔥 Absolute W."
        elif net_aura > 0:
            ai_message = "We are in the green, bestie. Keep stacking those SIPs. 📈"
        else:
            ai_message = "Oof, bestie. The vibes are slightly off. Time to look at those rebalance alerts. 💀"

    return {
        "vibe_check": {
            "total_invested": round(total_invested, 2),
            "current_value": round(total_current_value, 2),
            "net_aura": net_aura,
            "category_distribution": category_breakdown,
            "ai_message": ai_message
        },
        "funds": results
    }



if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)