from fastapi import FastAPI, Request, Header, HTTPException
from fastapi.responses import RedirectResponse, JSONResponse
import os
from typing import Optional

import requests
from dotenv import load_dotenv
import psycopg
from psycopg.rows import dict_row
from datetime import datetime, timezone, timedelta

load_dotenv()

app = FastAPI()

STRAVA_CLIENT_ID = os.getenv("STRAVA_CLIENT_ID")
STRAVA_CLIENT_SECRET = os.getenv("STRAVA_CLIENT_SECRET")
APP_BASE_URL = os.getenv("APP_BASE_URL")
DATABASE_URL = os.getenv("DATABASE_URL")
API_AUTH_TOKEN = os.getenv("API_AUTH_TOKEN")

DEFAULT_USER_ID = "default_user"


def get_db_connection():
    if not DATABASE_URL:
        raise ValueError("DATABASE_URL is not set")
    return psycopg.connect(DATABASE_URL, row_factory=dict_row)


def unix_to_timestamptz(unix_ts):
    if not unix_ts:
        return None
    return datetime.fromtimestamp(unix_ts, tz=timezone.utc)


def verify_api_key(authorization: Optional[str]):
    expected = f"Bearer {API_AUTH_TOKEN}"
    if not API_AUTH_TOKEN:
        raise HTTPException(status_code=500, detail="API_AUTH_TOKEN is not configured")
    if authorization != expected:
        raise HTTPException(status_code=401, detail="Unauthorized")


def save_service_token(service_name, user_id, access_token, refresh_token, expires_at, scope=None):
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                insert into service_tokens
                    (service_name, user_id, access_token, refresh_token, expires_at, scope, updated_at)
                values
                    (%s, %s, %s, %s, %s, %s, now())
                on conflict (service_name, user_id)
                do update set
                    access_token = excluded.access_token,
                    refresh_token = excluded.refresh_token,
                    expires_at = excluded.expires_at,
                    scope = excluded.scope,
                    updated_at = now()
                """,
                (
                    service_name,
                    user_id,
                    access_token,
                    refresh_token,
                    expires_at,
                    scope,
                ),
            )
        conn.commit()


def get_service_token(service_name, user_id):
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                select service_name, user_id, access_token, refresh_token, expires_at, scope
                from service_tokens
                where service_name = %s and user_id = %s
                """,
                (service_name, user_id),
            )
            return cur.fetchone()


def refresh_strava_access_token(user_id):
    token = get_service_token("strava", user_id)

    if not token:
        raise ValueError("No Strava token found for user")

    if not token.get("refresh_token"):
        raise ValueError("No Strava refresh token found")

    response = requests.post(
        "https://www.strava.com/oauth/token",
        data={
            "client_id": STRAVA_CLIENT_ID,
            "client_secret": STRAVA_CLIENT_SECRET,
            "grant_type": "refresh_token",
            "refresh_token": token["refresh_token"],
        },
        timeout=30,
    )

    data = response.json()

    if not response.ok:
        raise ValueError(f"Strava refresh failed: {data}")

    save_service_token(
        service_name="strava",
        user_id=user_id,
        access_token=data.get("access_token"),
        refresh_token=data.get("refresh_token"),
        expires_at=unix_to_timestamptz(data.get("expires_at")),
        scope=token.get("scope"),
    )

    return get_service_token("strava", user_id)


def get_valid_strava_token(user_id):
    token = get_service_token("strava", user_id)

    if not token:
        raise ValueError("No Strava token found")

    expires_at = token.get("expires_at")
    now_utc = datetime.now(timezone.utc)

    if expires_at is None or expires_at <= now_utc + timedelta(hours=1):
        return refresh_strava_access_token(user_id)

    return token


def save_strava_activity(user_id, activity):
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                insert into strava_activities (
                    id,
                    user_id,
                    name,
                    type,
                    start_date,
                    distance_meters,
                    moving_time_seconds,
                    elapsed_time_seconds,
                    total_elevation_gain,
                    average_heartrate,
                    max_heartrate,
                    updated_at
                )
                values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now())
                on conflict (id)
                do update set
                    user_id = excluded.user_id,
                    name = excluded.name,
                    type = excluded.type,
                    start_date = excluded.start_date,
                    distance_meters = excluded.distance_meters,
                    moving_time_seconds = excluded.moving_time_seconds,
                    elapsed_time_seconds = excluded.elapsed_time_seconds,
                    total_elevation_gain = excluded.total_elevation_gain,
                    average_heartrate = excluded.average_heartrate,
                    max_heartrate = excluded.max_heartrate,
                    updated_at = now()
                """,
                (
                    activity.get("id"),
                    user_id,
                    activity.get("name"),
                    activity.get("type"),
                    activity.get("start_date"),
                    activity.get("distance"),
                    activity.get("moving_time"),
                    activity.get("elapsed_time"),
                    activity.get("total_elevation_gain"),
                    activity.get("average_heartrate"),
                    activity.get("max_heartrate"),
                ),
            )
        conn.commit()


def get_recent_strava_activities(user_id, limit=10):
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                select
                    id,
                    user_id,
                    name,
                    type,
                    start_date,
                    distance_meters,
                    moving_time_seconds,
                    elapsed_time_seconds,
                    total_elevation_gain,
                    average_heartrate,
                    max_heartrate
                from strava_activities
                where user_id = %s
                order by start_date desc
                limit %s
                """,
                (user_id, limit),
            )
            return cur.fetchall()


def save_daily_pain_check(check_date, pain_score, notes=None):
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                insert into daily_pain_check (
                    check_date,
                    pain_score,
                    notes,
                    updated_at
                )
                values (%s, %s, %s, now())
                on conflict (check_date)
                do update set
                    pain_score = excluded.pain_score,
                    notes = excluded.notes,
                    updated_at = now()
                """,
                (check_date, pain_score, notes),
            )
        conn.commit()


def get_daily_pain_check(check_date):
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                select check_date, pain_score, notes
                from daily_pain_check
                where check_date = %s
                """,
                (check_date,),
            )
            return cur.fetchone()


def build_strava_summary(user_id):
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                select
                    coalesce(sum(moving_time_seconds) filter (
                        where type = 'Run'
                          and start_date >= now() - interval '7 days'
                    ), 0) as run_seconds_7d,

                    coalesce(sum(moving_time_seconds) filter (
                        where type = 'Run'
                          and start_date >= now() - interval '14 days'
                          and start_date < now() - interval '7 days'
                    ), 0) as run_seconds_prev_7d,

                    coalesce(sum(distance_meters) filter (
                        where type = 'Run'
                          and start_date >= now() - interval '7 days'
                    ), 0) as run_distance_7d,

                    coalesce(max(moving_time_seconds) filter (
                        where type = 'Run'
                          and start_date >= now() - interval '14 days'
                    ), 0) as long_run_seconds_14d
                from strava_activities
                where user_id = %s
                """,
                (user_id,),
            )
            totals = cur.fetchone()

            cur.execute(
                """
                select type, start_date
                from strava_activities
                where user_id = %s
                order by start_date desc
                limit 1
                """,
                (user_id,),
            )
            last_activity = cur.fetchone()

            cur.execute(
                """
                select count(*) as run_days_7d
                from (
                    select distinct (start_date at time zone 'utc')::date as run_day
                    from strava_activities
                    where user_id = %s
                      and type = 'Run'
                      and start_date >= now() - interval '7 days'
                ) d
                """,
                (user_id,),
            )
            run_days_row = cur.fetchone()

            cur.execute(
                """
                select count(*) as ran_yesterday
                from strava_activities
                where user_id = %s
                  and type = 'Run'
                  and (start_date at time zone 'utc')::date = (current_date - interval '1 day')::date
                """,
                (user_id,),
            )
            yesterday_row = cur.fetchone()

            cur.execute(
                """
                select count(*) as ran_today
                from strava_activities
                where user_id = %s
                  and type = 'Run'
                  and (start_date at time zone 'utc')::date = current_date
                """,
                (user_id,),
            )
            today_row = cur.fetchone()

    run_seconds_7d = totals["run_seconds_7d"] or 0
    run_seconds_prev_7d = totals["run_seconds_prev_7d"] or 0
    run_minutes_7d = int(run_seconds_7d / 60)
    run_distance_7d = float(totals["run_distance_7d"] or 0)
    long_run_seconds_14d = totals["long_run_seconds_14d"] or 0
    long_run_minutes_14d = int(long_run_seconds_14d / 60)
    last_activity_type = last_activity["type"] if last_activity else None

    run_days_7d = int(run_days_row["run_days_7d"] or 0)
    ran_yesterday = int(yesterday_row["ran_yesterday"] or 0) > 0
    ran_today = int(today_row["ran_today"] or 0) > 0

    today = datetime.now(timezone.utc).date()
    pain_row = get_daily_pain_check(today)
    pain_score = pain_row["pain_score"] if pain_row else None
    pain_notes = pain_row["notes"] if pain_row else None

    if run_seconds_prev_7d > 0:
        load_ratio = run_seconds_7d / run_seconds_prev_7d
    else:
        load_ratio = None

    if run_seconds_7d > 0:
        long_run_share = long_run_seconds_14d / run_seconds_7d
    else:
        long_run_share = None

    recovery_status = "green"
    recommended_session = "easy_run_20_30"
    reason_codes = []
    coach_note = ""

    if pain_score is not None and pain_score >= 4:
        recovery_status = "red"
        recommended_session = "rest"
        reason_codes = ["pain_override"]
        coach_note = "Your reported knee pain is high enough today that running is not recommended."

    elif pain_score is not None and pain_score >= 2:
        recovery_status = "yellow"
        recommended_session = "walk_mobility"
        reason_codes = ["mild_pain_caution"]
        coach_note = "You reported some knee pain today. Favor walking, mobility, or easy cross-training instead of running."

    elif ran_today:
        recovery_status = "yellow"
        recommended_session = "walk_mobility"
        reason_codes = ["already_ran_today"]
        coach_note = "You already logged a run today. Favor recovery, walking, or mobility."

    elif run_minutes_7d == 0:
        recovery_status = "green"
        recommended_session = "easy_run_20_30"
        reason_codes = ["no_recent_run_load"]
        coach_note = "You have not run in the last 7 days. A short easy run is reasonable if your knees feel calm."

    elif ran_yesterday:
        recovery_status = "yellow"
        recommended_session = "walk_mobility"
        reason_codes = ["ran_yesterday", "back_to_back_run_caution"]
        coach_note = "You ran yesterday. Given your knee history, avoid back-to-back runs unless you feel unusually good."

    elif run_days_7d >= 4:
        recovery_status = "yellow"
        recommended_session = "bike_easy"
        reason_codes = ["high_run_frequency"]
        coach_note = "You already have several run days in the last week. Use cross-training today to reduce knee irritation risk."

    elif load_ratio is not None and load_ratio > 1.3:
        recovery_status = "yellow"
        recommended_session = "easy_run_20_30"
        reason_codes = ["rapid_load_increase"]
        coach_note = "Your run load has increased quickly versus the previous week. Keep today's load light."

    elif long_run_share is not None and long_run_share > 0.45:
        recovery_status = "yellow"
        recommended_session = "walk_mobility"
        reason_codes = ["long_run_too_large_share"]
        coach_note = "One run makes up a large share of your recent volume. Favor recovery today rather than stacking more load."

    elif run_minutes_7d < 90:
        recovery_status = "green"
        recommended_session = "easy_run_30_45"
        reason_codes = ["manageable_recent_load"]
        coach_note = "Recent running load looks manageable. An easy run is appropriate."

    elif run_minutes_7d < 180:
        recovery_status = "yellow"
        recommended_session = "easy_run_20_30"
        reason_codes = ["moderate_recent_load"]
        coach_note = "Your recent running load is moderate. Keep today's run short and easy."

    else:
        recovery_status = "yellow"
        recommended_session = "walk_mobility"
        reason_codes = ["high_recent_load"]
        coach_note = "Your recent running load is relatively high. Favor recovery or gentle cross-training today."

    return {
        "summary_date": datetime.now(timezone.utc).date(),
        "strava_run_minutes_7d": run_minutes_7d,
        "strava_run_distance_7d": run_distance_7d,
        "strava_long_run_minutes_14d": long_run_minutes_14d,
        "strava_last_activity_type": last_activity_type,
        "recovery_status": recovery_status,
        "recommended_session": recommended_session,
        "reason_codes": reason_codes,
        "coach_note": coach_note,
        "run_days_7d": run_days_7d,
        "ran_yesterday": ran_yesterday,
        "ran_today": ran_today,
        "load_ratio_7d_vs_prev_7d": round(load_ratio, 2) if load_ratio is not None else None,
        "long_run_share_of_7d_load": round(long_run_share, 2) if long_run_share is not None else None,
        "pain_score": pain_score,
        "pain_notes": pain_notes,
    }


def save_daily_training_summary(summary):
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                insert into daily_training_summary (
                    summary_date,
                    strava_run_minutes_7d,
                    strava_run_distance_7d,
                    strava_long_run_minutes_14d,
                    strava_last_activity_type,
                    recovery_status,
                    recommended_session,
                    reason_codes,
                    coach_note,
                    run_days_7d,
                    ran_yesterday,
                    ran_today,
                    load_ratio_7d_vs_prev_7d,
                    long_run_share_of_7d_load,
                    pain_score,
                    pain_notes,
                    updated_at
                )
                values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now())
                on conflict (summary_date)
                do update set
                    strava_run_minutes_7d = excluded.strava_run_minutes_7d,
                    strava_run_distance_7d = excluded.strava_run_distance_7d,
                    strava_long_run_minutes_14d = excluded.strava_long_run_minutes_14d,
                    strava_last_activity_type = excluded.strava_last_activity_type,
                    recovery_status = excluded.recovery_status,
                    recommended_session = excluded.recommended_session,
                    reason_codes = excluded.reason_codes,
                    coach_note = excluded.coach_note,
                    run_days_7d = excluded.run_days_7d,
                    ran_yesterday = excluded.ran_yesterday,
                    ran_today = excluded.ran_today,
                    load_ratio_7d_vs_prev_7d = excluded.load_ratio_7d_vs_prev_7d,
                    long_run_share_of_7d_load = excluded.long_run_share_of_7d_load,
                    pain_score = excluded.pain_score,
                    pain_notes = excluded.pain_notes,
                    updated_at = now()
                """,
                (
                    summary["summary_date"],
                    summary["strava_run_minutes_7d"],
                    summary["strava_run_distance_7d"],
                    summary["strava_long_run_minutes_14d"],
                    summary["strava_last_activity_type"],
                    summary["recovery_status"],
                    summary["recommended_session"],
                    summary["reason_codes"],
                    summary["coach_note"],
                    summary["run_days_7d"],
                    summary["ran_yesterday"],
                    summary["ran_today"],
                    summary["load_ratio_7d_vs_prev_7d"],
                    summary["long_run_share_of_7d_load"],
                    summary["pain_score"],
                    summary["pain_notes"],
                ),
            )
        conn.commit()


def get_today_summary():
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                select
                    summary_date,
                    strava_run_minutes_7d,
                    strava_run_distance_7d,
                    strava_long_run_minutes_14d,
                    strava_last_activity_type,
                    recovery_status,
                    recommended_session,
                    reason_codes,
                    coach_note,
                    run_days_7d,
                    ran_yesterday,
                    ran_today,
                    load_ratio_7d_vs_prev_7d,
                    long_run_share_of_7d_load,
                    pain_score,
                    pain_notes
                from daily_training_summary
                where summary_date = current_date
                """
            )
            return cur.fetchone()


@app.get("/")
def root():
    return {
        "message": "API is running. Try /health, /env-check, /auth/strava/start, /strava/token-status, /refresh-data, or /today-summary"
    }


@app.get("/health")
def health():
    return {"ok": True, "message": "training coach api is running"}


@app.get("/env-check")
def env_check():
    return {
        "has_strava_client_id": bool(STRAVA_CLIENT_ID),
        "has_strava_client_secret": bool(STRAVA_CLIENT_SECRET),
        "has_app_base_url": bool(APP_BASE_URL),
        "has_database_url": bool(DATABASE_URL),
        "has_api_auth_token": bool(API_AUTH_TOKEN),
        "app_base_url": APP_BASE_URL,
    }


@app.get("/auth/strava/start")
def auth_strava_start():
    if not STRAVA_CLIENT_ID or not APP_BASE_URL:
        return JSONResponse(
            status_code=500,
            content={"error": "Missing STRAVA_CLIENT_ID or APP_BASE_URL"}
        )

    base_url = APP_BASE_URL.rstrip("/")
    redirect_uri = f"{base_url}/auth/strava/callback"

    auth_url = (
        "https://www.strava.com/oauth/authorize"
        f"?client_id={STRAVA_CLIENT_ID}"
        "&response_type=code"
        f"&redirect_uri={redirect_uri}"
        "&approval_prompt=force"
        "&scope=activity:read_all"
    )
    return RedirectResponse(auth_url)


@app.get("/auth/strava/callback")
def auth_strava_callback(request: Request):
    query_params = dict(request.query_params)

    error = query_params.get("error")
    code = query_params.get("code")

    if error:
        return {"ok": False, "error": error, "query_params": query_params}

    if not code:
        return {"ok": False, "error": "No code returned from Strava", "query_params": query_params}

    token_response = requests.post(
        "https://www.strava.com/oauth/token",
        data={
            "client_id": STRAVA_CLIENT_ID,
            "client_secret": STRAVA_CLIENT_SECRET,
            "code": code,
            "grant_type": "authorization_code",
        },
        timeout=30,
    )

    token_data = token_response.json()

    if token_response.ok:
        save_service_token(
            service_name="strava",
            user_id=DEFAULT_USER_ID,
            access_token=token_data.get("access_token"),
            refresh_token=token_data.get("refresh_token"),
            expires_at=unix_to_timestamptz(token_data.get("expires_at")),
            scope=token_data.get("scope"),
        )

    return {
        "ok": token_response.ok,
        "saved_to_db": token_response.ok,
        "token_data": {
            "access_token_present": bool(token_data.get("access_token")),
            "refresh_token_present": bool(token_data.get("refresh_token")),
            "expires_at": token_data.get("expires_at"),
            "athlete_id_present": bool(token_data.get("athlete", {}).get("id")),
        },
    }


@app.get("/strava/token-status")
def strava_token_status():
    try:
        token = get_valid_strava_token(DEFAULT_USER_ID)

        if not token:
            return {"ok": False, "message": "No Strava token saved in database yet"}

        return {
            "ok": True,
            "service_name": token.get("service_name"),
            "user_id": token.get("user_id"),
            "access_token_present": bool(token.get("access_token")),
            "refresh_token_present": bool(token.get("refresh_token")),
            "expires_at": str(token.get("expires_at")),
            "scope": token.get("scope"),
        }
    except Exception as e:
        return {"ok": False, "error": str(e)}


@app.get("/strava/force-refresh")
def strava_force_refresh():
    try:
        token = refresh_strava_access_token(DEFAULT_USER_ID)
        return {
            "ok": True,
            "access_token_present": bool(token.get("access_token")),
            "refresh_token_present": bool(token.get("refresh_token")),
            "expires_at": str(token.get("expires_at")),
        }
    except Exception as e:
        return {"ok": False, "error": str(e)}


@app.get("/strava/activities")
def strava_activities(per_page: int = 5):
    token = get_valid_strava_token(DEFAULT_USER_ID)

    if not token or not token.get("access_token"):
        return JSONResponse(
            status_code=401,
            content={"ok": False, "error": "No Strava access token available. Connect Strava first."}
        )

    response = requests.get(
        "https://www.strava.com/api/v3/athlete/activities",
        headers={"Authorization": f"Bearer {token['access_token']}"},
        params={"per_page": per_page},
        timeout=30,
    )

    try:
        data = response.json()
    except Exception:
        return JSONResponse(
            status_code=500,
            content={"ok": False, "error": "Could not parse Strava activities response"}
        )

    if not response.ok:
        return JSONResponse(
            status_code=response.status_code,
            content={"ok": False, "error": data}
        )

    simplified = []
    for act in data:
        simplified.append({
            "id": act.get("id"),
            "name": act.get("name"),
            "type": act.get("type"),
            "start_date": act.get("start_date"),
            "distance_meters": act.get("distance"),
            "moving_time_seconds": act.get("moving_time"),
            "elapsed_time_seconds": act.get("elapsed_time"),
            "total_elevation_gain": act.get("total_elevation_gain"),
            "average_heartrate": act.get("average_heartrate"),
            "max_heartrate": act.get("max_heartrate"),
        })

    return {
        "ok": True,
        "count": len(simplified),
        "activities": simplified,
    }


@app.get("/db-test")
def db_test():
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("select now() as current_time")
                row = cur.fetchone()
        return {"ok": True, "row": row}
    except Exception as e:
        return {"ok": False, "error": str(e)}


@app.get("/strava/import-activities")
def import_strava_activities(per_page: int = 30):
    token = get_valid_strava_token(DEFAULT_USER_ID)

    if not token or not token.get("access_token"):
        return JSONResponse(
            status_code=401,
            content={"ok": False, "error": "No Strava access token available. Connect Strava first."}
        )

    response = requests.get(
        "https://www.strava.com/api/v3/athlete/activities",
        headers={"Authorization": f"Bearer {token['access_token']}"},
        params={"per_page": per_page},
        timeout=30,
    )

    try:
        data = response.json()
    except Exception:
        return JSONResponse(
            status_code=500,
            content={"ok": False, "error": "Could not parse Strava activities response"}
        )

    if not response.ok:
        return JSONResponse(
            status_code=response.status_code,
            content={"ok": False, "error": data}
        )

    saved_count = 0
    try:
        for activity in data:
            save_strava_activity(DEFAULT_USER_ID, activity)
            saved_count += 1
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={
                "ok": False,
                "saved_count_before_error": saved_count,
                "error": str(e),
            }
        )

    return {
        "ok": True,
        "saved_count": saved_count,
        "message": "Activities imported successfully"
    }


@app.get("/strava/recent-db-activities")
def recent_db_activities(limit: int = 10):
    rows = get_recent_strava_activities(DEFAULT_USER_ID, limit=limit)
    return {
        "ok": True,
        "count": len(rows),
        "activities": rows,
    }


@app.get("/summary/build")
def summary_build():
    summary = build_strava_summary(DEFAULT_USER_ID)
    save_daily_training_summary(summary)
    return {"ok": True, "summary": summary}


@app.get("/today-summary")
def today_summary(authorization: Optional[str] = Header(None)):
    verify_api_key(authorization)

    summary = get_today_summary()

    if not summary:
        return {
            "ok": False,
            "message": "No summary for today yet. Run /refresh-data first."
        }

    return {
        "ok": True,
        "summary": summary,
    }


@app.get("/set-pain-level")
def set_pain_level(
    score: int,
    notes: Optional[str] = None,
    authorization: Optional[str] = Header(None)
):
    verify_api_key(authorization)

    if score < 0 or score > 10:
        return JSONResponse(
            status_code=400,
            content={"ok": False, "error": "Pain score must be between 0 and 10"}
        )

    today = datetime.now(timezone.utc).date()
    save_daily_pain_check(today, score, notes)

    return {
        "ok": True,
        "check_date": str(today),
        "pain_score": score,
        "notes": notes,
    }


@app.get("/today-pain")
def today_pain():
    today = datetime.now(timezone.utc).date()
    row = get_daily_pain_check(today)

    if not row:
        return {
            "ok": False,
            "message": "No pain score recorded for today"
        }

    return {
        "ok": True,
        "pain": row,
    }


@app.get("/refresh-data")
def refresh_data(
    per_page: int = 30,
    authorization: Optional[str] = Header(None)
):
    verify_api_key(authorization)

    try:
        token = get_valid_strava_token(DEFAULT_USER_ID)
    except Exception as e:
        return JSONResponse(
            status_code=401,
            content={"ok": False, "error": str(e)}
        )

    response = requests.get(
        "https://www.strava.com/api/v3/athlete/activities",
        headers={"Authorization": f"Bearer {token['access_token']}"},
        params={"per_page": per_page},
        timeout=30,
    )

    try:
        data = response.json()
    except Exception:
        return JSONResponse(
            status_code=500,
            content={"ok": False, "error": "Could not parse Strava activities response"}
        )

    if not response.ok:
        return JSONResponse(
            status_code=response.status_code,
            content={"ok": False, "error": data}
        )

    saved_count = 0
    try:
        for activity in data:
            save_strava_activity(DEFAULT_USER_ID, activity)
            saved_count += 1
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={
                "ok": False,
                "stage": "import_activities",
                "saved_count_before_error": saved_count,
                "error": str(e),
            }
        )

    try:
        summary = build_strava_summary(DEFAULT_USER_ID)
        save_daily_training_summary(summary)
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={
                "ok": False,
                "stage": "build_summary",
                "error": str(e),
            }
        )

    return {
        "ok": True,
        "imported_activities": saved_count,
        "summary": summary,
    }
