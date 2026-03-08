"""
Steam Taste Analyzer
====================
起動: uvicorn app:app --reload
設定: .env に STEAM_API_KEY を記述
"""

import asyncio
import json
import os
import random
import re
import sqlite3
import time
from contextlib import asynccontextmanager
from typing import Optional

import httpx
from dotenv import load_dotenv
from fastapi import BackgroundTasks, FastAPI, HTTPException
from fastapi.responses import HTMLResponse
from pydantic import BaseModel

load_dotenv()

STEAM_API_KEY = os.getenv("STEAM_API_KEY", "")
DB_PATH = os.getenv("DB_PATH", "steam_cache.db")
APPDETAILS_DELAY = 1.0  # Steam Store API へのリクエスト間隔 (秒)
GAME_CACHE_DAYS = 30    # ゲーム詳細キャッシュの有効期限
GEMINI_DAILY_LIMIT_GLOBAL = int(os.getenv("GEMINI_DAILY_LIMIT_GLOBAL", "200"))
GEMINI_DAILY_LIMIT_PER_USER = int(os.getenv("GEMINI_DAILY_LIMIT_PER_USER", "50"))

# 環境変数を優先し、なければ gemini_key.txt を読む（ローカル開発用）
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")
if not GEMINI_API_KEY:
    try:
        with open("gemini_key.txt", encoding="utf-8") as _f:
            GEMINI_API_KEY = _f.read().strip()
    except FileNotFoundError:
        pass

# バックグラウンド取得の進捗をメモリ上で管理
fetch_progress: dict[str, dict] = {}


# --- DB 初期化 ---

def init_db():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS games (
            app_id      INTEGER PRIMARY KEY,
            name        TEXT,
            genres      TEXT,
            categories  TEXT,
            cached_at   REAL,
            description TEXT,
            app_type    TEXT
        )
    """)
    # 既存DBへのマイグレーション
    for col in ["description TEXT", "app_type TEXT"]:
        try:
            conn.execute(f"ALTER TABLE games ADD COLUMN {col}")
        except sqlite3.OperationalError:
            pass
    conn.execute("""
        CREATE TABLE IF NOT EXISTS user_games (
            steam_id         TEXT,
            app_id           INTEGER,
            name             TEXT,
            playtime_minutes INTEGER,
            fetched_at       REAL,
            PRIMARY KEY (steam_id, app_id)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS exclusions (
            steam_id TEXT,
            app_id   INTEGER,
            PRIMARY KEY (steam_id, app_id)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS gemini_usage (
            date     TEXT,
            steam_id TEXT,
            count    INTEGER DEFAULT 0,
            PRIMARY KEY (date, steam_id)
        )
    """)
    conn.commit()
    conn.close()


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    yield

app = FastAPI(lifespan=lifespan)


# --- DB ヘルパー ---

def db_get_game(app_id: int) -> Optional[dict]:
    conn = sqlite3.connect(DB_PATH)
    row = conn.execute(
        "SELECT name, genres, categories, cached_at, description, app_type FROM games WHERE app_id = ?",
        (app_id,)
    ).fetchone()
    conn.close()
    if not row:
        return None
    if time.time() - row[3] > GAME_CACHE_DAYS * 86400:
        return None  # キャッシュ期限切れ
    return {
        "name": row[0],
        "genres": json.loads(row[1] or "[]"),
        "categories": json.loads(row[2] or "[]"),
        "description": row[4] or "",
        "app_type": row[5] or "",
    }


def db_save_game(app_id: int, name: str, genres: list, categories: list, description: str = "", app_type: str = ""):
    conn = sqlite3.connect(DB_PATH)
    conn.execute(
        "INSERT OR REPLACE INTO games VALUES (?, ?, ?, ?, ?, ?, ?)",
        (app_id, name, json.dumps(genres), json.dumps(categories), time.time(), description, app_type)
    )
    conn.commit()
    conn.close()


def db_get_user_games(steam_id: str) -> Optional[list]:
    conn = sqlite3.connect(DB_PATH)
    rows = conn.execute(
        "SELECT app_id, name, playtime_minutes FROM user_games WHERE steam_id = ?",
        (steam_id,)
    ).fetchall()
    conn.close()
    if not rows:
        return None
    return [{"app_id": r[0], "name": r[1], "playtime_minutes": r[2]} for r in rows]


def db_save_user_games(steam_id: str, games: list):
    conn = sqlite3.connect(DB_PATH)
    conn.execute("DELETE FROM user_games WHERE steam_id = ?", (steam_id,))
    for g in games:
        conn.execute(
            "INSERT INTO user_games VALUES (?, ?, ?, ?, ?)",
            (steam_id, g["appid"], g.get("name", ""), g.get("playtime_forever", 0), time.time())
        )
    conn.commit()
    conn.close()


def db_get_exclusions(steam_id: str) -> set:
    conn = sqlite3.connect(DB_PATH)
    rows = conn.execute(
        "SELECT app_id FROM exclusions WHERE steam_id = ?", (steam_id,)
    ).fetchall()
    conn.close()
    return {r[0] for r in rows}


def db_check_and_increment_usage(steam_id: str) -> dict:
    """使用量チェックして上限未満なら +1 してOKを返す。超過時は理由を返す。"""
    today = time.strftime("%Y-%m-%d")
    conn = sqlite3.connect(DB_PATH)

    # グローバル合計
    row = conn.execute(
        "SELECT SUM(count) FROM gemini_usage WHERE date = ?", (today,)
    ).fetchone()
    global_count = row[0] or 0
    if global_count >= GEMINI_DAILY_LIMIT_GLOBAL:
        conn.close()
        return {"ok": False, "reason": f"本日のサーバー全体の利用上限（{GEMINI_DAILY_LIMIT_GLOBAL}回）に達しました。また明日お試しください。", "global_remaining": 0, "user_remaining": 0}

    # ユーザー分
    row = conn.execute(
        "SELECT count FROM gemini_usage WHERE date = ? AND steam_id = ?", (today, steam_id)
    ).fetchone()
    user_count = row[0] if row else 0
    if user_count >= GEMINI_DAILY_LIMIT_PER_USER:
        conn.close()
        return {"ok": False, "reason": f"本日のあなたの利用上限（{GEMINI_DAILY_LIMIT_PER_USER}回）に達しました。また明日お試しください。", "global_remaining": GEMINI_DAILY_LIMIT_GLOBAL - global_count, "user_remaining": 0}

    # インクリメント
    conn.execute(
        "INSERT INTO gemini_usage (date, steam_id, count) VALUES (?, ?, 1) ON CONFLICT(date, steam_id) DO UPDATE SET count = count + 1",
        (today, steam_id)
    )
    conn.commit()
    conn.close()
    return {
        "ok": True,
        "global_remaining": GEMINI_DAILY_LIMIT_GLOBAL - global_count - 1,
        "user_remaining": GEMINI_DAILY_LIMIT_PER_USER - user_count - 1,
    }


def db_get_usage(steam_id: str) -> dict:
    today = time.strftime("%Y-%m-%d")
    conn = sqlite3.connect(DB_PATH)
    global_row = conn.execute("SELECT SUM(count) FROM gemini_usage WHERE date = ?", (today,)).fetchone()
    user_row = conn.execute("SELECT count FROM gemini_usage WHERE date = ? AND steam_id = ?", (today, steam_id)).fetchone()
    conn.close()
    global_count = global_row[0] or 0
    user_count = user_row[0] if user_row else 0
    return {
        "global_remaining": max(0, GEMINI_DAILY_LIMIT_GLOBAL - global_count),
        "user_remaining": max(0, GEMINI_DAILY_LIMIT_PER_USER - user_count),
    }


def db_set_exclusion(steam_id: str, app_id: int, excluded: bool):
    conn = sqlite3.connect(DB_PATH)
    if excluded:
        conn.execute("INSERT OR IGNORE INTO exclusions VALUES (?, ?)", (steam_id, app_id))
    else:
        conn.execute("DELETE FROM exclusions WHERE steam_id = ? AND app_id = ?", (steam_id, app_id))
    conn.commit()
    conn.close()


# --- Steam API ---

async def resolve_vanity(client: httpx.AsyncClient, vanity: str) -> Optional[str]:
    r = await client.get(
        "https://api.steampowered.com/ISteamUser/ResolveVanityURL/v1/",
        params={"key": STEAM_API_KEY, "vanityurl": vanity}
    )
    data = r.json()["response"]
    return data["steamid"] if data["success"] == 1 else None


async def get_owned_games(client: httpx.AsyncClient, steam_id: str) -> list:
    r = await client.get(
        "https://api.steampowered.com/IPlayerService/GetOwnedGames/v1/",
        params={
            "key": STEAM_API_KEY,
            "steamid": steam_id,
            "include_appinfo": 1,
            "include_played_free_games": 1,
        }
    )
    return r.json().get("response", {}).get("games", [])


async def get_app_details(client: httpx.AsyncClient, app_id: int, lang: str = "english") -> Optional[dict]:
    try:
        r = await client.get(
            "https://store.steampowered.com/api/appdetails",
            params={"appids": app_id, "l": lang},
            timeout=15.0
        )
        d = r.json().get(str(app_id), {})
        return d.get("data") if d.get("success") else None
    except Exception:
        return None


async def get_app_details_best(client: httpx.AsyncClient, app_id: int) -> Optional[dict]:
    """日本語→英語の順で試し、説明文が取れた最初の結果を返す"""
    last = None
    for lang in ["japanese", "english"]:
        details = await get_app_details(client, app_id, lang=lang)
        if details:
            last = details
            if details.get("short_description"):
                return details
    return last


# --- バックグラウンド取得タスク ---

async def do_fetch(steam_id: str):
    def set_progress(**kwargs):
        fetch_progress[steam_id] = kwargs

    set_progress(status="fetching", phase="games", message="ゲーム一覧を取得中...", progress=0, total=0)

    async with httpx.AsyncClient(timeout=30.0) as client:
        try:
            games = await get_owned_games(client, steam_id)
        except Exception as e:
            set_progress(status="error", message=f"ゲーム一覧の取得に失敗しました: {e}")
            return

        if not games:
            set_progress(status="error", message="ゲームが取得できません。プロフィールを公開設定にしてください。")
            return

        db_save_user_games(steam_id, games)

        # プレイ時間があるゲームのみ詳細を取得
        targets = [g for g in games if g.get("playtime_forever", 0) > 0]
        total = len(targets)
        done = 0

        set_progress(status="fetching", phase="details", progress=0, total=total,
                     message=f"ゲーム詳細を取得中... (0/{total})")

        for g in targets:
            app_id = g["appid"]

            if not db_get_game(app_id):
                details = await get_app_details_best(client, app_id)
                if details:
                    genres = [x["description"] for x in details.get("genres", [])]
                    categories = [x["description"] for x in details.get("categories", [])]
                    name = details.get("name") or g.get("name") or str(app_id)
                    description = details.get("short_description", "")
                    app_type = details.get("type", "")
                    db_save_game(app_id, name, genres, categories, description, app_type)
                await asyncio.sleep(APPDETAILS_DELAY)

            done += 1
            set_progress(status="fetching", phase="details", progress=done, total=total,
                         message=f"ゲーム詳細を取得中... ({done}/{total})")

        set_progress(status="done", message="完了", progress=total, total=total)


# --- リクエストモデル ---

class ResolveRequest(BaseModel):
    input: str

class FetchRequest(BaseModel):
    steam_id: str
    force: bool = False

class ExcludeRequest(BaseModel):
    steam_id: str
    app_id: int
    excluded: bool

class QuizJudgeRequest(BaseModel):
    correct_name: str
    user_answer: str
    steam_id: str = ""
    user_api_key: str = ""  # ユーザー独自のGemini APIキー（任意）


# --- Gemini ---

# (api_version, model_name) の優先順で試す
GEMINI_CANDIDATES = [
    ("v1beta", "gemma-3-4b-it"),
    ("v1beta", "gemini-2.5-flash"),
]


async def _gemini_text(prompt: str, api_key: str = "") -> str:
    """Gemini にテキストプロンプトを送り、応答テキストを返す。失敗時は例外を送出。"""
    key = api_key or GEMINI_API_KEY
    if not key:
        raise HTTPException(500, "Gemini APIキーが設定されていません (gemini_key.txt を確認してください)")

    last_error = "不明なエラー"
    async with httpx.AsyncClient(timeout=30.0) as client:
        for api_ver, model in GEMINI_CANDIDATES:
            try:
                r = await client.post(
                    f"https://generativelanguage.googleapis.com/{api_ver}/models/{model}:generateContent",
                    params={"key": key},
                    json={"contents": [{"parts": [{"text": prompt}]}]},
                )
                if r.is_success:
                    return r.json()["candidates"][0]["content"]["parts"][0]["text"].strip()
                try:
                    msg = r.json().get("error", {}).get("message", r.text[:200])
                except Exception:
                    msg = r.text[:200]
                last_error = f"{api_ver}/{model}: HTTP {r.status_code} - {msg}"
                # 404 以外のエラー（認証失敗など）はこれ以上試しても無駄
                if r.status_code not in (404, 429):
                    break
            except httpx.TimeoutException:
                last_error = f"{api_ver}/{model}: タイムアウト"
            except Exception as e:
                last_error = f"{api_ver}/{model}: {e}"

    raise HTTPException(502, f"Gemini API呼び出し失敗: {last_error}")


async def redact_name_in_description(description: str, game_name: str) -> str:
    """説明文中のゲーム名（略称・変形含む）を [ゲーム名] に置き換える。失敗時は原文を返す。"""
    prompt = (
        f"以下のゲーム説明文に、ゲーム名「{game_name}」またはその略称・変形・省略形が含まれているか確認してください。\n"
        "含まれている場合は、それらを全て「[ゲーム名]」という文字列に置き換えた説明文を返してください。\n"
        "含まれていない場合はそのまま返してください。\n"
        "説明文のみを返してください（前置きや説明は一切不要）。\n\n"
        f"ゲーム名: {game_name}\n"
        f"説明文:\n{description}"
    )
    try:
        result = await _gemini_text(prompt)
        return result
    except Exception:
        return description  # 失敗時は原文のまま（クイズは続行可能）


async def judge_with_gemini(correct_name: str, user_answer: str, api_key: str = "") -> dict:
    if not (api_key or GEMINI_API_KEY):
        raise HTTPException(500, "Gemini APIキーが設定されていません (gemini_key.txt を確認してください)")

    prompt = (
        f"Steamゲームの名前当てクイズの答え合わせをしてください。\n"
        f"正解のゲーム名: {correct_name}\n"
        f"ユーザーの回答: {user_answer}\n\n"
        "略称・別表記・軽微な誤字・部分一致なども考慮して、同じゲームを指しているかどうか判定してください。"
        "ただし明らかに違うゲームの場合は不正解としてください。\n"
        "例：ゲーム名『農家はreplaceされました』、解答『農家は Replace() されました』。大文字小文字の違いや発音しない記号の違い程度は無視してOK。正解とします。"
        "例：ゲーム名『東方風神録 〜 Mountain of Faith.』、解答『東方風神禄』。誤字の範疇と考えられる為正解判定でも良い。ただし、あなたの裁量で、よほど大きく間違っていれば誤りとしても良い。"
        "例：ゲーム名『バトルネットワーク ロックマンエグゼ3』、解答『BattleNetwork Rockman EXE 2』。英語・カタカナの違いは許容できるが、ナンバリングの数字が違うので明らかに違うゲームを指している。誤りとします。"
        'JSON形式のみで回答してください: {"correct": true または false, "reason": "理由を日本語で簡潔に"}'
    )

    text = await _gemini_text(prompt, api_key=api_key)  # 失敗時は HTTPException を送出

    m = re.search(r'\{.*\}', text, re.DOTALL)
    if not m:
        raise HTTPException(502, "Geminiからの応答を解析できませんでした")
    return json.loads(m.group())


# --- エンドポイント ---

@app.get("/", response_class=HTMLResponse)
async def index():
    with open("templates/index.html", encoding="utf-8") as f:
        return f.read()


@app.post("/api/resolve")
async def api_resolve(body: ResolveRequest):
    inp = body.input.strip()

    # URL からの抽出
    if "steamcommunity.com/profiles/" in inp:
        steam_id = inp.split("/profiles/")[1].strip("/").split("/")[0]
        return {"steam_id": steam_id}
    if "steamcommunity.com/id/" in inp:
        inp = inp.split("/id/")[1].strip("/").split("/")[0]

    # SteamID64 (17桁の数字)
    if inp.isdigit() and len(inp) == 17:
        return {"steam_id": inp}

    # vanity URL として解決
    async with httpx.AsyncClient(timeout=15.0) as client:
        steam_id = await resolve_vanity(client, inp)
    if not steam_id:
        raise HTTPException(404, "Steam IDが見つかりませんでした。SteamID64またはカスタムURL名を入力してください。")
    return {"steam_id": steam_id}


@app.post("/api/fetch")
async def api_fetch(body: FetchRequest, bg: BackgroundTasks):
    steam_id = body.steam_id

    prog = fetch_progress.get(steam_id, {})
    if prog.get("status") == "fetching":
        return {"status": "already_fetching"}

    if not body.force and db_get_user_games(steam_id):
        return {"status": "cached"}

    fetch_progress.pop(steam_id, None)
    bg.add_task(do_fetch, steam_id)
    return {"status": "started"}


@app.get("/api/status/{steam_id}")
async def api_status(steam_id: str):
    # fetch_progress にない & DB にデータあり → キャッシュ済み
    if steam_id not in fetch_progress and db_get_user_games(steam_id):
        return {"status": "cached"}
    prog = fetch_progress.get(steam_id)
    if not prog:
        return {"status": "not_started"}
    return prog


@app.get("/api/games/{steam_id}")
async def api_games(steam_id: str):
    user_games = db_get_user_games(steam_id)
    if not user_games:
        raise HTTPException(404, "データがありません")

    exclusions = db_get_exclusions(steam_id)
    result = []
    for g in sorted(user_games, key=lambda x: -x["playtime_minutes"]):
        details = db_get_game(g["app_id"])
        name = (details["name"] if details else None) or g["name"] or f"App {g['app_id']}"
        result.append({
            "app_id": g["app_id"],
            "name": name,
            "playtime_hours": round(g["playtime_minutes"] / 60, 1),
            "genres": details["genres"] if details else [],
            "excluded": g["app_id"] in exclusions,
        })
    return result


@app.post("/api/exclude")
async def api_exclude(body: ExcludeRequest):
    db_set_exclusion(body.steam_id, body.app_id, body.excluded)
    return {"ok": True}


@app.get("/api/quiz/{steam_id}")
async def api_quiz(steam_id: str, exclude_tools: bool = False):
    user_games = db_get_user_games(steam_id)
    if not user_games:
        raise HTTPException(404, "データがありません。先にデータを取得してください。")

    exclusions = db_get_exclusions(steam_id)
    candidates = [g for g in user_games if g["app_id"] not in exclusions and g["playtime_minutes"] > 0]
    if not candidates:
        raise HTTPException(404, "クイズに使えるゲームがありません。")

    random.shuffle(candidates)

    # 説明文があるゲームを優先して探す（最大10本試す）
    async with httpx.AsyncClient(timeout=15.0) as client:
        for g in candidates[:10]:
            app_id = g["app_id"]
            details = db_get_game(app_id)

            # キャッシュに説明文がなければ日本語→英語の順で取得
            if not details or not details.get("description"):
                raw = await get_app_details_best(client, app_id)
                if raw:
                    genres = [x["description"] for x in raw.get("genres", [])]
                    cats = [x["description"] for x in raw.get("categories", [])]
                    name = raw.get("name") or g["name"] or str(app_id)
                    desc = raw.get("short_description", "")
                    app_type = raw.get("type", "")
                    db_save_game(app_id, name, genres, cats, desc, app_type)
                    details = db_get_game(app_id)

            if not details:
                continue

            # ツール除外（詳細取得後に判定）
            if exclude_tools and is_tool_app(details):
                continue

            if details.get("description"):
                correct_name = details["name"] or g["name"]
                # 説明文中のゲーム名を [ゲーム名] に置き換え（使用量をカウント）
                usage = db_check_and_increment_usage(steam_id)
                if usage["ok"]:
                    description = await redact_name_in_description(details["description"], correct_name)
                else:
                    description = details["description"]  # 上限超過時はリダクションをスキップ
                return {
                    "app_id": app_id,
                    "description": description,
                    "correct_name": correct_name,
                }

    raise HTTPException(404, "説明文があるゲームが見つかりませんでした。データを再取得してみてください。")


@app.get("/api/quiz/usage/{steam_id}")
async def api_quiz_usage(steam_id: str):
    return db_get_usage(steam_id)


@app.post("/api/quiz/judge")
async def api_quiz_judge(body: QuizJudgeRequest):
    # ユーザー独自キーがあれば上限チェックをスキップ
    if not body.user_api_key:
        usage = db_check_and_increment_usage(body.steam_id)
        if not usage["ok"]:
            raise HTTPException(429, usage["reason"])
        extra = {"global_remaining": usage["global_remaining"], "user_remaining": usage["user_remaining"]}
    else:
        extra = {"global_remaining": None, "user_remaining": None}

    result = await judge_with_gemini(body.correct_name, body.user_answer, api_key=body.user_api_key)
    result.update(extra)
    return result


TOOL_GENRES = {"Game Development", "Utilities"}


def is_tool_app(details: dict) -> bool:
    if details.get("app_type") == "software":
        return True
    return bool(TOOL_GENRES & set(details.get("genres", [])))


@app.get("/api/analyze/{steam_id}")
async def api_analyze(
    steam_id: str,
    filter_type: str = "none",
    filter_value: float = 0.0,
    exclude_tools: bool = False,
):
    user_games = db_get_user_games(steam_id)
    if not user_games:
        raise HTTPException(404, "データがありません。先にデータを取得してください。")

    exclusions = db_get_exclusions(steam_id)

    # 除外ゲームを除く
    non_excluded = [g for g in user_games if g["app_id"] not in exclusions]

    # ツール類の除外
    if exclude_tools:
        def is_tool(g):
            details = db_get_game(g["app_id"])
            return details is not None and is_tool_app(details)
        non_excluded = [g for g in non_excluded if not is_tool(g)]

    total_playtime = sum(g["playtime_minutes"] for g in non_excluded)

    if filter_type == "hours":
        min_min = filter_value * 60
        filtered = [g for g in non_excluded if g["playtime_minutes"] >= min_min]
    elif filter_type == "percent":
        min_min = total_playtime * (filter_value / 100)
        filtered = [g for g in non_excluded if g["playtime_minutes"] >= min_min]
    else:
        filtered = [g for g in non_excluded if g["playtime_minutes"] > 0]

    filtered.sort(key=lambda x: x["playtime_minutes"], reverse=True)

    genre_pt: dict[str, float] = {}
    cat_pt: dict[str, float] = {}
    game_list = []

    for g in filtered:
        app_id = g["app_id"]
        pt = g["playtime_minutes"]
        details = db_get_game(app_id)
        genres = details["genres"] if details else []
        cats = details["categories"] if details else []
        name = (details["name"] if details else None) or g["name"] or f"App {app_id}"

        game_list.append({
            "app_id": app_id,
            "name": name,
            "playtime_hours": round(pt / 60, 1),
            "genres": genres,
        })
        for genre in genres:
            genre_pt[genre] = genre_pt.get(genre, 0) + pt
        for cat in cats:
            cat_pt[cat] = cat_pt.get(cat, 0) + pt

    return {
        "total_games": len(user_games),
        "excluded_games": len(exclusions),
        "filtered_games": len(filtered),
        "total_playtime_hours": round(total_playtime / 60, 1),
        "filtered_playtime_hours": round(sum(g["playtime_minutes"] for g in filtered) / 60, 1),
        "genres": [
            {"name": k, "hours": round(v / 60, 1)}
            for k, v in sorted(genre_pt.items(), key=lambda x: -x[1])[:15]
        ],
        "categories": [
            {"name": k, "hours": round(v / 60, 1)}
            for k, v in sorted(cat_pt.items(), key=lambda x: -x[1])[:15]
        ],
        "top_games": game_list[:30],
    }
