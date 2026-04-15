from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
import psycopg

app = FastAPI()

conn = psycopg.connect(
    host="localhost",
    port=5432,
    dbname="chemdb",
    user="postgres",
    password="tkddkqkr52~!~!"
)

def get_chemical_info(name: str):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT name_ko, summary
            FROM chemicals
            WHERE name_ko = %s
        """, (name,))
        return cur.fetchone()

def get_hazards(name: str):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT h.hazard_type, h.description
            FROM hazards h
            JOIN chemicals c ON h.chemical_id = c.id
            WHERE c.name_ko = %s
            ORDER BY h.id
        """, (name,))
        return cur.fetchall()

def get_responses(name: str):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT r.situation, r.action
            FROM responses r
            JOIN chemicals c ON r.chemical_id = c.id
            WHERE c.name_ko = %s
            ORDER BY r.id
        """, (name,))
        return cur.fetchall()

def extract_chemical_name(user_text: str):
    candidates = ["휘발유", "염산", "황산", "암모니아", "벤젠"]
    for c in candidates:
        if c in user_text:
            return c
    return None

@app.get("/")
def health_check():
    return {"ok": True}

@app.post("/kakao/skill")
async def kakao_skill(request: Request):
    return JSONResponse(content={
        "version": "2.0",
        "template": {
            "outputs": [
                {
                    "simpleText": {
                        "text": "연결 테스트 성공"
                    }
                }
            ]
        }
    })

@app.post("/kakao/chat")
async def kakao_chat(request: Request):
    payload = await request.json()
    user_text = payload.get("userRequest", {}).get("utterance", "").strip()

    chemical_name = extract_chemical_name(user_text)

    if not chemical_name:
        return JSONResponse(content={
            "version": "2.0",
            "template": {
                "outputs": [
                    {
                        "simpleText": {
                            "text": "물질명을 포함해서 질문해주세요. 예: 휘발유 위험성 알려줘"
                        }
                    }
                ]
            }
        })

    chemical = get_chemical_info(chemical_name)
    hazards = get_hazards(chemical_name)
    responses = get_responses(chemical_name)

    if not chemical:
        return JSONResponse(content={
            "version": "2.0",
            "template": {
                "outputs": [
                    {
                        "simpleText": {
                            "text": f"{chemical_name} 정보가 아직 등록되어 있지 않습니다."
                        }
                    }
                ]
            }
        })

    name_ko, summary = chemical
    lines = [f"{name_ko}", f"요약: {summary}"]

    if hazards:
        lines.append("")
        lines.append("[위험성]")
        for hazard_type, description in hazards[:3]:
            lines.append(f"- {hazard_type}: {description}")

    if responses:
        lines.append("")
        lines.append("[대응법]")
        for situation, action in responses[:3]:
            lines.append(f"- {situation}: {action}")

    return JSONResponse(content={
        "version": "2.0",
        "template": {
            "outputs": [
                {
                    "simpleText": {
                        "text": "\n".join(lines)[:1000]
                    }
                }
            ]
        }
    })