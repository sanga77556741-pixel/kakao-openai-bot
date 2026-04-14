import os
from typing import Optional

import psycopg
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

app = FastAPI()

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL 환경변수가 설정되지 않았습니다.")


def get_connection():
    return psycopg.connect(DATABASE_URL)


def get_chemical_info(name: str):
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT name_ko, summary
                FROM chemicals
                WHERE name_ko = %s
                """,
                (name,),
            )
            return cur.fetchone()


def get_hazards(name: str):
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT h.hazard_type, h.description
                FROM hazards h
                JOIN chemicals c ON h.chemical_id = c.id
                WHERE c.name_ko = %s
                ORDER BY h.id
                """,
                (name,),
            )
            return cur.fetchall()


def get_responses(name: str):
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT r.situation, r.action
                FROM responses r
                JOIN chemicals c ON r.chemical_id = c.id
                WHERE c.name_ko = %s
                ORDER BY r.id
                """,
                (name,),
            )
            return cur.fetchall()


def extract_chemical_name(user_text: str) -> Optional[str]:
    candidates = {
        "휘발유": ["휘발유", "gasoline"],
        "염산": ["염산", "hydrochloric acid", "hcl"],
        "황산": ["황산", "sulfuric acid"],
        "암모니아": ["암모니아", "ammonia"],
        "벤젠": ["벤젠", "benzene"],
    }

    lower_text = user_text.lower()
    for chemical_name, keywords in candidates.items():
        for keyword in keywords:
            if keyword.lower() in lower_text:
                return chemical_name
    return None


def make_simple_text_response(text: str) -> JSONResponse:
    return JSONResponse(
        content={
            "version": "2.0",
            "template": {
                "outputs": [
                    {
                        "simpleText": {
                            "text": text[:1000]
                        }
                    }
                ]
            }
        }
    )


@app.get("/")
def health_check():
    return {"ok": True}


@app.post("/kakao/skill")
async def kakao_skill(request: Request):
    return make_simple_text_response("연결 테스트 성공")


@app.post("/kakao/chat")
async def kakao_chat(request: Request):
    try:
        payload = await request.json()
        user_text = payload.get("userRequest", {}).get("utterance", "").strip()

        if not user_text:
            return make_simple_text_response("질문을 입력해주세요.")

        chemical_name = extract_chemical_name(user_text)

        if not chemical_name:
            return make_simple_text_response(
                "물질명을 포함해서 질문해주세요. 예: 휘발유 위험성 알려줘"
            )

        chemical = get_chemical_info(chemical_name)
        hazards = get_hazards(chemical_name)
        responses = get_responses(chemical_name)

        if not chemical:
            return make_simple_text_response(
                f"{chemical_name} 정보가 아직 등록되어 있지 않습니다."
            )

        name_ko, summary = chemical
        lines = [name_ko, f"요약: {summary}"]

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

        return make_simple_text_response("\n".join(lines))

    except Exception as e:
        return make_simple_text_response(f"오류가 발생했습니다: {str(e)[:150]}")