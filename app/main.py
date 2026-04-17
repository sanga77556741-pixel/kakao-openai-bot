import os
import re
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


def normalize_text(text: str) -> str:
    return re.sub(r"\s+", " ", text.strip().lower())


def get_chemical_info_by_user_text(user_text: str):
    normalized_text = normalize_text(user_text)

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    c.id,
                    c.name_ko,
                    c.summary,
                    c.physical_state,
                    c.ppe,
                    c.precautions,
                    c.human_hazard,
                    c.fire_explosion_response,
                    c.marine_spill_response
                FROM chemicals c
                LEFT JOIN chemical_aliases a
                    ON c.id = a.chemical_id
                WHERE LOWER(c.name_ko) LIKE %s
                   OR LOWER(a.alias) LIKE %s
                ORDER BY c.id
                LIMIT 1
                """,
                (f"%{normalized_text}%", f"%{normalized_text}%"),
            )
            return cur.fetchone()


def get_hazards_by_chemical_id(chemical_id: int):
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    hazard_type,
                    description
                FROM hazards
                WHERE chemical_id = %s
                ORDER BY id
                """,
                (chemical_id,),
            )
            return cur.fetchall()


def extract_intent(user_text: str) -> str:
    text = normalize_text(user_text)

    if "위험" in text or "유해" in text:
        return "hazards"

    if "대응" in text or "조치" in text or "사고" in text:
        return "response"

    if "보호" in text or "장구" in text or "보호구" in text:
        return "ppe"

    if "주의" in text or "주의사항" in text:
        return "precautions"

    if "인체" in text or "건강" in text or "독성" in text:
        return "human_hazard"

    if "특성" in text or "성상" in text or "상태" in text:
        return "physical_state"

    if "요약" in text:
        return "summary"

    return "all"


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

        intent = extract_intent(user_text)
        chemical = get_chemical_info_by_user_text(user_text)

        if not chemical:
            return make_simple_text_response(
                "물질명을 포함해서 질문해주세요. 예: 염산 위험성 알려줘"
            )

        (
            chemical_id,
            name_ko,
            summary,
            physical_state,
            ppe,
            precautions,
            human_hazard,
            fire_explosion_response,
            marine_spill_response,
        ) = chemical

        hazards = get_hazards_by_chemical_id(chemical_id)

        lines = [name_ko]

        if intent == "all":
            if summary:
                lines.append("")
                lines.append(f"요약: {summary}")

            if physical_state:
                lines.append("")
                lines.append("[물질특성]")
                lines.append(f"- {physical_state}")

            if hazards:
                lines.append("")
                lines.append("[주요 위험성]")

                seen = set()
                for hazard_type, description in hazards:
                    key = (hazard_type, description)
                    if key in seen:
                        continue
                    seen.add(key)

                    if description:
                        lines.append(f"- {hazard_type}: {description}")
                    else:
                        lines.append(f"- {hazard_type}")

            if ppe:
                lines.append("")
                lines.append("[주요 개인보호장구]")
                lines.append(f"- {ppe}")

            if precautions:
                lines.append("")
                lines.append("[주의사항]")
                lines.append(f"- {precautions}")

            if fire_explosion_response or marine_spill_response:
                lines.append("")
                lines.append("[대응방법]")

                if fire_explosion_response:
                    lines.append(f"- 화재·폭발: {fire_explosion_response}")

                if marine_spill_response:
                    lines.append(f"- 해상유출: {marine_spill_response}")

            if human_hazard:
                lines.append("")
                lines.append("[인체유해성]")
                lines.append(f"- {human_hazard}")

        elif intent == "summary":
            lines.append("")
            if summary:
                lines.append(f"요약: {summary}")
            else:
                lines.append("요약 정보가 없습니다.")

        elif intent == "physical_state":
            lines.append("")
            if physical_state:
                lines.append("[물질특성]")
                lines.append(f"- {physical_state}")
            else:
                lines.append("물질특성 정보가 없습니다.")

        elif intent == "hazards":
            lines.append("")
            if hazards:
                lines.append("[주요 위험성]")

                seen = set()
                for hazard_type, description in hazards:
                    key = (hazard_type, description)
                    if key in seen:
                        continue
                    seen.add(key)

                    if description:
                        lines.append(f"- {hazard_type}: {description}")
                    else:
                        lines.append(f"- {hazard_type}")
            else:
                lines.append("위험성 정보가 없습니다.")

        elif intent == "ppe":
            lines.append("")
            if ppe:
                lines.append("[주요 개인보호장구]")
                lines.append(f"- {ppe}")
            else:
                lines.append("개인보호장구 정보가 없습니다.")

        elif intent == "precautions":
            lines.append("")
            if precautions:
                lines.append("[주의사항]")
                lines.append(f"- {precautions}")
            else:
                lines.append("주의사항 정보가 없습니다.")

        elif intent == "response":
            lines.append("")
            if fire_explosion_response or marine_spill_response:
                lines.append("[대응방법]")

                if fire_explosion_response:
                    lines.append(f"- 화재·폭발: {fire_explosion_response}")

                if marine_spill_response:
                    lines.append(f"- 해상유출: {marine_spill_response}")
            else:
                lines.append("대응방법 정보가 없습니다.")

        elif intent == "human_hazard":
            lines.append("")
            if human_hazard:
                lines.append("[인체유해성]")
                lines.append(f"- {human_hazard}")
            else:
                lines.append("인체유해성 정보가 없습니다.")

        else:
            lines.append("")
            lines.append("질문 의도를 파악하지 못했습니다.")

        return make_simple_text_response("\n".join(lines))

    except Exception as e:
        return make_simple_text_response(f"오류가 발생했습니다: {str(e)[:150]}")