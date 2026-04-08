import os
import json
import re
from typing import Any, Dict, List

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from openai import OpenAI

app = FastAPI()

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

SYSTEM_PROMPT = """
너는 카카오톡 챗봇이다.

규칙:
- 항상 한국어로 답변
- 너무 길게 쓰지 말 것
- 먼저 한 줄 요약
- 그 다음 핵심 포인트 3개 이내
- 위험물, 안전, 응급 관련 질문은 '즉시 해야 할 행동'을 우선
- 과도한 꾸밈말 없이 친절하고 단정하게
- 출력은 반드시 JSON만 반환
- JSON 스키마:
{
  "summary": "한 줄 요약",
  "points": ["핵심1", "핵심2", "핵심3"],
  "followups": ["후속질문1", "후속질문2"]
}
"""

def extract_user_text(payload: Dict[str, Any]) -> str:
    return (
        payload.get("userRequest", {}).get("utterance")
        or payload.get("action", {}).get("params", {}).get("question", "")
        or ""
    ).strip()

def safe_parse_model_json(text: str) -> Dict[str, Any]:
    text = text.strip()
    text = re.sub(r"^```json\s*", "", text)
    text = re.sub(r"\s*```$", "", text)

    try:
        data = json.loads(text)
        if isinstance(data, dict):
            return normalize_response(data)
    except Exception:
        pass

    return normalize_response({
        "summary": text[:120] if text else "답변을 준비했어요.",
        "points": [],
        "followups": ["다시 설명해줘", "예시로 알려줘"]
    })

def normalize_response(data: Dict[str, Any]) -> Dict[str, Any]:
    summary = str(data.get("summary", "답변을 준비했어요.")).strip()

    points = data.get("points", [])
    if not isinstance(points, list):
        points = []
    points = [str(p).strip() for p in points if str(p).strip()][:3]

    followups = data.get("followups", [])
    if not isinstance(followups, list):
        followups = []
    followups = [str(f).strip() for f in followups if str(f).strip()][:2]

    if not followups:
        followups = ["자세히 설명해줘", "다른 예시 알려줘"]

    return {
        "summary": summary[:120],
        "points": [p[:120] for p in points],
        "followups": [f[:20] for f in followups]
    }

def render_kakao_response(data: Dict[str, Any]) -> Dict[str, Any]:
    desc_lines: List[str] = []

    for idx, point in enumerate(data["points"], start=1):
        desc_lines.append(f"{idx}. {point}")

    description = "\n".join(desc_lines).strip()
    if not description:
        description = "궁금한 내용을 더 물어보시면 이어서 설명해드릴게요."

    buttons = []
    for followup in data["followups"]:
        buttons.append({
            "action": "message",
            "label": followup[:14],
            "messageText": followup
        })

    return {
        "version": "2.0",
        "template": {
            "outputs": [
                {
                    "basicCard": {
                        "title": data["summary"],
                        "description": description[:500],
                        "buttons": buttons
                    }
                }
            ]
        }
    }

@app.get("/")
def health_check():
    return {"ok": True}

# 검증용
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

# 실제 답변용
@app.post("/kakao/chat")
async def kakao_chat(request: Request):
    try:
        payload = await request.json()
        user_text = extract_user_text(payload)

        if not user_text:
            return JSONResponse(content={
                "version": "2.0",
                "template": {
                    "outputs": [
                        {
                            "simpleText": {
                                "text": "질문을 입력해주세요."
                            }
                        }
                    ]
                }
            })

        response = client.responses.create(
            model="gpt-4.1-mini",
            input=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": user_text}
            ]
        )

        model_text = response.output_text
        structured = safe_parse_model_json(model_text)
        kakao_json = render_kakao_response(structured)

        return JSONResponse(content=kakao_json)

    except Exception as e:
        return JSONResponse(content={
            "version": "2.0",
            "template": {
                "outputs": [
                    {
                        "simpleText": {
                            "text": f"일시적인 오류가 발생했어요. 다시 시도해주세요.\n({str(e)[:120]})"
                        }
                    }
                ]
            }
        })