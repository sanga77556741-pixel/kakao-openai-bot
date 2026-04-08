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

    except Exception:
        return JSONResponse(content={
            "version": "2.0",
            "template": {
                "outputs": [
                    {
                        "simpleText": {
                            "text": "일시적인 오류가 발생했어요. 다시 시도해주세요."
                        }
                    }
                ]
            }
        })