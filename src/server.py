import os
import json
import asyncio
import logging
import base64
import audioop
import websockets
from fastapi import FastAPI, WebSocket, Request
from fastapi.responses import HTMLResponse, JSONResponse
from twilio.rest import Client
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("ava.native")

TWILIO_ACCOUNT_SID = os.getenv("TWILIO_ACCOUNT_SID")
TWILIO_AUTH_TOKEN = os.getenv("TWILIO_AUTH_TOKEN")
TWILIO_NUMBER = "+18147396855"
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

URL_DO_NGROK = "faithlessly-nonadventurous-giovanni.ngrok-free.dev"

client = Client(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)
app = FastAPI()

@app.get("/teste-ligacao")
async def teste_ligacao(numero: str = None):
    if not numero:
        return JSONResponse({"status": "Erro: Adicione ?numero="})
    try:
        num_limpo = numero.strip().replace(" ", "").replace("-", "")
        if not num_limpo.startswith("+"):
            num_limpo = f"+{num_limpo}"

        call = client.calls.create(
            url=f"https://{URL_DO_NGROK}/twiml",
            to=num_limpo,
            from_=TWILIO_NUMBER
        )
        return JSONResponse({"status": "Ligação disparada!", "sid": call.sid})
    except Exception as e:
        return JSONResponse({"error": "Falha na Twilio", "detalhes": str(e)})

@app.api_route("/twiml", methods=["GET", "POST"])
async def twiml_route(request: Request):
    twiml_response = f"""<?xml version="1.0" encoding="UTF-8"?>
<Response>
    <Connect>
        <Stream url="wss://{URL_DO_NGROK}/stream" />
    </Connect>
    <Pause length="3600"/>
</Response>"""
    return HTMLResponse(content=twiml_response, media_type="text/xml")

@app.websocket("/stream")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    logger.info("Conexão WebSocket aceita da Twilio.")

    stream_sid = None
    # Endpoint oficial v1beta para Gemini Live API
    gemini_ws_url = f"wss://generativelanguage.googleapis.com/ws/google.ai.generativelanguage.v1beta.GenerativeService.BidiGenerateContent?key={GEMINI_API_KEY}"

    try:
        async with websockets.connect(gemini_ws_url) as gemini_ws:
            logger.info("Conectado à Gemini Live API via WebSocket puro com sucesso.")

            # Setup atualizado com o modelo mais recente e suporte a áudio nativo
            setup_message = {
                "setup": {
                    "model": "models/gemini-3.1-flash-live-preview",
                    "generation_config": {
                        "response_modalities": ["AUDIO"],
                        "speech_config": {
                            "voice_config": {
                                "prebuilt_voice_config": {
                                    "voice_name": "Puck"
                                }
                            }
                        }
                    },
                    "system_instruction": {
                        "parts": [{"text": "Você é a AVA, assistente virtual da Central Moura Fácil. Atenda em português brasileiro com voz natural, cordial e frases curtas."}]
                    }
                }
            }
            await gemini_ws.send(json.dumps(setup_message))

            async def twilio_to_gemini():
                nonlocal stream_sid
                try:
                    while True:
                        message = await websocket.receive_text()
                        data = json.loads(message)
                        event = data.get("event")

                        if event == "start":
                            stream_sid = data["start"]["streamSid"]
                            logger.info(f"Stream iniciado: {stream_sid}")
                        elif event == "media":
                            payload = data["media"]["payload"]
                            
                            # Conversão de áudio da Twilio (mulaw 8kHz -> PCM 16kHz)
                            mulaw_bytes = base64.b64decode(payload)
                            pcm_8k = audioop.ulaw2lin(mulaw_bytes, 2)
                            pcm_16k, _ = audioop.ratecv(pcm_8k, 2, 1, 8000, 16000, None)
                            pcm_b64 = base64.b64encode(pcm_16k).decode("utf-8")

                            # NOVO FORMATO OFICIAL: Utilizando o campo 'audio' direto em vez de 'media_chunks'
                            realtime_message = {
                                "realtime_input": {
                                    "audio": {
                                        "mime_type": "audio/pcm;rate=16000",
                                        "data": pcm_b64
                                    }
                                }
                            }
                            await gemini_ws.send(json.dumps(realtime_message))
                        elif event == "stop":
                            break
                except Exception as e:
                    logger.error(f"Erro na ponte Twilio -> Gemini: {e}")

            async def gemini_to_twilio():
                nonlocal stream_sid
                try:
                    async for raw_response in gemini_ws:
                        response = json.loads(raw_response)
                        server_content = response.get("serverContent")
                        if server_content and "modelTurn" in server_content:
                            parts = server_content["modelTurn"].get("parts", [])
                            for part in parts:
                                if "inlineData" in part and stream_sid:
                                    inline_data = part["inlineData"]
                                    audio_b64 = inline_data.get("data")
                                    audio_bytes = base64.b64decode(audio_b64)
                                    
                                    # Conversão de áudio do Gemini para a Twilio (PCM 24kHz -> mulaw 8kHz)
                                    pcm_8k, _ = audioop.ratecv(audio_bytes, 2, 1, 24000, 8000, None)
                                    mulaw_data = audioop.lin2ulaw(pcm_8k, 2)
                                    b64_audio = base64.b64encode(mulaw_data).decode("utf-8")
                                    
                                    await websocket.send_json({
                                        "event": "media",
                                        "streamSid": stream_sid,
                                        "media": {"payload": b64_audio}
                                    })
                except Exception as e:
                    logger.error(f"Erro na ponte Gemini -> Twilio: {e}")

            await asyncio.gather(twilio_to_gemini(), gemini_to_twilio())

    except Exception as e:
        logger.error(f"Erro no WebSocket endpoint: {e}")
    finally:
        logger.info("Conexão encerrada.")