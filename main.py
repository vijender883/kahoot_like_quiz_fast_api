from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Dict, Optional
import asyncio
import json
from datetime import datetime
import uuid
from motor.motor_asyncio import AsyncIOMotorClient
import os
from enum import Enum

app = FastAPI(title="Kahoot Clone API")

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# MongoDB connection
MONGODB_URL = os.getenv("MONGODB_URL", "mongodb://localhost:27017")
client = AsyncIOMotorClient(MONGODB_URL)
db = client.kahoot_clone

# Pydantic models
class GameStatus(str, Enum):
    WAITING = "waiting"
    STARTED = "started"
    QUESTION = "question"
    RESULTS = "results"
    FINISHED = "finished"

class Question(BaseModel):
    question: str
    options: List[str]
    correct_answer: int
    time_limit: int = 20

class Quiz(BaseModel):
    title: str
    questions: List[Question]

class Player(BaseModel):
    id: str
    name: str
    score: int = 0
    current_answer: Optional[int] = None
    answer_time: Optional[float] = None

class Game(BaseModel):
    code: str
    quiz: Quiz
    players: List[Player] = []
    status: GameStatus = GameStatus.WAITING
    current_question: int = 0
    question_start_time: Optional[datetime] = None
    host_connected: bool = False

# In-memory storage for active games and connections
active_games: Dict[str, Game] = {}
connections: Dict[str, List[WebSocket]] = {}  # game_code -> list of websockets
player_connections: Dict[str, WebSocket] = {}  # player_id -> websocket

# Helper functions
def generate_game_code() -> str:
    return str(uuid.uuid4())[:6].upper()

async def broadcast_to_game(game_code: str, message: dict):
    if game_code in connections:
        disconnected = []
        for websocket in connections[game_code]:
            try:
                await websocket.send_text(json.dumps(message))
            except:
                disconnected.append(websocket)
        
        # Remove disconnected websockets
        for ws in disconnected:
            connections[game_code].remove(ws)

async def save_game_to_db(game: Game):
    await db.games.replace_one(
        {"code": game.code}, 
        game.dict(), 
        upsert=True
    )

async def get_game_from_db(game_code: str) -> Optional[Game]:
    game_data = await db.games.find_one({"code": game_code})
    if game_data:
        return Game(**game_data)
    return None

# API Routes

@app.post("/create-quiz")
async def create_quiz(quiz: Quiz):
    game_code = generate_game_code()
    
    # Ensure unique game code
    while game_code in active_games or await get_game_from_db(game_code):
        game_code = generate_game_code()
    
    game = Game(code=game_code, quiz=quiz)
    active_games[game_code] = game
    connections[game_code] = []
    
    await save_game_to_db(game)
    
    return {"game_code": game_code, "message": "Quiz created successfully"}

@app.post("/join-game/{game_code}")
async def join_game(game_code: str, player_name: str):
    game = active_games.get(game_code) or await get_game_from_db(game_code)
    
    if not game:
        raise HTTPException(status_code=404, detail="Game not found")
    
    if game.status != GameStatus.WAITING:
        raise HTTPException(status_code=400, detail="Game already started")
    
    if len(game.players) >= 500:
        raise HTTPException(status_code=400, detail="Game is full")
    
    # Check if player name already exists
    if any(p.name == player_name for p in game.players):
        raise HTTPException(status_code=400, detail="Player name already taken")
    
    player_id = str(uuid.uuid4())
    player = Player(id=player_id, name=player_name)
    game.players.append(player)
    
    active_games[game_code] = game
    await save_game_to_db(game)
    
    # Broadcast updated player list
    await broadcast_to_game(game_code, {
        "type": "player_joined",
        "players": [{"id": p.id, "name": p.name} for p in game.players],
        "total_players": len(game.players)
    })
    
    return {"player_id": player_id, "message": "Joined game successfully"}

@app.post("/start-game/{game_code}")
async def start_game(game_code: str):
    game = active_games.get(game_code) or await get_game_from_db(game_code)
    
    if not game:
        raise HTTPException(status_code=404, detail="Game not found")
    
    if game.status != GameStatus.WAITING:
        raise HTTPException(status_code=400, detail="Game already started")
    
    if len(game.players) == 0:
        raise HTTPException(status_code=400, detail="No players in game")
    
    game.status = GameStatus.STARTED
    game.current_question = 0
    active_games[game_code] = game
    await save_game_to_db(game)
    
    # Broadcast game start
    await broadcast_to_game(game_code, {
        "type": "game_started",
        "message": "Game is starting!"
    })
    
    # Start first question after 3 seconds
    asyncio.create_task(start_question(game_code, 0))
    
    return {"message": "Game started successfully"}

async def start_question(game_code: str, question_index: int):
    await asyncio.sleep(3)  # 3 second delay
    
    game = active_games.get(game_code)
    if not game or game.status == GameStatus.FINISHED:
        return
    
    if question_index >= len(game.quiz.questions):
        await end_game(game_code)
        return
    
    question = game.quiz.questions[question_index]
    game.status = GameStatus.QUESTION
    game.current_question = question_index
    game.question_start_time = datetime.now()
    
    # Reset player answers
    for player in game.players:
        player.current_answer = None
        player.answer_time = None
    
    active_games[game_code] = game
    await save_game_to_db(game)
    
    # Broadcast question
    await broadcast_to_game(game_code, {
        "type": "question",
        "question_number": question_index + 1,
        "total_questions": len(game.quiz.questions),
        "question": question.question,
        "options": question.options,
        "time_limit": question.time_limit
    })
    
    # Auto move to results after time limit
    asyncio.create_task(show_question_results(game_code, question_index))

async def show_question_results(game_code: str, question_index: int):
    game = active_games.get(game_code)
    if not game:
        return
    
    question = game.quiz.questions[question_index]
    await asyncio.sleep(question.time_limit)
    
    game = active_games.get(game_code)
    if not game or game.current_question != question_index:
        return
    
    game.status = GameStatus.RESULTS
    
    # Calculate scores
    question_obj = game.quiz.questions[question_index]
    max_score = 1000
    
    for player in game.players:
        if player.current_answer == question_obj.correct_answer and player.answer_time:
            # Score based on speed (faster = more points)
            time_bonus = max(0, (question_obj.time_limit - player.answer_time) / question_obj.time_limit)
            points = int(max_score * (0.5 + 0.5 * time_bonus))
            player.score += points
    
    active_games[game_code] = game
    await save_game_to_db(game)
    
    # Prepare results
    results = []
    answer_counts = [0] * len(question_obj.options)
    
    for player in game.players:
        if player.current_answer is not None:
            answer_counts[player.current_answer] += 1
            results.append({
                "player_id": player.id,
                "name": player.name,
                "answer": player.current_answer,
                "correct": player.current_answer == question_obj.correct_answer,
                "score": player.score
            })
    
    # Broadcast results
    await broadcast_to_game(game_code, {
        "type": "question_results",
        "correct_answer": question_obj.correct_answer,
        "answer_counts": answer_counts,
        "results": results,
        "leaderboard": sorted([{"name": p.name, "score": p.score} for p in game.players], 
                            key=lambda x: x["score"], reverse=True)[:10]
    })
    
    # Move to next question or end game
    if question_index + 1 < len(game.quiz.questions):
        asyncio.create_task(start_question(game_code, question_index + 1))
    else:
        asyncio.create_task(end_game(game_code))

async def end_game(game_code: str):
    await asyncio.sleep(5)  # Show results for 5 seconds
    
    game = active_games.get(game_code)
    if not game:
        return
    
    game.status = GameStatus.FINISHED
    active_games[game_code] = game
    await save_game_to_db(game)
    
    # Final leaderboard
    final_leaderboard = sorted([{"name": p.name, "score": p.score} for p in game.players], 
                              key=lambda x: x["score"], reverse=True)
    
    await broadcast_to_game(game_code, {
        "type": "game_finished",
        "final_leaderboard": final_leaderboard
    })

@app.post("/submit-answer/{game_code}/{player_id}")
async def submit_answer(game_code: str, player_id: str, answer: int):
    game = active_games.get(game_code)
    if not game:
        raise HTTPException(status_code=404, detail="Game not found")
    
    if game.status != GameStatus.QUESTION:
        raise HTTPException(status_code=400, detail="Not accepting answers right now")
    
    player = next((p for p in game.players if p.id == player_id), None)
    if not player:
        raise HTTPException(status_code=404, detail="Player not found")
    
    if player.current_answer is not None:
        raise HTTPException(status_code=400, detail="Answer already submitted")
    
    # Calculate answer time
    if game.question_start_time:
        answer_time = (datetime.now() - game.question_start_time).total_seconds()
        player.answer_time = answer_time
    
    player.current_answer = answer
    active_games[game_code] = game
    await save_game_to_db(game)
    
    return {"message": "Answer submitted successfully"}

@app.get("/game-status/{game_code}")
async def get_game_status(game_code: str):
    game = active_games.get(game_code) or await get_game_from_db(game_code)
    if not game:
        raise HTTPException(status_code=404, detail="Game not found")
    
    return {
        "status": game.status,
        "current_question": game.current_question,
        "total_questions": len(game.quiz.questions),
        "players_count": len(game.players)
    }

# WebSocket endpoint
@app.websocket("/ws/{game_code}")
async def websocket_endpoint(websocket: WebSocket, game_code: str):
    await websocket.accept()
    
    # Add connection to game
    if game_code not in connections:
        connections[game_code] = []
    connections[game_code].append(websocket)
    
    try:
        while True:
            data = await websocket.receive_text()
            message = json.loads(data)
            
            if message.get("type") == "ping":
                await websocket.send_text(json.dumps({"type": "pong"}))
            elif message.get("type") == "host_connected":
                game = active_games.get(game_code)
                if game:
                    game.host_connected = True
                    active_games[game_code] = game
    
    except WebSocketDisconnect:
        connections[game_code].remove(websocket)
        if len(connections[game_code]) == 0:
            del connections[game_code]

# Health check
@app.get("/health")
async def health_check():
    return {"status": "healthy", "timestamp": datetime.now()}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)