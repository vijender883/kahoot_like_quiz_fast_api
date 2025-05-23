from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException, Body
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
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
MONGODB_URL = os.getenv("MONGODB_URL", "mongodb+srv://admin:RCyKhyxEw0LmSSXN@cluster0.rq9f.mongodb.net/main?retryWrites=true&w=majority&appName=Cluster0")
client = AsyncIOMotorClient(MONGODB_URL)
db = client.test

# Pydantic models
class GameStatus(str, Enum):
    WAITING = "waiting"
    STARTED = "started"
    QUESTION = "question"
    RESULTS = "results"
    FINISHED = "finished"

class Question(BaseModel):
    question_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    question: str
    options: List[str]
    correct_answer: int
    time_limit: int = 20

class Quiz(BaseModel):
    title: str
    questions: List[Question]

class Game(BaseModel):
    code: str
    quiz: Quiz
    status: GameStatus = GameStatus.WAITING
    current_question: int = 0
    question_start_time: Optional[datetime] = None
    host_connected: bool = False

class UserAnswer(BaseModel):
    question_id: str
    answer: Optional[int] = None
    is_correct: Optional[bool] = None
    time_taken: Optional[float] = None
    score: int = 0

class UserSubmission(BaseModel):
    player_id: str
    quiz_code: str
    answers: Dict[str, UserAnswer] = {}
    total_score: int = 0

# User validation models
class UserValidationRequest(BaseModel):
    email_id: str

class UserValidationResponse(BaseModel):
    name: str
    player_id: str

class UserInfo(BaseModel):
    name: str
    email_id: str
    phone_no: str
    player_id: Optional[str] = None

class JoinGameRequest(BaseModel):
    player_id: str

class SubmitAnswerRequest(BaseModel):
    submitter_answer: int
    time_taken: float

# In-memory storage for active games and connections
active_games: Dict[str, Game] = {}
connections: Dict[str, List[WebSocket]] = {}
player_connections: Dict[str, WebSocket] = {}

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

async def get_user_submission_from_db(quiz_code: str, player_id: str) -> Optional[UserSubmission]:
    submission_data = await db.user_submissions.find_one({"quiz_code": quiz_code, "player_id": player_id})
    if submission_data:
        return UserSubmission(**submission_data)
    return None

async def get_user_info_from_db(player_id: str) -> Optional[UserInfo]:
    user_data = await db.user_info.find_one({"player_id": player_id})
    if user_data:
        return UserInfo(**user_data)
    return None

# API Routes

@app.post("/validate-user", response_model=UserValidationResponse)
async def validate_user(request: UserValidationRequest):
    user = await db.user_info.find_one({"email_id": request.email_id})
    
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    
    # Check if a player_id is already assigned to this user
    player_id = user.get("player_id")
    
    # If no player_id exists, generate a new one and update the user_info document
    if not player_id:
        player_id = str(uuid.uuid4())
        await db.user_info.update_one(
            {"_id": user["_id"]},
            {"$set": {"player_id": player_id}}
        )
    
    return UserValidationResponse(name=user["name"], player_id=player_id)

@app.post("/create-quiz")
async def create_quiz(quiz: Quiz):
    game_code = generate_game_code()
    
    while game_code in active_games or await get_game_from_db(game_code):
        game_code = generate_game_code()
    
    for q in quiz.questions:
        q.question_id = str(uuid.uuid4())

    game = Game(code=game_code, quiz=quiz)
    active_games[game_code] = game
    connections[game_code] = []
    
    await save_game_to_db(game)
    
    return {"game_code": game_code, "message": "Quiz created successfully"}

@app.post("/join-game/{game_code}")
async def join_game(game_code: str, request: JoinGameRequest):
    game = active_games.get(game_code) or await get_game_from_db(game_code)
    
    if not game:
        raise HTTPException(status_code=404, detail="Game not found")
    
    # if game.status != GameStatus.WAITING:
    #     raise HTTPException(status_code=400, detail="Game already started")
    
    user_info = await get_user_info_from_db(request.player_id)
    if not user_info:
        raise HTTPException(status_code=404, detail="Player not found. Please validate user first.")

    existing_submission = await db.user_submissions.find_one({"quiz_code": game_code, "player_id": request.player_id})
    if existing_submission:
        return {
            "player_id": existing_submission["player_id"], 
            "message": f"Welcome back {user_info.name}! You are already registered for this game."
        }
    
    new_submission = UserSubmission(
        player_id=request.player_id,
        quiz_code=game_code,
        answers={},
        total_score=0
    )
    await db.user_submissions.insert_one(new_submission.dict())
    
    all_submissions_cursor = db.user_submissions.find({"quiz_code": game_code})
    players_in_game = []
    for s in await all_submissions_cursor.to_list(length=None):
        player_user_info = await get_user_info_from_db(s["player_id"])
        if player_user_info:
            players_in_game.append({
                "player_id": s["player_id"],
                "email_id": player_user_info.email_id,
                "name": player_user_info.name
            })

    await broadcast_to_game(game_code, {
        "type": "player_joined",
        "players": players_in_game,
        "total_players": len(players_in_game)
    })
    
    return {
        "player_id": request.player_id, 
        "message": f"Welcome {user_info.name}! You have successfully joined the game."
    }

@app.post("/start-game/{game_code}")
async def start_game(game_code: str):
    game = active_games.get(game_code) or await get_game_from_db(game_code)
    
    if not game:
        raise HTTPException(status_code=404, detail="Game not found")
    
    if game.status != GameStatus.WAITING:
        raise HTTPException(status_code=400, detail="Game already started")
    
    players_count = await db.user_submissions.count_documents({"quiz_code": game_code})
    if players_count == 0:
        raise HTTPException(status_code=400, detail="No players in game")
    
    game.status = GameStatus.STARTED
    game.current_question = 0
    active_games[game_code] = game
    await save_game_to_db(game)
    
    await broadcast_to_game(game_code, {
        "type": "game_started",
        "message": "Game is starting!"
    })
    
    asyncio.create_task(start_question(game_code, 0))
    
    return {"message": "Game started successfully"}

async def start_question(game_code: str, question_index: int):
    print(f"Starting question {question_index + 1} for game {game_code}")
    await asyncio.sleep(3)
    
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
    
    active_games[game_code] = game
    await save_game_to_db(game)
    
    print(f"Broadcasting question {question_index + 1} for game {game_code}")
    await broadcast_to_game(game_code, {
        "type": "question",
        "question_number": question_index + 1,
        "total_questions": len(game.quiz.questions),
        "question_id": question.question_id,
        "question": question.question,
        "options": question.options,
        "time_limit": question.time_limit
    })
    
    # Start the timer AFTER broadcasting the question
    asyncio.create_task(show_question_results(game_code, question_index))

async def show_question_results(game_code: str, question_index: int):
    game = active_games.get(game_code)
    if not game:
        return
    
    question_obj = game.quiz.questions[question_index]
    
    print(f"Waiting {question_obj.time_limit} seconds for question {question_index + 1} in game {game_code}")
    # Wait for the full question time limit
    await asyncio.sleep(question_obj.time_limit)
    
    game = active_games.get(game_code)
    if not game or game.current_question != question_index:
        return
    
    game.status = GameStatus.RESULTS
    active_games[game_code] = game
    await save_game_to_db(game)
    
    all_submissions_cursor = db.user_submissions.find({"quiz_code": game_code})
    all_submissions = await all_submissions_cursor.to_list(length=None)
    
    results = []
    answer_counts = [0] * len(question_obj.options)
    
    for submission_data in all_submissions:
        user_submission = UserSubmission(**submission_data)
        
        player_answer_data = user_submission.answers.get(question_obj.question_id)
        
        if player_answer_data and player_answer_data.answer is not None:
            answer_counts[player_answer_data.answer] += 1
            
            player_user_info = await get_user_info_from_db(user_submission.player_id)
            if player_user_info:
                results.append({
                    "player_id": user_submission.player_id,
                    "email_id": player_user_info.email_id,
                    "name": player_user_info.name,
                    "answer": player_answer_data.answer,
                    "is_correct": player_answer_data.is_correct,
                    "score_added_this_question": player_answer_data.score,
                    "total_score": user_submission.total_score
                })
    
    leaderboard = []
    for s in all_submissions:
        player_user_info = await get_user_info_from_db(s["player_id"])
        if player_user_info:
            leaderboard.append({
                "email_id": player_user_info.email_id,
                "name": player_user_info.name,
                "score": s["total_score"]
            })
    leaderboard = sorted(leaderboard, key=lambda x: x["score"], reverse=True)[:10]

    print(f"Broadcasting results for question {question_index + 1} in game {game_code}")
    await broadcast_to_game(game_code, {
        "type": "question_results",
        "question_id": question_obj.question_id,
        "correct_answer": question_obj.correct_answer,
        "answer_counts": answer_counts,
        "results": results,
        "leaderboard": leaderboard
    })
    
    # Wait a bit before next question so users can see results
    print(f"Waiting 3 seconds before next question for game {game_code}")
    await asyncio.sleep(3)
    
    if question_index + 1 < len(game.quiz.questions):
        print(f"Starting next question {question_index + 2} for game {game_code}")
        asyncio.create_task(start_question(game_code, question_index + 1))
    else:
        print(f"Game {game_code} finished, ending game")
        asyncio.create_task(end_game(game_code))

async def end_game(game_code: str):
    await asyncio.sleep(5)
    
    game = active_games.get(game_code)
    if not game:
        return
    
    game.status = GameStatus.FINISHED
    active_games[game_code] = game
    await save_game_to_db(game)
    
    final_leaderboard_cursor = db.user_submissions.find({"quiz_code": game_code})
    final_submissions = await final_leaderboard_cursor.to_list(length=None)

    final_leaderboard = []
    for s in final_submissions:
        player_user_info = await get_user_info_from_db(s["player_id"])
        if player_user_info:
            final_leaderboard.append({
                "email_id": player_user_info.email_id,
                "name": player_user_info.name,
                "score": s["total_score"]
            })
    final_leaderboard = sorted(final_leaderboard, key=lambda x: x["score"], reverse=True)
    
    await broadcast_to_game(game_code, {
        "type": "game_finished",
        "final_leaderboard": final_leaderboard
    })

@app.post("/submit-answer/{game_code}/{player_id}")
async def submit_answer(game_code: str, player_id: str, request: SubmitAnswerRequest):
    game = active_games.get(game_code)
    if not game:
        raise HTTPException(status_code=404, detail="Game not found")
    
    if game.status != GameStatus.QUESTION:
        raise HTTPException(status_code=400, detail="Not accepting answers right now")
    
    question_index = game.current_question
    question_obj = game.quiz.questions[question_index]
    
    user_submission = await get_user_submission_from_db(game_code, player_id)
    if not user_submission:
        raise HTTPException(status_code=404, detail="Player submission not found. Please join the game first.")
    
    if question_obj.question_id in user_submission.answers:
        raise HTTPException(status_code=400, detail="Answer already submitted for this question")
    
    answer_time = request.time_taken
    
    is_correct = (request.submitter_answer == question_obj.correct_answer)
    score_to_add = 0
    max_score = 1000

    if is_correct:
        time_bonus = max(0, (question_obj.time_limit - answer_time) / question_obj.time_limit)
        score_to_add = int(max_score * (0.5 + 0.5 * time_bonus))
    
    new_user_answer = UserAnswer(
        question_id=question_obj.question_id,
        answer=request.submitter_answer,
        is_correct=is_correct,
        time_taken=answer_time,
        score=score_to_add
    )
    
    user_submission.answers[question_obj.question_id] = new_user_answer
    user_submission.total_score += score_to_add
    
    await db.user_submissions.replace_one(
        {"quiz_code": game_code, "player_id": player_id},
        user_submission.dict(),
        upsert=True
    )
    
    return {"message": "Answer submitted successfully", "score_added": score_to_add}


@app.get("/game-status/{game_code}")
async def get_game_status(game_code: str):
    game = active_games.get(game_code) or await get_game_from_db(game_code)
    if not game:
        raise HTTPException(status_code=404, detail="Game not found")
    
    players_count = await db.user_submissions.count_documents({"quiz_code": game_code})

    return {
        "status": game.status,
        "current_question": game.current_question,
        "total_questions": len(game.quiz.questions),
        "players_count": players_count
    }

@app.websocket("/ws/{game_code}")
async def websocket_endpoint(websocket: WebSocket, game_code: str):
    await websocket.accept()
    print(f"WebSocket connection accepted for game {game_code}")
    
    if game_code not in connections:
        connections[game_code] = []
    connections[game_code].append(websocket)
    
    try:
        # Keep connection alive without blocking
        while True:
            try:
                # Use wait_for with timeout to avoid blocking indefinitely
                data = await asyncio.wait_for(websocket.receive_text(), timeout=1.0)
                message = json.loads(data)
                
                if message.get("type") == "ping":
                    await websocket.send_text(json.dumps({"type": "pong"}))
                elif message.get("type") == "host_connected":
                    game = active_games.get(game_code)
                    if game:
                        game.host_connected = True
                        active_games[game_code] = game
                elif message.get("type") == "player_connected":
                    print(f"Player connected to game {game_code}: {message.get('player_id')}")
                    
            except asyncio.TimeoutError:
                # No message received, continue the loop to keep connection alive
                continue
                
    except WebSocketDisconnect:
        print(f"WebSocket disconnected for game {game_code}")
    except Exception as e:
        print(f"WebSocket error for game {game_code}: {e}")
    finally:
        # Cleanup connection
        if game_code in connections and websocket in connections[game_code]:
            connections[game_code].remove(websocket)
            if len(connections[game_code]) == 0:
                del connections[game_code]