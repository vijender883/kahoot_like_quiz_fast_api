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
import logging
import weakref

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

# Enhanced connection tracking
class ConnectionManager:
    def __init__(self):
        self.active_connections: Dict[str, List[WebSocket]] = {}
        self.connection_metadata: Dict[WebSocket, Dict] = {}
        self.heartbeat_tasks: Dict[WebSocket, asyncio.Task] = {}
    
    async def connect(self, websocket: WebSocket, game_code: str, player_id: str = None):
        await websocket.accept()
        
        if game_code not in self.active_connections:
            self.active_connections[game_code] = []
        
        self.active_connections[game_code].append(websocket)
        self.connection_metadata[websocket] = {
            "game_code": game_code,
            "player_id": player_id,
            "connected_at": datetime.now(),
            "last_ping": datetime.now()
        }
        
        # Start heartbeat for this connection
        self.heartbeat_tasks[websocket] = asyncio.create_task(
            self._heartbeat_monitor(websocket)
        )
        
        print(f"✅ WebSocket connected to game {game_code}. Total connections: {len(self.active_connections[game_code])}")
    
    async def disconnect(self, websocket: WebSocket):
        # Cancel heartbeat task
        if websocket in self.heartbeat_tasks:
            self.heartbeat_tasks[websocket].cancel()
            del self.heartbeat_tasks[websocket]
        
        # Remove from metadata
        metadata = self.connection_metadata.pop(websocket, {})
        game_code = metadata.get("game_code")
        
        # Remove from active connections
        if game_code and game_code in self.active_connections:
            if websocket in self.active_connections[game_code]:
                self.active_connections[game_code].remove(websocket)
                
            if len(self.active_connections[game_code]) == 0:
                del self.active_connections[game_code]
        
        print(f"❌ WebSocket disconnected from game {game_code}")
    
    async def _heartbeat_monitor(self, websocket: WebSocket):
        """Monitor connection health"""
        try:
            while True:
                await asyncio.sleep(30)  # Check every 30 seconds
                try:
                    await websocket.send_text(json.dumps({"type": "heartbeat"}))
                    self.connection_metadata[websocket]["last_ping"] = datetime.now()
                except:
                    # Connection is dead, will be cleaned up in disconnect
                    break
        except asyncio.CancelledError:
            pass
    
    async def broadcast_to_game(self, game_code: str, message: dict):
        """Ultra-robust broadcast with multiple retry attempts"""
        if game_code not in self.active_connections:
            print(f"⚠️ No connections found for game {game_code}")
            return
        
        message_json = json.dumps(message)
        connections = self.active_connections[game_code].copy()  # Work with a copy
        successful_sends = 0
        failed_connections = []
        
        print(f"📡 Broadcasting to {len(connections)} connections for game {game_code}")
        
        # First attempt - send to all connections
        for websocket in connections:
            success = await self._send_with_retries(websocket, message_json, max_retries=3)
            if success:
                successful_sends += 1
            else:
                failed_connections.append(websocket)
        
        # Clean up failed connections
        for failed_ws in failed_connections:
            await self.disconnect(failed_ws)
        
        print(f"✅ Broadcast complete: {successful_sends} successful, {len(failed_connections)} failed")
    
    async def _send_with_retries(self, websocket: WebSocket, message: str, max_retries: int = 3):
        """Send message with exponential backoff retries"""
        for attempt in range(max_retries):
            try:
                await websocket.send_text(message)
                return True
            except Exception as e:
                if attempt < max_retries - 1:
                    await asyncio.sleep(0.1 * (2 ** attempt))  # Exponential backoff
                    continue
                else:
                    print(f"❌ Failed to send after {max_retries} attempts: {e}")
                    return False
        return False
    
    def get_connection_count(self, game_code: str) -> int:
        return len(self.active_connections.get(game_code, []))

# Global connection manager
connection_manager = ConnectionManager()

# In-memory storage for active games
active_games: Dict[str, Game] = {}

# Helper functions
def generate_game_code() -> str:
    return str(uuid.uuid4())[:6].upper()

async def broadcast_to_game(game_code: str, message: dict):
    await connection_manager.broadcast_to_game(game_code, message)

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
    # Initialize connections for this game (handled by ConnectionManager now)
    
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
    
    # Verify we have active connections
    connection_count = connection_manager.get_connection_count(game_code)
    print(f"Starting game {game_code} with {connection_count} active connections")
    
    game.status = GameStatus.STARTED
    game.current_question = 0
    active_games[game_code] = game
    await save_game_to_db(game)
    
    # Robust broadcast with verification
    await broadcast_to_game(game_code, {
        "type": "game_started",
        "message": "Game is starting!",
        "timestamp": datetime.now().isoformat()
    })
    
    # Wait a bit to ensure message is delivered
    await asyncio.sleep(1)
    
    asyncio.create_task(start_question(game_code, 0))
    
    return {"message": "Game started successfully", "connections": connection_count}

async def start_question(game_code: str, question_index: int):
    print(f"🎯 Starting question {question_index + 1} for game {game_code}")
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
    
    question_message = {
        "type": "question",
        "question_number": question_index + 1,
        "total_questions": len(game.quiz.questions),
        "question_id": question.question_id,
        "question": question.question,
        "options": question.options,
        "time_limit": question.time_limit,
        "timestamp": datetime.now().isoformat()
    }
    
    print(f"📡 Broadcasting question {question_index + 1} for game {game_code}")
    await broadcast_to_game(game_code, question_message)
    
    # Wait to ensure message delivery before starting timer
    await asyncio.sleep(2)
    
    # Start the results timer
    asyncio.create_task(show_question_results(game_code, question_index))

async def show_question_results(game_code: str, question_index: int):
    game = active_games.get(game_code)
    if not game:
        return
    
    question_obj = game.quiz.questions[question_index]
    
    print(f"⏳ Waiting {question_obj.time_limit} seconds for question {question_index + 1} in game {game_code}")
    await asyncio.sleep(question_obj.time_limit)
    
    game = active_games.get(game_code)
    if not game or game.current_question != question_index:
        return
    
    game.status = GameStatus.RESULTS
    active_games[game_code] = game
    await save_game_to_db(game)
    
    # Get results (your existing logic)
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

    results_message = {
        "type": "question_results",
        "question_id": question_obj.question_id,
        "correct_answer": question_obj.correct_answer,
        "answer_counts": answer_counts,
        "results": results,
        "leaderboard": leaderboard,
        "timestamp": datetime.now().isoformat()
    }
    
    print(f"📊 Broadcasting results for question {question_index + 1} in game {game_code}")
    await broadcast_to_game(game_code, results_message)
    
    # Wait before next question
    print(f"⏸️ Waiting 5 seconds before next question for game {game_code}")
    await asyncio.sleep(5)
    
    if question_index + 1 < len(game.quiz.questions):
        print(f"➡️ Starting next question {question_index + 2} for game {game_code}")
        asyncio.create_task(start_question(game_code, question_index + 1))
    else:
        print(f"🏁 Game {game_code} finished, ending game")
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
        "final_leaderboard": final_leaderboard,
        "timestamp": datetime.now().isoformat()
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

# Bulletproof WebSocket endpoint
@app.websocket("/ws/{game_code}")
async def websocket_endpoint(websocket: WebSocket, game_code: str):
    player_id = None
    
    try:
        # Connect with full error handling
        await connection_manager.connect(websocket, game_code)
        
        # Message handling loop with robust error recovery
        while True:
            try:
                # Wait for message with timeout
                data = await asyncio.wait_for(websocket.receive_text(), timeout=60.0)
                message = json.loads(data)
                
                # Handle different message types
                if message.get("type") == "ping":
                    try:
                        await websocket.send_text(json.dumps({"type": "pong"}))
                    except Exception as e:
                        print(f"Failed to send pong: {e}")
                        break
                        
                elif message.get("type") == "heartbeat":
                    # Update last ping time
                    if websocket in connection_manager.connection_metadata:
                        connection_manager.connection_metadata[websocket]["last_ping"] = datetime.now()
                        
                elif message.get("type") == "host_connected":
                    game = active_games.get(game_code)
                    if game:
                        game.host_connected = True
                        active_games[game_code] = game
                        
                elif message.get("type") == "player_connected":
                    player_id = message.get('player_id')
                    if websocket in connection_manager.connection_metadata:
                        connection_manager.connection_metadata[websocket]["player_id"] = player_id
                    print(f"Player {player_id} connected to game {game_code}")
                    
                    # Send immediate confirmation
                    try:
                        await websocket.send_text(json.dumps({
                            "type": "connection_confirmed",
                            "message": "Successfully connected to game",
                            "game_code": game_code
                        }))
                    except Exception as e:
                        print(f"Failed to send confirmation: {e}")
                
            except asyncio.TimeoutError:
                # Timeout is normal - continue loop
                continue
                
            except json.JSONDecodeError as e:
                print(f"Invalid JSON received: {e}")
                continue
                
            except Exception as e:
                print(f"Error processing message: {e}")
                continue
                
    except WebSocketDisconnect:
        print(f"WebSocket disconnected normally for game {game_code}")
        
    except Exception as e:
        print(f"Unexpected WebSocket error for game {game_code}: {e}")
        
    finally:
        # Always clean up the connection
        await connection_manager.disconnect(websocket)