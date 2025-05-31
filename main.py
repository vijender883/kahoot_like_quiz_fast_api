from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
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
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

app = FastAPI(title="Quiz Application API")

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# MongoDB connection
MONGODB_URL = os.getenv("MONGODB_URL")
if not MONGODB_URL:
    raise ValueError("MONGODB_URL environment variable is not set")

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
    finished_at: Optional[datetime] = None

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

class UserRegistrationRequest(BaseModel):
    name: str
    email_id: str
    phone_no: str

class UserRegistrationResponse(BaseModel):
    message: str
    name: str
    player_id: str

# Simple connection manager
class ConnectionManager:
    def __init__(self):
        self.active_connections: Dict[str, List[WebSocket]] = {}
    
    async def connect(self, websocket: WebSocket, game_code: str):
        await websocket.accept()
        if game_code not in self.active_connections:
            self.active_connections[game_code] = []
        self.active_connections[game_code].append(websocket)
        print(f"✅ WebSocket connected to game {game_code}")
    
    async def disconnect(self, websocket: WebSocket, game_code: str):
        if game_code in self.active_connections:
            if websocket in self.active_connections[game_code]:
                self.active_connections[game_code].remove(websocket)
            if len(self.active_connections[game_code]) == 0:
                del self.active_connections[game_code]
        print(f"❌ WebSocket disconnected from game {game_code}")
    
    async def broadcast_to_game(self, game_code: str, message: dict):
        if game_code not in self.active_connections:
            return
        
        message_json = json.dumps(message)
        connections = self.active_connections[game_code].copy()
        failed_connections = []
        
        for websocket in connections:
            try:
                await websocket.send_text(message_json)
            except Exception:
                failed_connections.append(websocket)
        
        # Clean up failed connections
        for failed_ws in failed_connections:
            await self.disconnect(failed_ws, game_code)
    
    def get_connection_count(self, game_code: str) -> int:
        return len(self.active_connections.get(game_code, []))

# Global connection manager
connection_manager = ConnectionManager()

# In-memory storage for active games
active_games: Dict[str, Game] = {}

# Helper functions
def generate_game_code() -> str:
    return str(uuid.uuid4())[:6].upper()

async def save_game_to_db(game: Game):
    try:
        await db.games.replace_one(
            {"code": game.code}, 
            game.dict(), 
            upsert=True
        )
    except Exception as e:
        print(f"❌ Failed to save game: {e}")
        raise HTTPException(status_code=500, detail="Database operation failed")

async def get_game_from_db(game_code: str) -> Optional[Game]:
    try:
        game_data = await db.games.find_one({"code": game_code})
        if game_data:
            return Game(**game_data)
        return None
    except Exception as e:
        print(f"❌ Failed to get game: {e}")
        return None

async def ensure_game_in_memory(game_code: str) -> Optional[Game]:
    if game_code in active_games:
        return active_games[game_code]
    
    game = await get_game_from_db(game_code)
    if game:
        active_games[game_code] = game
        return game
    
    return None

async def get_user_submission_from_db(quiz_code: str, player_id: str) -> Optional[UserSubmission]:
    try:
        submission_data = await db.user_submissions.find_one({"quiz_code": quiz_code, "player_id": player_id})
        if submission_data:
            return UserSubmission(**submission_data)
        return None
    except Exception as e:
        print(f"❌ Failed to get user submission: {e}")
        return None

async def get_user_info_from_db(player_id: str) -> Optional[UserInfo]:
    try:
        user_data = await db.user_info.find_one({"player_id": player_id})
        if user_data:
            return UserInfo(**user_data)
        return None
    except Exception as e:
        print(f"❌ Failed to get user info: {e}")
        return None

# Startup event
@app.on_event("startup")
async def startup_event():
    try:
        await client.admin.command('ping')
        print("✅ MongoDB connected successfully")
        
        # Load active games into memory
        active_games_cursor = db.games.find({
            "status": {"$in": ["waiting", "started", "question", "results"]}
        })
        loaded_games = await active_games_cursor.to_list(length=None)
        
        for game_data in loaded_games:
            game = Game(**game_data)
            active_games[game.code] = game
        
        print(f"✅ Loaded {len(loaded_games)} active games into memory")
        
    except Exception as e:
        print(f"❌ Startup error: {e}")

# Health check
@app.get("/health")
async def health_check():
    try:
        await client.admin.command('ping')
        return {
            "status": "healthy",
            "database": "connected",
            "active_games": len(active_games),
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        return {
            "status": "unhealthy",
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }

# API Routes
@app.post("/validate-user", response_model=UserValidationResponse)
async def validate_user(request: UserValidationRequest):
    try:
        user = await db.user_info.find_one({"email_id": request.email_id})
        
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
        
        player_id = user.get("player_id")
        
        if not player_id:
            player_id = str(uuid.uuid4())
            await db.user_info.update_one(
                {"_id": user["_id"]},
                {"$set": {"player_id": player_id}}
            )
        
        return UserValidationResponse(name=user["name"], player_id=player_id)
    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ Error validating user: {e}")
        raise HTTPException(status_code=500, detail="Database operation failed")

@app.post("/register-user", response_model=UserRegistrationResponse)
async def register_user(request: UserRegistrationRequest):
    try:
        existing_user = await db.user_info.find_one({"email_id": request.email_id})
        
        if existing_user:
            raise HTTPException(status_code=400, detail="User with this email already exists")
        
        if not request.name.strip():
            raise HTTPException(status_code=400, detail="Name is required")
        
        if not request.email_id.strip() or "@" not in request.email_id:
            raise HTTPException(status_code=400, detail="Valid email is required")
        
        if not request.phone_no.strip():
            raise HTTPException(status_code=400, detail="Phone number is required")
        
        player_id = str(uuid.uuid4())
        
        new_user = {
            "name": request.name.strip(),
            "email_id": request.email_id.strip().lower(),
            "phone_no": request.phone_no.strip(),
            "player_id": player_id,
            "created_at": datetime.now()
        }
        
        result = await db.user_info.insert_one(new_user)
        
        if not result.inserted_id:
            raise HTTPException(status_code=500, detail="Failed to create user")
        
        return UserRegistrationResponse(
            message="User registered successfully",
            name=request.name.strip(),
            player_id=player_id
        )
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ Error registering user: {e}")
        raise HTTPException(status_code=500, detail="Database operation failed")

@app.post("/create-quiz")
async def create_quiz(quiz: Quiz):
    try:
        game_code = generate_game_code()
        
        # Check for duplicate codes
        attempts = 0
        while attempts < 10:
            if game_code not in active_games:
                existing_game = await get_game_from_db(game_code)
                if not existing_game:
                    break
            game_code = generate_game_code()
            attempts += 1
        
        if attempts >= 10:
            raise HTTPException(status_code=500, detail="Failed to generate unique game code")
        
        for q in quiz.questions:
            q.question_id = str(uuid.uuid4())

        game = Game(code=game_code, quiz=quiz)
        active_games[game_code] = game
        
        await save_game_to_db(game)
        
        return {"game_code": game_code, "message": "Quiz created successfully"}
    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ Error creating quiz: {e}")
        raise HTTPException(status_code=500, detail="Failed to create quiz")

@app.post("/join-game/{game_code}")
async def join_game(game_code: str, request: JoinGameRequest):
    try:
        game = await ensure_game_in_memory(game_code)
        
        if not game:
            raise HTTPException(status_code=404, detail="Game not found")
        
        user_info = await get_user_info_from_db(request.player_id)
        if not user_info:
            raise HTTPException(status_code=404, detail="Player not found")

        existing_submission = await db.user_submissions.find_one({"quiz_code": game_code, "player_id": request.player_id})
        if existing_submission:
            return {
                "player_id": existing_submission["player_id"], 
                "message": f"Welcome back {user_info.name}!"
            }
        
        new_submission = UserSubmission(
            player_id=request.player_id,
            quiz_code=game_code,
            answers={},
            total_score=0
        )
        await db.user_submissions.insert_one(new_submission.dict())
        
        # Get all players for broadcast
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

        await connection_manager.broadcast_to_game(game_code, {
            "type": "player_joined",
            "players": players_in_game,
            "total_players": len(players_in_game)
        })
        
        return {
            "player_id": request.player_id, 
            "message": f"Welcome {user_info.name}!"
        }
    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ Error joining game: {e}")
        raise HTTPException(status_code=500, detail="Failed to join game")

@app.post("/start-game/{game_code}")
async def start_game(game_code: str):
    try:
        game = await ensure_game_in_memory(game_code)
        
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
        
        await connection_manager.broadcast_to_game(game_code, {
            "type": "game_started",
            "message": "Game is starting!",
            "timestamp": datetime.now().isoformat()
        })
        
        # Start first question after a delay
        asyncio.create_task(start_question(game_code, 0))
        
        return {"message": "Game started successfully"}
    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ Error starting game: {e}")
        raise HTTPException(status_code=500, detail="Failed to start game")

async def start_question(game_code: str, question_index: int):
    print(f"🎯 Starting question {question_index + 1} for game {game_code}")
    await asyncio.sleep(3)  # Wait 3 seconds before showing question
    
    game = await ensure_game_in_memory(game_code)
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
        "correct_answer": question.correct_answer,
        "timestamp": datetime.now().isoformat()
    }
    
    await connection_manager.broadcast_to_game(game_code, question_message)
    
    # Start timer for next question
    asyncio.create_task(proceed_to_next_question(game_code, question_index))

async def proceed_to_next_question(game_code: str, question_index: int):
    game = await ensure_game_in_memory(game_code)
    if not game:
        return
    
    question_obj = game.quiz.questions[question_index]
    
    # Wait for the question time limit
    await asyncio.sleep(question_obj.time_limit)
    
    # Show results briefly
    game.status = GameStatus.RESULTS
    active_games[game_code] = game
    await save_game_to_db(game)
    
    await connection_manager.broadcast_to_game(game_code, {
        "type": "question_results",
        "question_id": question_obj.question_id,
        "correct_answer": question_obj.correct_answer,
        "timestamp": datetime.now().isoformat()
    })
    
    # Wait briefly to show results
    await asyncio.sleep(5)
    
    # Check if there are more questions
    if question_index + 1 < len(game.quiz.questions):
        asyncio.create_task(start_question(game_code, question_index + 1))
    else:
        asyncio.create_task(end_game(game_code))

async def end_game(game_code: str):
    await asyncio.sleep(2)
    
    game = await ensure_game_in_memory(game_code)
    if not game:
        return
    
    game.status = GameStatus.FINISHED
    game.finished_at = datetime.now()
    active_games[game_code] = game
    await save_game_to_db(game)
    
    try:
        # Get final leaderboard
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
        
        await connection_manager.broadcast_to_game(game_code, {
            "type": "game_finished",
            "final_leaderboard": final_leaderboard,
            "timestamp": datetime.now().isoformat()
        })
    except Exception as e:
        print(f"❌ Error ending game: {e}")

@app.post("/submit-answer/{game_code}/{player_id}")
async def submit_answer(game_code: str, player_id: str, request: SubmitAnswerRequest):
    try:
        game = await ensure_game_in_memory(game_code)
        if not game:
            raise HTTPException(status_code=404, detail="Game not found")
        
        # Allow submissions in both QUESTION and RESULTS states
        if game.status not in [GameStatus.QUESTION, GameStatus.RESULTS]:
            raise HTTPException(status_code=400, detail=f"Not accepting answers. Game status: {game.status}")
        
        # Check if question time has expired
        if game.question_start_time:
            question_obj = game.quiz.questions[game.current_question]
            time_elapsed = (datetime.now() - game.question_start_time).total_seconds()
            if time_elapsed > question_obj.time_limit:
                raise HTTPException(status_code=400, detail="Time limit exceeded")
        
        question_index = game.current_question
        if question_index >= len(game.quiz.questions):
            raise HTTPException(status_code=400, detail="Invalid question index")
            
        question_obj = game.quiz.questions[question_index]
        
        user_submission = await get_user_submission_from_db(game_code, player_id)
        if not user_submission:
            raise HTTPException(status_code=404, detail="Player not found")
        
        if question_obj.question_id in user_submission.answers:
            raise HTTPException(status_code=400, detail="Answer already submitted")
        
        answer_time = request.time_taken
        is_correct = (request.submitter_answer == question_obj.correct_answer)
        score_to_add = 0
        max_score = 1000

        if is_correct:
            time_bonus = max(0, (question_obj.time_limit - answer_time) / question_obj.time_limit)
            score_to_add = int(max_score * time_bonus)
        else:
            score_to_add = 0  # No negative scoring
        
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
        
        return {
            "message": "Answer submitted successfully", 
            "score_added": score_to_add,
            "is_correct": is_correct
        }
    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ Error submitting answer: {e}")
        raise HTTPException(status_code=500, detail="Failed to submit answer")

@app.get("/game-status/{game_code}")
async def get_game_status(game_code: str):
    try:
        game = await ensure_game_in_memory(game_code)
        if not game:
            raise HTTPException(status_code=404, detail="Game not found")
        
        players_count = await db.user_submissions.count_documents({"quiz_code": game_code})

        return {
            "status": game.status,
            "current_question": game.current_question,
            "total_questions": len(game.quiz.questions),
            "players_count": players_count
        }
    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ Error getting game status: {e}")
        raise HTTPException(status_code=500, detail="Failed to get game status")

# WebSocket endpoint
@app.websocket("/ws/{game_code}")
async def websocket_endpoint(websocket: WebSocket, game_code: str):
    try:
        # Ensure game exists
        game = await ensure_game_in_memory(game_code)
        if not game:
            await websocket.close(code=1008, reason="Game not found")
            return
            
        await connection_manager.connect(websocket, game_code)
        
        # Send welcome message
        await websocket.send_text(json.dumps({
            "type": "welcome",
            "message": "Connected to game",
            "game_code": game_code,
            "game_status": game.status,
            "timestamp": datetime.now().isoformat()
        }))
        
        # Message handling loop
        while True:
            try:
                data = await asyncio.wait_for(websocket.receive_text(), timeout=60.0)
                message = json.loads(data)
                
                if message.get("type") == "ping":
                    await websocket.send_text(json.dumps({"type": "pong"}))
                    
                elif message.get("type") == "player_connected":
                    player_id = message.get('player_id')
                    print(f"👤 Player {player_id} connected to game {game_code}")
                    
                    # Send confirmation with current game state
                    confirmation = {
                        "type": "connection_confirmed",
                        "message": "Successfully connected",
                        "game_code": game_code,
                        "player_id": player_id
                    }
                    
                    # Include current question if game is in progress
                    current_game = await ensure_game_in_memory(game_code)
                    if current_game and current_game.status == GameStatus.QUESTION:
                        current_question = current_game.quiz.questions[current_game.current_question]
                        time_elapsed = 0
                        if current_game.question_start_time:
                            time_elapsed = (datetime.now() - current_game.question_start_time).total_seconds()
                        
                        confirmation["current_question"] = {
                            "type": "question",
                            "question_number": current_game.current_question + 1,
                            "total_questions": len(current_game.quiz.questions),
                            "question_id": current_question.question_id,
                            "question": current_question.question,
                            "options": current_question.options,
                            "time_limit": current_question.time_limit,
                            "correct_answer": current_question.correct_answer,
                            "time_elapsed": int(time_elapsed)
                        }
                    
                    await websocket.send_text(json.dumps(confirmation))
                    
            except asyncio.TimeoutError:
                # Send ping to keep connection alive
                await websocket.send_text(json.dumps({"type": "ping"}))
                continue
                
            except json.JSONDecodeError:
                continue
                
            except Exception as e:
                print(f"Error processing message: {e}")
                continue
                
    except WebSocketDisconnect:
        print(f"WebSocket disconnected for game {game_code}")
        
    except Exception as e:
        print(f"WebSocket error for game {game_code}: {e}")
        
    finally:
        await connection_manager.disconnect(websocket, game_code)