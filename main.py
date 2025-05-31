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
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

app = FastAPI(title="Kahoot Clone API")

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# MongoDB connection - Load from .env file
MONGODB_URL = os.getenv("MONGODB_URL")

if not MONGODB_URL:
    raise ValueError("MONGODB_URL environment variable is not set. Please check your .env file.")

print(f"📡 Connecting to MongoDB...")
client = AsyncIOMotorClient(MONGODB_URL)
db = client.test

# Test MongoDB connection function
async def test_mongodb_connection():
    """Test MongoDB connection before starting the server"""
    try:
        # Test connection with a timeout
        await client.admin.command('ping')
        print("✅ MongoDB connected successfully")
        return True
    except Exception as e:
        print(f"❌ MongoDB connection failed: {e}")
        print("Please check your MONGODB_URL in the .env file and cluster status")
        return False

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

class UserRegistrationRequest(BaseModel):
    name: str
    email_id: str
    phone_no: str

class UserRegistrationResponse(BaseModel):
    message: str
    name: str
    player_id: str

# Enhanced connection tracking with better monitoring
class ConnectionManager:
    def __init__(self):
        self.active_connections: Dict[str, List[WebSocket]] = {}
        self.connection_metadata: Dict[WebSocket, Dict] = {}
        self.heartbeat_tasks: Dict[WebSocket, asyncio.Task] = {}
        self.stats = {
            "total_connections": 0,
            "failed_connections": 0,
            "reconnections": 0
        }
    
    async def connect(self, websocket: WebSocket, game_code: str, player_id: str = None):
        try:
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
            
            self.stats["total_connections"] += 1
            
            print(f"✅ WebSocket connected to game {game_code}. Total connections: {len(self.active_connections[game_code])}")
            
        except Exception as e:
            self.stats["failed_connections"] += 1
            print(f"❌ Failed to connect WebSocket: {e}")
            raise
    
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
        """Monitor connection health with more frequent pings"""
        try:
            while True:
                await asyncio.sleep(15)  # Check every 15 seconds for better reliability
                try:
                    await websocket.send_text(json.dumps({"type": "heartbeat"}))
                    self.connection_metadata[websocket]["last_ping"] = datetime.now()
                except:
                    # Connection is dead, will be cleaned up in disconnect
                    break
        except asyncio.CancelledError:
            pass
    
    async def broadcast_to_game(self, game_code: str, message: dict):
        """Ultra-robust broadcast with multiple retry attempts and state recovery"""
        if game_code not in self.active_connections:
            print(f"⚠️ No connections found for game {game_code}")
            return
        
        message_json = json.dumps(message)
        connections = self.active_connections[game_code].copy()  # Work with a copy
        successful_sends = 0
        failed_connections = []
        
        print(f"📡 Broadcasting to {len(connections)} connections for game {game_code}")
        
        # First attempt - send to all connections with aggressive retry
        for websocket in connections:
            success = await self._send_with_retries(websocket, message_json, max_retries=5)
            if success:
                successful_sends += 1
            else:
                failed_connections.append(websocket)
        
        # Clean up failed connections
        for failed_ws in failed_connections:
            await self.disconnect(failed_ws)
        
        print(f"✅ Broadcast complete: {successful_sends} successful, {len(failed_connections)} failed")
        
        # If we have very few successful connections, log a warning
        if successful_sends < len(connections) * 0.5:  # Less than 50% success rate
            print(f"⚠️ Low broadcast success rate for game {game_code}: {successful_sends}/{len(connections)}")
    
    async def _send_with_retries(self, websocket: WebSocket, message: str, max_retries: int = 5):
        """Send message with exponential backoff retries - more aggressive for reliability"""
        for attempt in range(max_retries):
            try:
                await websocket.send_text(message)
                return True
            except Exception as e:
                if attempt < max_retries - 1:
                    await asyncio.sleep(0.05 * (2 ** attempt))  # Faster retries
                    continue
                else:
                    print(f"❌ Failed to send after {max_retries} attempts: {e}")
                    return False
        return False
    
    def get_connection_count(self, game_code: str) -> int:
        return len(self.active_connections.get(game_code, []))
    
    def get_stats(self):
        """Get connection statistics"""
        active_count = sum(len(conns) for conns in self.active_connections.values())
        return {
            **self.stats,
            "active_connections": active_count,
            "active_games_with_connections": len(self.active_connections)
        }

# Global connection manager
connection_manager = ConnectionManager()

# In-memory storage for active games
active_games: Dict[str, Game] = {}

# Helper functions
def generate_game_code() -> str:
    return str(uuid.uuid4())[:6].upper()

async def broadcast_to_game(game_code: str, message: dict):
    await connection_manager.broadcast_to_game(game_code, message)

async def safe_broadcast_to_game(game_code: str, message: dict, max_retries: int = 3):
    """Safe broadcast with retry logic for critical messages"""
    for attempt in range(max_retries):
        try:
            await connection_manager.broadcast_to_game(game_code, message)
            return True
        except Exception as e:
            print(f"❌ Broadcast attempt {attempt + 1} failed: {e}")
            if attempt < max_retries - 1:
                await asyncio.sleep(1)  # Wait before retry
    
    print(f"❌ Failed to broadcast to game {game_code} after {max_retries} attempts")
    return False

async def save_game_to_db(game: Game):
    try:
        await db.games.replace_one(
            {"code": game.code}, 
            game.dict(), 
            upsert=True
        )
    except Exception as e:
        print(f"❌ Failed to save game to database: {e}")
        raise HTTPException(status_code=500, detail="Database operation failed")

async def get_game_from_db(game_code: str) -> Optional[Game]:
    try:
        game_data = await db.games.find_one({"code": game_code})
        if game_data:
            return Game(**game_data)
        return None
    except Exception as e:
        print(f"❌ Failed to get game from database: {e}")
        return None

async def ensure_game_in_memory(game_code: str) -> Optional[Game]:
    """Ensure game is loaded in memory, fetch from DB if needed - CRITICAL FOR RELIABILITY"""
    if game_code in active_games:
        return active_games[game_code]
    
    # Try to load from database
    game = await get_game_from_db(game_code)
    if game:
        active_games[game_code] = game
        print(f"🔄 Loaded game {game_code} from database into memory")
        return game
    
    return None

async def get_user_submission_from_db(quiz_code: str, player_id: str) -> Optional[UserSubmission]:
    try:
        submission_data = await db.user_submissions.find_one({"quiz_code": quiz_code, "player_id": player_id})
        if submission_data:
            return UserSubmission(**submission_data)
        return None
    except Exception as e:
        print(f"❌ Failed to get user submission from database: {e}")
        return None

async def get_user_info_from_db(player_id: str) -> Optional[UserInfo]:
    try:
        user_data = await db.user_info.find_one({"player_id": player_id})
        if user_data:
            return UserInfo(**user_data)
        return None
    except Exception as e:
        print(f"❌ Failed to get user info from database: {e}")
        return None

# Cleanup and monitoring tasks
async def cleanup_finished_games():
    """Remove finished games from memory periodically"""
    while True:
        try:
            await asyncio.sleep(300)  # Run every 5 minutes
            
            finished_games = []
            current_time = datetime.now()
            
            for game_code, game in list(active_games.items()):
                if game.status == GameStatus.FINISHED:
                    # Keep finished games for 2 hours, then remove from memory
                    if game.finished_at:
                        if (current_time - game.finished_at).total_seconds() > 7200:  # 2 hours
                            finished_games.append(game_code)
                    else:
                        # Add finished timestamp if not present
                        game.finished_at = current_time
                        active_games[game_code] = game
                        await save_game_to_db(game)
            
            for game_code in finished_games:
                del active_games[game_code]
                print(f"🧹 Cleaned up finished game {game_code} from memory")
                
        except Exception as e:
            print(f"❌ Error in cleanup task: {e}")

# Startup event to test MongoDB connection and load games
@app.on_event("startup")
async def startup_event():
    """Test MongoDB connection and load active games on startup"""
    mongodb_ok = await test_mongodb_connection()
    if not mongodb_ok:
        print("⚠️ Server starting without MongoDB connection - some features may not work")
        return
    
    # Load all active games from database into memory
    try:
        active_games_cursor = db.games.find({
            "status": {"$in": ["waiting", "started", "question", "results"]}
        })
        loaded_games = await active_games_cursor.to_list(length=None)
        
        for game_data in loaded_games:
            game = Game(**game_data)
            active_games[game.code] = game
            print(f"🔄 Loaded active game {game.code} into memory")
        
        print(f"✅ Loaded {len(loaded_games)} active games into memory")
        
        # Start cleanup task
        asyncio.create_task(cleanup_finished_games())
        
    except Exception as e:
        print(f"❌ Failed to load games from database: {e}")

# Health check and monitoring endpoints
@app.get("/health")
async def health_check():
    """Health check endpoint for monitoring"""
    try:
        # Test database connection
        await client.admin.command('ping')
        
        # Count active games and connections
        active_game_count = len(active_games)
        total_connections = sum(
            len(connections) for connections in connection_manager.active_connections.values()
        )
        
        return {
            "status": "healthy",
            "database": "connected",
            "active_games": active_game_count,
            "total_websocket_connections": total_connections,
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        return {
            "status": "unhealthy",
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }

@app.get("/admin/stats")
async def get_stats():
    """Get system statistics (for admin monitoring)"""
    try:
        # Game statistics
        game_stats = {}
        for status in GameStatus:
            game_stats[status.value] = len([
                g for g in active_games.values() if g.status == status
            ])
        
        # Database statistics
        db_stats = {
            "total_games": await db.games.count_documents({}),
            "total_users": await db.user_info.count_documents({}),
            "total_submissions": await db.user_submissions.count_documents({})
        }
        
        # Connection statistics
        conn_stats = connection_manager.get_stats()
        
        return {
            "games": game_stats,
            "database": db_stats,
            "connections": conn_stats,
            "memory_usage": {
                "active_games_in_memory": len(active_games)
            },
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get stats: {e}")

# API Routes

@app.post("/validate-user", response_model=UserValidationResponse)
async def validate_user(request: UserValidationRequest):
    try:
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
    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ Error validating user: {e}")
        raise HTTPException(status_code=500, detail="Database operation failed")
    
@app.post("/register-user", response_model=UserRegistrationResponse)
async def register_user(request: UserRegistrationRequest):
    try:
        # Check if user already exists with this email
        existing_user = await db.user_info.find_one({"email_id": request.email_id})
        
        if existing_user:
            raise HTTPException(status_code=400, detail="User with this email already exists")
        
        # Validate input data
        if not request.name.strip():
            raise HTTPException(status_code=400, detail="Name is required")
        
        if not request.email_id.strip() or "@" not in request.email_id:
            raise HTTPException(status_code=400, detail="Valid email is required")
        
        if not request.phone_no.strip():
            raise HTTPException(status_code=400, detail="Phone number is required")
        
        # Generate a new player_id
        player_id = str(uuid.uuid4())
        
        # Create new user document
        new_user = {
            "name": request.name.strip(),
            "email_id": request.email_id.strip().lower(),
            "phone_no": request.phone_no.strip(),
            "player_id": player_id,
            "created_at": datetime.now()
        }
        
        # Insert new user into database
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
        
        # Check for duplicate game codes with error handling
        max_attempts = 10
        attempts = 0
        while attempts < max_attempts:
            if game_code not in active_games:
                existing_game = await get_game_from_db(game_code)
                if not existing_game:
                    break
            game_code = generate_game_code()
            attempts += 1
        
        if attempts >= max_attempts:
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

        await safe_broadcast_to_game(game_code, {
            "type": "player_joined",
            "players": players_in_game,
            "total_players": len(players_in_game)
        })
        
        return {
            "player_id": request.player_id, 
            "message": f"Welcome {user_info.name}! You have successfully joined the game."
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
        
        # Verify we have active connections
        connection_count = connection_manager.get_connection_count(game_code)
        print(f"Starting game {game_code} with {connection_count} active connections")
        
        game.status = GameStatus.STARTED
        game.current_question = 0
        active_games[game_code] = game
        await save_game_to_db(game)
        
        # Robust broadcast with verification
        await safe_broadcast_to_game(game_code, {
            "type": "game_started",
            "message": "Game is starting!",
            "timestamp": datetime.now().isoformat()
        })
        
        # Wait a bit to ensure message is delivered
        await asyncio.sleep(2)
        
        asyncio.create_task(start_question(game_code, 0))
        
        return {"message": "Game started successfully", "connections": connection_count}
    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ Error starting game: {e}")
        raise HTTPException(status_code=500, detail="Failed to start game")

async def start_question(game_code: str, question_index: int):
    print(f"🎯 Starting question {question_index + 1} for game {game_code}")
    await asyncio.sleep(3)
    
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
    
    print(f"📡 Broadcasting question {question_index + 1} for game {game_code}")
    success = await safe_broadcast_to_game(game_code, question_message)
    
    if not success:
        print(f"⚠️ Failed to broadcast question, but continuing...")
    
    # Wait to ensure message delivery before starting timer
    await asyncio.sleep(3)
    
    # Start the next question or end game timer
    asyncio.create_task(proceed_to_next_question(game_code, question_index))

async def proceed_to_next_question(game_code: str, question_index: int):
    game = await ensure_game_in_memory(game_code)
    if not game:
        return
    
    question_obj = game.quiz.questions[question_index]
    
    print(f"⏳ Waiting {question_obj.time_limit} seconds for question {question_index + 1} in game {game_code}")
    await asyncio.sleep(question_obj.time_limit)
    
    # Re-check game state after waiting
    game = await ensure_game_in_memory(game_code)
    if not game or game.current_question != question_index:
        return
    
    # Show correct answer briefly
    game.status = GameStatus.RESULTS
    active_games[game_code] = game
    await save_game_to_db(game)
    
    # Broadcast the correct answer
    await safe_broadcast_to_game(game_code, {
        "type": "question_results",
        "question_id": question_obj.question_id,
        "correct_answer": question_obj.correct_answer,
        "timestamp": datetime.now().isoformat()
    })
    
    # Wait briefly to show the correct answer
    print(f"⏸️ Showing correct answer for 5 seconds for game {game_code}")
    await asyncio.sleep(5)
    
    # Check if there are more questions
    if question_index + 1 < len(game.quiz.questions):
        print(f"➡️ Starting next question {question_index + 2} for game {game_code}")
        asyncio.create_task(start_question(game_code, question_index + 1))
    else:
        print(f"🏁 Game {game_code} finished, ending game")
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
        
        # Broadcast final results with leaderboard
        await safe_broadcast_to_game(game_code, {
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
            score_to_add = int(max_score * time_bonus)
        else:
            score_to_add = 0
        
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

@app.post("/admin/recover-game/{game_code}")
async def recover_game(game_code: str):
    """Manually recover a game that might be stuck"""
    try:
        game = await get_game_from_db(game_code)
        if not game:
            raise HTTPException(status_code=404, detail="Game not found in database")
        
        # Force reload into memory
        active_games[game_code] = game
        
        # Get current connections
        connection_count = connection_manager.get_connection_count(game_code)
        
        # Broadcast current state to all connections
        await safe_broadcast_to_game(game_code, {
            "type": "game_recovered",
            "current_status": game.status,
            "current_question": game.current_question,
            "message": "Game state has been recovered",
            "timestamp": datetime.now().isoformat()
        })
        
        return {
            "message": "Game recovered successfully",
            "game_status": game.status,
            "connections": connection_count
        }
    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ Error recovering game: {e}")
        raise HTTPException(status_code=500, detail="Failed to recover game")

# Bulletproof WebSocket endpoint with aggressive reconnection support
@app.websocket("/ws/{game_code}")
async def websocket_endpoint(websocket: WebSocket, game_code: str):
    player_id = None
    
    try:
        # Ensure game is loaded before connecting
        game = await ensure_game_in_memory(game_code)
        if not game:
            await websocket.close(code=1008, reason="Game not found")
            return
            
        # Connect with full error handling
        await connection_manager.connect(websocket, game_code)
        
        # Send immediate welcome message
        try:
            await websocket.send_text(json.dumps({
                "type": "welcome",
                "message": "Connected to game successfully",
                "game_code": game_code,
                "game_status": game.status,
                "timestamp": datetime.now().isoformat()
            }))
        except:
            pass
        
        # Message handling loop with robust error recovery
        while True:
            try:
                # Wait for message with longer timeout for stability
                data = await asyncio.wait_for(websocket.receive_text(), timeout=90.0)
                message = json.loads(data)
                
                # Handle different message types
                if message.get("type") == "ping":
                    try:
                        await websocket.send_text(json.dumps({"type": "pong"}))
                    except Exception as e:
                        print(f"Failed to send pong: {e}")
                        break
                        
                elif message.get("type") == "heartbeat":
                    # Update last ping time and respond
                    if websocket in connection_manager.connection_metadata:
                        connection_manager.connection_metadata[websocket]["last_ping"] = datetime.now()
                    try:
                        await websocket.send_text(json.dumps({"type": "heartbeat_ack"}))
                    except:
                        pass
                        
                elif message.get("type") == "host_connected":
                    game = await ensure_game_in_memory(game_code)
                    if game:
                        game.host_connected = True
                        active_games[game_code] = game
                        print(f"🎮 Host connected to game {game_code}")
                        
                elif message.get("type") == "player_connected":
                    player_id = message.get('player_id')
                    if websocket in connection_manager.connection_metadata:
                        connection_manager.connection_metadata[websocket]["player_id"] = player_id
                    print(f"👤 Player {player_id} connected to game {game_code}")
                    
                    # Send immediate confirmation with current game state
                    try:
                        current_game = await ensure_game_in_memory(game_code)
                        confirmation = {
                            "type": "connection_confirmed",
                            "message": "Successfully connected to game",
                            "game_code": game_code,
                            "player_id": player_id
                        }
                        
                        # Include current question if game is in progress
                        if current_game and current_game.status == GameStatus.QUESTION:
                            current_question = current_game.quiz.questions[current_game.current_question]
                            time_elapsed = 0
                            if current_game.question_start_time:
                                time_elapsed = (datetime.now() - current_game.question_start_time).total_seconds()
                            
                            confirmation.update({
                                "current_question": {
                                    "type": "question",
                                    "question_number": current_game.current_question + 1,
                                    "total_questions": len(current_game.quiz.questions),
                                    "question_id": current_question.question_id,
                                    "question": current_question.question,
                                    "options": current_question.options,
                                    "time_limit": current_question.time_limit,
                                    "correct_answer": current_question.correct_answer,
                                    "time_elapsed": int(time_elapsed),
                                    "timestamp": datetime.now().isoformat()
                                }
                            })
                        
                        await websocket.send_text(json.dumps(confirmation))
                    except Exception as e:
                        print(f"Failed to send confirmation: {e}")
                        
                elif message.get("type") == "request_game_state":
                    # Client requesting current game state (for reconnection)
                    try:
                        current_game = await ensure_game_in_memory(game_code)
                        if current_game:
                            state_message = {
                                "type": "game_state",
                                "status": current_game.status,
                                "current_question": current_game.current_question,
                                "total_questions": len(current_game.quiz.questions)
                            }
                            
                            # If there's an active question, include it
                            if current_game.status == GameStatus.QUESTION:
                                current_question = current_game.quiz.questions[current_game.current_question]
                                time_elapsed = 0
                                if current_game.question_start_time:
                                    time_elapsed = (datetime.now() - current_game.question_start_time).total_seconds()
                                
                                state_message["active_question"] = {
                                    "question_number": current_game.current_question + 1,
                                    "question_id": current_question.question_id,
                                    "question": current_question.question,
                                    "options": current_question.options,
                                    "time_limit": current_question.time_limit,
                                    "correct_answer": current_question.correct_answer,
                                    "time_elapsed": int(time_elapsed)
                                }
                            
                            await websocket.send_text(json.dumps(state_message))
                    except Exception as e:
                        print(f"Failed to send game state: {e}")
                
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