from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from typing import List, Dict, Optional
import json
from datetime import datetime, timedelta
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
    FINISHED = "finished"

class Question(BaseModel):
    question_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    question: str
    options: List[str]
    correct_answer: int
    time_limit: int = 15

class Quiz(BaseModel):
    title: str
    questions: List[Question]

class Game(BaseModel):
    code: str
    quiz: Quiz
    status: GameStatus = GameStatus.WAITING
    scheduled_start_time: Optional[datetime] = None
    started_at: Optional[datetime] = None
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
    question_number: int

class UserRegistrationRequest(BaseModel):
    name: str
    email_id: str
    phone_no: str

class UserRegistrationResponse(BaseModel):
    message: str
    name: str
    player_id: str

class QuestionResponse(BaseModel):
    question_number: int
    total_questions: int
    question_id: str
    question: str
    options: List[str]
    time_remaining: int
    quiz_status: str

class GameStatusResponse(BaseModel):
    status: str
    scheduled_start_time: Optional[datetime]
    current_time: datetime
    total_questions: int
    players_count: int

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

def get_current_question_number(game: Game) -> int:
    """Calculate current question number based on time elapsed since start"""
    if not game.started_at or game.status != GameStatus.STARTED:
        return 0
    
    time_elapsed = (datetime.now() - game.started_at).total_seconds()
    # Each question takes 15 seconds + 3 seconds gap = 18 seconds total
    question_cycle_time = 18
    current_question = int(time_elapsed // question_cycle_time)
    
    return min(current_question, len(game.quiz.questions) - 1)

def get_time_remaining_for_question(game: Game, question_number: int) -> int:
    """Calculate time remaining for current question"""
    if not game.started_at or game.status != GameStatus.STARTED:
        return 0
    
    time_elapsed = (datetime.now() - game.started_at).total_seconds()
    question_cycle_time = 18  # 15 seconds question + 3 seconds gap
    
    # Time elapsed in current question cycle
    time_in_current_cycle = time_elapsed % question_cycle_time
    
    if time_in_current_cycle <= 15:
        # We're in the question phase
        return int(15 - time_in_current_cycle)
    else:
        # We're in the gap phase, no time remaining for answering
        return 0

# Startup event
@app.on_event("startup")
async def startup_event():
    try:
        await client.admin.command('ping')
        print("✅ MongoDB connected successfully")
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
            existing_game = await get_game_from_db(game_code)
            if not existing_game:
                break
            game_code = generate_game_code()
            attempts += 1
        
        if attempts >= 10:
            raise HTTPException(status_code=500, detail="Failed to generate unique game code")
        
        for q in quiz.questions:
            q.question_id = str(uuid.uuid4())
            q.time_limit = 15  # Force 15 seconds for all questions

        # Schedule start time 2 minutes from now
        scheduled_start = datetime.now() + timedelta(minutes=2)
        
        game = Game(
            code=game_code, 
            quiz=quiz,
            scheduled_start_time=scheduled_start
        )
        
        await save_game_to_db(game)
        
        return {
            "game_code": game_code, 
            "message": "Quiz created successfully",
            "scheduled_start_time": scheduled_start.isoformat()
        }
    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ Error creating quiz: {e}")
        raise HTTPException(status_code=500, detail="Failed to create quiz")

@app.post("/join-game/{game_code}")
async def join_game(game_code: str, request: JoinGameRequest):
    try:
        game = await get_game_from_db(game_code)
        
        if not game:
            raise HTTPException(status_code=404, detail="Game not found")
        
        user_info = await get_user_info_from_db(request.player_id)
        if not user_info:
            raise HTTPException(status_code=404, detail="Player not found")

        existing_submission = await db.user_submissions.find_one({"quiz_code": game_code, "player_id": request.player_id})
        if existing_submission:
            return {
                "player_id": existing_submission["player_id"], 
                "message": f"Welcome back {user_info.name}!",
                "scheduled_start_time": game.scheduled_start_time.isoformat() if game.scheduled_start_time else None
            }
        
        new_submission = UserSubmission(
            player_id=request.player_id,
            quiz_code=game_code,
            answers={},
            total_score=0
        )
        await db.user_submissions.insert_one(new_submission.dict())
        
        return {
            "player_id": request.player_id, 
            "message": f"Welcome {user_info.name}!",
            "scheduled_start_time": game.scheduled_start_time.isoformat() if game.scheduled_start_time else None
        }
    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ Error joining game: {e}")
        raise HTTPException(status_code=500, detail="Failed to join game")

@app.get("/game-status/{game_code}", response_model=GameStatusResponse)
async def get_game_status(game_code: str):
    try:
        game = await get_game_from_db(game_code)
        if not game:
            raise HTTPException(status_code=404, detail="Game not found")
        
        # Auto-start game if scheduled time has passed
        current_time = datetime.now()
        if (game.status == GameStatus.WAITING and 
            game.scheduled_start_time and 
            current_time >= game.scheduled_start_time):
            
            game.status = GameStatus.STARTED
            game.started_at = game.scheduled_start_time
            await save_game_to_db(game)
        
        # Auto-finish game if all questions are done
        if game.status == GameStatus.STARTED and game.started_at:
            total_quiz_time = len(game.quiz.questions) * 18  # 18 seconds per question
            if (current_time - game.started_at).total_seconds() >= total_quiz_time:
                game.status = GameStatus.FINISHED
                game.finished_at = current_time
                await save_game_to_db(game)
        
        players_count = await db.user_submissions.count_documents({"quiz_code": game_code})

        return GameStatusResponse(
            status=game.status,
            scheduled_start_time=game.scheduled_start_time,
            current_time=current_time,
            total_questions=len(game.quiz.questions),
            players_count=players_count
        )
    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ Error getting game status: {e}")
        raise HTTPException(status_code=500, detail="Failed to get game status")

@app.get("/current-question/{game_code}/{player_id}")
async def get_current_question(game_code: str, player_id: str):
    try:
        game = await get_game_from_db(game_code)
        if not game:
            raise HTTPException(status_code=404, detail="Game not found")
        
        if game.status != GameStatus.STARTED:
            raise HTTPException(status_code=400, detail="Game not started")
        
        current_question_num = get_current_question_number(game)
        
        # Check if quiz is finished
        if current_question_num >= len(game.quiz.questions):
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
            
            return {
                "type": "quiz_finished",
                "final_leaderboard": final_leaderboard
            }
        
        question = game.quiz.questions[current_question_num]
        time_remaining = get_time_remaining_for_question(game, current_question_num)
        
        # Check if user already answered this question
        user_submission = await get_user_submission_from_db(game_code, player_id)
        user_answered = False
        user_answer = None
        if user_submission and question.question_id in user_submission.answers:
            user_answered = True
            user_answer = user_submission.answers[question.question_id]
        
        return {
            "type": "question",
            "question_number": current_question_num + 1,
            "total_questions": len(game.quiz.questions),
            "question_id": question.question_id,
            "question": question.question,
            "options": question.options,
            "time_remaining": time_remaining,
            "correct_answer": question.correct_answer,
            "user_answered": user_answered,
            "user_answer": user_answer.dict() if user_answer else None,
            "total_score": user_submission.total_score if user_submission else 0
        }
    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ Error getting current question: {e}")
        raise HTTPException(status_code=500, detail="Failed to get current question")

@app.post("/submit-answer/{game_code}/{player_id}")
async def submit_answer(game_code: str, player_id: str, request: SubmitAnswerRequest):
    try:
        game = await get_game_from_db(game_code)
        if not game:
            raise HTTPException(status_code=404, detail="Game not found")
        
        if game.status != GameStatus.STARTED:
            raise HTTPException(status_code=400, detail="Game not started")
        
        current_question_num = get_current_question_number(game)
        
        # Validate question number
        if request.question_number != current_question_num + 1:
            raise HTTPException(status_code=400, detail="Invalid question number")
        
        if current_question_num >= len(game.quiz.questions):
            raise HTTPException(status_code=400, detail="Quiz finished")
            
        question_obj = game.quiz.questions[current_question_num]
        
        # Check if time is still available for answering
        time_remaining = get_time_remaining_for_question(game, current_question_num)
        if time_remaining <= 0:
            raise HTTPException(status_code=400, detail="Time limit exceeded")
        
        user_submission = await get_user_submission_from_db(game_code, player_id)
        if not user_submission:
            raise HTTPException(status_code=404, detail="Player not found")
        
        if question_obj.question_id in user_submission.answers:
            raise HTTPException(status_code=400, detail="Answer already submitted")
        
        # Calculate time taken (15 seconds - time_remaining)
        time_taken = 15 - time_remaining
        is_correct = (request.submitter_answer == question_obj.correct_answer)
        score_to_add = 0
        max_score = 1000

        if is_correct:
            # Score based on how quickly they answered
            time_bonus = max(0, time_remaining / 15)
            score_to_add = int(max_score * (0.5 + 0.5 * time_bonus))  # Minimum 50% for correct answer
        
        new_user_answer = UserAnswer(
            question_id=question_obj.question_id,
            answer=request.submitter_answer,
            is_correct=is_correct,
            time_taken=time_taken,
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
            "is_correct": is_correct,
            "total_score": user_submission.total_score
        }
    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ Error submitting answer: {e}")
        raise HTTPException(status_code=500, detail="Failed to submit answer")