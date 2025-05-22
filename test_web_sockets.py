import asyncio
import websockets
import json
import requests
import time
from typing import List, Dict, Optional
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class KahootWebSocketTester:
    def __init__(self, base_url: str = "http://localhost:8000", ws_url: str = "ws://localhost:8000"):
        self.base_url = base_url
        self.ws_url = ws_url
        self.game_code: Optional[str] = None
        self.player_ids: List[str] = []
        self.websockets: List[websockets.WebSocketServerProtocol] = []
        
    async def create_test_quiz(self) -> str:
        """Create a test quiz and return the game code"""
        quiz_data = {
            "title": "Test Quiz",
            "questions": [
                {
                    "question": "What is 2 + 2?",
                    "options": ["3", "4", "5", "6"],
                    "correct_answer": 1,
                    "time_limit": 10
                },
                {
                    "question": "What is the capital of France?",
                    "options": ["London", "Berlin", "Paris", "Madrid"],
                    "correct_answer": 2,
                    "time_limit": 15
                },
                {
                    "question": "Which planet is closest to the sun?",
                    "options": ["Venus", "Mercury", "Earth", "Mars"],
                    "correct_answer": 1,
                    "time_limit": 12
                }
            ]
        }
        
        logger.info("Creating test quiz...")
        response = requests.post(f"{self.base_url}/create-quiz", json=quiz_data)
        
        if response.status_code == 200:
            result = response.json()
            self.game_code = result["game_code"]
            logger.info(f"Quiz created successfully! Game code: {self.game_code}")
            return self.game_code
        else:
            raise Exception(f"Failed to create quiz: {response.text}")
    
    async def join_game_as_player(self, player_name: str) -> str:
        """Join the game as a player and return player ID"""
        if not self.game_code:
            raise Exception("No game code available. Create a quiz first.")
        
        logger.info(f"Player '{player_name}' joining game {self.game_code}...")
        response = requests.post(
            f"{self.base_url}/join-game/{self.game_code}",
            params={"player_name": player_name}
        )
        
        if response.status_code == 200:
            result = response.json()
            player_id = result["player_id"]
            self.player_ids.append(player_id)
            logger.info(f"Player '{player_name}' joined successfully! Player ID: {player_id}")
            return player_id
        else:
            raise Exception(f"Failed to join game: {response.text}")
    
    async def start_game(self):
        """Start the game"""
        if not self.game_code:
            raise Exception("No game code available.")
        
        logger.info(f"Starting game {self.game_code}...")
        response = requests.post(f"{self.base_url}/start-game/{self.game_code}")
        
        if response.status_code == 200:
            logger.info("Game started successfully!")
        else:
            raise Exception(f"Failed to start game: {response.text}")
    
    async def connect_websocket(self, connection_name: str = "host") -> websockets.WebSocketServerProtocol:
        """Connect to WebSocket and return the connection"""
        if not self.game_code:
            raise Exception("No game code available.")
        
        ws_uri = f"{self.ws_url}/ws/{self.game_code}"
        logger.info(f"Connecting {connection_name} to WebSocket: {ws_uri}")
        
        websocket = await websockets.connect(ws_uri)
        self.websockets.append(websocket)
        
        # Send host connected message if this is the host
        if connection_name == "host":
            await websocket.send(json.dumps({"type": "host_connected"}))
        
        logger.info(f"{connection_name} connected to WebSocket successfully!")
        return websocket
    
    async def listen_to_websocket(self, websocket: websockets.WebSocketServerProtocol, 
                                 connection_name: str, duration: int = 30):
        """Listen to WebSocket messages for a specified duration"""
        logger.info(f"Listening to {connection_name} WebSocket for {duration} seconds...")
        
        try:
            start_time = time.time()
            while time.time() - start_time < duration:
                try:
                    # Set a timeout for receiving messages
                    message = await asyncio.wait_for(websocket.recv(), timeout=1.0)
                    data = json.loads(message)
                    logger.info(f"[{connection_name}] Received: {data}")
                    
                    # Handle different message types
                    await self.handle_websocket_message(data, connection_name)
                    
                except asyncio.TimeoutError:
                    # Send ping to keep connection alive
                    await websocket.send(json.dumps({"type": "ping"}))
                    continue
                except websockets.exceptions.ConnectionClosed:
                    logger.warning(f"[{connection_name}] WebSocket connection closed")
                    break
                    
        except Exception as e:
            logger.error(f"Error listening to {connection_name} WebSocket: {e}")
    
    async def handle_websocket_message(self, data: Dict, connection_name: str):
        """Handle different types of WebSocket messages"""
        message_type = data.get("type")
        
        if message_type == "pong":
            logger.debug(f"[{connection_name}] Received pong")
        elif message_type == "player_joined":
            logger.info(f"[{connection_name}] Players updated: {len(data.get('players', []))} total players")
        elif message_type == "game_started":
            logger.info(f"[{connection_name}] Game started!")
        elif message_type == "question":
            logger.info(f"[{connection_name}] Question {data.get('question_number')}: {data.get('question')}")
            logger.info(f"[{connection_name}] Options: {data.get('options')}")
            
            # Simulate answering the question if this is a player connection
            if connection_name.startswith("player") and len(self.player_ids) > 0:
                await self.simulate_answer(data.get('question_number', 1))
                
        elif message_type == "question_results":
            logger.info(f"[{connection_name}] Question results - Correct answer: {data.get('correct_answer')}")
            logger.info(f"[{connection_name}] Answer counts: {data.get('answer_counts')}")
            leaderboard = data.get('leaderboard', [])[:3]  # Top 3
            logger.info(f"[{connection_name}] Top 3: {leaderboard}")
        elif message_type == "game_finished":
            logger.info(f"[{connection_name}] Game finished!")
            final_leaderboard = data.get('final_leaderboard', [])[:3]
            logger.info(f"[{connection_name}] Final Top 3: {final_leaderboard}")
    
    async def simulate_answer(self, question_number: int):
        """Simulate players answering questions"""
        await asyncio.sleep(1)  # Small delay to simulate thinking time
        
        for i, player_id in enumerate(self.player_ids):
            # Simulate different answer patterns
            if question_number == 1:  # Math question: 2+2=4 (option 1)
                answer = 1 if i % 2 == 0 else 0  # Some correct, some wrong
            elif question_number == 2:  # Capital of France: Paris (option 2)
                answer = 2 if i % 3 != 0 else 1  # Most correct, some wrong
            else:  # Mercury question (option 1)
                answer = 1 if i % 4 != 0 else 3  # Most correct, some wrong
            
            try:
                response = requests.post(
                    f"{self.base_url}/submit-answer/{self.game_code}/{player_id}",
                    json=answer,
                    headers={"Content-Type": "application/json"}
                )
                
                if response.status_code == 200:
                    logger.info(f"Player {i+1} submitted answer: {answer}")
                else:
                    logger.error(f"Failed to submit answer for player {i+1}: {response.text}")
                    
            except Exception as e:
                logger.error(f"Error submitting answer for player {i+1}: {e}")
            
            await asyncio.sleep(0.5)  # Stagger the answers
    
    async def get_game_status(self):
        """Get current game status"""
        if not self.game_code:
            return None
        
        response = requests.get(f"{self.base_url}/game-status/{self.game_code}")
        if response.status_code == 200:
            return response.json()
        return None
    
    async def close_all_connections(self):
        """Close all WebSocket connections"""
        logger.info("Closing all WebSocket connections...")
        for websocket in self.websockets:
            try:
                await websocket.close()
            except Exception as e:
                logger.error(f"Error closing WebSocket: {e}")
        self.websockets.clear()

async def run_comprehensive_test():
    """Run a comprehensive test of the WebSocket functionality"""
    tester = KahootWebSocketTester()
    
    try:
        # Step 1: Create a quiz
        game_code = await tester.create_test_quiz()
        
        # Step 2: Add some players
        player_names = ["Alice", "Bob", "Charlie", "Diana"]
        for name in player_names:
            await tester.join_game_as_player(name)
        
        # Step 3: Connect WebSockets
        host_ws = await tester.connect_websocket("host")
        player_ws = await tester.connect_websocket("player_observer")
        
        # Step 4: Start listening to WebSockets in background
        host_task = asyncio.create_task(
            tester.listen_to_websocket(host_ws, "host", duration=60)
        )
        player_task = asyncio.create_task(
            tester.listen_to_websocket(player_ws, "player_observer", duration=60)
        )
        
        # Step 5: Wait a bit then start the game
        await asyncio.sleep(2)
        await tester.start_game()
        
        # Step 6: Monitor game status
        status_checks = 0
        while status_checks < 10:
            await asyncio.sleep(5)
            status = await tester.get_game_status()
            if status:
                logger.info(f"Game Status: {status}")
                if status.get('status') == 'finished':
                    break
            status_checks += 1
        
        # Step 7: Wait for tasks to complete or timeout
        try:
            await asyncio.wait_for(asyncio.gather(host_task, player_task), timeout=10)
        except asyncio.TimeoutError:
            logger.info("WebSocket listening tasks timed out (expected)")
        
    except Exception as e:
        logger.error(f"Test failed: {e}")
    finally:
        await tester.close_all_connections()

async def run_simple_connection_test():
    """Run a simple WebSocket connection test"""
    tester = KahootWebSocketTester()
    
    try:
        # Create quiz and get game code
        game_code = await tester.create_test_quiz()
        
        # Test basic WebSocket connection
        websocket = await tester.connect_websocket("test_connection")
        
        # Send a ping message
        await websocket.send(json.dumps({"type": "ping"}))
        logger.info("Sent ping message")
        
        # Wait for pong response
        try:
            response = await asyncio.wait_for(websocket.recv(), timeout=5)
            data = json.loads(response)
            logger.info(f"Received response: {data}")
        except asyncio.TimeoutError:
            logger.warning("No response received within timeout")
        
        await websocket.close()
        logger.info("Connection test completed")
        
    except Exception as e:
        logger.error(f"Simple connection test failed: {e}")
    finally:
        await tester.close_all_connections()

if __name__ == "__main__":
    print("Kahoot Clone WebSocket Tester")
    print("=============================")
    print("1. Simple Connection Test")
    print("2. Comprehensive Game Test")
    
    choice = input("Enter your choice (1 or 2): ").strip()
    
    if choice == "1":
        print("\nRunning simple connection test...")
        asyncio.run(run_simple_connection_test())
    elif choice == "2":
        print("\nRunning comprehensive game test...")
        asyncio.run(run_comprehensive_test())
    else:
        print("Invalid choice. Running comprehensive test by default...")
        asyncio.run(run_comprehensive_test())