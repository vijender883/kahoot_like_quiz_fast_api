import asyncio
import pandas as pd
from motor.motor_asyncio import AsyncIOMotorClient
from datetime import datetime
import logging
import uuid

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# MongoDB connection
MONGODB_URL = "mongodb+srv://admin:RCyKhyxEw0LmSSXN@cluster0.rq9f5.mongodb.net/main?retryWrites=true&w=majority&appName=Cluster0"

async def upload_csv_users(csv_file_path: str, database_name: str = "test", collection_name: str = "user_info"):
    """
    Upload users from CSV to MongoDB with upsert functionality
    If a user's email already exists, update the existing entry
    """
    client = AsyncIOMotorClient(MONGODB_URL)
    db = client[database_name]
    collection = db[collection_name]
    
    try:
        # Read CSV file
        logger.info(f"Reading CSV file: {csv_file_path}")
        df = pd.read_csv(csv_file_path)
        
        # Display CSV info
        logger.info(f"CSV contains {len(df)} rows and {len(df.columns)} columns")
        logger.info(f"Columns: {list(df.columns)}")
        
        # Clean and prepare data
        df = df.dropna()  # Remove rows with missing values
        
        # Standardize column names to match your existing schema
        column_mapping = {
            'fullName': 'name',
            'email': 'email_id', 
            'phone': 'phone_no'
        }
        
        # Rename columns if needed
        df = df.rename(columns=column_mapping)
        
        # Ensure phone numbers are strings and properly formatted
        if 'phone_no' in df.columns:
            df['phone_no'] = df['phone_no'].astype(str)
            # Add + prefix if not present and remove any decimal points
            df['phone_no'] = df['phone_no'].apply(lambda x: f"+{x.replace('.0', '')}" if not x.startswith('+') else x.replace('.0', ''))
        
        # Convert DataFrame to list of dictionaries
        users_data = df.to_dict('records')
        
        # Counters for tracking operations
        inserted_count = 0
        updated_count = 0
        error_count = 0
        
        logger.info(f"Starting upload of {len(users_data)} users...")
        
        # Process each user
        for i, user_data in enumerate(users_data):
            try:
                current_time = datetime.utcnow()
                
                # Check if user already exists to determine if we need to generate player_id
                existing_user = await collection.find_one({"email_id": user_data["email_id"]})
                
                # Prepare update data (excluding created_at to avoid conflict)
                update_data = user_data.copy()
                update_data['updated_at'] = current_time
                
                # Generate player_id if it doesn't exist (for both new and existing users without player_id)
                if existing_user:
                    # User exists - only add player_id if they don't have one
                    if 'player_id' not in existing_user or not existing_user.get('player_id'):
                        update_data['player_id'] = str(uuid.uuid4())
                else:
                    # New user - always add player_id
                    update_data['player_id'] = str(uuid.uuid4())
                
                # Use upsert operation - update if email exists, insert if not
                result = await collection.update_one(
                    {"email_id": user_data["email_id"]},  # Filter by email
                    {
                        "$set": update_data,  # Update all fields including updated_at
                        "$setOnInsert": {"created_at": current_time}  # Only set created_at on insert
                    },
                    upsert=True  # Insert if document doesn't exist
                )
                
                if result.upserted_id:
                    inserted_count += 1
                    if (inserted_count + updated_count) % 50 == 0:
                        logger.info(f"Progress: {inserted_count + updated_count}/{len(users_data)} processed")
                elif result.modified_count > 0:
                    updated_count += 1
                    if (inserted_count + updated_count) % 50 == 0:
                        logger.info(f"Progress: {inserted_count + updated_count}/{len(users_data)} processed")
                        
            except Exception as e:
                error_count += 1
                logger.error(f"Error processing user {i+1}: {e}")
                continue
        
        # Final statistics
        logger.info("\n" + "="*50)
        logger.info("UPLOAD SUMMARY")
        logger.info("="*50)
        logger.info(f"Total users processed: {len(users_data)}")
        logger.info(f"New users inserted: {inserted_count}")
        logger.info(f"Existing users updated: {updated_count}")
        logger.info(f"Errors encountered: {error_count}")
        
        # Verify final count in database
        total_count = await collection.count_documents({})
        logger.info(f"Total users in database: {total_count}")
        
        # Show sample of uploaded users with all required fields
        logger.info(f"\nSample of users in database (showing required fields):")
        async for user in collection.find().limit(5):
            logger.info(f"- Name: {user.get('name', 'N/A')}")
            logger.info(f"  Email: {user.get('email_id', 'N/A')}")
            logger.info(f"  Phone: {user.get('phone_no', 'N/A')}")
            logger.info(f"  Player ID: {user.get('player_id', 'N/A')}")
            logger.info(f"  Created: {user.get('created_at', 'N/A')}")
            logger.info("  ---")
            
    except FileNotFoundError:
        logger.error(f"CSV file not found: {csv_file_path}")
    except pd.errors.EmptyDataError:
        logger.error("CSV file is empty")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
    finally:
        client.close()

async def fix_missing_player_ids(database_name: str = "test", collection_name: str = "user_info"):
    """Add player_id to existing users who don't have one"""
    client = AsyncIOMotorClient(MONGODB_URL)
    db = client[database_name]
    collection = db[collection_name]
    
    try:
        # Find users without player_id
        users_without_player_id = []
        async for user in collection.find({"player_id": {"$exists": False}}):
            users_without_player_id.append(user)
        
        logger.info(f"Found {len(users_without_player_id)} users without player_id")
        
        if users_without_player_id:
            # Update each user with a new player_id
            for user in users_without_player_id:
                await collection.update_one(
                    {"_id": user["_id"]},
                    {"$set": {"player_id": str(uuid.uuid4()), "updated_at": datetime.utcnow()}}
                )
            
            logger.info(f"Added player_id to {len(users_without_player_id)} users")
        else:
            logger.info("All users already have player_id - no updates needed")
            
    except Exception as e:
        logger.error(f"Error fixing missing player_ids: {e}")
    finally:
        client.close()
    """Verify the contents of the database"""
    client = AsyncIOMotorClient(MONGODB_URL)
    db = client[database_name]
    collection = db[collection_name]
    
    try:
        total_count = await collection.count_documents({})
        logger.info(f"Total users in {database_name}.{collection_name}: {total_count}")
    except Exception as e:
        logger.error(f"Error fixing missing player_ids: {e}")
        
async def verify_database_contents(database_name: str = "test", collection_name: str = "user_info"):
    """Verify the contents of the database and check for required fields"""
    client = AsyncIOMotorClient(MONGODB_URL)
    db = client[database_name]
    collection = db[collection_name]
    
    try:
        total_count = await collection.count_documents({})
        logger.info(f"Total users in {database_name}.{collection_name}: {total_count}")
        
        # Check for users missing required fields
        missing_player_id = await collection.count_documents({"player_id": {"$exists": False}})
        missing_name = await collection.count_documents({"name": {"$exists": False}})
        missing_email = await collection.count_documents({"email_id": {"$exists": False}})
        missing_phone = await collection.count_documents({"phone_no": {"$exists": False}})
        
        logger.info(f"\nRequired fields check:")
        logger.info(f"- Users missing player_id: {missing_player_id}")
        logger.info(f"- Users missing name: {missing_name}")
        logger.info(f"- Users missing email_id: {missing_email}")
        logger.info(f"- Users missing phone_no: {missing_phone}")
        
        # Show duplicates check
        pipeline = [
            {"$group": {"_id": "$email_id", "count": {"$sum": 1}}},
            {"$match": {"count": {"$gt": 1}}}
        ]
        
        duplicates = []
        async for doc in collection.aggregate(pipeline):
            duplicates.append(doc)
        
        if duplicates:
            logger.warning(f"Found {len(duplicates)} duplicate emails:")
            for dup in duplicates[:5]:  # Show first 5
                logger.warning(f"- {dup['_id']}: {dup['count']} occurrences")
        else:
            logger.info("No duplicate emails found - good!")
        
        # Check for duplicate player_ids
        player_id_pipeline = [
            {"$group": {"_id": "$player_id", "count": {"$sum": 1}}},
            {"$match": {"count": {"$gt": 1}}}
        ]
        
        duplicate_player_ids = []
        async for doc in collection.aggregate(player_id_pipeline):
            duplicate_player_ids.append(doc)
        
        if duplicate_player_ids:
            logger.warning(f"Found {len(duplicate_player_ids)} duplicate player_ids:")
            for dup in duplicate_player_ids[:5]:
                logger.warning(f"- {dup['_id']}: {dup['count']} occurrences")
        else:
            logger.info("All player_ids are unique - good!")
            
    except Exception as e:
        logger.error(f"Error verifying database: {e}")
    finally:
        client.close()

async def main():
    """Main function"""
    print("CSV to MongoDB User Upload Script")
    print("="*40)
    
    # Get CSV file path
    csv_file = input("Enter CSV file path (or press Enter for 'registered_users.csv'): ").strip()
    if not csv_file:
        csv_file = "registered_users.csv"
    
    # Get database details
    database = input("Enter database name (or press Enter for 'test'): ").strip()
    if not database:
        database = "test"
        
    collection = input("Enter collection name (or press Enter for 'user_info'): ").strip()
    if not collection:
        collection = "user_info"
    
    print(f"\nUploading users from '{csv_file}' to '{database}.{collection}'")
    print("This will update existing users based on email and insert new ones.")
    print("All users will have the required fields: name, email_id, phone_no, player_id")
    
    confirm = input("Continue? (y/n): ").strip().lower()
    if confirm != 'y':
        print("Operation cancelled.")
        return
    
    # First, fix any existing users without player_id
    print("\nChecking for existing users without player_id...")
    await fix_missing_player_ids(database, collection)
    
    # Upload users
    await upload_csv_users(csv_file, database, collection)
    
    # Verify results
    print("\nVerifying database contents...")
    await verify_database_contents(database, collection)

if __name__ == "__main__":
    asyncio.run(main())