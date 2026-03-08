"""
Migration script to transfer data from PocketBase to MongoDB.
"""

import os
from dotenv import load_dotenv
from pocketbase import PocketBase
from pymongo import MongoClient
from collections import Counter
from datetime import datetime

# Load environment variables from .env file
load_dotenv()

# PocketBase configuration
POCKETBASE_URL = os.getenv("POCKETBASE_URL")
POCKETBASE_ADMIN_EMAIL = os.getenv("POCKETBASE_ADMIN_EMAIL")
POCKETBASE_ADMIN_PASSWORD = os.getenv("POCKETBASE_ADMIN_PASSWORD")
COLLECTION_NAME = "distinct_citations"

# MongoDB configuration
MONGODB_USERNAME = os.getenv("MONGODB_USERNAME")
MONGODB_PASSWORD = os.getenv("MONGODB_PASSWORD")
MONGODB_CONNECTION_STRING = f"mongodb+srv://{MONGODB_USERNAME}:{MONGODB_PASSWORD}@cluster0.crjrjjf.mongodb.net/?appName=Cluster0"
MONGODB_DATABASE = os.getenv("MONGODB_DATABASE", "orbittrends")
MONGODB_COLLECTION = "distinct_citations"


def fetch_pocketbase_records(base_url: str, collection: str) -> list:
    """
    Fetch all records from a PocketBase collection using the PocketBase SDK.
    """
    print(f"Connecting to PocketBase at {base_url}")
    client = PocketBase(base_url)
    
    # Authenticate as admin
    print("Authenticating as admin...")
    admin_data = client.admins.auth_with_password(POCKETBASE_ADMIN_EMAIL, POCKETBASE_ADMIN_PASSWORD)
    if admin_data.is_valid:
        print("Successfully authenticated as admin!")
    else:
        print("Warning: Admin authentication may have failed.")
    
    print(f"Fetching all records from collection: {collection}")
    
    try:
        # Fetch all records using get_full_list
        records = client.collection(collection).get_full_list()
        
        # Convert record objects to dictionaries
        records_list = []
        for record in records:
            # Convert RecordModel to dict
            record_dict = {
                "id": record.id,
                "collectionId": record.collection_id,
                "collectionName": record.collection_name,
            }
            # Add all other fields from the record
            for key, value in record.__dict__.items():
                if not key.startswith('_') and key not in ['id', 'collection_id', 'collection_name']:
                    record_dict[key] = value
            records_list.append(record_dict)
        
        print(f"Total records fetched: {len(records_list)}")
        return records_list
        
    except Exception as e:
        print(f"Error fetching records: {e}")
        return []


def transform_record(record: dict) -> dict:
    """
    Transform a PocketBase record for MongoDB insertion.
    Converts date strings to datetime objects and removes PocketBase-specific fields.
    """
    transformed = {}
    
    # Map fields from PocketBase to MongoDB
    for key, value in record.items():
        # Skip PocketBase-specific metadata fields
        if key in ["collectionId", "collectionName"]:
            continue
        
        # Convert date strings to datetime objects
        if key in ["issueDate", "receivedTime"] and value:
            try:
                # Handle PocketBase date format: "2022-01-01 10:00:00.123Z"
                if isinstance(value, str):
                    # Replace space with 'T' for ISO format
                    value = value.replace(" ", "T")
                    if not value.endswith("Z"):
                        value += "Z"
                    transformed[key] = datetime.fromisoformat(value.replace("Z", "+00:00"))
                else:
                    transformed[key] = value
            except ValueError:
                transformed[key] = value
        else:
            transformed[key] = value
    
    # Preserve the original PocketBase ID as _pocketbase_id and use it as _id
    if "id" in transformed:
        transformed["_pocketbase_id"] = transformed["id"]
        # Remove the original 'id' field as MongoDB uses '_id'
        del transformed["id"]
    
    return transformed


def migrate_to_mongodb(records: list, connection_string: str, database: str, collection: str):
    """
    Insert records into MongoDB collection.
    """
    if not records:
        print("No records to migrate.")
        return
    
    print(f"\nConnecting to MongoDB...")
    
    try:
        client = MongoClient(connection_string)
        
        # Test connection
        client.admin.command('ping')
        print("Successfully connected to MongoDB!")
        
        db = client[database]
        coll = db[collection]
        
        # Transform records
        print(f"\nTransforming {len(records)} records...")
        transformed_records = [transform_record(r) for r in records]
        
        # Insert records
        print(f"Inserting records into {database}.{collection}...")
        
        # Use insert_many with ordered=False to continue on duplicate key errors
        try:
            result = coll.insert_many(transformed_records, ordered=False)
            print(f"Successfully inserted {len(result.inserted_ids)} records.")
        except pymongo.errors.BulkWriteError as e:
            # Some records may have been inserted before the error
            inserted_count = e.details.get('nInserted', 0)
            print(f"Bulk write completed with some errors. Inserted: {inserted_count} records.")
            print(f"Errors: {len(e.details.get('writeErrors', []))} duplicates or failures.")
        
        # Print collection stats
        count = coll.count_documents({})
        print(f"\nTotal documents in collection: {count}")
        
        client.close()
        print("Migration completed!")
        
    except pymongo.errors.ConnectionFailure as e:
        print(f"Failed to connect to MongoDB: {e}")
    except Exception as e:
        print(f"Error during migration: {e}")


def analyze_substring_frequency(field_name: str, start_index: int = 6, length: int = 3):
    """
    Fetch a field from MongoDB documents and count the frequency of substrings
    at specified positions (default: characters 6, 7, 8 = index 6-8).
    
    Args:
        field_name: The field to extract from documents (e.g., 'citationNumber', '_id')
        start_index: Starting index for substring extraction (0-based, default 6)
        length: Length of substring to extract (default 3 for positions 6,7,8)
    """
    print("=" * 60)
    print(f"Analyzing substring frequency for field: {field_name}")
    print(f"Substring positions: {start_index} to {start_index + length - 1}")
    print("=" * 60)
    
    try:
        client = MongoClient(MONGODB_CONNECTION_STRING)
        client.admin.command('ping')
        print("Connected to MongoDB!")
        
        db = client[MONGODB_DATABASE]
        coll = db[MONGODB_COLLECTION]
        
        # Fetch all documents, projecting only the field we need
        documents = coll.find({}, {field_name: 1, "_id": 0})
        
        substrings = []
        total_docs = 0
        skipped = 0
        
        for doc in documents:
            total_docs += 1
            value = doc.get(field_name)
            
            if value and isinstance(value, str) and len(value) >= start_index + length:
                # Extract substring at positions 6, 7, 8 (indices 6-8)
                substring = value[start_index:start_index + length]
                substrings.append(substring)
            else:
                skipped += 1
        
        # Count frequency of each unique substring
        frequency = Counter(substrings)
        
        print(f"\nTotal documents processed: {total_docs}")
        print(f"Documents with valid substrings: {len(substrings)}")
        print(f"Documents skipped (field too short or missing): {skipped}")
        print(f"\nDistinct substrings found: {len(frequency)}")
        
        # Print frequency distribution sorted by count (most common first)
        print(f"\nSubstring frequency (positions {start_index}-{start_index + length - 1}):")
        print("-" * 40)
        for substring, count in frequency.most_common():
            print(f"  '{substring}': {count}")
        
        client.close()
        return frequency
        
    except Exception as e:
        print(f"Error analyzing data: {e}")
        return None


def main():
    """
    Main function to run the migration.
    """
    print("=" * 60)
    print("PocketBase to MongoDB Migration")
    print("=" * 60)
    
    # Fetch records from PocketBase
    records = fetch_pocketbase_records(POCKETBASE_URL, COLLECTION_NAME)
    
    if not records:
        print("\nNo records found in PocketBase. Exiting.")
        return
    
    # Show sample record
    print("\nSample record from PocketBase:")
    if records:
        sample = records[0]
        for key, value in sample.items():
            print(f"  {key}: {value}")
    
    # Migrate to MongoDB
    migrate_to_mongodb(
        records=records,
        connection_string=MONGODB_CONNECTION_STRING,
        database=MONGODB_DATABASE,
        collection=MONGODB_COLLECTION
    )


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == "analyze":
        # Run substring analysis
        # Usage: python3 migrate_to_mongodb.py analyze [field_name]
        field = sys.argv[2] if len(sys.argv) > 2 else "citationNumber"
        analyze_substring_frequency(field)
    else:
        # Run migration
        main()
