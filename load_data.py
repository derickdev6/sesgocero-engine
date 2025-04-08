# This script gets the data from the database and returns a JSON object with the data

from pymongo import MongoClient
from dotenv import load_dotenv
import os
import json

load_dotenv()


def load_data():
    # Connect to the database
    mongo_uri = os.getenv("MONGODB_URI")
    mongo_db = os.getenv("MONGODB_DB")
    mongo_collection = os.getenv("MONGODB_COL")

    if not all([mongo_uri, mongo_db, mongo_collection]):
        raise ValueError("Missing required environment variables")

    client = MongoClient(mongo_uri)
    db = client[str(mongo_db)]
    collection = db[str(mongo_collection)]

    # Get all articles with date in descending order
    result = collection.find().sort("date", -1)

    # Convert to list and handle ObjectId serialization
    result = list(result)
    for doc in result:
        doc["_id"] = str(doc["_id"])
        # Join content array into a string with UTF-8 encoding
        doc["content"] = " ".join(
            str(item).encode("utf-8").decode("utf-8") for item in doc["content"]
        )

    return json.dumps(result, ensure_ascii=False)


if __name__ == "__main__":
    data = load_data()
    # print data in json format
    print(data)
