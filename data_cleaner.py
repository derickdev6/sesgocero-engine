# This script takes JSON data from load_data.py and cleans it using DeepSeek AI.
# It processes each article individually to manage token limits.
# Cleaned articles are saved to the "clean_articles" collection in MongoDB.

from dotenv import load_dotenv
import os
import json
import requests
import time
from datetime import datetime
from typing import Union, Dict, Any, List, Optional
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from requests.exceptions import ReadTimeout, ConnectTimeout, ConnectionError
from dataclasses import dataclass
from functools import lru_cache
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, OperationFailure
from bson.objectid import ObjectId
from bson.errors import InvalidId

load_dotenv()


@dataclass
class APIConfig:
    """Configuration for the DeepSeek API."""

    url: str
    key: str
    model: str = "deepseek-chat"
    max_tokens: int = 8192
    temperature: float = 0.5
    top_p: float = 0.95
    presence_penalty: float = 0.1
    frequency_penalty: float = 0.1
    connect_timeout: int = 10
    read_timeout: int = 300
    chunk_size: int = 8192


class APIError(Exception):
    """Base exception for API-related errors."""

    pass


class ConfigurationError(APIError):
    """Raised when there are configuration issues."""

    pass


class ResponseError(APIError):
    """Raised when there are issues with the API response."""

    pass


@lru_cache(maxsize=1)
def get_timestamp() -> str:
    """Get current timestamp in a consistent format."""
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def print_step(message: str, start_time: Optional[float] = None) -> None:
    """Print a step message with optional elapsed time."""
    timestamp = get_timestamp()
    if start_time:
        elapsed = time.time() - start_time
        print(f"[{timestamp}] {message} (Elapsed: {elapsed:.2f}s)")
    else:
        print(f"[{timestamp}] {message}")


def create_session_with_retries() -> requests.Session:
    """Create a requests session with retry strategy."""
    session = requests.Session()
    retry_strategy = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[500, 502, 503, 504],
        allowed_methods=["POST"],
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


def get_api_config() -> APIConfig:
    """Get API configuration from environment variables."""
    api_url = os.getenv("DEEPSEEK_API_URL")
    api_key = os.getenv("DEEPSEEK_API_KEY")

    if not all([api_url, api_key]):
        raise ConfigurationError("Missing required environment variables")

    return APIConfig(url=str(api_url), key=str(api_key))


def prepare_clean_payload(article: Dict[str, Any], config: APIConfig) -> Dict[str, Any]:
    """Prepare the API request payload for cleaning a single article."""
    return {
        "model": config.model,
        "messages": [
            {
                "role": "system",
                "content": "You are a helpful assistant that cleans and processes article data. You must always provide complete responses without truncation.",
            },
            {
                "role": "user",
                "content": f"""
                Please clean and process the following article according to these instructions:

                1. Remove all HTML tags from the content and any other fields
                2. Clean any special characters or formatting
                3. Ensure all text is properly encoded in UTF-8
                4. Keep the original structure but with cleaned content
                5. Return ONLY the cleaned article in JSON format

                Article to clean:
                {json.dumps(article, ensure_ascii=False)}
                """,
            },
        ],
        "temperature": config.temperature,
        "max_tokens": config.max_tokens,
        "top_p": config.top_p,
        "presence_penalty": config.presence_penalty,
        "frequency_penalty": config.frequency_penalty,
        "response_format": {"type": "json_object"},
    }


def clean_article(
    article: Dict[str, Any], config: APIConfig, session: requests.Session
) -> Dict[str, Any]:
    """Clean a single article using DeepSeek AI."""
    try:
        payload = prepare_clean_payload(article, config)
        headers = {
            "Authorization": f"Bearer {config.key}",
            "Content-Type": "application/json",
        }

        response = session.post(
            config.url,
            json=payload,
            headers=headers,
            timeout=(config.connect_timeout, config.read_timeout),
            stream=True,
        )
        response.raise_for_status()

        # Read the response content in chunks
        content = []
        for chunk in response.iter_content(chunk_size=config.chunk_size):
            if chunk:
                content.append(chunk.decode("utf-8"))

        # Join the chunks and parse the JSON
        full_response = "".join(content)
        response_data = json.loads(full_response)

        # Extract the cleaned article from the first choice's message content
        if "choices" in response_data and len(response_data["choices"]) > 0:
            cleaned_content = response_data["choices"][0]["message"]["content"]
            return json.loads(cleaned_content)
        else:
            raise ResponseError("No choices found in API response")

    except Exception as e:
        print_step(f"Error cleaning article: {str(e)}")
        return article  # Return original article if cleaning fails


def clean_data(data: Union[str, List[Dict[str, Any]]]) -> None:
    """Clean all articles in the input data, save them to MongoDB, and update original articles."""
    start_time = time.time()
    print_step("Starting data cleaning, saving, and updating process")

    client = None
    try:
        # Convert string data to JSON if needed
        if isinstance(data, str):
            try:
                data = json.loads(data)
            except json.JSONDecodeError as e:
                raise ResponseError(f"Invalid JSON data: {str(e)}")

        if not isinstance(data, list):
            raise ResponseError("Input data must be a list of articles")

        config = get_api_config()

        # --- MongoDB Connection ---
        mongo_uri = os.getenv("MONGODB_URI")
        mongo_db_name = os.getenv("MONGODB_DB")
        mongo_original_col_name = os.getenv("MONGODB_COL")
        mongo_clean_col_name = "clean_articles"

        if not all([mongo_uri, mongo_db_name, mongo_original_col_name]):
            raise ConfigurationError(
                "Missing MongoDB URI, DB Name, or Original Collection Name in environment variables"
            )

        try:
            client = MongoClient(mongo_uri)
            db = client[str(mongo_db_name)]
            original_collection = db[str(mongo_original_col_name)]
            clean_collection = db[mongo_clean_col_name]
            # Test connection
            client.admin.command("ping")
            print_step(f"Successfully connected to MongoDB database '{mongo_db_name}'.")
            print_step(
                f"Using original collection: '{mongo_original_col_name}' and clean collection: '{mongo_clean_col_name}'"
            )
        except ConnectionFailure as e:
            raise ConfigurationError(f"Could not connect to MongoDB: {e}")
        # --- End MongoDB Connection ---

        articles_saved_count = 0
        articles_updated_count = 0
        articles_failed_count = 0
        with create_session_with_retries() as session:
            total_articles = len(data)
            for i, article in enumerate(data, 1):
                print_step(
                    f"💭Processing article {i}/{total_articles} (ID: {article.get('_id', 'N/A')})"
                )

                # --- Check if already cleaned --- # Added check
                if article.get("cleaned") is True:
                    print_step(
                        f"  🏳️Skipping article {i} (ID: {article.get('_id', 'N/A')}) - Already marked as cleaned."
                    )
                    # Optionally increment a specific skip counter here if needed
                    continue  # Skip to the next article
                # --- End Check ---

                original_article = article  # Keep reference to original article data
                original_article_id_str = original_article.get(
                    "_id"
                )  # Get the string ID

                if not original_article_id_str:
                    print_step(f"  🏁Skipping article {i} due to missing '_id'.")
                    articles_failed_count += 1
                    continue

                cleaned_article = clean_article(article, config, session)

                # Check if cleaning was successful (returned dict is not the original one)
                if cleaned_article is not original_article and isinstance(
                    cleaned_article, dict
                ):
                    save_successful = False
                    try:
                        # 1. Insert the cleaned article into the target collection
                        insert_result = clean_collection.insert_one(cleaned_article)
                        print_step(
                            f"  🟢Successfully saved cleaned article {i} to '{mongo_clean_col_name}' (New ID: {insert_result.inserted_id})"
                        )
                        articles_saved_count += 1
                        save_successful = True

                    except OperationFailure as e:
                        print_step(
                            f"  🔴Error saving cleaned article {i} to MongoDB: {e}"
                        )
                        articles_failed_count += 1
                    except Exception as e:
                        print_step(
                            f"  🔴An unexpected error occurred while saving cleaned article {i}: {e}"
                        )
                        articles_failed_count += 1

                    # 2. Update the original article ONLY if save was successful
                    if save_successful:
                        try:
                            original_object_id = ObjectId(original_article_id_str)
                            update_result = original_collection.update_one(
                                {"_id": original_object_id}, {"$set": {"cleaned": True}}
                            )
                            if update_result.matched_count > 0:
                                if update_result.modified_count > 0:
                                    print_step(
                                        f"  🟢Successfully marked original article {i} (ID: {original_article_id_str}) as cleaned in '{mongo_original_col_name}'."
                                    )
                                    articles_updated_count += 1
                                else:
                                    print_step(
                                        f"  🏳️Original article {i} (ID: {original_article_id_str}) was already marked as cleaned."
                                    )
                            else:
                                print_step(
                                    f"  🚨Warning: Could not find original article {i} (ID: {original_article_id_str}) in '{mongo_original_col_name}' to mark as cleaned."
                                )

                        except InvalidId:
                            print_step(
                                f"  🔴Error: Invalid format for original article ID '{original_article_id_str}'. Cannot update status."
                            )
                        except OperationFailure as e:
                            print_step(
                                f"  🔴Error updating original article {i} in '{mongo_original_col_name}': {e}"
                            )
                        except Exception as e:
                            print_step(
                                f"  🔴An unexpected error occurred while updating original article {i}: {e}"
                            )
                else:
                    # Cleaning failed or returned unexpected format, log it
                    print_step(
                        f"  🏁Skipping save and update for article {i} due to cleaning failure or invalid format."
                    )
                    articles_failed_count += 1

        print_step(
            f"Finished processing. ✅Saved: {articles_saved_count}, ✅Updated Original: {articles_updated_count}, ❌Failed/Skipped: {articles_failed_count}",
            start_time,
        )

    except ConfigurationError as e:
        raise e
    except Exception as e:
        raise APIError(f"Error during data cleaning/saving/updating process: {str(e)}")
    finally:
        # Ensure MongoDB client is closed if it was initialized
        if client:
            client.close()
            print_step("MongoDB connection closed.")


if __name__ == "__main__":
    try:
        from load_data import load_data

        print_step("Loading data...")
        data_json_string = load_data()
        print_step("Data loaded.")

        clean_data(data_json_string)

        print_step(
            "Data cleaning, saving, and updating process initiated successfully."
        )

    except Exception as e:
        print_step(f"An error occurred in the main execution block: {str(e)}")
