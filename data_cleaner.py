# This script takes JSON data from load_data.py and cleans it using DeepSeek AI.
# It processes each article individually to manage token limits.
# Cleaned articles are saved to the "clean_articles" collection in MongoDB.
# Original articles are updated with a 'cleaned' flag set to True.

from dotenv import load_dotenv
import os
import json
import time
from datetime import datetime
from typing import Union, Dict, Any, List, Optional
from dataclasses import dataclass
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, OperationFailure
from bson.objectid import ObjectId
from bson.errors import InvalidId
import asyncio
import aiohttp

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


async def clean_article(
    article: Dict[str, Any],
    config: APIConfig,
    session: aiohttp.ClientSession,
    semaphore: asyncio.Semaphore,
    article_index: int,
    total_articles: int,
) -> Dict[str, Any]:
    """Clean a single article using DeepSeek AI."""
    try:
        await semaphore.acquire()
        print_step(
            f"💭 {article_index}/{total_articles}\tID: {article.get('_id', 'N/A')}"
        )

        payload = prepare_clean_payload(article, config)
        headers = {
            "Authorization": f"Bearer {config.key}",
            "Content-Type": "application/json",
        }

        async with session.post(
            config.url,
            json=payload,
            headers=headers,
            timeout=aiohttp.ClientTimeout(total=config.read_timeout),
        ) as response:
            response.raise_for_status()
            response_data = await response.json()

            # Extract the cleaned article from the first choice's message content
            if "choices" in response_data and len(response_data["choices"]) > 0:
                cleaned_content = response_data["choices"][0]["message"]["content"]
                print_step(
                    f"🧹 {article_index}/{total_articles}\tID: {article.get('_id', 'N/A')}"
                )
                return json.loads(cleaned_content)
            else:
                error_msg = f"API Response missing choices. Response: {json.dumps(response_data, indent=2)}"
                print_step(f"🔴 Error in article {article_index}: {error_msg}")
                raise ResponseError(error_msg)

    except aiohttp.ClientError as e:
        error_msg = f"Network error while cleaning article {article_index}: {str(e)}"
        print_step(f"🔴 {error_msg}")
        print_step(
            f"🔴 Article content that caused the error: {json.dumps(article, indent=2)}"
        )
        return article
    except json.JSONDecodeError as e:
        error_msg = f"JSON parsing error in article {article_index}: {str(e)}"
        print_step(f"🔴 {error_msg}")
        print_step(
            f"🔴 Article content that caused the error: {json.dumps(article, indent=2, ensure_ascii=False)}"
        )
        return article
    except ResponseError as e:
        error_msg = f"API response error in article {article_index}: {str(e)}"
        print_step(f"🔴 {error_msg}")
        print_step(
            f"🔴 Article content that caused the error: {json.dumps(article, indent=2, ensure_ascii=False)}"
        )
        return article
    except Exception as e:
        error_msg = f"Unexpected error cleaning article {article_index}: {str(e)}"
        print_step(f"🔴 {error_msg}")
        print_step(f"🔴 Error type: {type(e).__name__}")
        print_step(
            f"🔴 Article content that caused the error: {json.dumps(article, indent=2, ensure_ascii=False)}"
        )
        return article
    finally:
        semaphore.release()


async def clean_data(data: Union[str, List[Dict[str, Any]]]) -> None:
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
        skipped_count = 0  # Counter for skipped articles

        # Create a semaphore to limit concurrent requests
        max_concurrent_tasks = 5
        semaphore = asyncio.Semaphore(max_concurrent_tasks)

        async with aiohttp.ClientSession() as session:  # Use aiohttp session
            total_articles = len(data)
            tasks = []  # List to hold tasks
            for i, article in enumerate(data, 1):
                # --- Check if already cleaned --- # Added check
                if article.get("cleaned") is True:
                    skipped_count += 1
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

                # Create a task for cleaning the article
                task = asyncio.create_task(
                    clean_article(
                        article,
                        config,
                        session,
                        semaphore,
                        i,
                        total_articles,
                    )
                )
                tasks.append(
                    (i, original_article, original_article_id_str, task)
                )  # Store task and metadata

            # Print total skipped articles after the loop
            if skipped_count > 0:
                print_step(
                    f"🏳️Skipped {skipped_count} articles due to already being cleaned."
                )

            # Wait for all tasks to complete
            for i, original_article, original_article_id_str, task in tasks:
                try:
                    cleaned_article = await task  # Await the task result

                    save_successful = False
                    try:
                        # 1. Insert the cleaned article into the target collection
                        insert_result = clean_collection.insert_one(cleaned_article)
                        articles_saved_count += 1
                        save_successful = True
                        print_step(
                            f"💾 {i}/{total_articles}\tID: {insert_result.inserted_id}"
                        )

                        # 2. Update the original article ONLY if save was successful
                        try:
                            original_object_id = ObjectId(original_article_id_str)
                            update_result = original_collection.update_one(
                                {"_id": original_object_id},
                                {"$set": {"cleaned": True}},
                            )
                            if update_result.matched_count > 0:
                                if update_result.modified_count > 0:
                                    print_step(
                                        f"🟢 {i}/{total_articles}\tID: {original_object_id}"
                                    )
                                    articles_updated_count += 1
                                else:
                                    print_step(
                                        f"🏳️ {i}/{total_articles}\tID: {original_object_id}"
                                    )
                            else:
                                print_step(
                                    f"🚨 Article {i} not found in '{mongo_original_col_name}'"
                                )

                        except InvalidId:
                            print_step(f"🔴 Invalid ID format for article {i}")
                        except OperationFailure as e:
                            print_step(f"🔴 Error updating article {i}: {e}")
                        except Exception as e:
                            print_step(f"🔴 Unexpected error updating article {i}: {e}")

                    except OperationFailure as e:
                        print_step(f"🔴 Error saving article {i}: {e}")
                        articles_failed_count += 1
                    except Exception as e:
                        print_step(f"🔴 Unexpected error saving article {i}: {e}")
                        articles_failed_count += 1

                except Exception as e:
                    print_step(f"🔴 Error processing article {i}: {e}")
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

        asyncio.run(clean_data(data_json_string))

        print_step(
            "Data cleaning, saving, and updating process initiated successfully."
        )

    except Exception as e:
        print_step(f"An error occurred in the main execution block: {str(e)}")
