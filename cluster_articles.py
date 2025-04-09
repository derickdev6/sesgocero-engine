# This script loads articles from the clean_articles collection and assigns them to clusters
# using DeepSeek AI. It either adds articles to existing clusters or creates new ones.

from dotenv import load_dotenv
import os
import json
import time
import requests
from datetime import datetime
from typing import Union, Dict, Any, List, Optional
from dataclasses import dataclass
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
    temperature: float = 0.3  # Low temperature for more deterministic responses
    top_p: float = 0.9
    presence_penalty: float = 0.0
    frequency_penalty: float = 0.2
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


def prepare_cluster_payload(
    article: Dict[str, Any], cluster_names: List[str], config: APIConfig
) -> Dict[str, Any]:
    """Prepare the API request payload for determining article cluster."""
    return {
        "model": config.model,
        "messages": [
            {
                "role": "system",
                "content": """You are a helpful assistant that determines which cluster a news article belongs to.
                Your task is to analyze the article and determine if it belongs to an existing cluster or if a new cluster should be created.
                If the article belongs to an existing cluster, return ONLY the name of that cluster.
                If the article doesn't belong to any existing cluster, create a new cluster name that consists of only relevant words or facts in Spanish.
                The cluster name should be concise and descriptive of the main topic or event.
                DO NOT include any explanation or additional text, just the cluster name.""",
            },
            {
                "role": "user",
                "content": f"""
                Please analyze this article and determine which cluster it belongs to:

                Article:
                {json.dumps(article, ensure_ascii=False)}

                Existing clusters:
                {json.dumps(cluster_names, ensure_ascii=False)}

                If the article belongs to an existing cluster, return ONLY the name of that cluster.
                If the article doesn't belong to any existing cluster, create a new cluster name that consists of only relevant words or facts in Spanish.
                The cluster name should be concise and descriptive of the main topic or event.
                DO NOT include any explanation or additional text, just the cluster name.
                """,
            },
        ],
        "temperature": config.temperature,
        "max_tokens": config.max_tokens,
        "top_p": config.top_p,
        "presence_penalty": config.presence_penalty,
        "frequency_penalty": config.frequency_penalty,
        "response_format": {"type": "text"},
    }


def determine_cluster(
    article: Dict[str, Any],
    cluster_names: List[str],
    config: APIConfig,
    article_index: int,
    total_articles: int,
) -> str:
    """Determine which cluster an article belongs to using DeepSeek AI."""
    try:
        print_step(
            f"💭 {article_index}/{total_articles}\tID: {article.get('_id', 'N/A')}"
        )

        payload = prepare_cluster_payload(article, cluster_names, config)
        headers = {
            "Authorization": f"Bearer {config.key}",
            "Content-Type": "application/json",
        }

        response = requests.post(
            config.url,
            json=payload,
            headers=headers,
            timeout=config.read_timeout,
        )
        response.raise_for_status()
        response_data = response.json()

        # Extract the cluster name from the first choice's message content
        if "choices" in response_data and len(response_data["choices"]) > 0:
            cluster_name = response_data["choices"][0]["message"]["content"].strip()
            print_step(
                f"🪣 {article_index}/{total_articles}\tID: {article.get('_id', 'N/A')}\tCluster: {cluster_name}"
            )
            return cluster_name
        else:
            error_msg = f"API Response missing choices. Response: {json.dumps(response_data, indent=2)}"
            print_step(f"🔴 Error in article {article_index}: {error_msg}")
            raise ResponseError(error_msg)

    except requests.RequestException as e:
        error_msg = f"Network error while processing article {article_index}: {str(e)}"
        print_step(f"🔴 {error_msg}")
        print_step(
            f"🔴 Article content that caused the error: {json.dumps(article, indent=2, ensure_ascii=False)}"
        )
        return "error"
    except json.JSONDecodeError as e:
        error_msg = f"JSON parsing error in article {article_index}: {str(e)}"
        print_step(f"🔴 {error_msg}")
        print_step(
            f"🔴 Article content that caused the error: {json.dumps(article, indent=2, ensure_ascii=False)}"
        )
        return "error"
    except ResponseError as e:
        error_msg = f"API response error in article {article_index}: {str(e)}"
        print_step(f"🔴 {error_msg}")
        print_step(
            f"🔴 Article content that caused the error: {json.dumps(article, indent=2, ensure_ascii=False)}"
        )
        return "error"
    except Exception as e:
        error_msg = f"Unexpected error processing article {article_index}: {str(e)}"
        print_step(f"🔴 {error_msg}")
        print_step(f"🔴 Error type: {type(e).__name__}")
        print_step(
            f"🔴 Article content that caused the error: {json.dumps(article, indent=2, ensure_ascii=False)}"
        )
        return "error"


def cluster_articles() -> None:
    """Load articles from clean_articles collection and assign them to clusters."""
    start_time = time.time()
    print_step("Starting article clustering process")

    client = None
    try:
        config = get_api_config()

        # --- MongoDB Connection ---
        mongo_uri = os.getenv("MONGODB_URI")
        mongo_db_name = os.getenv("MONGODB_DB")
        mongo_clean_col_name = "clean_articles"
        mongo_clusters_col_name = "clusters"

        if not all([mongo_uri, mongo_db_name]):
            raise ConfigurationError(
                "Missing MongoDB URI or DB Name in environment variables"
            )

        try:
            client = MongoClient(mongo_uri)
            db = client[str(mongo_db_name)]
            clean_collection = db[mongo_clean_col_name]
            clusters_collection = db[mongo_clusters_col_name]
            # Test connection
            client.admin.command("ping")
            print_step(f"Successfully connected to MongoDB database '{mongo_db_name}'.")
            print_step(
                f"Using clean collection: '{mongo_clean_col_name}' and clusters collection: '{mongo_clusters_col_name}'"
            )
        except ConnectionFailure as e:
            raise ConfigurationError(f"Could not connect to MongoDB: {e}")
        # --- End MongoDB Connection ---

        # 1. Load all articles from the clean_articles collection
        print_step("Loading articles from clean_articles collection...")
        articles = list(clean_collection.find())
        total_articles = len(articles)
        print_step(f"Loaded {total_articles} articles from clean_articles collection")

        # 2. Load all cluster names from the clusters collection
        print_step("Loading cluster names from clusters collection...")
        clusters = list(clusters_collection.find())
        cluster_names = [
            cluster.get("name", "") for cluster in clusters if "name" in cluster
        ]
        print_step(
            f"Loaded {len(cluster_names)} cluster names from clusters collection"
        )

        # Track statistics
        articles_processed = 0
        articles_added_to_existing = 0
        new_clusters_created = 0
        articles_failed = 0
        articles_skipped = 0

        # Process articles
        for i, article in enumerate(articles, 1):
            # Skip articles that are already in a cluster
            if article.get("cluster_id"):
                print_step(
                    f"⏩ {i}/{total_articles}\tID: {article.get('_id', 'N/A')}\tAlready in cluster"
                )
                articles_processed += 1
                articles_skipped += 1
                continue

            # Determine which cluster the article belongs to
            cluster_name = determine_cluster(
                article,
                cluster_names,
                config,
                i,
                total_articles,
            )

            if cluster_name == "error":
                articles_failed += 1
                continue

            # Check if the cluster exists
            cluster = clusters_collection.find_one({"name": cluster_name})

            if cluster:
                # Update existing cluster
                try:
                    # Add article to the cluster's articles list
                    update_result = clusters_collection.update_one(
                        {"_id": cluster["_id"]}, {"$push": {"articles": article["_id"]}}
                    )

                    # Update the article with the cluster_id
                    clean_collection.update_one(
                        {"_id": article["_id"]},
                        {"$set": {"cluster_id": cluster["_id"]}},
                    )

                    if update_result.modified_count > 0:
                        print_step(
                            f"🟢 {i}/{total_articles}\tID: {article.get('_id', 'N/A')}\tAdded to existing cluster: {cluster_name}"
                        )
                        articles_added_to_existing += 1
                    else:
                        print_step(
                            f"⚠️ {i}/{total_articles}\tID: {article.get('_id', 'N/A')}\tFailed to add to cluster: {cluster_name}"
                        )
                        articles_failed += 1
                except Exception as e:
                    print_step(f"🔴 Error updating cluster {cluster_name}: {str(e)}")
                    articles_failed += 1
            else:
                # Create new cluster
                try:
                    # Create a new cluster document
                    new_cluster = {
                        "name": cluster_name,
                        "articles": [article["_id"]],
                        "created_at": datetime.now(),
                        "updated_at": datetime.now(),
                    }

                    # Insert the new cluster
                    insert_result = clusters_collection.insert_one(new_cluster)

                    # Update the article with the cluster_id
                    clean_collection.update_one(
                        {"_id": article["_id"]},
                        {"$set": {"cluster_id": insert_result.inserted_id}},
                    )

                    # Add the new cluster name to our list
                    cluster_names.append(cluster_name)

                    print_step(
                        f"🟢 {i}/{total_articles}\tID: {article.get('_id', 'N/A')}\t➕Created new cluster: {cluster_name}"
                    )
                    new_clusters_created += 1
                except Exception as e:
                    print_step(
                        f"🔴 Error creating new cluster {cluster_name}: {str(e)}"
                    )
                    articles_failed += 1

            articles_processed += 1

        print_step(
            f"Finished clustering. ✅Processed: {articles_processed}, ✅Added to existing: {articles_added_to_existing}, ✅New clusters: {new_clusters_created}, ❌Failed: {articles_failed}, ⏩Skipped: {articles_skipped}",
            start_time,
        )

    except ConfigurationError as e:
        raise e
    except Exception as e:
        raise APIError(f"Error during article clustering process: {str(e)}")
    finally:
        # Ensure MongoDB client is closed if it was initialized
        if client:
            client.close()
            print_step("MongoDB connection closed.")


if __name__ == "__main__":
    try:
        cluster_articles()
        print_step("Article clustering process completed successfully.")
    except Exception as e:
        print_step(f"An error occurred in the main execution block: {str(e)}")
