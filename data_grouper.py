# This script takes cleaned JSON data and groups articles by similarity using DeepSeek AI.
# Articles that don't belong to any group are placed in a separate list.

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

load_dotenv()


@dataclass
class APIConfig:
    """Configuration for the DeepSeek API."""

    url: str
    key: str
    model: str = "deepseek-chat"
    max_tokens = 8192  # Keep this value high if sending many articles
    temperature = 0.3  # Low temperature for coherence and avoid arbitrary groupings
    top_p = 0.9  # Slight sampling restriction for greater semantic precision
    presence_penalty = 0.0  # Don't penalize repeating topics if they are related
    frequency_penalty = 0.2  # Slight penalty to avoid unnecessary repetitions
    connect_timeout = 10
    read_timeout = 300
    chunk_size = 8192


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


def prepare_group_payload(
    articles: List[Dict[str, Any]], config: APIConfig
) -> Dict[str, Any]:
    """Prepare the API request payload for grouping articles."""
    # Create simplified articles with only relevant fields
    simplified_articles = []
    for article in articles:
        simplified_article = {
            "_id": article.get("_id", ""),
            "id": article.get("id", ""),
            "title": article.get("title", ""),
            "subtitle": article.get("subtitle", ""),
            "source": article.get("source", ""),
            "date": article.get("date", ""),
            "url": article.get("url", ""),
        }
        simplified_articles.append(simplified_article)

    print_step(f"Requesting grouping for {len(simplified_articles)} articles")
    return {
        "model": config.model,
        "messages": [
            {
                "role": "system",
                "content": """You are a helpful assistant that groups news articles by similarity.
                Your task is to analyze articles and group them based on the facts they communicate.
                Focus only on the titles and subtitles to determine similarity.
                Articles about the same news event should be in the same group, regardless of their source.
                Articles that don't fit into any group should be placed in the single_news list.
                Provide a clear, concise cluster name that summarizes the main topic of each group.""",
            },
            {
                "role": "user",
                "content": f"""
                Please analyze and group the following articles according to these instructions:

                1. Read all articles titles and subtitles, and group them based on the facts they communicate.
                2. Create a descriptive cluster name that summarizes the main topic of the group in spanish.
                3. Include all relevant articles in each group, even if from the same source
                4. Place ALL articles that DON'T belong to ANY group in the single_news list
                5. Return the result in this exact JSON format:
                {{
                    "clustered_news": [
                        {{
                            "cluster_name": "Summary of the group's topic in spanish",
                            "articles": [
                                {{
                                    "_id": "Article ID",
                                    "id": "Article ID",
                                    "title": "Article title",
                                    "subtitle": "Article subtitle",
                                    "source": "Source name",
                                    "date": "Publication date",
                                    "url": "Article URL"
                                }}
                            ]
                        }}
                    ],
                    "single_news": [
                        {{
                            "_id": "Article ID",
                            "id": "Article ID",
                            "title": "Article title",
                            "subtitle": "Article subtitle",
                            "source": "Source name",
                            "date": "Publication date",
                            "url": "Article URL"
                        }}
                    ]
                }}

                Articles to analyze:
                {json.dumps(simplified_articles, ensure_ascii=False)}
                """,
            },
        ],
        "temperature": config.temperature,
        "max_tokens": config.max_tokens,
        "top_p": config.top_p,
        "presence_penalty": config.presence_penalty,
        "frequency_penalty": config.frequency_penalty,
        "stream": False,
        "response_format": {"type": "json_object"},
    }


def group_articles(
    articles: List[Dict[str, Any]], config: APIConfig, session: requests.Session
) -> Dict[str, Any]:
    """Group articles using DeepSeek AI."""
    try:
        payload = prepare_group_payload(articles, config)
        headers = {
            "Authorization": f"Bearer {config.key}",
            "Content-Type": "application/json",
        }

        response = session.post(
            config.url,
            json=payload,
            headers=headers,
            timeout=(config.connect_timeout, config.read_timeout),
            stream=False,
        )

        if response.status_code != 200:
            print_step(f"API Error Response: {response.text}")
            raise ResponseError(
                f"API returned status code {response.status_code}: {response.text}"
            )

        response_data = response.json()

        # Extract the grouped articles from the first choice's message content
        if "choices" in response_data and len(response_data["choices"]) > 0:
            grouped_content = response_data["choices"][0]["message"]["content"]
            return json.loads(grouped_content)
        else:
            raise ResponseError("No choices found in API response")

    except Exception as e:
        print_step(f"Error grouping articles: {str(e)}")
        # Return a structure with all articles in single_news if grouping fails
        simplified_articles = [
            {
                "_id": article.get("_id", ""),
                "id": article.get("id", ""),
                "title": article.get("title", ""),
                "subtitle": article.get("subtitle", ""),
                "source": article.get("source", ""),
                "date": article.get("date", ""),
                "url": article.get("url", ""),
            }
            for article in articles
        ]
        return {"clustered_news": [], "single_news": simplified_articles}


def group_data(data: Union[str, List[Dict[str, Any]]]) -> str:
    """Group articles by similarity."""
    start_time = time.time()
    print_step("Starting article grouping process")

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

        with create_session_with_retries() as session:
            grouped_data = group_articles(data, config, session)

        print_step("Successfully grouped articles", start_time)
        return json.dumps(grouped_data, ensure_ascii=False, indent=4)

    except Exception as e:
        raise APIError(f"Error during article grouping: {str(e)}")


if __name__ == "__main__":
    try:
        from load_data import load_data

        # Load and clean the data first
        print_step("Loading data...")
        raw_data = load_data()
        raw_json = json.loads(raw_data)
        print_step(f"Loaded {len(raw_json)} articles")

        # Group the cleaned data
        print_step("Grouping articles...")
        grouped_data = group_data(raw_json)
        grouped_json = json.loads(grouped_data)

        # Save the grouped data to a file
        print_step("Saving grouped data...")
        with open("grouped_data.json", "w", encoding="utf-8") as f:
            f.write(grouped_data)
        print_step("Successfully saved grouped data")

    except Exception as e:
        print_step(f"Error: {str(e)}")
