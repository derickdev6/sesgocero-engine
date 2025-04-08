# This script takes JSON data from load_data.py and cleans it using DeepSeek AI.
# It processes each article individually to manage token limits.

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


def clean_data(data: Union[str, List[Dict[str, Any]]]) -> str:
    """Clean all articles in the input data."""
    start_time = time.time()
    print_step("Starting data cleaning process")

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
        cleaned_articles = []

        with create_session_with_retries() as session:
            total_articles = len(data)
            for i, article in enumerate(data, 1):
                print_step(f"Cleaning article {i}/{total_articles}")
                cleaned_article = clean_article(article, config, session)
                cleaned_articles.append(cleaned_article)

        print_step("Successfully cleaned all articles", start_time)
        return json.dumps(cleaned_articles, ensure_ascii=False, indent=4)

    except Exception as e:
        raise APIError(f"Error during data cleaning: {str(e)}")


if __name__ == "__main__":
    try:
        from load_data import load_data

        data = load_data()
        cleaned_data = clean_data(data)
        print(cleaned_data)

    except Exception as e:
        print_step(f"Error: {str(e)}")
