# This script gets an input of data in JSON format, uses DeepSeek AI to transform it
# and returns a JSON object with the transformed data

from dotenv import load_dotenv
import os
import json
import requests
import time
from datetime import datetime
from typing import Union, Dict, Any, Optional
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
    temperature: float = 1.0
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
        total=5,
        backoff_factor=2,
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


def prepare_payload(
    data: Union[str, Dict[str, Any]], config: APIConfig
) -> Dict[str, Any]:
    """Prepare the API request payload."""
    if isinstance(data, str):
        try:
            data = json.loads(data)
        except json.JSONDecodeError as e:
            raise ResponseError(f"Invalid JSON data: {str(e)}")

    return {
        "model": config.model,
        "messages": [
            {
                "role": "system",
                "content": "You are a helpful assistant that processes and transforms data. You must always provide complete responses without truncation.",
            },
            {
                "role": "user",
                "content": f"""
                Please process the following data according to these instructions:

                1. Clean all HTML tags from the content and any other fields
                2. Group articles by similarity (articles talking about the same fact/event)
                3. Dont include groups with less than 3 articles.
                4. For each group:
                   - Create a summary of the articles' titles
                   - Include the full articles in the group
                   - Calculate coverage percentage across 5 political stances:
                     * Left
                     * Center-left
                     * Center
                     * Center-right
                     * Right
                   - Identify up to 5 important pieces of information that some articles omit
                     (to highlight potential media bias). All this should be written in spanish

                The response must be ONLY a JSON list with this exact structure.
                Dont include any other text, dont use identation, all in one line, dont use ```json, just the json object:
                [
                {{
                    "Fact": "Summary of articles titles",
                    "Articles": [article objects in json format (only first 100 characters of the content)],
                    "Coverage": [
                        {{"left": percentage}},
                        {{"center-left": percentage}},
                        {{"center": percentage}},
                        {{"center-right": percentage}},
                        {{"right": percentage}}
                    ],
                    "RelevantData": [
                        {{
                            "media": "source name",
                            "tag": "omitted",
                            "data": "specific information"
                        }}
                    ]
                }}
                ]

                IMPORTANT: 
                - Ensure the response is complete and not truncated
                - If the response is too long, prioritize the most important information
                - Keep article content concise but maintain key information
                - Focus on the most significant political stance differences
                - Highlight only the most relevant omitted information

                Data to process:
                {json.dumps(data, ensure_ascii=False)}
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


def process_data(data: Union[str, Dict[str, Any]]) -> str:
    """Process data using DeepSeek AI API."""
    start_time = time.time()
    print_step("Starting data processing")

    try:
        config = get_api_config()
        payload = prepare_payload(data, config)
        headers = {
            "Authorization": f"Bearer {config.key}",
            "Content-Type": "application/json",
        }

        with create_session_with_retries() as session:
            print_step("Sending request to DeepSeek AI...")
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
            result = json.loads(full_response)

            print_step("Successfully received response from DeepSeek AI", start_time)
            return json.dumps(result, ensure_ascii=False)

    except ReadTimeout:
        raise TimeoutError(
            "Read timeout occurred while waiting for DeepSeek AI response"
        )
    except ConnectTimeout:
        raise TimeoutError(
            "Connection timeout occurred while connecting to DeepSeek AI"
        )
    except ConnectionError as e:
        raise ConnectionError(f"Connection error occurred: {str(e)}")
    except json.JSONDecodeError as e:
        raise ResponseError(f"Invalid response from DeepSeek AI: {str(e)}")
    except Exception as e:
        raise APIError(f"Unexpected error occurred: {str(e)}")


if __name__ == "__main__":
    try:
        from load_data import load_data

        data = load_data()
        processed_data = process_data(data)
        print(processed_data)

    except Exception as e:
        print_step(f"Error: {str(e)}")
