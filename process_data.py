# This script gets an input of data in JSON format, uses DeepSeek AI to transform it
# and returns a JSON object with the transformed data

from dotenv import load_dotenv
import os
import json
import requests
import time
from datetime import datetime
from typing import Union, Dict, Any
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from requests.exceptions import ReadTimeout, ConnectTimeout, ConnectionError

load_dotenv()


def get_timestamp():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def print_step(message: str, start_time: Union[float, None] = None):
    timestamp = get_timestamp()
    if start_time:
        elapsed = time.time() - start_time
        print(f"[{timestamp}] {message} (Elapsed: {elapsed:.2f}s)")
    else:
        print(f"[{timestamp}] {message}")


def create_session_with_retries():
    session = requests.Session()
    retry_strategy = Retry(
        total=5,  # increased number of retries
        backoff_factor=2,  # increased wait time between retries
        status_forcelist=[500, 502, 503, 504],  # HTTP status codes to retry on
        allowed_methods=["POST"],  # only retry on POST requests
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


def process_data(data: Union[str, Dict[str, Any]]) -> str:
    start_time = time.time()
    print_step("Starting data processing")

    # DeepSeek API endpoint
    api_url = os.getenv("DEEPSEEK_API_URL")
    api_key = os.getenv("DEEPSEEK_API_KEY")

    if not all([api_url, api_key]):
        raise ValueError("Missing required environment variables")

    # Ensure api_url is a string
    api_url = str(api_url)

    # Convert string data to JSON if needed
    if isinstance(data, str):
        try:
            data = json.loads(data)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON data: {str(e)}")

    # Prepare the request payload
    payload = {
        "model": "deepseek-chat",
        "messages": [
            {
                "role": "system",
                "content": "You are a helpful assistant that processes and transforms data.",
            },
            {
                "role": "user",
                "content": f"""
                Please process the following data according to these instructions:

                1. Clean all HTML tags from the content and any other fields
                2. Group articles by similarity (articles talking about the same fact/event)
                3. For each group:
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
                    "Articles": [article objects],
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

                Data to process:
                {json.dumps(data, ensure_ascii=False)}
                """,
            },
        ],
        "temperature": 0.5,
    }

    # Make the API request with retries
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    session = create_session_with_retries()

    try:
        print_step("Sending request to DeepSeek AI...")

        # First, send the request and get the response
        response = session.post(
            api_url,
            json=payload,
            headers=headers,
            timeout=(
                10,
                300,
            ),  # (connect timeout, read timeout) - increased significantly
            stream=True,  # Enable streaming
        )
        response.raise_for_status()

        # Read the response content in chunks
        content = []
        for chunk in response.iter_content(chunk_size=8192):
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
        raise ValueError(f"Invalid response from DeepSeek AI: {str(e)}")
    except Exception as e:
        raise Exception(f"Unexpected error occurred: {str(e)}")
    finally:
        session.close()


if __name__ == "__main__":
    try:
        from load_data import load_data

        data = load_data()
        processed_data = process_data(data)
        print(processed_data)

    except Exception as e:
        print_step(f"Error: {str(e)}")
