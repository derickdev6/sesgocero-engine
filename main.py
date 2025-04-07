# This script loads data from a MongoDB collection, processes it using DeepSeek AI,
# and saves the processed data to a new JSON file.

from load_data import load_data
from process_data import process_data
import time
from datetime import datetime
import os
import json


def get_timestamp():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def print_step(message, start_time=None):
    timestamp = get_timestamp()
    if start_time:
        elapsed = time.time() - start_time
        print(f"[{timestamp}] {message} (Elapsed: {elapsed:.2f}s)")
    else:
        print(f"[{timestamp}] {message}")


if __name__ == "__main__":
    start_time = time.time()
    print_step("Starting data processing pipeline")

    # Load data
    print_step("Loading data from MongoDB...")
    data = load_data()
    print_step(f"Loaded {len(json.loads(data))} articles")

    # Process data
    print_step("Processing data with DeepSeek AI...")
    processed_data = process_data(data)

    # processed_data must be only the model response, no other text
    processed_data = json.loads(processed_data)
    processed_data = processed_data["choices"][0]["message"]["content"]
    processed_data = processed_data.replace("```json", "").replace("```", "")

    # Save processed data
    output_file = "processed_data.json"
    print_step(f"Saving processed data to {output_file}...")

    with open(output_file, "w", encoding="utf-8") as f:
        f.write(processed_data)

    total_time = time.time() - start_time
    print_step(f"Pipeline completed successfully! Total time: {total_time:.2f}s")
