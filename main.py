# This script loads data from MongoDB, cleans it, groups it by similarity,
# and saves the processed data to output.json.

from load_data import load_data
from data_cleaner import clean_data
from data_grouper import group_data
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

    try:
        # Load data
        print_step("Loading data from MongoDB...")
        raw_data = load_data()
        raw_articles = json.loads(raw_data)
        print_step(f"Loaded {len(raw_articles)} articles")

        # Clean data
        print_step("Cleaning articles...")
        cleaned_data = clean_data(raw_data)
        cleaned_articles = json.loads(cleaned_data)
        print_step(f"Cleaned {len(cleaned_articles)} articles")

        # Group data
        print_step("Grouping articles by similarity...")
        grouped_data = group_data(cleaned_data)
        grouped_articles = json.loads(grouped_data)

        # Count articles in clusters and single news
        clustered_count = sum(
            len(cluster["articles"]) for cluster in grouped_articles["clustered_news"]
        )
        single_count = len(grouped_articles["single_news"])
        print_step(
            f"Grouped {clustered_count} articles into {len(grouped_articles['clustered_news'])} clusters"
        )
        print_step(f"Found {single_count} ungrouped articles")

        # Save processed data
        output_file = "output.json"
        print_step(f"Saving processed data to {output_file}...")
        with open(output_file, "w", encoding="utf-8") as f:
            f.write(grouped_data)

        total_time = time.time() - start_time
        print_step(f"Pipeline completed successfully! Total time: {total_time:.2f}s")

    except Exception as e:
        print_step(f"Error in pipeline: {str(e)}")
        raise
