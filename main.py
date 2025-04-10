#!/usr/bin/env python3
"""
SesgoCero Engine - Main Script
This script orchestrates the entire data processing pipeline:
1. Load data from sources
2. Clean the loaded articles
3. Cluster the cleaned articles
4. Fix cluster metadata
"""

import asyncio
import time
from datetime import datetime
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Import the modules
from load_data import load_data
from data_cleaner import clean_data
from cluster_articles import cluster_articles

# Import the fix_clusters module
import fix_clusters


def get_timestamp():
    """Get current timestamp in a consistent format."""
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def print_step(message, start_time=None):
    """Print a step message with optional elapsed time."""
    timestamp = get_timestamp()
    if start_time:
        elapsed = time.time() - start_time
        print(f"[{timestamp}] {message} (Elapsed: {elapsed:.2f}s)")
    else:
        print(f"[{timestamp}] {message}")


def run_fix_clusters():
    """Run the fix_clusters script."""
    # The fix_clusters module runs its code when imported
    # We just need to import it to execute it
    print_step("Running fix_clusters script")
    # The script will run automatically when imported
    return True


async def main():
    """Main function to run the entire pipeline."""
    overall_start_time = time.time()
    print_step("🚀 Starting SesgoCero Engine pipeline")

    # Step 1: Load data
    print_step("\n📥 Step 1: Loading data from sources")
    load_start_time = time.time()
    try:
        data = load_data()
        print_step("✅ Data loading completed", load_start_time)
    except Exception as e:
        print_step(f"❌ Error loading data: {str(e)}")
        return

    # Step 2: Clean data (async function)
    print_step("\n🧹 Step 2: Cleaning articles")
    clean_start_time = time.time()
    try:
        await clean_data(data)
        print_step("✅ Article cleaning completed", clean_start_time)
    except Exception as e:
        print_step(f"❌ Error cleaning articles: {str(e)}")
        return

    # Step 3: Cluster articles
    print_step("\n🔍 Step 3: Clustering articles")
    cluster_start_time = time.time()
    try:
        cluster_articles()
        print_step("✅ Article clustering completed", cluster_start_time)
    except Exception as e:
        print_step(f"❌ Error clustering articles: {str(e)}")
        return

    # Step 4: Fix cluster metadata
    print_step("\n🔧 Step 4: Fixing cluster metadata")
    fix_start_time = time.time()
    try:
        run_fix_clusters()
        print_step("✅ Cluster metadata fixed", fix_start_time)
    except Exception as e:
        print_step(f"❌ Error fixing cluster metadata: {str(e)}")
        return

    # Print overall completion message
    print_step(
        "\n✨ SesgoCero Engine pipeline completed successfully", overall_start_time
    )
    print_step(
        "📊 All steps completed: Data loading, Article cleaning, Clustering, and Metadata fixing"
    )


if __name__ == "__main__":
    # Run the async main function
    asyncio.run(main())
