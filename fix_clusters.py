import pymongo
from pymongo import MongoClient
from dotenv import load_dotenv
import os
import time

# Load environment variables
load_dotenv()

# Connect to MongoDB
client = MongoClient(os.getenv("MONGODB_URI"))
db = client["sesgocero"]
clusters = db["clusters"]
articles = db["clean_articles"]

# Count total clusters
total_clusters = clusters.count_documents({})

print(f"🔌 Connected to MongoDB: {client}")
print(f"📊 Found {total_clusters} clusters to process")

# Add articles_count using aggregation pipeline
print("\n🔄 Updating articles count for all clusters...")
clusters_set = clusters.update_many(
    {}, [{"$set": {"articles_count": {"$size": "$articles"}}}]
)
print(f"✅ Articles count set for {clusters_set.modified_count} clusters")

# Initialize coverage field for clusters that don't have it
print("\n🔄 Initializing coverage field for clusters without it...")
clusters_set = clusters.update_many(
    {"coverage": {"$exists": False}},
    {
        "$set": {
            "coverage": {
                "left": 0,
                "center-left": 0,
                "center": 0,
                "center-right": 0,
                "right": 0,
            }
        }
    },
)
print(f"✅ Coverage field initialized for {clusters_set.modified_count} clusters")


def get_coverage(article_ids):
    """Calculate coverage statistics for a list of article IDs."""
    coverage = {"left": 0, "center-left": 0, "center": 0, "center-right": 0, "right": 0}

    for article_id in article_ids:
        article = articles.find_one({"_id": article_id})
        if article and "political_orientation" in article:
            stance = article["political_orientation"]
            if stance in coverage:
                coverage[stance] += 1
                print(f"\t\t✅ Found political stance: {stance}")
            else:
                print(f"\t\t⚠️ Unknown political stance: {stance}")
        else:
            print(f"\t\t❌ No political orientation found for article {article_id}")

    return coverage


def sum_coverage(coverage):
    """Calculate the sum of all coverage values."""
    return sum(coverage.values())


def should_skip_cluster(cluster):
    """Determine if a cluster should be skipped based on its current state."""
    # Check if cluster has articles
    if not cluster.get("articles"):
        return True, "No articles in cluster"

    # Check if cluster has coverage field
    if "coverage" not in cluster:
        return False, "No coverage field"

    # Check if articles_count matches the sum of coverage values
    articles_count = len(cluster["articles"])
    coverage_sum = sum_coverage(cluster["coverage"])

    if articles_count == coverage_sum:
        return (
            True,
            f"Coverage already matches articles count ({coverage_sum}/{articles_count})",
        )

    return False, "Coverage needs updating"


# Process clusters in batches for better performance
batch_size = 50
processed = 0
skipped = 0
start_time = time.time()

print("\n🔄 Processing clusters and computing coverage...")
for i, cluster in enumerate(clusters.find()):
    processed += 1
    cluster_name = cluster.get("name", "Unnamed")

    print(f"\n📦 Processing cluster {processed}/{total_clusters}: {cluster_name}")

    # Check if cluster should be skipped
    skip, reason = should_skip_cluster(cluster)
    if skip:
        print(f"\t⏩ Skipping cluster - {reason}")
        skipped += 1
        continue

    articles_count = len(cluster["articles"])
    print(f"\t📄 Found {articles_count} articles in cluster")

    coverage = get_coverage(cluster["articles"])

    # Print coverage summary
    print("\t📊 Coverage summary:")
    for stance, count in coverage.items():
        if count > 0:
            print(f"\t\t{stance}: {count} articles")

    # Update cluster with coverage data
    clusters.update_one({"_id": cluster["_id"]}, {"$set": {"coverage": coverage}})
    print(f"\t✅ Updated coverage for cluster {cluster_name}")

    # Print progress every 10 clusters
    if processed % 10 == 0:
        elapsed = time.time() - start_time
        print(
            f"\n⏱️ Progress: {processed}/{total_clusters} clusters processed (Skipped: {skipped}, Elapsed: {elapsed:.2f}s)"
        )

# Print final summary
elapsed = time.time() - start_time
print(f"\n✨ Finished processing all {processed} clusters in {elapsed:.2f} seconds")
print(
    f"📊 Summary: {processed} total clusters, {skipped} skipped, {processed-skipped} updated"
)
print(f"🔌 Closing MongoDB connection...")
client.close()
print("✅ MongoDB connection closed")
