# SesgoCero Engine

A Python-based engine for analyzing news articles to identify media bias and political stance through content analysis and grouping.

## Overview

SesgoCero Engine processes news articles from MongoDB, cleans their content, and groups them by similarity to identify patterns in media coverage. The engine uses DeepSeek AI to analyze and process the articles, focusing on identifying similar news events across different sources.

1. **load_data.py**: Loads article data from various sources into MongoDB
2. **data_cleaner.py**: Cleans articles using DeepSeek AI and saves them to a separate collection
3. **cluster_articles.py**: Clusters cleaned articles based on content similarity using DeepSeek AI

## Requirements

- Python 3.8+
- MongoDB
- DeepSeek API access

## Installation

1. Clone the repository:
   ```
   git clone https://github.com/yourusername/sesgocero-engine.git
   cd sesgocero-engine
   ```

2. Create and activate a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Create a `.env` file with the following variables:
   ```
   MONGODB_URI=mongodb://localhost:27017
   MONGODB_DB=your_database_name
   MONGODB_COL=your_articles_collection
   DEEPSEEK_API_URL=https://api.deepseek.com/v1/chat/completions
   DEEPSEEK_API_KEY=your_api_key
   ```

## Usage

### Loading Data

To load article data into MongoDB:

```
python load_data.py
```

This will load articles from the configured sources into the specified MongoDB collection.

### Cleaning Articles

To clean articles using DeepSeek AI:

```
python data_cleaner.py
```

This script:
- Loads articles from the original collection
- Cleans each article using DeepSeek AI
- Saves cleaned articles to the `clean_articles` collection
- Updates the original articles with a `cleaned` flag

### Clustering Articles

To cluster cleaned articles:

```
python cluster_articles.py
```

This script:
- Loads articles from the `clean_articles` collection
- Loads existing cluster names from the `clusters` collection
- Uses DeepSeek AI to determine which cluster each article belongs to
- Creates new clusters or adds articles to existing clusters
- Updates articles with their cluster IDs

## MongoDB Collections

- **Original Articles Collection**: Contains the raw article data
- **clean_articles**: Contains cleaned articles with a `cleaned` flag
- **clusters**: Contains article clusters with the following structure:
  ```json
  {
    "name": "Cluster name in Spanish",
    "articles": ["Article ID 1", "Article ID 2", ...],
    "created_at": "2023-01-01T00:00:00Z",
    "updated_at": "2023-01-01T00:00:00Z"
  }
  ```

## Error Handling

All scripts include comprehensive error handling and logging:
- Detailed error messages with timestamps
- Graceful handling of API errors
- Tracking of processed, failed, and skipped articles

## License

[MIT License](LICENSE) 