# SesgoCero Engine

A Python-based engine for analyzing news articles to identify media bias and political stance through content analysis and grouping.

## Overview

SesgoCero Engine processes news articles from MongoDB, cleans their content, and groups them by similarity to identify patterns in media coverage. The engine uses DeepSeek AI to analyze and process the articles, focusing on identifying similar news events across different sources.

## Features

- **MongoDB Integration**: Fetches news articles from a MongoDB database
- **Content Cleaning**: Removes HTML tags and formats content for analysis
- **AI-Powered Analysis**: Uses DeepSeek AI for content processing and grouping
- **Similarity Grouping**: Groups articles based on similar facts and events
- **Error Handling**: Robust error handling and retry mechanisms
- **Progress Tracking**: Detailed progress logging with timestamps

## Project Structure

```
sesgocero-engine/
├── load_data.py      # MongoDB data loading
├── data_cleaner.py   # Article content cleaning
├── data_grouper.py   # Article grouping by similarity
├── main.py          # Main execution pipeline
├── output.json      # Output file with grouped articles
└── .env             # Environment variables
```

## Prerequisites

- Python 3.8+
- MongoDB database
- DeepSeek AI API access
- Required Python packages (see requirements.txt)

## Installation

1. Clone the repository:
```bash
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

4. Create a `.env` file with the following variables:
```
MONGODB_URI=your_mongodb_connection_string
DEEPSEEK_API_URL=your_deepseek_api_url
DEEPSEEK_API_KEY=your_deepseek_api_key
```

## Usage

Run the main script to process articles:
```bash
python main.py
```

The script will:
1. Load articles from MongoDB
2. Clean the article content
3. Group articles by similarity
4. Save results to `output.json`

## Output Format

The output is saved in `output.json` with the following structure:

```json
{
    "clustered_news": [
        {
            "cluster_name": "Summary of the group's topic",
            "articles": [
                {
                    "_id": "Article ID",
                    "id": "Article ID",
                    "title": "Article title",
                    "subtitle": "Article subtitle",
                    "source": "Source name",
                    "date": "Publication date",
                    "url": "Article URL"
                }
            ]
        }
    ],
    "single_news": [
        {
            "_id": "Article ID",
            "id": "Article ID",
            "title": "Article title",
            "subtitle": "Article subtitle",
            "source": "Source name",
            "date": "Publication date",
            "url": "Article URL"
        }
    ]
}
```

## Error Handling

The engine includes comprehensive error handling:
- MongoDB connection errors
- API request timeouts
- JSON parsing errors
- Missing environment variables
- Invalid data formats

Each error is logged with timestamps and appropriate error messages.

## Performance

- Streaming response handling for large datasets
- Chunked processing to manage memory usage
- Retry mechanisms for API requests
- Efficient article grouping based on titles and subtitles

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgments

- DeepSeek AI for providing the API
- MongoDB for data storage
- Contributors and maintainers 