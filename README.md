# Sesgocero Engine

A Python-based data loading engine that interfaces with MongoDB to retrieve and process data.

## Prerequisites

- Python 3.x
- MongoDB instance
- Required Python packages:
  - pymongo
  - python-dotenv

## Setup

1. Clone the repository
2. Install the required packages:
   ```bash
   pip install pymongo python-dotenv
   ```
3. Create a `.env` file in the project root with the following variables:
   ```
   MONGODB_URI=your_mongodb_connection_string
   MONGODB_DATABASE=your_database_name
   MONGODB_COLLECTION=your_collection_name
   ```

## Usage

The main functionality is provided by the `load_data()` function in `load_data.py`. This function:
- Connects to MongoDB using environment variables
- Retrieves data from the specified collection
- Returns the data as a list of dictionaries

To run the script directly:
```bash
python load_data.py
```

To import and use in other Python files:
```python
from load_data import load_data

data = load_data()
```

## Error Handling

The script includes error handling for:
- Missing environment variables
- MongoDB connection issues
- Data retrieval problems

## Project Structure

- `load_data.py`: Main script for loading data from MongoDB
- `.env`: Configuration file for environment variables (not included in repository) 