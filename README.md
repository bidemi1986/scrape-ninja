
# How to Ask Questions to Any Website by Building a RAG App with LangChain

This project demonstrates how to create a Retrieval-Augmented Generation (RAG) application using LangChain. The example provided scrapes the crewAI documentation website and allows users to ask questions about the content, returning structured and relevant answers.

## Installation

### Prerequisites

- Python 3.x
- API Key for [OpenAI API](https://platform.openai.com/account/api-keys)

### Setup

1. Clone the repository:
```bash
git clone https://github.com/paquino11/rag-ask-website
cd rag-ask-website
```
2. Create a virtual environment:
```bash
python -m venv venv
source venv/bin/activate
```
3. Install the required packages:
```bash
pip install -r requirements.txt
```
4. Set up the environment variables:
- Create a `.env` file in the root directory and add the following:
```bash
OPENAI_API_KEY=your_openai_api_key
```

## Usage

### Step 1: Scrape the Website
1. Define the website URL you want to scrape in the `main.py` file.
2. Run the script to scrape the website:
```bash
python main.py
```

### Step 2: Ask Questions
1. Modify the `question` variable in the `main.py` file to specify the question you want to ask.
2. Run the RAG pipeline to get an answer:
```bash
python main.py
```

3. The script will output the question and the corresponding answer generated by the RAG app.

## Example Questions
- "What's an Agent?"
- "How can I start a new crewAI project?"
- "What are the key features of using a sequential process?"

