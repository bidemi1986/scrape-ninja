# main.py   
from scrape_website import scrape_all, BatchConfig
from rag import setup_and_run_rag_pipeline
from urllib.parse import urlparse
import time
import asyncio
import logging
from twisted.internet import reactor, defer
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass
from firebaseClient import (
    check_firestore_for_answer, 
    store_answer_in_firestore, 
    fetch_data_from_firebase,
    check_scraped_data_exists  # New function to check if we have data
)
from sklearn.metrics.pairwise import cosine_similarity
from langchain.embeddings import OpenAIEmbeddings
from langchain.vectorstores import FAISS
import numpy as np
import signal

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

RED = '\033[91m'
GREEN = '\033[92m'
YELLOW = '\033[93m'
RESET = '\033[0m'

@dataclass
class ProcessingConfig:
    max_workers: int = 4
    batch_timeout: int = 300
    min_confidence_threshold: float = 0.6
    rescrape_threshold_days: int = 30  # Rescrape if data is older than this

def handle_keyboard_interrupt(signal, frame):
    logging.info("\nGracefully shutting down...")
    if reactor.running:
        reactor.stop()
    

signal.signal(signal.SIGINT, handle_keyboard_interrupt)

async def process_website_data_async(hostname: str, question: str) -> Optional[Dict]:
    try:
        data_generator = fetch_data_from_firebase(hostname)
        
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(
            None,
            setup_and_run_rag_pipeline,
            question,
            data_generator
        )
        
        if result and result.get('answer'):
            logging.info(f"Successfully processed data for {hostname}")
            return result
        
        return None
    except Exception as e:
        logging.error(f"Error processing data for {hostname}: {e}")
        return None

async def check_existing_data(urls: List[str]) -> Tuple[List[str], List[str]]:
    """
    Check which URLs need scraping and which have recent data
    Returns: (urls_to_scrape, urls_with_data)
    """
    urls_to_scrape = []
    urls_with_data = []
    
    for url in urls:
        hostname = urlparse(url).netloc
        has_recent_data = await asyncio.get_event_loop().run_in_executor(
            None,
            check_scraped_data_exists,
            hostname,
            ProcessingConfig.rescrape_threshold_days
        )
        
        if has_recent_data:
            logging.info(f"{GREEN}Found existing data for {hostname}{RESET}")
            urls_with_data.append(url)
        else:
            logging.info(f"{YELLOW}Need to scrape {hostname}{RESET}")
            urls_to_scrape.append(url)
    
    return urls_to_scrape, urls_with_data


async def process_batch_results(urls: List[str], question: str) -> Dict:
    tasks = []
    for url in urls:
        hostname = urlparse(url).netloc
        tasks.append(process_website_data_async(hostname, question))
    
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    # Step 1: Collect answers and embeddings
    answers = []
    sources = set()
    confidence_scores = []
    embeddings = []
    embedding_model = OpenAIEmbeddings()  # Ensure you have your embedding model

    for result in results:
        if isinstance(result, dict) and result.get('answer'):
            answers.append(result['answer'])
            sources.update(result.get('sources', []))
            confidence_scores.append(result.get('confidence', 0))
            
            # Get embeddings for semantic similarity
            embedding = embedding_model.embed_query(result['answer'])
            embeddings.append(embedding)

    if not answers:
        return {'answer': "No valid answers found.", 'sources': [], 'confidence': 0.0}

    # Step 2: Normalize and deduplicate answers
    normalized_answers = [answer.lower().strip() for answer in answers]
    unique_answers = list(set(normalized_answers))

    # Step 3: Cluster answers by semantic similarity
    if len(embeddings) > 1:
        similarity_matrix = cosine_similarity(embeddings)
        clusters = cluster_answers_by_similarity(similarity_matrix, threshold=0.85)
    else:
        clusters = {0: [0]}  # Single answer case

    # Step 4: Select the best cluster
    best_cluster_idx = max(clusters, key=lambda c: len(clusters[c]))
    best_cluster_answers = [answers[i] for i in clusters[best_cluster_idx]]
    best_answer = max(set(best_cluster_answers), key=best_cluster_answers.count)

    # Step 5: Aggregate confidence scores
    avg_confidence = np.mean([confidence_scores[i] for i in clusters[best_cluster_idx]])

    return {
        'answer': best_answer,
        'sources': list(sources),
        'confidence': avg_confidence
    }

def cluster_answers_by_similarity(similarity_matrix, threshold=0.85):
    """
    Clusters answers based on cosine similarity.
    """
    clusters = {}
    for i, row in enumerate(similarity_matrix):
        for j, sim_score in enumerate(row):
            if sim_score >= threshold:
                if i not in clusters:
                    clusters[i] = []
                clusters[i].append(j)
    return clusters

async def run_scraper(urls: List[str]):
    """Run the scraper in the reactor thread with timeout"""
    if not urls:  # Skip if no URLs to scrape
        return
        
    def _run_scraper():
        d = scrape_all(urls)
        # Add timeout
        timeout_dc = reactor.callLater(300, d.cancel)  # 5 minute timeout
        d.addBoth(lambda _: timeout_dc.cancel() if not timeout_dc.called else None)
        return d

    loop = asyncio.get_event_loop()
    try:
        await loop.run_in_executor(None, reactor.callFromThread, _run_scraper)
        
        # Wait for reactor with timeout
        start_time = time.time()
        while reactor.running and time.time() - start_time < 300:  # 5 minute timeout
            await asyncio.sleep(0.1)
            
    except Exception as e:
        logging.error(f"Error during scraping: {e}")
    finally:
        if reactor.running:
            reactor.stop()

async def scrape_and_process_async():
    urls = [
        "https://barbieriwines.com/",
        "https://www.grassinifamilyvineyards.com/",
        "https://longoriawines.com/",
        "https://www.corksandcrowns.com/"
        "https://www.margerumwines.com/"
    ]
    
    question = "can you suggest some upcoming wine events?"
    start_time = time.time()
    
    try:
        # Check cache first
        cached_answer = check_firestore_for_answer(question)
        if cached_answer:
            logging.info(f"{GREEN}Found cached answer{RESET}")
            return cached_answer
        
        # Check which URLs need scraping vs have existing data
        urls_to_scrape, urls_with_data = await check_existing_data(urls)
        
        # Process existing data first
        if urls_with_data:
            logging.info(f"{GREEN}Processing existing data...{RESET}")
            initial_result = await process_batch_results(urls_with_data, question)
            
            # If we got a good answer, we might not need to scrape
            if (initial_result and 
                initial_result.get('answer') and 
                initial_result.get('confidence', 0) >= ProcessingConfig.min_confidence_threshold):
                logging.info(f"{GREEN}Got satisfactory answer from existing data{RESET}")
                return initial_result
        
        # If we need to scrape, do it now
        if urls_to_scrape:
            BatchConfig.batch_size = 2
            BatchConfig.max_concurrent_requests = 16
            
            logging.info(f"{GREEN}Starting batch scraping for {len(urls_to_scrape)} URLs...{RESET}")
            await run_scraper(urls_to_scrape)
            logging.info("Scraping completed")
        
        # Process all data (both new and existing)
        logging.info("Processing all scraped data...")
        final_result = await process_batch_results(urls, question)
        
        if final_result and final_result.get('answer'):
            store_answer_in_firestore(
                question=question,
                answer=final_result['answer'],
                sources=final_result['sources'],
                confidence=final_result['confidence']
            )
            
            logging.info(f"{GREEN}Final Answer:{RESET} {final_result['answer']}")
            logging.info(f"Sources: {final_result['sources']}")
            logging.info(f"Confidence: {final_result['confidence']:.2f}")
        else:
            logging.warning(f"{RED}No valid answers found{RESET}")
        
        processing_time = time.time() - start_time
        logging.info(f"Total time taken: {processing_time:.2f} seconds")
        
        return final_result
        
    except Exception as e:
        logging.error(f"{RED}Error during processing: {e}{RESET}")
        return None

def main():
    try:
        loop = asyncio.get_event_loop()
        result = loop.run_until_complete(scrape_and_process_async())
        
        if result:
            print(f"Answer: {result['answer']}")
            print(f"Sources: {result['sources']}")
            return result
        
    except KeyboardInterrupt:
        logging.info("Process interrupted by user")
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
    finally:
        if reactor.running:
            reactor.stop()
        loop.close()


if __name__ == "__main__":
    main()

# def main():
#     # Step 1: Run the web crawler to scrape the website
#     # url = 'https://docs.crewai.com'
#     url = 'https://barbieriwines.com/'
#     host = urlparse(url).hostname 
#     output_path = f'{host}/output.json'
#     scrape(start_url=url, output_file=output_path)
    
#     # Step 2: Process the scraped data using the RAG pipeline
#     question = "tell me about The Solomon Hills Vineyard"
#     answer = setup_and_run_rag_pipeline(
#         question=question,
#         file_path=output_path
#     )
    
#     print(f"{RED}Question: {question}{RESET}")
#     print(f"{GREEN}Answer: {answer}{RESET}")

# if __name__ == "__main__":
#     main()