# firebaseClient.py
from firebase_admin import firestore, credentials, initialize_app
import logging
from typing import List, Dict, Any, Generator
import json
from datetime import datetime
from scrape_website import storage 
from datetime import datetime, timezone

scrape_folder = 'scrapes'
 
firebase_initialized = False
if not firebase_initialized:
    cred = credentials.Certificate("sample-firebase-ai-app-3e813-firebase-adminsdk-l2l4r-da0a22960f.json")
    initialize_app(cred, {
        'storageBucket': 'sample-firebase-ai-app-3e813.firebasestorage.app'
    })
    firebase_initialized = True
db = firestore.client()



def check_firestore_for_answer(question):
    """
    Check Firestore for an existing answer to the given question.

    :param question: The question to query in Firestore.
    :return: The answer document (if found), else None.
    """
    try:
        docs = db.collection("answers").where("question", "==", question).stream()
        for doc in docs:
            return doc.to_dict()  # Return the first matching document
        return None
    except Exception as e:
        logging.error(f"Error querying Firestore: {e}")
        return None



def check_firestore_for_answer(question):
    """
    Check Firestore for an existing answer to the given question.

    :param question: The question to query in Firestore.
    :return: The answer document (if found), else None.
    """
    try:
        docs = db.collection("answers").where("question", "==", question).stream()
        for doc in docs:
            return doc.to_dict()  # Return the first matching document
        return None
    except Exception as e:
        logging.error(f"Error querying Firestore: {e}")
        return None

 

def fetch_data_from_firebase(hostname: str) -> Generator[Dict[str, Any], None, None]:
    """
    Stream data from Firebase Storage for a given hostname.
    Returns a generator to minimize memory usage.
    """
    try:
        bucket = storage.bucket()
        blobs = bucket.list_blobs(prefix=f"scrapes/{hostname}/chunk_")
        
        for blob in blobs:
            try:
                # Download and parse each chunk
                content = blob.download_as_string()
                chunk_data = json.loads(content)
                
                # Yield each document in the chunk
                for doc in chunk_data:
                    yield doc
                    
            except Exception as e:
                logging.error(f"Error processing chunk {blob.name}: {e}")
                continue
                
    except Exception as e:
        logging.error(f"Error fetching data from Firebase: {e}")
        yield None
 

def store_answer_in_firestore(question, answer, sources, confidence=0.0):
    """
    Store an answer in Firestore with relevant metadata.

    :param question: The question for which the answer was generated.
    :param answer: The generated answer.
    :param sources: List of sources used to generate the answer.
    :param confidence: Initial confidence score for the answer.
    """
    try:
        db.collection("answers").add({
            "question": question,
            "answer": answer,
            "sources": sources,
            "timestamp": firestore.SERVER_TIMESTAMP,
            "metadata": {
                "confidence": confidence,
                "feedback_count": 0,
                "feedback_sum": 0,
                "average_feedback": 0.0,
            },
        })
        logging.info(f"Stored answer for question: {question}")
    except Exception as e:
        logging.error(f"Error storing answer in Firestore: {e}")


def check_scraped_data_exists(hostname: str, max_age_days: int = 30) -> bool:
    """
    Check if we have relatively recent data for a given hostname
    
    Args:
        hostname: The hostname to check
        max_age_days: Maximum age of data in days before considering it stale
        
    Returns:
        bool: True if recent data exists, False otherwise
    """
    try:
        bucket = storage.bucket()
        blobs = bucket.list_blobs(prefix=f"scrapes/{hostname}/")
        
        # Check if we have any blobs and their age
        for blob in blobs:
            if blob.time_created:
                age_days = (datetime.now(timezone.utc) - blob.time_created).days
                if age_days <= max_age_days:
                    return True
        return False
    except Exception as e:
        logging.error(f"Error checking scraped data: {e}")
        return False


def update_feedback_in_firestore(question, feedback_rating):
    """
    Update feedback metrics for an answer in Firestore.

    :param question: The question for which feedback is provided.
    :param feedback_rating: The feedback rating (e.g., 1 to 5).
    """
    try:
        # Query the document for the given question
        docs = db.collection("answers").where("question", "==", question).stream()
        for doc in docs:
            doc_ref = db.collection("answers").document(doc.id)
            data = doc.to_dict()

            # Update feedback metrics
            feedback_count = data.get("metadata", {}).get("feedback_count", 0)
            feedback_sum = data.get("metadata", {}).get("feedback_sum", 0)

            feedback_count += 1
            feedback_sum += feedback_rating

            # Calculate new average and variance
            average_feedback = feedback_sum / feedback_count
            # Variance requires storing all ratings; calculate as needed
            # For simplicity, variance can be excluded for now
            updated_metadata = {
                "feedback_count": feedback_count,
                "feedback_sum": feedback_sum,
                "average_feedback": average_feedback,
            }

            # Update document
            doc_ref.update({"metadata": updated_metadata})
            logging.info(f"Updated feedback for question '{question}': {updated_metadata}")
            return

        logging.warning(f"No answer found for question '{question}' to update feedback.")
    except Exception as e:
        logging.error(f"Error updating feedback in Firestore: {e}")




def fetch_and_calculate_confidence(question):
    """
    Fetch answer from Firestore and calculate confidence score.

    :param question: The question to query.
    :return: Tuple of (answer, confidence_score).
    """
    try:
        docs = db.collection("answers").where("question", "==", question).stream()
        for doc in docs:
            data = doc.to_dict()
            metadata = data.get("metadata", {})
            feedback_count = metadata.get("feedback_count", 0)
            feedback_sum = metadata.get("feedback_sum", 0)

            if feedback_count > 0:
                average_feedback = feedback_sum / feedback_count
                # Variance can be factored into confidence if tracked
                confidence_score = average_feedback  # Simple confidence
                return data["answer"], confidence_score

        logging.info(f"No cached answer found for question '{question}'.")
        return None, 0.0
    except Exception as e:
        logging.error(f"Error fetching data from Firestore: {e}")
        return None, 0.0



# Firestore Answers Model
# Firestore Collection: answers
# Document ID: A unique hash or combination of question and domain(s)
# Fields:
#   - question (string): The input question.
#   - answer (string): The generated answer.
#   - sources (array of strings): List of URLs used for the answer.
#   - timestamp (timestamp): When the answer was generated.
#   - metadata (map):
#       - feedback_count (integer): Number of feedback submissions.
#       - feedback_sum (integer): Sum of all feedback ratings.
#       - average_feedback (float): Average rating (calculated).
#       - variance (float): Variance in feedback scores.
#       - scraping_time (float): Time taken to scrape and process data.
#   - status (string): Status of the answer (e.g., "complete", "needs review").