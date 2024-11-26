from scrape_website import scrape_all
from firebase_admin import credentials, initialize_app
import logging

# Initialize Firebase
cred = credentials.Certificate("sample-firebase-ai-app-3e813-firebase-adminsdk-l2l4r-da0a22960f.json")
initialize_app(cred, {'storageBucket': "sample-firebase-ai-app-3e813.firebasestorage.ap"})

logging.basicConfig(level=logging.INFO)
 
# URLs to scrape
urls = [
    "https://barbieriwines.com/",
    "https://www.grassinifamilyvineyards.com/",
]

# Run the scraper
scrape_all(urls)
