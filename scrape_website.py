# scrape_website.py
import scrapy
from scrapy.crawler import CrawlerRunner
from scrapy.utils.log import configure_logging
from twisted.internet import reactor, defer
from urllib.parse import urlparse, urljoin
import json
import logging
import time
from firebase_admin import storage
from concurrent.futures import ThreadPoolExecutor
from typing import List, Dict
from dataclasses import dataclass
from twisted.internet.defer import inlineCallbacks, succeed

@dataclass
class BatchConfig:
    """Configuration for batch processing"""
    batch_size: int = 4
    chunk_size: int = 1000000
    max_concurrent_requests: int = 32
    delay_between_batches: float = 0.5

class BatchWebsiteSpider(scrapy.Spider):
    name = 'batch_website'
    
    def __init__(self, start_urls=None, batch_id=None, *args, **kwargs):
        super(BatchWebsiteSpider, self).__init__(*args, **kwargs)
        self.start_urls = start_urls or []
        self.batch_id = batch_id
        self.allowed_domains = [urlparse(url).netloc for url in start_urls]
        self.buffer = {}
        self.buffer_sizes = {}
        self.max_buffer_size = BatchConfig.chunk_size
        self.bucket = storage.bucket()
        self.executor = ThreadPoolExecutor(max_workers=4)
        self.upload_tasks = []  # Track upload tasks
        
    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(
                url, 
                callback=self.parse,
                meta={'hostname': urlparse(url).netloc},
                errback=self.handle_error,
                dont_filter=True
            )

    def handle_error(self, failure):
        logging.error(f"Request failed: {failure.value}")
        
    def parse(self, response):
        hostname = response.meta['hostname']
        
        try:
            if hostname not in self.buffer:
                self.buffer[hostname] = []
                self.buffer_sizes[hostname] = 0
            
            content_parts = []
            for selector in ['title::text', 'p::text', 'h1::text', 'h2::text', 
                           'h3::text', 'h4::text', 'h5::text', 'h6::text']:
                content_parts.extend(response.css(selector).getall())
            
            text_content = ' '.join(filter(None, [part.strip() for part in content_parts]))
            
            if text_content:
                page_data = {
                    'url': response.url,
                    'content': text_content,
                    'timestamp': time.time()
                }
                
                self.buffer[hostname].append(page_data)
                self.buffer_sizes[hostname] += len(json.dumps(page_data))
                
                if self.buffer_sizes[hostname] >= self.max_buffer_size:
                    task = self._upload_buffer(hostname)
                    self.upload_tasks.append(task)
            
            for href in response.css('a::attr(href)').getall():
                url = urljoin(response.url, href)
                parsed_url = urlparse(url)
                
                if parsed_url.netloc in self.allowed_domains and url.startswith('http'):
                    yield response.follow(
                        url, 
                        self.parse, 
                        meta={'hostname': hostname},
                        errback=self.handle_error
                    )
                    
        except Exception as e:
            logging.error(f"Error parsing {response.url}: {e}")

    def _upload_buffer(self, hostname):
        if not self.buffer[hostname]:
            return succeed(None)
            
        try:
            chunk_number = int(time.time() * 1000)
            blob = self.bucket.blob(
                f"scrapes/{hostname}/batch_{self.batch_id}_chunk_{chunk_number}.json"
            )
            
            compressed_data = json.dumps(self.buffer[hostname])
            
            def upload():
                blob.upload_from_string(compressed_data, content_type='application/json')
                return True

            future = self.executor.submit(upload)
            self.buffer[hostname] = []
            self.buffer_sizes[hostname] = 0
            
            logging.info(f"Uploaded chunk {chunk_number} for {hostname}")
            return future
            
        except Exception as e:
            logging.error(f"Error uploading to Firebase: {e}")
            return None

    def closed(self, reason):
        # Upload remaining buffers and wait for completion
        remaining_tasks = []
        for hostname in self.buffer:
            if self.buffer[hostname]:
                task = self._upload_buffer(hostname)
                if task:
                    remaining_tasks.append(task)
        
        # Wait for all upload tasks to complete
        for task in remaining_tasks:
            task.result()
        
        self.executor.shutdown(wait=True)
        return succeed(None)

@inlineCallbacks
def process_batch(urls: List[str], batch_id: int):
    settings = {
        'LOG_LEVEL': 'INFO',
        'ROBOTSTXT_OBEY': True,
        'CONCURRENT_REQUESTS': BatchConfig.max_concurrent_requests,
        'CONCURRENT_REQUESTS_PER_DOMAIN': 16,
        'DOWNLOAD_DELAY': 0.5,
        'DEPTH_LIMIT': 3,
        'COOKIES_ENABLED': False,
        'RETRY_ENABLED': True,
        'RETRY_TIMES': 3,
        'DOWNLOAD_TIMEOUT': 15,
    }
    
    runner = CrawlerRunner(settings)
    yield runner.crawl(BatchWebsiteSpider, start_urls=urls, batch_id=batch_id)

@inlineCallbacks
def batch_scrape_all(urls: List[str]):
    total_batches = (len(urls) + BatchConfig.batch_size - 1) // BatchConfig.batch_size
    
    for batch_idx in range(total_batches):
        start_idx = batch_idx * BatchConfig.batch_size
        end_idx = min(start_idx + BatchConfig.batch_size, len(urls))
        batch_urls = urls[start_idx:end_idx]
        
        logging.info(f"Processing batch {batch_idx + 1}/{total_batches}")
        yield process_batch(batch_urls, batch_idx)
        
        if batch_idx < total_batches - 1:
            yield succeed(None)  # Allow other tasks to run between batches

def scrape_all(urls: List[str]):
    configure_logging()
    return defer.ensureDeferred(batch_scrape_all(urls))