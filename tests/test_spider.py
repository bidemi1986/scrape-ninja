from scrapy.crawler import CrawlerProcess
import scrapy
import logging
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse, urljoin

def clean_escaped_fragment(url):
    parsed_url = urlparse(url)
    query_params = parse_qs(parsed_url.query)  # Parse query parameters into a dictionary

    # Remove '_escaped_fragment_' if it exists
    if '_escaped_fragment_' in query_params:
        logging.warning(f"Detected '_escaped_fragment_' in URL: {url}")
        query_params.pop('_escaped_fragment_')
        cleaned_query = urlencode(query_params, doseq=True)  # Re-encode the remaining query parameters
        cleaned_url = urlunparse(parsed_url._replace(query=cleaned_query))
        logging.info(f"Cleaned URL: {cleaned_url}")
        return cleaned_url

    # Return the original URL if no cleaning is needed
    return url


def get_root_domain(hostname):
    """Extract the root domain from a hostname (e.g., 'barbieriwines.com' from 'shop.barbieriwines.com')."""
    parts = hostname.split('.')
    if len(parts) > 2:  # Subdomain exists
        return '.'.join(parts[-2:])  # Return last two parts
    return hostname


class SimpleSpider(scrapy.Spider):
    name = 'test_spider'
    start_urls = ['https://barbieriwines.com/']

    def parse(self, response):
        logging.info(f"Scraping {response.url}")
        page_links = response.css('a::attr(href)').getall()
        logging.info(f"Found {len(page_links)} links on {response.url}")

        # Extract title and content
        title = response.css('title::text').get()
        content = response.css('p::text').getall()
        logging.info(f"Extracted title: {title}")
        logging.info(f"Extracted paragraphs: {content}")

        yield {
            'url': response.url,
            'title': title,
            'content': ' '.join(content),
        }
       
        # Get the root domain of the start URL (e.g., 'barbieriwines.com')
        start_root_domain = get_root_domain(urlparse(self.start_urls[0]).hostname)

        # Follow valid links within the same host
        for href in page_links:
            url = urljoin(response.url, href)  # Resolve relative links
            url = clean_escaped_fragment(url)  # Clean the link if needed

            # Check if the link belongs to the same root domain
            link_host = urlparse(url).hostname
            if link_host and get_root_domain(link_host) == start_root_domain:
                logging.info(f"Following internal link: {url}")
                yield response.follow(url, self.parse)
            else:
                logging.warning(f"Skipping external link: {url}")

process = CrawlerProcess(settings={
    'FEEDS': {'output.json': {'format': 'json'}},
    'LOG_LEVEL': 'INFO',
})

process.crawl(SimpleSpider)
process.start()