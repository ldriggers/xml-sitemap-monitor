"""
1.0 Sitemap Fetcher Module
Fetches XML sitemap content from URLs with retry logic.

Key features:
- Automatic retry on transient failures (429, 500, 502, 503, 504)
- Exponential backoff between retries
- Configurable timeout and user agent
- Session reuse for connection pooling
- Simple download delay for politeness (not stealth - sitemaps are public)
"""

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import logging
import time
from typing import Optional, Dict, Any

logger = logging.getLogger(__name__)


class SitemapFetcher:
    """
    2.0 SitemapFetcher Class
    Fetches sitemap XML content with built-in retry logic.
    """
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        2.1 Initialize the SitemapFetcher with retry strategy.
        
        Args:
            config: Configuration dictionary with optional keys:
                - user_agent: Custom user agent string
                - timeout: Request timeout in seconds (default: 30)
                - max_retries: Number of retry attempts (default: 3)
                - download_delay: Delay between requests in seconds (default: 1.5)
        """
        # 2.1.1 Extract config values with defaults
        config = config or {}
        
        self.user_agent = config.get("user_agent", "SitemapMonitor/1.0")
        if not isinstance(self.user_agent, str) or not self.user_agent.strip():
            self.user_agent = "SitemapMonitor/1.0"
            logger.warning(f"Invalid user_agent in config. Using default: {self.user_agent}")
        
        self.timeout = config.get("timeout", 30)
        self.max_retries = config.get("max_retries", 3)
        self.download_delay = float(config.get("download_delay", 1.5))
        
        # 2.1.2 Track requests for delay logic
        self.request_count = 0
        self.last_request_time = 0
        
        # 2.1.3 Create session with retry strategy
        self.session = self._create_session_with_retries()
        
        logger.info(
            f"SitemapFetcher initialized: "
            f"User-Agent={self.user_agent[:50]}..., "
            f"timeout={self.timeout}s, "
            f"delay={self.download_delay}s"
        )

    def _create_session_with_retries(self) -> requests.Session:
        """
        2.2 Create a requests Session with automatic retry logic.
        
        Retry strategy:
        - Retries on: 429 (rate limit), 500, 502, 503, 504 (server errors)
        - Backoff: 1s, 2s, 4s between retries (exponential)
        - Also retries on connection errors
        
        Returns:
            Configured requests.Session object
        """
        session = requests.Session()
        
        # Define retry strategy
        retry_strategy = Retry(
            total=self.max_retries,
            backoff_factor=1,  # 1s, 2s, 4s between retries
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET"],  # Only retry safe methods
            raise_on_status=False,  # Don't raise, let us handle it
        )
        
        # Mount adapter to both http and https
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        # Set default headers
        session.headers.update({"User-Agent": self.user_agent})
        
        return session

    def _apply_politeness_delay(self) -> None:
        """
        2.3 Apply delay between requests for politeness.
        
        Sitemaps are public and sites expect bots to fetch them,
        so this is just basic politeness - not stealth.
        """
        if self.request_count == 0:
            self.request_count += 1
            self.last_request_time = time.time()
            return
        
        # Ensure minimum time since last request
        elapsed = time.time() - self.last_request_time
        wait_time = max(0, self.download_delay - elapsed)
        
        if wait_time > 0:
            time.sleep(wait_time)
        
        self.request_count += 1
        self.last_request_time = time.time()

    def fetch_sitemap_xml(self, sitemap_url: str, timeout: Optional[int] = None) -> Optional[str]:
        """
        2.4 Fetch XML content from a sitemap URL.
        
        Args:
            sitemap_url: The URL of the sitemap to fetch
            timeout: Optional override for request timeout
            
        Returns:
            XML content as string if successful, None otherwise
        """
        # 2.4.1 Validate URL
        if not sitemap_url or not sitemap_url.startswith(("http://", "https://")):
            logger.error(f"Invalid sitemap URL: {sitemap_url}")
            return None
        
        # 2.4.2 Apply politeness delay
        self._apply_politeness_delay()
        
        timeout = timeout or self.timeout
        
        logger.info(f"Fetching sitemap: {sitemap_url}")
        
        try:
            # 2.4.3 Make request (retries handled automatically by adapter)
            response = self.session.get(sitemap_url, timeout=timeout)
            
            # 2.4.4 Check for success
            if response.status_code == 200:
                content_length = len(response.text)
                logger.info(
                    f"Successfully fetched {sitemap_url} "
                    f"(status={response.status_code}, size={content_length:,} bytes)"
                )
                return response.text
            else:
                logger.error(
                    f"Failed to fetch {sitemap_url}: "
                    f"status={response.status_code} after {self.max_retries} retries"
                )
                return None
                
        except requests.exceptions.Timeout:
            logger.error(f"Timeout fetching {sitemap_url} after {timeout}s")
            return None
            
        except requests.exceptions.ConnectionError as e:
            logger.error(f"Connection error fetching {sitemap_url}: {e}")
            return None
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Request error fetching {sitemap_url}: {e}")
            return None
            
        except Exception as e:
            logger.error(f"Unexpected error fetching {sitemap_url}: {e}")
            return None


# Example usage for testing
if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Test with retry logic
    config = {
        "user_agent": "TestBot/1.0",
        "timeout": 10,
        "max_retries": 2,
        "download_delay": 1.0,
    }
    
    fetcher = SitemapFetcher(config=config)
    
    # Test valid sitemap
    test_url = "https://www.google.com/sitemap.xml"
    content = fetcher.fetch_sitemap_xml(test_url)
    
    if content:
        logger.info(f"Fetched {len(content):,} bytes from {test_url}")
    else:
        logger.error(f"Failed to fetch {test_url}")
