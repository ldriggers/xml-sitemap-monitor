import requests
import logging
from typing import Optional, Dict, Any

logger = logging.getLogger(__name__)

class SitemapFetcher:
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initializes the SitemapFetcher.

        Args:
            config: A dictionary containing configuration, expected to have a 'user_agent' key.
        """
        self.user_agent = "DefaultSitemapMonitor/1.0"
        if config and isinstance(config.get("user_agent"), str) and config["user_agent"].strip():
            self.user_agent = config["user_agent"]
        else:
            logger.warning(
                "User agent not found or invalid in config. Using default: %s", 
                self.user_agent
            )
        
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": self.user_agent})
        logger.info(f"SitemapFetcher initialized with User-Agent: {self.user_agent}")

    def fetch_sitemap_xml(self, sitemap_url: str, timeout: int = 30) -> Optional[str]:
        """
        Fetches the XML content of a sitemap from the given URL.

        Args:
            sitemap_url: The URL of the sitemap to fetch.
            timeout: The timeout in seconds for the HTTP request.

        Returns:
            The XML content as a string if successful, None otherwise.
        """
        if not sitemap_url or not sitemap_url.startswith(("http://", "https://")):
            logger.error(f"Invalid sitemap URL provided: {sitemap_url}")
            return None

        logger.info(f"Fetching sitemap from: {sitemap_url}")
        try:
            response = self.session.get(sitemap_url, timeout=timeout)
            response.raise_for_status()  # Raises an HTTPError for bad responses (4XX or 5XX)
            
            # Check if content type suggests it's XML, though not strictly necessary for sitemaps
            # content_type = response.headers.get("Content-Type", "").lower()
            # if "xml" not in content_type:
            #     logger.warning(
            #         f"Content-Type for {sitemap_url} is '{response.headers.get('Content-Type')}', not XML."
            #     )
            
            logger.info(f"Successfully fetched sitemap from {sitemap_url} (Status: {response.status_code})")
            return response.text
        except requests.exceptions.Timeout:
            logger.error(f"Timeout occurred while fetching sitemap: {sitemap_url} (after {timeout}s)")
            return None
        except requests.exceptions.HTTPError as e:
            logger.error(f"HTTP error occurred for {sitemap_url}: {e}")
            return None
        except requests.exceptions.RequestException as e:
            logger.error(f"An error occurred while fetching sitemap {sitemap_url}: {e}")
            return None
        except Exception as e:
            logger.error(f"An unexpected error occurred in fetch_sitemap_xml for {sitemap_url}: {e}")
            return None

# Example usage (for testing this module directly)
if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    # Mock config for testing
    mock_config_data = {
        "user_agent": "TestSitemapFetcher/1.0 (+http://test.com)",
        "domains": [] # Not used by fetcher directly, but part of typical config
    }
    
    fetcher = SitemapFetcher(config=mock_config_data)
    
    # Test with a known sitemap (replace with a real one for actual testing)
    # Note: For real tests, ensure the URL is accessible and a sitemap.
    test_sitemap_url_valid = "https://www.google.com/sitemap.xml" # A sitemap index
    test_sitemap_url_news = "https://www.google.com/nonexistentnews/sitemap/sitemap.xml" # A specific sitemap, might be 404
    test_sitemap_url_invalid_format = "htp://shouldfail.com/sitemap.xml"
    test_sitemap_url_timeout = "http://httpstat.us/200?sleep=5000" # Test timeout (if 5s is too long)

    xml_content = fetcher.fetch_sitemap_xml(test_sitemap_url_valid)
    if xml_content:
        logger.info(f"Fetched valid sitemap content (first 200 chars):\n{xml_content[:200]}")
    else:
        logger.error(f"Failed to fetch content from {test_sitemap_url_valid}")

    logger.info("--- Next Test: Non-existent sitemap ---")
    xml_content_404 = fetcher.fetch_sitemap_xml(test_sitemap_url_news)
    if not xml_content_404:
        logger.info(f"Correctly failed to fetch non-existent sitemap as expected.")

    logger.info("--- Next Test: Invalid URL format ---")
    xml_content_invalid = fetcher.fetch_sitemap_xml(test_sitemap_url_invalid_format)
    if not xml_content_invalid:
        logger.info(f"Correctly failed due to invalid URL format as expected.")

    # # Example timeout test - uncomment to run, might take time
    # logger.info("--- Next Test: Timeout (adjust timeout in call if needed) ---")
    # xml_content_timeout_test = fetcher.fetch_sitemap_xml(test_sitemap_url_timeout, timeout=3)
    # if not xml_content_timeout_test:
    #     logger.info(f"Correctly timed out as expected.")

    # Test without providing config (should use default user_agent)
    logger.info("--- Next Test: Fetcher with default User-Agent ---")
    default_fetcher = SitemapFetcher()
    xml_content_default_ua = default_fetcher.fetch_sitemap_xml(test_sitemap_url_valid)
    if xml_content_default_ua:
        logger.info(f"Fetched with default UA (first 200 chars):\n{xml_content_default_ua[:200]}")

    logger.info("SitemapFetcher testing complete.") 