import logging
from typing import List, Dict, Optional, Union, Tuple, Any
from lxml import etree # Using lxml for robust parsing and namespace handling
import pandas as pd
from datetime import datetime

logger = logging.getLogger(__name__)

# Common sitemap namespaces
SITEMAP_NS = {
    'sm': 'http://www.sitemaps.org/schemas/sitemap/0.9',
    # Add other namespaces if observed in target sitemaps (e.g., image, video, news)
    'image': 'http://www.google.com/schemas/sitemap-image/1.1',
    'news': 'http://www.google.com/schemas/sitemap-news/0.9',
    'video': 'http://www.google.com/schemas/sitemap-video/1.1'
}

class SitemapParser:
    def __init__(self):
        logger.info("SitemapParser initialized.")

    def parse_sitemap(self, xml_content: str, sitemap_url: str = "") -> Dict[str, Union[str, List[Dict[str, Any]], None]]:
        """
        Parses the given XML sitemap content.

        Determines if it's a sitemap index or a URL set and extracts relevant data.

        Args:
            xml_content: The XML content of the sitemap as a string.
            sitemap_url: The URL from which this sitemap was fetched (for logging/context).

        Returns:
            A dictionary with:
                'type': 'sitemapindex' or 'urlset' or 'error'
                'urls': A list of URL dictionaries (for urlset) or sitemap URL strings (for sitemapindex).
                        None if error or not applicable.
                'error_message': A string describing the error, if any.
        """
        if not xml_content:
            logger.error(f"Cannot parse empty XML content (from {sitemap_url}).")
            return {"type": "error", "urls": None, "error_message": "Empty XML content"}
        
        try:
            # lxml requires bytes for parsing, so encode the string
            # Also, recover mode attempts to parse even mildly malformed XML
            parser = etree.XMLParser(recover=True, remove_blank_text=True)
            root = etree.fromstring(xml_content.encode('utf-8'), parser=parser)
            
            # Determine if it's a sitemap index or a urlset
            # The localname part extracts tag name without namespace
            root_tag_name = etree.QName(root.tag).localname

            if root_tag_name == 'sitemapindex':
                logger.info(f"Parsing as sitemap index: {sitemap_url}")
                sitemap_links = self._extract_sitemap_links_from_index(root)
                return {"type": "sitemapindex", "urls": sitemap_links, "error_message": None}
            elif root_tag_name == 'urlset':
                logger.info(f"Parsing as URL set: {sitemap_url}")
                page_urls = self._extract_urls_from_urlset(root)
                return {"type": "urlset", "urls": page_urls, "error_message": None}
            else:
                logger.warning(
                    f"Unknown root tag '{root.tag}' in sitemap from {sitemap_url}. Attempting to find URLs."
                )
                # Fallback: try to find urlset or sitemapindex tags anyway
                if root.xpath('//sm:sitemap', namespaces=SITEMAP_NS):
                    sitemap_links = self._extract_sitemap_links_from_index(root)
                    return {"type": "sitemapindex", "urls": sitemap_links, "error_message": "Unknown root, but sitemap tags found"}
                elif root.xpath('//sm:url', namespaces=SITEMAP_NS):
                    page_urls = self._extract_urls_from_urlset(root)
                    return {"type": "urlset", "urls": page_urls, "error_message": "Unknown root, but url tags found"}
                else:
                    msg = f"Unknown root element '{root.tag}' and no sitemap/url tags found in {sitemap_url}."
                    logger.error(msg)
                    return {"type": "error", "urls": None, "error_message": msg}

        except etree.XMLSyntaxError as e:
            logger.error(f"XML syntax error while parsing sitemap from {sitemap_url}: {e}")
            return {"type": "error", "urls": None, "error_message": f"XMLSyntaxError: {e}"}
        except Exception as e:
            logger.error(f"An unexpected error occurred during sitemap parsing for {sitemap_url}: {e}")
            return {"type": "error", "urls": None, "error_message": f"Unexpected error: {e}"}

    def _extract_sitemap_links_from_index(self, root_element: etree._Element) -> List[str]:
        """Extracts sitemap URLs from a sitemapindex element."""
        sitemap_urls = []
        # XPath query for <loc> inside <sitemap> using the namespace
        for loc_element in root_element.xpath('//sm:sitemap/sm:loc', namespaces=SITEMAP_NS):
            if loc_element.text:
                sitemap_urls.append(loc_element.text.strip())
        logger.debug(f"Extracted {len(sitemap_urls)} sitemap links from index.")
        return sitemap_urls

    def _extract_urls_from_urlset(self, root_element: etree._Element) -> List[Dict[str, Optional[str]]]:
        """Extracts URL entries from a urlset element."""
        url_entries = []
        # XPath query for <url> elements using the namespace
        for url_element in root_element.xpath('//sm:url', namespaces=SITEMAP_NS):
            entry = {
                'loc': None,
                'lastmod': None,
                'changefreq': None,
                'priority': None
            }
            loc_el = url_element.find('sm:loc', SITEMAP_NS)
            if loc_el is not None and loc_el.text:
                entry['loc'] = loc_el.text.strip()
            else:
                # A URL entry without a <loc> is invalid according to sitemap protocol, skip it.
                logger.warning(f"Skipping URL entry without <loc> tag. Context: {etree.tostring(url_element, pretty_print=True).decode().strip()[:200]}")
                continue

            lastmod_el = url_element.find('sm:lastmod', SITEMAP_NS)
            if lastmod_el is not None and lastmod_el.text:
                entry['lastmod'] = lastmod_el.text.strip()
            
            changefreq_el = url_element.find('sm:changefreq', SITEMAP_NS)
            if changefreq_el is not None and changefreq_el.text:
                entry['changefreq'] = changefreq_el.text.strip()

            priority_el = url_element.find('sm:priority', SITEMAP_NS)
            if priority_el is not None and priority_el.text:
                entry['priority'] = priority_el.text.strip()
            
            url_entries.append(entry)
        logger.debug(f"Extracted {len(url_entries)} URL entries from urlset.")
        return url_entries

# Example usage (for testing this module directly)
if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG, # Use DEBUG for more verbose output from parser
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    parser = SitemapParser()

    # Example Sitemap Index XML
    sitemap_index_xml = """
    <?xml version="1.0" encoding="UTF-8"?>
    <sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
       <sitemap>
          <loc>http://www.example.com/sitemap1.xml.gz</loc>
          <lastmod>2004-10-01T18:23:17+00:00</lastmod>
       </sitemap>
       <sitemap>
          <loc>http://www.example.com/sitemap2.xml.gz</loc>
          <lastmod>2005-01-01</lastmod>
       </sitemap>
    </sitemapindex>
    """
    logger.info("--- Testing Sitemap Index Parsing ---")
    parsed_index = parser.parse_sitemap(sitemap_index_xml, "http://test.com/sitemap_index.xml")
    logger.info(f"Parsed Index Result: {parsed_index}")
    if parsed_index['type'] == 'sitemapindex':
        assert len(parsed_index['urls']) == 2
        assert parsed_index['urls'][0] == "http://www.example.com/sitemap1.xml.gz"

    # Example URL Set XML
    urlset_xml = """
    <?xml version="1.0" encoding="UTF-8"?>
    <urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
       <url>
          <loc>http://www.example.com/</loc>
          <lastmod>2005-01-01</lastmod>
          <changefreq>monthly</changefreq>
          <priority>0.8</priority>
       </url>
       <url>
          <loc>http://www.example.com/catalog?item=12&amp;desc=vacation_hawaii</loc>
          <changefreq>weekly</changefreq>
       </url>
       <url>
          <loc>http://www.example.com/catalog?item=73&amp;desc=vacation_new_zealand</loc>
          <lastmod>2004-12-23</lastmod>
          <changefreq>weekly</changefreq>
       </url>
    </urlset>
    """
    logger.info("--- Testing URL Set Parsing ---")
    parsed_urlset = parser.parse_sitemap(urlset_xml, "http://test.com/urlset.xml")
    logger.info(f"Parsed URL Set Result: {parsed_urlset}")
    if parsed_urlset['type'] == 'urlset':
        assert len(parsed_urlset['urls']) == 3
        assert parsed_urlset['urls'][0]['loc'] == "http://www.example.com/"
        assert parsed_urlset['urls'][0]['lastmod'] == "2005-01-01"

    # Example malformed XML
    malformed_xml = "<urlset><url><loc>http://bad.com</loc></badurl></urlset>"
    logger.info("--- Testing Malformed XML Parsing ---")
    parsed_malformed = parser.parse_sitemap(malformed_xml, "http://test.com/malformed.xml")
    logger.info(f"Parsed Malformed Result: {parsed_malformed}")
    assert parsed_malformed['type'] == 'error' 
    # With recover=True, lxml might still parse parts of it, so the error might not be a syntax error
    # but a content error. Or it could be a syntax error for severely broken XML.

    # Example empty XML
    empty_xml = ""
    logger.info("--- Testing Empty XML Parsing ---")
    parsed_empty = parser.parse_sitemap(empty_xml, "http://test.com/empty.xml")
    logger.info(f"Parsed Empty Result: {parsed_empty}")
    assert parsed_empty['type'] == 'error'

    # Example with a different root tag (but still sitemap-like content)
    other_root_xml = """
    <root xmlns:sm="http://www.sitemaps.org/schemas/sitemap/0.9">
        <sm:url>
            <sm:loc>http://www.example.com/other_root</sm:loc>
            <sm:lastmod>2023-01-01</sm:lastmod>
        </sm:url>
    </root>
    """
    logger.info("--- Testing Other Root Tag Parsing ---")
    parsed_other_root = parser.parse_sitemap(other_root_xml, "http://test.com/other_root.xml")
    logger.info(f"Parsed Other Root Result: {parsed_other_root}")
    if parsed_other_root['type'] == 'urlset':
         assert len(parsed_other_root['urls']) == 1

    logger.info("SitemapParser testing complete.") 