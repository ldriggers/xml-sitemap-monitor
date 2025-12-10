"""
Robots.txt Checker - Respect robots.txt when selecting user agents

Fetches and parses robots.txt to determine which bots are blocked,
so we can filter our user agent selection to only use allowed bots.

Usage:
    from src.robots_checker import RobotsChecker
    
    checker = RobotsChecker()
    allowed_bots = checker.get_allowed_bots(domain, bot_list)
"""

import logging
import re
import requests
from typing import Dict, List, Optional, Set
from urllib.parse import urlparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path

logger = logging.getLogger(__name__)

# Cache location for robots.txt data
DEFAULT_CACHE_PATH = "output/robots_cache.json"

# =============================================================================
# BOT NAME MAPPING
# Maps user agent string patterns to robots.txt directive names
# Source: https://dejan.ai/blog/ai-bots/ (Nov 2025)
# =============================================================================

BOT_NAME_PATTERNS = {
    # -------------------------------------------------------------------------
    # Our own bot (never blocked by robots.txt unless explicitly)
    # -------------------------------------------------------------------------
    "SitemapMonitor": ["SitemapMonitor"],
    
    # -------------------------------------------------------------------------
    # OpenAI bots
    # -------------------------------------------------------------------------
    "GPTBot": ["GPTBot"],
    "OAI-SearchBot": ["OAI-SearchBot"],
    "ChatGPT-User": ["ChatGPT-User"],
    
    # -------------------------------------------------------------------------
    # Anthropic bots (ClaudeBot replaced anthropic-ai in July 2024)
    # -------------------------------------------------------------------------
    "ClaudeBot": ["ClaudeBot", "Claude-Web", "anthropic-ai"],
    "Claude-User": ["Claude-User", "Claude-Web", "anthropic-ai"],
    "Claude-SearchBot": ["Claude-SearchBot", "ClaudeBot", "anthropic-ai"],
    
    # -------------------------------------------------------------------------
    # Perplexity bots (controversial - sometimes ignores robots.txt)
    # -------------------------------------------------------------------------
    "PerplexityBot": ["PerplexityBot", "Perplexity-User"],
    
    # -------------------------------------------------------------------------
    # Traditional search engines (rarely blocked)
    # -------------------------------------------------------------------------
    "Googlebot": ["Googlebot", "Google-Extended"],  # Google-Extended controls AI training
    "bingbot": ["bingbot"],
    "YandexBot": ["YandexBot"],
    "DuckDuckBot": ["DuckDuckBot", "DuckAssistBot"],
    
    # -------------------------------------------------------------------------
    # Other AI company bots (commonly blocked)
    # -------------------------------------------------------------------------
    "Bytespider": ["Bytespider"],  # ByteDance - often ignores robots.txt
    "CCBot": ["CCBot"],  # Common Crawl - powers many LLMs
    "Amazonbot": ["Amazonbot"],
    "Applebot": ["Applebot", "Applebot-Extended"],
    "meta-externalagent": ["meta-externalagent", "FacebookBot", "facebookexternalhit"],
    "cohere-ai": ["cohere-ai", "cohere-training-data-crawler"],
    "Diffbot": ["Diffbot"],
    "AI2Bot": ["AI2Bot", "AI2Bot-Dolma"],
    "YouBot": ["YouBot"],
    "PetalBot": ["PetalBot"],  # Huawei
    "MistralAI-User": ["MistralAI-User"],
    
    # -------------------------------------------------------------------------
    # Data brokers and third-party scrapers (commonly blocked)
    # -------------------------------------------------------------------------
    "Omgilibot": ["Omgilibot", "omgili", "webzio-extended"],
    "ImagesiftBot": ["ImagesiftBot"],
    "VelenPublicWebCrawler": ["VelenPublicWebCrawler"],
    
    # -------------------------------------------------------------------------
    # SEO and analytics AI crawlers
    # -------------------------------------------------------------------------
    "DataForSeoBot": ["DataForSeoBot"],
    "SemrushBot": ["SemrushBot-OCOB", "SemrushBot"],
}

# Bots that are commonly blocked across many sites (for logging/awareness)
COMMONLY_BLOCKED_BOTS = {
    "ClaudeBot", "Claude-Web", "anthropic-ai",
    "PerplexityBot", "Perplexity-User",
    "CCBot",
    "Bytespider",
    "cohere-ai",
    "Omgilibot", "omgili",
    "ImagesiftBot",
    "Diffbot",
}

# Bots that typically respect robots.txt (for trust scoring)
COMPLIANT_BOTS = {
    "Googlebot",
    "bingbot", 
    "GPTBot",
    "ClaudeBot",
    "Applebot",
    "Amazonbot",
    "YandexBot",
    "DuckDuckBot",
    "MistralAI-User",
}


class RobotsChecker:
    """
    Check robots.txt to filter allowed bots.
    
    Caches robots.txt content to avoid repeated fetches.
    """
    
    def __init__(self, cache_path: Optional[str] = None, cache_ttl_hours: int = 24):
        self.cache_path = Path(cache_path or DEFAULT_CACHE_PATH)
        self.cache_ttl_hours = cache_ttl_hours
        self.cache = self._load_cache()
        
    def _load_cache(self) -> Dict:
        """Load robots.txt cache from disk."""
        if self.cache_path.exists():
            try:
                with open(self.cache_path, 'r') as f:
                    return json.load(f)
            except Exception as e:
                logger.warning(f"Could not load robots cache: {e}")
        return {"domains": {}}
    
    def _save_cache(self):
        """Save robots.txt cache to disk."""
        try:
            os.makedirs(self.cache_path.parent, exist_ok=True)
            with open(self.cache_path, 'w') as f:
                json.dump(self.cache, f, indent=2)
        except Exception as e:
            logger.warning(f"Could not save robots cache: {e}")
    
    def _is_cache_valid(self, domain: str) -> bool:
        """Check if cached robots.txt is still valid."""
        if domain not in self.cache["domains"]:
            return False
        
        cached = self.cache["domains"][domain]
        cached_time = datetime.fromisoformat(cached.get("fetched_at", "1970-01-01T00:00:00+00:00"))
        age_hours = (datetime.now(timezone.utc) - cached_time).total_seconds() / 3600
        
        return age_hours < self.cache_ttl_hours
    
    def fetch_robots_txt(self, domain: str, timeout: int = 10) -> Optional[str]:
        """
        Fetch robots.txt for a domain.
        
        Returns content string or None if failed.
        """
        # Check cache first
        if self._is_cache_valid(domain):
            logger.debug(f"Using cached robots.txt for {domain}")
            return self.cache["domains"][domain].get("content")
        
        url = f"https://{domain}/robots.txt"
        
        try:
            response = requests.get(
                url,
                timeout=timeout,
                headers={"User-Agent": "Mozilla/5.0 (compatible; SitemapMonitor/1.0)"}
            )
            
            if response.status_code == 200:
                content = response.text
                
                # Cache it
                self.cache["domains"][domain] = {
                    "content": content,
                    "fetched_at": datetime.now(timezone.utc).isoformat(),
                    "status_code": 200
                }
                self._save_cache()
                
                logger.info(f"Fetched robots.txt for {domain} ({len(content)} bytes)")
                return content
            else:
                logger.warning(f"robots.txt for {domain} returned {response.status_code}")
                return None
                
        except Exception as e:
            logger.warning(f"Could not fetch robots.txt for {domain}: {e}")
            return None
    
    def parse_blocked_bots(self, robots_content: str) -> Set[str]:
        """
        Parse robots.txt and return set of blocked bot names.
        
        A bot is considered "blocked" if it has Disallow: / (entire site).
        Partial blocks (like Disallow: /thmb/) are NOT considered blocked
        since we're fetching sitemaps, not those paths.
        """
        blocked = set()
        current_agent = None
        
        for line in robots_content.split('\n'):
            line = line.strip()
            
            # Skip comments and empty lines
            if not line or line.startswith('#'):
                continue
            
            # Parse User-agent directive
            if line.lower().startswith('user-agent:'):
                agent = line.split(':', 1)[1].strip()
                current_agent = agent
                continue
            
            # Parse Disallow directive
            if line.lower().startswith('disallow:') and current_agent:
                path = line.split(':', 1)[1].strip()
                
                # Disallow: / means entire site is blocked
                if path == '/':
                    blocked.add(current_agent)
                    logger.debug(f"Bot '{current_agent}' is fully blocked")
        
        return blocked
    
    def get_blocked_bots(self, domain: str) -> Set[str]:
        """
        Get set of bot names that are blocked for a domain.
        
        Fetches robots.txt (cached) and parses it.
        """
        content = self.fetch_robots_txt(domain)
        
        if not content:
            # If we can't fetch robots.txt, assume nothing is blocked
            return set()
        
        return self.parse_blocked_bots(content)
    
    def is_bot_blocked(self, domain: str, bot_ua: str) -> bool:
        """
        Check if a specific bot user agent is blocked for a domain.
        
        Args:
            domain: The domain to check (e.g., "www.investopedia.com")
            bot_ua: The full user agent string
            
        Returns:
            True if blocked, False if allowed
        """
        blocked = self.get_blocked_bots(domain)
        
        if not blocked:
            return False
        
        # Check if any of our bot name patterns match blocked bots
        for pattern_name, robot_names in BOT_NAME_PATTERNS.items():
            if pattern_name in bot_ua:
                # This UA contains this bot name
                for robot_name in robot_names:
                    if robot_name in blocked:
                        logger.debug(f"Bot '{pattern_name}' blocked by robots.txt ({robot_name})")
                        return True
        
        return False
    
    def filter_allowed_bots(self, domain: str, bot_uas: List[str]) -> List[str]:
        """
        Filter a list of bot user agents to only include allowed ones.
        
        Args:
            domain: The domain to check
            bot_uas: List of full user agent strings
            
        Returns:
            List of user agents that are NOT blocked by robots.txt
        """
        blocked = self.get_blocked_bots(domain)
        
        if not blocked:
            # Nothing blocked, return all
            return bot_uas
        
        allowed = []
        for ua in bot_uas:
            is_blocked = False
            
            # Check each bot pattern
            for pattern_name, robot_names in BOT_NAME_PATTERNS.items():
                if pattern_name in ua:
                    for robot_name in robot_names:
                        if robot_name in blocked:
                            is_blocked = True
                            break
                    if is_blocked:
                        break
            
            if not is_blocked:
                allowed.append(ua)
            else:
                logger.debug(f"Filtering out blocked UA: {ua[:50]}...")
        
        logger.info(f"robots.txt filter: {len(allowed)}/{len(bot_uas)} bots allowed for {domain}")
        
        return allowed


def get_domain_from_url(url: str) -> str:
    """Extract domain from URL."""
    parsed = urlparse(url)
    return parsed.netloc

