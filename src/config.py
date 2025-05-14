import json
import logging
import os
from typing import Dict, List, Optional, Any

logger = logging.getLogger(__name__)

CONFIG_FILE_PATH = "config.json"

def load_config() -> Optional[Dict[str, Any]]:
    """Loads the configuration from config.json."""
    if not os.path.exists(CONFIG_FILE_PATH):
        logger.error(f"Configuration file not found: {CONFIG_FILE_PATH}")
        return None
    try:
        with open(CONFIG_FILE_PATH, 'r') as f:
            config_data = json.load(f)
        logger.info(f"Successfully loaded configuration from {CONFIG_FILE_PATH}")
        if not validate_config(config_data):
            return None
        return config_data
    except json.JSONDecodeError as e:
        logger.error(f"Error decoding JSON from {CONFIG_FILE_PATH}: {e}")
        return None
    except Exception as e:
        logger.error(f"An unexpected error occurred while loading config: {e}")
        return None

def validate_config(config: Dict[str, Any]) -> bool:
    """Validates the structure and content of the configuration."""
    if not isinstance(config, dict):
        logger.error("Configuration must be a dictionary.")
        return False

    if "targets" not in config or not isinstance(config["targets"], list):
        logger.error("'targets' key is missing or not a list in config.")
        return False

    if not config["targets"]:
        logger.warning("'targets' list is empty. No sitemaps will be monitored.")
        # Allow empty targets list for now, might be a valid use case for setup.

    for i, target_entry in enumerate(config["targets"]):
        if not isinstance(target_entry, dict):
            logger.error(f"Target entry at index {i} is not a dictionary.")
            return False
        required_keys = ["domain", "sitemap_url"]
        for key in required_keys:
            if key not in target_entry:
                logger.error(f"Target entry at index {i} is missing required key: '{key}'.")
                return False
            if not isinstance(target_entry[key], str) or not target_entry[key].strip():
                logger.error(f"Value for key '{key}' in target entry at index {i} must be a non-empty string.")
                return False

    if "user_agent" not in config or not isinstance(config["user_agent"], str) or not config["user_agent"].strip():
        logger.warning("'user_agent' key is missing or not a non-empty string. Using a default one is recommended.")
        # Not making this a fatal error, but good to warn.

    logger.info("Configuration validation successful.")
    return True

if __name__ == '__main__':
    # Basic test for the config loader
    logging.basicConfig(level=logging.INFO)
    config = load_config()
    if config:
        logger.info(f"Loaded user agent: {config.get('user_agent')}")
        logger.info(f"Number of targets to monitor: {len(config.get('targets', []))}")
    else:
        logger.error("Failed to load or validate configuration.") 