"""
Test script for FBref connectivity.
"""

import os
import sys
import time
import logging
import requests

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_fbref_connectivity():
    """Test basic connectivity to FBref.com."""
    logger.info("=== Testing FBref Connectivity ===")
    
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8"
        }
        
        # Test main page
        logger.info("Testing connection to FBref main page")
        start = time.time()
        response = requests.get("https://fbref.com/en/", headers=headers, timeout=10)
        duration = time.time() - start
        
        logger.info(f"Main page response: status={response.status_code}, time={duration:.2f}s, length={len(response.content)} bytes")
        
        # Test a specific team page
        logger.info("Testing connection to a specific team page")
        start = time.time()
        response = requests.get("https://fbref.com/en/squads/19538871/Manchester-United-Stats", headers=headers, timeout=10)
        duration = time.time() - start
        
        logger.info(f"Team page response: status={response.status_code}, time={duration:.2f}s, length={len(response.content)} bytes")
        
        # Test search page
        logger.info("Testing connection to search page")
        start = time.time()
        response = requests.get("https://fbref.com/en/search/search.fcgi?search=Arsenal", headers=headers, timeout=10)
        duration = time.time() - start
        
        logger.info(f"Search page response: status={response.status_code}, time={duration:.2f}s, length={len(response.content)} bytes")
        
        return True
    except Exception as e:
        logger.error(f"FBref connectivity test failed: {str(e)}", exc_info=True)
        return False

if __name__ == "__main__":
    logger.info("Starting FBref connectivity test")
    test_fbref_connectivity()
    logger.info("FBref connectivity test completed")