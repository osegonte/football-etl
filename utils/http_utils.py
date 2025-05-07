"""
HTTP utilities for making robust web requests.
"""

import random
import time
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup
import json
from typing import Dict, Optional, Union, Any

from config import USER_AGENTS, REQUEST_HEADERS, MAX_RETRIES, RETRY_DELAY


def get_random_user_agent() -> str:
    """Get a random user agent from the configured list.
    
    Returns:
        str: Random user agent string
    """
    return random.choice(USER_AGENTS)


def create_session() -> requests.Session:
    """Create a requests session with retry capabilities.
    
    Returns:
        requests.Session: Configured session with retries
    """
    session = requests.Session()
    
    # Set up retry strategy
    retry_strategy = Retry(
        total=MAX_RETRIES,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "POST"],
        backoff_factor=RETRY_DELAY
    )
    
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    
    # Set default headers
    headers = REQUEST_HEADERS.copy()
    headers["User-Agent"] = get_random_user_agent()
    session.headers.update(headers)
    
    return session


def make_request(
    url: str,
    method: str = "GET",
    params: Optional[Dict] = None,
    data: Optional[Dict] = None,
    headers: Optional[Dict] = None,
    cookies: Optional[Dict] = None,
    timeout: int = 30,
    verify: bool = True,
    retry_on_failure: bool = True
) -> requests.Response:
    """Make an HTTP request with retry capabilities.
    
    Args:
        url: URL to request
        method: HTTP method (GET or POST)
        params: URL parameters
        data: Request body for POST requests
        headers: HTTP headers
        cookies: HTTP cookies
        timeout: Request timeout in seconds
        verify: Whether to verify SSL certificates
        retry_on_failure: Whether to retry on failure
        
    Returns:
        requests.Response: Response object
        
    Raises:
        requests.RequestException: If the request fails after retries
    """
    if not headers:
        headers = {}
    
    if "User-Agent" not in headers:
        headers["User-Agent"] = get_random_user_agent()
    
    session = create_session() if retry_on_failure else requests.Session()
    
    try:
        response = session.request(
            method=method,
            url=url,
            params=params,
            data=data,
            headers=headers,
            cookies=cookies,
            timeout=timeout,
            verify=verify
        )
        
        response.raise_for_status()
        return response
    
    except requests.exceptions.RequestException as e:
        if retry_on_failure:
            # Try one more time with a different user agent
            headers["User-Agent"] = get_random_user_agent()
            time.sleep(RETRY_DELAY)
            
            response = session.request(
                method=method,
                url=url,
                params=params,
                data=data,
                headers=headers,
                cookies=cookies,
                timeout=timeout,
                verify=verify
            )
            
            response.raise_for_status()
            return response
        else:
            raise e


def get_soup(url: str, **kwargs) -> BeautifulSoup:
    """Get BeautifulSoup object from URL.
    
    Args:
        url: URL to request
        **kwargs: Additional arguments for make_request()
        
    Returns:
        BeautifulSoup: Parsed HTML
    """
    response = make_request(url, **kwargs)
    return BeautifulSoup(response.content, "html.parser")


def get_json(url: str, **kwargs) -> Any:
    """Get JSON from URL.
    
    Args:
        url: URL to request
        **kwargs: Additional arguments for make_request()
        
    Returns:
        Any: Parsed JSON
    """
    response = make_request(url, **kwargs)
    return json.loads(response.text)