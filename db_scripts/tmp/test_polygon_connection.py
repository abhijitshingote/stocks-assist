#!/usr/bin/env python3
"""
Simple test script to verify Polygon.io API connectivity

This script quickly tests if your POLYGON_API_KEY is working
without running the full earnings fetcher.
"""

import os
import logging
from dotenv import load_dotenv
from polygon import RESTClient

load_dotenv()

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_polygon_api():
    """Test Polygon.io API connectivity"""
    
    # Check if API key is configured
    api_key = os.getenv('POLYGON_API_KEY')
    if not api_key:
        logger.error("❌ POLYGON_API_KEY environment variable not found")
        return False
    
    logger.info(f"✅ API key found: {len(api_key)} characters, starts with {api_key[:8]}...")
    
    try:
        logger.info("🔗 Testing API connection...")
        
        # Create Polygon client
        client = RESTClient(api_key)
        
        # Test with a simple ticker details request
        response = client.get_ticker_details("AAPL")
        
        if response and hasattr(response, 'name'):
            company_name = getattr(response, 'name', 'Unknown')
            logger.info(f"📝 API Response: Found ticker AAPL - {company_name}")
            logger.info("✅ API connectivity test PASSED!")
            return True
        else:
            logger.error(f"❌ API test failed - unexpected response format")
            logger.error(f"Response type: {type(response)}")
            if hasattr(response, '__dict__'):
                logger.error(f"Response attributes: {list(response.__dict__.keys())}")
            return False
            
    except Exception as e:
        logger.error(f"❌ API connectivity test failed: {str(e)}")
        return False

if __name__ == "__main__":
    logger.info("🚀 Starting Polygon.io API connectivity test")
    success = test_polygon_api()
    
    if success:
        logger.info("🎉 Test completed successfully! Your Polygon.io API setup is working.")
        print("\n✅ You can now run the earnings fetcher script!")
    else:
        logger.error("💥 Test failed. Please check your API key and internet connection.")
        print("\n❌ Please fix the API connection before running the earnings fetcher.")
    
    exit(0 if success else 1) 