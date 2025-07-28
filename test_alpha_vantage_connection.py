#!/usr/bin/env python3
"""
Simple test script to verify Alpha Vantage API connectivity

This script quickly tests if your ALPHA_VANTAGE_API_KEY is working
without running the full earnings fetcher.
"""

import os
import requests
import logging
from dotenv import load_dotenv

load_dotenv()

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_alpha_vantage_api():
    """Test Alpha Vantage API connectivity"""
    
    # Check if API key is configured
    api_key = os.getenv('ALPHA_VANTAGE_API_KEY')
    
    # Try other API keys as fallback
    if not api_key:
        api_key = os.getenv('POLYGON_API_KEY')
        if api_key:
            logger.warning("ALPHA_VANTAGE_API_KEY not found, trying POLYGON_API_KEY as fallback")
    
    if not api_key:
        logger.error("❌ ALPHA_VANTAGE_API_KEY environment variable not found")
        logger.error("❌ Get a free API key at: https://www.alphavantage.co/support/#api-key")
        return False
    
    logger.info(f"✅ API key found: {len(api_key)} characters, starts with {api_key[:8]}...")
    
    try:
        logger.info("🔗 Testing API connection...")
        
        # Test with a simple company overview request
        url = f"https://www.alphavantage.co/query?function=OVERVIEW&symbol=AAPL&apikey={api_key}"
        response = requests.get(url, timeout=30)
        
        if response.status_code == 200:
            data = response.json()
            if data and 'Symbol' in data and data.get('Symbol') == 'AAPL':
                company_name = data.get('Name', 'Unknown')
                logger.info(f"📝 API Response: Found ticker AAPL - {company_name}")
                logger.info("✅ API connectivity test PASSED!")
                return True
            elif 'Error Message' in data:
                logger.error(f"❌ API test failed: {data['Error Message']}")
                return False
            elif 'Note' in data:
                logger.error(f"❌ API test failed: {data['Note']}")
                return False
            else:
                logger.error(f"❌ API test failed - unexpected response format")
                logger.error(f"Response: {data}")
                return False
        else:
            logger.error(f"❌ API test failed with status {response.status_code}")
            logger.error(f"Response: {response.text}")
            return False
            
    except Exception as e:
        logger.error(f"❌ API connectivity test failed: {str(e)}")
        return False

if __name__ == "__main__":
    logger.info("🚀 Starting Alpha Vantage API connectivity test")
    success = test_alpha_vantage_api()
    
    if success:
        logger.info("🎉 Test completed successfully! Your Alpha Vantage API setup is working.")
        print("\n✅ You can now run the earnings fetcher script!")
    else:
        logger.error("💥 Test failed. Please check your API key and internet connection.")
        print("\n❌ Please fix the API connection before running the earnings fetcher.")
        print("\n🔗 Get a free Alpha Vantage API key at: https://www.alphavantage.co/support/#api-key")
    
    exit(0 if success else 1) 