#!/usr/bin/env python3
"""
Stock News Summary using Claude API with Web Search.
Uses Claude's built-in web search to find recent stock-moving events.
"""

import os
import time
from dotenv import load_dotenv
from anthropic import Anthropic

# Load environment variables
load_dotenv()

# Path to Claude prompt template
CLAUDE_PROMPT_FILE = os.path.join(os.path.dirname(__file__), '..', 'user_data', 'ai_prompts', 'claude_stock_research_prompt.txt')


def load_claude_prompt_template():
    """Load the Claude AI prompt template from file."""
    try:
        if os.path.exists(CLAUDE_PROMPT_FILE):
            with open(CLAUDE_PROMPT_FILE, 'r') as f:
                return f.read()
        else:
            print(f"⚠️ Claude prompt file not found at {CLAUDE_PROMPT_FILE}")
            return None
    except Exception as e:
        print(f"❌ Error loading Claude prompt template: {str(e)}")
        return None


def get_stock_news_summary(ticker: str, company_name: str, months: int = 12) -> dict:
    """
    Get a summary of significant stock-moving news for a company.
    
    Args:
        ticker: Stock ticker symbol (e.g., "ONDS")
        company_name: Full company name (e.g., "Ondas Holdings Inc.")
        months: How many months back to search (default: 12)
    
    Returns:
        dict with 'success', 'summary', and 'error' keys
    """
    api_key = os.getenv('ANTHROPIC_API_KEY')
    if not api_key:
        return {
            'success': False,
            'summary': None,
            'error': 'ANTHROPIC_API_KEY not found in environment'
        }
    
    client = Anthropic(api_key=api_key)
    
    # Load prompt from file
    template = load_claude_prompt_template()
    if not template:
        return {
            'success': False,
            'summary': None,
            'error': f'Claude prompt template not found at {CLAUDE_PROMPT_FILE}'
        }
    
    # Replace placeholders
    prompt = template.replace('[company_name]', company_name)
    prompt = prompt.replace('[ticker]', ticker)
    prompt = prompt.replace('[months]', str(months))

    # Retry logic for rate limits
    max_retries = 3
    for attempt in range(max_retries):
        try:
            response = client.messages.create(
                model="claude-sonnet-4-5-20250929",
                max_tokens=4096,
                system="You are a financial analyst. Use web search to find current, accurate information about stock-moving events. Be thorough and factual.",
                tools=[{
                    "type": "web_search_20250305",
                    "name": "web_search"
                }],
                messages=[{"role": "user", "content": prompt}]
            )
            
            # Extract all text from response blocks
            text_parts = []
            search_count = 0
            for block in response.content:
                if hasattr(block, 'type'):
                    if block.type == "text":
                        text_parts.append(block.text)
                    elif block.type == "web_search_tool_result":
                        search_count = len(block.content) if hasattr(block, 'content') else 0
            
            summary = "\n".join(text_parts)
            
            return {
                'success': True,
                'summary': summary,
                'error': None,
                'web_searches': search_count,
                'usage': {
                    'input_tokens': response.usage.input_tokens,
                    'output_tokens': response.usage.output_tokens
                }
            }
            
        except Exception as e:
            error_msg = str(e)
            
            # Rate limit - wait and retry
            if "429" in error_msg or "rate_limit" in error_msg:
                if attempt < max_retries - 1:
                    wait_time = 60 * (attempt + 1)  # 60s, 120s, 180s
                    print(f"⏳ Rate limit hit. Waiting {wait_time}s before retry...")
                    time.sleep(wait_time)
                    continue
            
            return {
                'success': False,
                'summary': None,
                'error': error_msg
            }
    
    return {
        'success': False,
        'summary': None,
        'error': 'Max retries exceeded'
    }


# =============================================================================
# Test the function
# =============================================================================
if __name__ == "__main__":
    print("🔍 Stock News Summary Tool")
    print("=" * 50)
    
    # Test with Ondas Holdings
    ticker = "ONDS"
    company = "Ondas Holdings Inc."
    
    print(f"📊 Fetching news for {company} ({ticker})...")
    print()
    
    result = get_stock_news_summary(ticker, company, months=12)
    
    if result['success']:
        print("✅ Success!")
        if 'web_searches' in result:
            print(f"🔍 Web searches performed: {result['web_searches']}")
        print()
        print("=" * 50)
        print("📰 NEWS SUMMARY:")
        print("=" * 50)
        print(result['summary'])
        print("=" * 50)
        if 'usage' in result:
            print(f"\n💰 Tokens used: {result['usage']['input_tokens']} in, {result['usage']['output_tokens']} out")
    else:
        print(f"❌ Error: {result['error']}")
