import requests
import json
import os
import re
from dotenv import load_dotenv

load_dotenv()

def query_perplexity(prompt, show_thinking=False):
    """Query Perplexity API with a given prompt"""
    
    url = "https://api.perplexity.ai/chat/completions"
    
    headers = {
        "Authorization": f"Bearer {os.getenv('PERPLEXITY_API_KEY')}",
        "Content-Type": "application/json"
    }
    
    request_data = {
        # "model": "sonar",
        # "model": "sonar-reasoning-pro",
        "model": "sonar-deep-research",
        "messages": [{"role": "user", "content": prompt}],
        "stream": False
    }
    
    try:
        response = requests.post(url, headers=headers, json=request_data)
        response.raise_for_status()
        result = response.json()
        content = result["choices"][0]["message"]["content"]
        
        # Remove <think> tags if show_thinking is False
        if not show_thinking:
            content = re.sub(r'<think>.*?</think>', '', content, flags=re.DOTALL)
            content = content.strip()
        
        return content
    except Exception as e:
        return f"Error: {str(e)}"

def test_prompts(show_thinking=False):
    """Test different prompt variations"""
    
    prompts = [
        # "What are the key factors driving NVDA stock price today?",
        # "Analyze the current market sentiment for NVIDIA stock",
        # "Give me a technical analysis of NVDA's recent performance",
#         """Provide a detailed, strictly chronological timeline of all major fundamental events affecting CoreWeave (CRWV) over the last 6 months. For each event, include the exact date or as precise as possible, a brief description, and the source confirming the date (e.g., SEC filing, company press release, reputable news outlet).
# Cross-check all dates against multiple reputable sources.
# If there is a difference between announcement and implementation date, clearly state both.
# Avoid vague monthly references or approximations unless necessary, and avoid assumptions without clear qualification""",
"""Provide a detailed, chronological timeline of all major fundamental events affecting CoreWeave (CRWV) over the last 12 months. For each event, include the date (exact if available; otherwise, approximate with a clear note on date certainty), description, and source type.
Include a tag indicating the confidence level of the date: Exact, Approximate, or Estimated.
Cross-check dates against multiple reputable sources where possible.
Clearly state when dates are ranges, differ between announcement and implementation, or are not precisely known.
Avoid omitting important events solely due to lack of precise dates; instead, qualify the date information."""
    ]
    
    for i, prompt in enumerate(prompts, 1):
        print(f"\n--- Test {i} ---")
        print(f"Prompt: {prompt}")
        print(f"Response: {query_perplexity(prompt, show_thinking)}")
        print("-" * 50)

if __name__ == "__main__":
    # Set to True to show <think> content, False to hide it
    SHOW_THINKING = False
    
    # Single test
    # result = query_perplexity("What's the current outlook for tech stocks?", SHOW_THINKING)
    # print(result)
    
    # Test multiple prompts
    test_prompts(SHOW_THINKING) 