#!/usr/bin/env python3
"""
Simple Perplexity API test script for testing prompts.
Requires API key in .env file as PERPLEXITY_API_KEY
"""

import os
import requests
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def test_prompt(prompt: str, model: str = "sonar") -> None:
    """
    Test a prompt with Perplexity API and print the Q&A
    """
    api_key = os.getenv("PERPLEXITY_API_KEY")
    if not api_key:
        raise ValueError("PERPLEXITY_API_KEY not found in .env file")
    
    url = "https://api.perplexity.ai/chat/completions"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }
    
    data = {
        "model": model,
        "messages": [{"role": "user", "content": prompt}],
        "stream": False
    }
    
    try:
        response = requests.post(url, headers=headers, json=data)
        response.raise_for_status()
        result = response.json()
        
        # Print in Q&A format
        print("\nQ:", prompt)
        print("\nA:", result["choices"][0]["message"]["content"])
        print("\n" + "="*80)
    
    except Exception as e:
        print(f"Error: {str(e)}")

if __name__ == "__main__":
    # Example usage
    test_prompts = [
        # "What are the latest developments in quantum computing?",
        # "Explain the concept of zero-knowledge proofs in simple terms.",
        "Why was OKLO stock up yesterday",
        "Why has OKLO stock been up over the last few months. Provide a chronological timeline of news events that have occurred. Do NOT provide reasoning that is related to stock momentum or technical analysis."
    ]
    
    for prompt in test_prompts:
        test_prompt(prompt)