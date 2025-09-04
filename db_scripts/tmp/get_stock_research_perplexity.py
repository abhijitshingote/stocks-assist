import requests
import json
import os
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

def load_research_symbols():
    """Load symbols from researchlist.csv"""
    with open('researchlist.csv', 'r') as f:
        content = f.read().strip()
    symbols = [symbol.strip() for symbol in content.split(',')]
    return symbols

def load_company_names():
    """Load company names from stock_list.csv"""
    try:
        df = pd.read_csv('stock_list.csv')
        return dict(zip(df['symbol'], df['companyName']))
    except:
        return {}

def query_perplexity(ticker, company_name):
    """Query Perplexity API for stock research"""
    url = "https://api.perplexity.ai/chat/completions"
    headers = {
        "Authorization": f"Bearer {os.getenv('PERPLEXITY_API_KEY')}",
        "Content-Type": "application/json"
    }
    
    entity_name = f"{company_name} ({ticker})" if company_name else ticker
    prompt = f"""Search the web to identify the most recent and significant fundamental drivers behind {entity_name}.

Focus only on developments that could materially impact the company’s business operations, growth outlook, or competitive positioning. This includes:
	•	Company-specific news: press releases, partnerships, executive changes, earnings, product launches
	•	Sector-wide developments: regulation, macro policy changes, geopolitical drivers
	•	Government/political events: executive actions, defense/military policy shifts, procurement changes, high-level public statements that mention or affect the company or its industry
	•	Viral moments: news segments, interviews (e.g., Fox, CNBC), social media posts (Twitter/X, Reddit, YouTube) that went viral and specifically influenced investor sentiment

Use both traditional media (e.g., Bloomberg, Reuters, CNBC, PR Newswire) and non-traditional sources (e.g., Reddit threads, X/Twitter influencers, government websites, YouTube commentary).

Exclude purely technical trading signals, chart patterns, or sentiment-based moves unless they were directly triggered by a material event.

Summarize the findings in order of significance and explain how each could impact the company. Include citations/links where appropriate."""

    data = {
        "model": "sonar-pro",
        "messages": [{"role": "user", "content": prompt}],
        "stream": False
    }
    
    response = requests.post(url, headers=headers, json=data)
    result = response.json()
    return result["choices"][0]["message"]["content"]

def save_to_markdown(ticker, company_name, content, output_file):
    """Save research to markdown file"""
    title = f"{ticker} {company_name}" if company_name else ticker
    with open(output_file, 'a', encoding='utf-8') as f:
        f.write(f"# {title}\n\n{content}\n\n---\n\n")

if __name__ == "__main__":
    symbols = load_research_symbols()
    company_names = load_company_names()
    output_file = "stock_research_results.md"
    
    # Clear output file
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write("# Stock Research Report\n\n")
    
    for ticker in symbols[:]:
        company_name = company_names.get(ticker, "")
        entity_name = f"{company_name} ({ticker})" if company_name else ticker
        print(f"Processing {entity_name}...")
        content = query_perplexity(ticker, company_name)
        save_to_markdown(ticker, company_name, content, output_file)
        print(f"✓ Completed {entity_name}")
    
    print(f"Done! Results saved to {output_file}")