import requests
import json
import os
import re
import time
import pandas as pd
from dotenv import load_dotenv
from models import init_db, Comment
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

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

def load_company_names():
    """Load company names from stock_list.csv"""
    try:
        df = pd.read_csv('stock_list.csv')
        # Create a mapping of ticker to company name
        company_map = dict(zip(df['symbol'], df['companyName']))
        print(f"Loaded {len(company_map)} company names from stock_list.csv")
        return company_map
    except Exception as e:
        print(f"Warning: Could not load company names from CSV: {e}")
        return {}

def get_db_session():
    """Get database session"""
    # Use the correct connection string directly (matching docker-compose.yml)
    database_url = 'postgresql://postgres:postgres@localhost:5432/stocks_db'
    
    print(f"Connecting to database: {database_url}")
    engine = create_engine(database_url)
    Session = sessionmaker(bind=engine)
    return Session()

def save_ai_comment(session, ticker, comment_text, ai_source="perplexity-sonar-deep-research"):
    """Save AI comment to database"""
    try:
        comment = Comment(
            ticker=ticker,
            comment_text=comment_text,
            comment_type='ai',
            status='approved',
            ai_source=ai_source
        )
        session.add(comment)
        session.commit()
        print(f"✓ Saved AI comment for {ticker}")
        return True
    except Exception as e:
        session.rollback()
        print(f"✗ Error saving comment for {ticker}: {str(e)}")
        return False

def process_symbols(symbols, prompt_template=None):
    """Process list of symbols and save AI comments"""
    
    # Load company names
    company_map = load_company_names()
    
    session = get_db_session()
    
    for i, ticker in enumerate(symbols, 1):
        start_time = time.time()
        
        # Get company name
        company_name = company_map.get(ticker, "")
        entity_name = f"company: {company_name} ticker: {ticker}" if company_name else ticker
        
        print(f"\nProcessing {entity_name} ({i} of {len(symbols)})...")
        
        # Generate prompt for this ticker
        prompt = prompt_template.format(ticker=ticker, company_name=company_name, entity=entity_name)
        
        # Query Perplexity
        response = query_perplexity(prompt, show_thinking=False)
        
        elapsed_time = time.time() - start_time
        
        if response and not response.startswith("Error:"):
            # Save to database
            save_ai_comment(session, ticker, response)
            print(f"✓ Completed {entity_name} in {elapsed_time:.1f} seconds")
        else:
            print(f"✗ Failed to get response for {entity_name}: {response} (took {elapsed_time:.1f} seconds)")
    
    session.close()
    print("\nProcessing complete!")

if __name__ == "__main__":
    # List of symbols to process
    symbols = [
       "AI", "AKRO", "ALB", "ALGM", "ALLO", "AMBA", "AMC", "AMSC", "ANAB", "ANF", "APLD", "APP", "APPS", "AQN", "ARM", "AS", "ASND", "ATGE", "ATI", "AUR", "AVAV", "AVDX", "AXON", "AXSM", "BB", "BBAI", "BBD", "BCRX", "BILI", "BNTX", "BOX", "BP", "BPMC", "BROS", "BTBT", "BUR", "BURL", "CAR", "CASY", "CDTX", "CELH", "CEP", "CFLT", "CGON", "CIFR", "CLF", "CNXC", "COGT", "COIN", "COMP", "COOP", "CORT", "CORZ", "CPRI", "CRDO", "CROX", "CRSP", "CRWV", "CTMX", "CVS", "CWAN", "DAL", "DAVE", "DB", "DBRG", "DDOG", "DG", "DIS", "DJT", "DKNG", "DOCS", "DOCU", "DUOL", "DY", "EAT", "ECG", "EDU", "EH", "ELAN", "ELF", "EOSE", "EPAM", "ESTC", "ETNB", "ETWO", "EXEL", "EXPE", "FARO", "FERG", "FL", "FSLR", "G", "GAP", "GDS", "GGAL", "GH", "GME", "GRAB", "GRMN", "GRPN", "GRRR", "GT", "GWRE", "HALO", "HIMS", "HOOD", "HPE", "HSAI", "HTZ", "HUM", "IBM", "INFA", "INOD", "INSM", "INTU", "IONQ", "IRTC", "ITRI", "JBTM", "JD", "JOBY", "KAI", "KLG", "KYMR", "LEU", "LI", "LSCC", "LTBR", "LUNR", "LVWR", "LW", "LXRX", "LYFT", "MAG", "MASS", "MAT", "MDB", "MDGL", "MELI", "MGM", "MIDD", "MLYS", "MNDY", "MNTN", "MP", "MRUS", "MTN", "NBIS", "NET", "NFLX", "NKE", "NNE", "NOK", "NRG", "NTES", "NTNX", "NUE", "NVAX", "NVO", "NVTS", "NXT", "NXTT", "ODD", "OKLO", "OKTA", "ON", "ONON", "ONTO", "ORCL", "ORGO", "OSCR", "OSK", "OST", "OUST", "PCOR", "PCT", "PEGA", "PEN", "PGY", "PHAT", "PINS", "PL", "PLTR", "PLUG", "PODD", "PONY", "PRA", "PRCH", "PRGO", "PROK", "PTGX", "PTON", "PVH", "PZZA", "QBTS", "QDEL", "QS", "QUBT", "RACE", "RBA", "RBRK", "RDUS", "RDW", "RGTI", "RH", "RKLB", "RL", "ROK", "ROKU", "ROOT", "RUN", "RVMD", "RXRX", "RYAAY", "RYTM", "SAFX", "SAGE", "SAIL", "SATS", "SBET", "SEDG", "SEPN", "SES", "SEZL", "SGRY", "SHAK", "SIG", "SKX", "SLDB", "SLNO", "SMCI", "SMR", "SMTC", "SNOW", "SOC", "SOUN", "SPOT", "STR", "SYM", "TAL", "TEAM", "TECX", "TEM", "TGI", "TIGR", "TMC", "TMDX", "TME", "TNXP", "TOST", "TPR", "TRIP", "TRVI", "TSSI", "TTD", "TTEK", "TTWO", "TWLO", "TXNM", "U", "UAL", "UEC", "ULTA", "UPST", "UPXI", "URBN", "VEEV", "VERA", "VERV", "VIAV", "VOYA", "VRNA", "VRNT", "WBA", "WGS", "WOLF", "WRB", "WRBY", "WRD", "WSO", "XPEV", "ZETA", "ZIM"
    ]
    
    # Custom prompt template (from perplexity_test.py)
    custom_prompt = """Provide a detailed, chronological timeline of all major fundamental events affecting {entity} over the last 12 months. For each event, include the date (exact if available; otherwise, approximate with a clear note on date certainty), description, and source type.
Include a tag indicating the confidence level of the date: Exact, Approximate, or Estimated.
Cross-check dates against multiple reputable sources where possible.
Clearly state when dates are ranges, differ between announcement and implementation, or are not precisely known.
Avoid omitting important events solely due to lack of precise dates; instead, qualify the date information."""
    
    process_symbols(symbols, custom_prompt) 