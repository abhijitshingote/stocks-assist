I'll extract all the GET endpoints from your backend API:

[1 tool called]

Based on the backend code, here are all the **GET endpoints** you can paste directly into your browser at `http://localhost:5001`:

## 📊 **Scanner/Screener Endpoints**

### Returns (1D, 5D, 20D, 60D, 120D)
```
http://localhost:5001/api/Return1D-Above200M
http://localhost:5001/api/Return1D-Below200M
http://localhost:5001/api/Return5D-Above200M
http://localhost:5001/api/Return5D-Below200M
http://localhost:5001/api/Return20D-Above200M
http://localhost:5001/api/Return20D-Below200M
http://localhost:5001/api/Return60D-Above200M
http://localhost:5001/api/Return60D-Below200M
http://localhost:5001/api/Return120D-Above200M
http://localhost:5001/api/Return120D-Below200M
```

### Gap Scanner
```
http://localhost:5001/api/Gapper-Above200M
http://localhost:5001/api/Gapper-Below200M
```

### Volume Scanner
```
http://localhost:5001/api/Volume-Above200M
http://localhost:5001/api/Volume-Below200M
```

### Sea Change Scanner
```
http://localhost:5001/api/SeaChange-Above200M
http://localhost:5001/api/SeaChange-Below200M
```

### All Returns (Combined)
```
http://localhost:5001/api/AllReturns-Above200M
http://localhost:5001/api/AllReturns-Below200M
```

### RSI
```
http://localhost:5001/api/rsi
```

## 📈 **Stock Data Endpoints**

### General Stock Info
```
http://localhost:5001/api/stocks
http://localhost:5001/api/sectors
http://localhost:5001/api/industries
http://localhost:5001/api/exchanges
http://localhost:5001/api/market_cap_distribution
```

### Specific Stock Data (replace `TICKER` with actual ticker like AAPL)
```
http://localhost:5001/api/stock/AAPL
http://localhost:5001/api/stock/AAPL/prices
http://localhost:5001/api/stock/AAPL/earnings
```

## 💬 **Comments & Notes**

### Comments (replace `TICKER` with actual ticker)
```
http://localhost:5001/api/comments/AAPL
http://localhost:5001/api/ai-comments/AAPL
```

### Concise Notes
```
http://localhost:5001/api/concise-notes/AAPL
```

## 🚩 **Flags & Lists**

### Flags (replace `TICKER` with actual ticker)
```
http://localhost:5001/api/flags/AAPL
```

### Lists
```
http://localhost:5001/api/shortlist
http://localhost:5001/api/blacklist
```

## 📰 **News**

### Finviz News (replace `TICKER` with actual ticker)
```
http://localhost:5001/api/finviz-news/AAPL
```

## 🗓️ **System Info**

### Latest Date in Database
```
http://localhost:5001/api/latest_date
```

---

**Note:** The POST/PUT/DELETE endpoints (like adding comments, updating flags, etc.) can't be accessed directly in a browser - you'll need to use the frontend at `http://localhost:5002` or a tool like Postman/curl to test those.

Try starting with `http://localhost:5001/api/latest_date` to verify your backend is working! 🚀