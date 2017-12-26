# Grapefruit-crawler
Grapefruit torrent-network sniffer.

## Requirements
* Python 3.6.3 or higher
* MongoDB 3.2.17 or higher

## Execution
1. Install requirements
```bash
pip install -r requirements.txt
```
2. Set environment variables
```
MONGODB_URL=mongodb://mongodb:27017/grapefruit
```
Optional (default = `grapefruit`):
```
MONGODB_BASE_NAME=grapefruit
```
3. Start crawler
```bash
python app.py
```