# Grapefruit-crawler
[![Codacy Badge](https://api.codacy.com/project/badge/Grade/fcd09fccacfd45a7a3590c64d9ea95aa)](https://www.codacy.com/app/bashkirtsevich/grapefruit-crawler?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=bashkirtsevich-llc/grapefruit-crawler&amp;utm_campaign=Badge_Grade)
[![Build Status](https://travis-ci.org/bashkirtsevich-llc/grapefruit-crawler.svg?branch=master)](https://travis-ci.org/bashkirtsevich-llc/grapefruit-crawler)

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
