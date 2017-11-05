# Grapefruit-crawler
Grapefruit torrent-network sniffer.

## Requirements
* Python 3.6.3 or higher
* MongoDB 3.2.17 or higher

## Execution
### Direct execution
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
3. Start MongoDB
4. Start crawler
```bash
python app.py
```

### Docker
1. Install Docker
2. Start Docker daemon (optional step)
```bash
sudo usermod -aG docker $(whoami)
sudo service docker start
```
3. Build container
```bash
sudo docker build -t grapefruit-crawler .
```
4. Start container
```bash
# Regular execution
sudo docker run -p 6881:6881/udp grapefruit-crawler

# Run in interactive mode:
# sudo docker run -it -p 6881:6881/udp grapefruit-crawler
```