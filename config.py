import os

# Configuration settings for the application
BASE_URL = 'https://api.github.com'
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/53.0.2785.143 Safari/537.36',
    'Authorization': f'token {os.environ.get('PAT_KEY')}',
    'Accept': 'application/vnd.github+json'
}
PARAMS_BASE = {
    'since': 1,
    'per_page': 100
}