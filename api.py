import requests
from database import open_connection, close_connection, create_tables, get_max_id, insert_organization_data, insert_user_data, insert_repository_data, insert_issue_data
from config import BASE_URL, PARAMS_BASE, HEADERS
import time

def get_rate_limits():
    """
    Fetches the current rate limits from GitHub API and returns the rate limit data.
    """
    rate_limit_url = f"{BASE_URL}/rate_limit"
    response = requests.get(rate_limit_url, headers=HEADERS)
    if response.status_code == 200:
        rate_limit_data = response.json()
        return rate_limit_data['resources']['core']
    else:
        print(f"Failed to fetch rate limits: HTTP {response.status_code}, Error: {response.text}")
        return None


def fetch_organizations():
    """
    Fetches organizations from the GitHub API and inserts data into the database.
    """
    conn = open_connection()
    create_tables(conn)

    max_id = get_max_id(conn, "organizations")

    base_url = f"{BASE_URL}/organizations"
    params = PARAMS_BASE.copy()
    params.update({
        'since': max_id
    })
    
    has_more = True
    
    while has_more:
        rate_limits = get_rate_limits()
        if rate_limits and rate_limits['remaining'] == 0:
            reset_time = rate_limits['reset']
            sleep_duration = max(reset_time - time.time(), 1)
            print(f"Rate limit exceeded. Sleeping for {sleep_duration} seconds.")
            time.sleep(sleep_duration)
            continue

        response = requests.get(base_url, headers=HEADERS, params=params)
        if response.status_code == 200:
            data = response.json()
            if not data:
                print("No more organizations to fetch.")
                break

            for org in data:
                insert_organization_data(conn, 
                                         org['id'], 
                                         org.get('login'), 
                                         org.get('node_id'), 
                                         org.get('description'))
            
            print(f"Page of organizations since ID {params['since']} has been processed.")
            params['since'] = data[-1]['id']  # Update 'since' to the last organization's ID
        else:
            print(f"Failed to fetch data: HTTP {response.status_code}, Error: {response.text}")    
            break

        # Dynamically adjust sleep time based on remaining rate limit
        if rate_limits:
            sleep_time = 1 if rate_limits['remaining'] > 100 else 60
            time.sleep(sleep_time)
    
    close_connection(conn)
