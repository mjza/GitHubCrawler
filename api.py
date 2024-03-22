import requests
import json
from database import open_connection, close_connection, create_tables, get_max_id, insert_organization_data, insert_user_data, insert_repository_data, insert_issue_data
from config import BASE_URL, PARAMS_BASE, HEADERS
import time

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
    sleep = 5
    
    while has_more:
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
            
            time.sleep(sleep)
        else:
            print(f"Failed to fetch data: HTTP {response.status_code}, Error: {response.text}")    
            break
    close_connection(conn) 
