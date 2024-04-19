import requests
from database import open_connection, close_connection, create_tables, get_max_id, fetch_users_batch, fetch_organizations_batch, fetch_repos_batch, insert_organization_data, insert_user_data, insert_repository_data, insert_issue_data, insert_log_data, get_max_ids
from config import BASE_URL, PARAMS_BASE, HEADERS
from requests.exceptions import ConnectionError, Timeout
import time
import traceback

def safe_request(url, headers, params=None, max_retries=3, delay=5):
    """
    Makes a HTTP GET request to the specified URL with retries.
    - max_retries: Maximum number of retries.
    - delay: Wait time between retries in seconds.
    """
    for attempt in range(max_retries):
        try:
            response = requests.get(url, headers=headers, params=params)
            return response
        except (ConnectionError, Timeout) as e:
            print(f"Request failed: {e}. Attempt {attempt + 1} of {max_retries}. Retrying in {delay} seconds...")
            time.sleep(delay)
    # Return None or raise an exception if all retries fail
    print("All retry attempts failed.")
    return None

def get_rate_limits():
    """
    Fetches the current rate limits from GitHub API and returns the rate limit data.
    """
    rate_limit_url = f"{BASE_URL}/rate_limit"
    response = safe_request(rate_limit_url, headers=HEADERS)
    if response and response.status_code == 200:
        rate_limit_data = response.json()
        return rate_limit_data['resources']['core']
    elif response:
        print(f"Failed to fetch rate limits: HTTP {response.status_code}, Error: {response.text}")
        return None
    else:
        print(f"Failed to fetch rate limits. No response is available.")
        return None

def fetch_organizations():
    """
    Fetches organizations from the GitHub API and inserts data into the database.
    """
    conn = open_connection()
    try:
        create_tables(conn)

        max_id = get_max_id(conn, "organizations")

        base_url = f"{BASE_URL}/organizations"
        params = PARAMS_BASE.copy()
        params.update({
            'since': max_id
        })
        iterate = 0
        has_more = True        
        while has_more:
            if iterate == 100:  
                iterate = 0
                rate_limits = get_rate_limits()
                if rate_limits and rate_limits['remaining'] == 0:
                    reset_time = rate_limits['reset']
                    sleep_duration = max(reset_time - time.time(), 1)
                    print(f"Rate limit exceeded. Sleeping for {sleep_duration} seconds.")
                    time.sleep(sleep_duration)
                    continue
            iterate += 1

            response = safe_request(base_url, headers=HEADERS, params=params)
            if response and response.status_code == 200:
                data = response.json()
                if not data:
                    print("No more organizations to fetch.")
                    break

                for org in data:
                    insert_organization_data(conn, org)
                
                print(f"Page of organizations since ID {params['since']} has been processed.")
                params['since'] = data[-1]['id']  # Update 'since' to the last organization's ID
            elif response and response.status_code >= 400:
                print(f"Failed to fetch organization data since {params['since']}. HTTP {response.status_code}, Error: {response.text}")
            else:
                print(f"Failed to fetch organization data since {params['since']}. No response is available")    
    finally:
        close_connection(conn)

def fetch_users():
    """
    Fetches users from the GitHub API and inserts data into the database.
    """
    conn = open_connection()
    try:
        create_tables(conn)

        max_id = get_max_id(conn, "users")

        base_url = f"{BASE_URL}/users"
        params = PARAMS_BASE.copy()
        params.update({
            'since': max_id
        })
        iterate = 0
        has_more = True        
        while has_more:
            if iterate == 100:  
                iterate = 0
                rate_limits = get_rate_limits()
                if rate_limits and rate_limits['remaining'] == 0:
                    reset_time = rate_limits['reset']
                    sleep_duration = max(reset_time - time.time(), 1)
                    print(f"Rate limit exceeded. Sleeping for {sleep_duration} seconds.")
                    time.sleep(sleep_duration)
                    continue
            iterate += 1

            response = safe_request(base_url, headers=HEADERS, params=params)
            if response and response.status_code == 200:
                users = response.json()
                if not users:
                    print("No more users to fetch.")
                    break

                for user_summary in users:
                    # Fetch detailed user information
                    user_detail_url = user_summary.get('url')
                    detail_response = safe_request(user_detail_url, headers=HEADERS)
                    if detail_response and detail_response.status_code == 200:
                        user_data = detail_response.json()
                        insert_user_data(conn, user_data)
                    elif detail_response and detail_response.status_code >= 400:
                        print(f"Failed to fetch detailed data for user: {user_summary.get('url')}\nHTTP {detail_response.status_code}, Error: {detail_response.text}")
                        user_summary['error'] = True
                        insert_user_data(conn, user_summary)
                    else:
                        print(f"Failed to fetch detailed data for user: {user_summary.get('url')}. No response is available.")
                        user_summary['error'] = True
                        insert_user_data(conn, user_summary)
                
                print(f"Page of users since ID {params['since']} has been processed.")
                params['since'] = users[-1]['id']  # Update 'since' to the last user's ID
            elif response and response.status_code >= 400:
                print(f"Failed to fetch users: HTTP {response.status_code}, Error: {response.text}")    
            else:
                print(f"Failed to fetch users data since {params['since']}. No response is available.")     
    finally:
        close_connection(conn)
        
def fetch_repositories(type='organizations'):
    """
    Fetches repositories for each user from the GitHub API and inserts data into the database.
    """
    conn = open_connection()
    try:
        create_tables(conn)
        iterate = 0
        # Keep trak of last processed owner
        max_ids = get_max_ids(conn)
        if type=='organizations':
            last_owner_id = max_ids.get('max_last_org_id')
        else:
            last_owner_id = max_ids.get('max_last_user_id')
            
        while True:
            if iterate == 100:  
                iterate = 0
                rate_limits = get_rate_limits()
                if rate_limits and rate_limits['remaining'] == 0:
                    reset_time = rate_limits['reset']
                    sleep_duration = max(reset_time - time.time(), 1)
                    print(f"Rate limit exceeded. Sleeping for {sleep_duration} seconds.")
                    time.sleep(sleep_duration)
                    continue
            iterate += 1
            # Fetch a batch of owners from the database
            
            if type=='organizations':
                owners = fetch_organizations_batch(conn, last_owner_id=last_owner_id, batch_size=100)
            else:
                owners = fetch_users_batch(conn, last_owner_id=last_owner_id, batch_size=100)
                
            if not owners:
                print(f"No more {type} to process.")
                break

            for owner in owners:
                login = owner['login']
                repos_url = f"{BASE_URL}/users/{login}/repos"

                params= {
                    'type': 'public',
                    'sort': 'created',
                    'direction': 'asc',
                    'per_page': 100,
                    'page': 1
                }

                has_more_pages = True
                while has_more_pages:
                    response = safe_request(repos_url, headers=HEADERS, params=params)
                    if response.status_code == 200:
                        repos = response.json()
                        if not repos:
                            # print(f"No more repositories to fetch for {type} {login}.")
                            break

                        for repo in repos:
                            repo['owner'] = login
                            repo['owner_id'] = owner['id']
                            repo['owner_type'] = owner['type']
                            insert_repository_data(conn, repo)

                        if 'next' in response.links:
                            params['page'] += 1  # Go to the next page
                        else:
                            has_more_pages = False
                    elif response and response.status_code >= 400:
                        has_more_pages = False
                        print(f"Failed to fetch repositories for user {repos_url}. HTTP {response.status_code}, Error: {response.text}")
                    else:
                        has_more_pages = False
                        print(f"Failed to fetch repositories for user {repos_url}. No response is available")
                                    
            # Assuming the 'id' of the last user in the batch is the highest 'id' processed in this batch
            last_owner_id = owners[-1]['id']
            
            # Store the lates owner that we processed
            if type=='organizations':
                log_data = {'last_org_id': last_owner_id}
            else:
                log_data = {'last_user_id': last_owner_id}    
            insert_log_data(conn, log_data)
    finally:
        close_connection(conn)
        
def fetch_issues(type='organizations'):
    """
    Fetches issues for each repository from the GitHub API and inserts data into the database.
    """
    conn = open_connection()
    try:
        create_tables(conn)
        iterate = 0
        # Keep trak of last processed repository
        max_ids = get_max_ids(conn)
        if type=='organizations':
            last_repository_id = max_ids.get('max_last_org_repository_id')
        else:
            last_repository_id = max_ids.get('max_last_user_repository_id')
            
        while True:
            if iterate == 100:  
                iterate = 0        
                rate_limits = get_rate_limits()
                if rate_limits and rate_limits['remaining'] == 0:
                    reset_time = rate_limits['reset']
                    sleep_duration = max(reset_time - time.time(), 1)
                    print(f"Rate limit exceeded. Sleeping for {sleep_duration} seconds.")
                    time.sleep(sleep_duration)
                    continue
            iterate += 1
            # Fetch a batch of repositories from the database
            # Fetch a batch of owners from the database
            if type=='organizations':
                repos = fetch_repos_batch(conn, last_repository_id=last_repository_id, batch_size=100, owner_type='Organization')
            else:
                repos = fetch_repos_batch(conn, last_repository_id=last_repository_id, batch_size=100, owner_type='User')
            
            
            if not repos:
                print("No more repositories to process.")
                break

            for repo in repos:
                full_name = repo['full_name']
                issues_url = f"{BASE_URL}/repos/{full_name}/issues"

                params = {
                    'state': 'all',
                    'sort': 'created',
                    'direction': 'asc',
                    'per_page': 100,
                    'page': 1
                }

                has_more_pages = True
                while has_more_pages:
                    try:
                        response = safe_request(issues_url, headers=HEADERS, params=params)
                        if response and response.status_code == 200:
                            issues = response.json()
                            if not issues:
                                # print(f"No more issues to fetch for {full_name}.")
                                break

                            for issue in issues:
                                try:
                                    issue['repository_id'] = repo['id']
                                    insert_issue_data(conn, issue)
                                except Exception as e:
                                    print(f"Error processing issue {issue.get('number', 'Unknown')} for repository {full_name}: {e}")
                                    traceback.print_exc()  # Provides the stack trace
                                    exit(1)

                            if 'next' in response.links:
                                params['page'] += 1
                            else:
                                has_more_pages = False
                        elif response and response.status_code >= 400:
                            print(f"Failed to fetch issues for repository {issues_url}. HTTP {response.status_code}, Error: {response.text}")
                            has_more_pages = False
                        else:
                            print(f"Failed to fetch issues for repository {issues_url}. No response is available")
                            has_more_pages = False
                    except Exception as e:
                        print(f"Exception occurred while fetching issues for {full_name}: {e}")
                        traceback.print_exc()  # Print the stack trace to understand the cause of the exception
                        has_more_pages = False  # Prevent further attempts to fetch pages
                        exit(1)


            # Assuming the 'id' of the last repository in the batch is the highest 'id' processed in this batch
            last_repository_id = repos[-1]['id']
            
            # Store the lates repository that we processed
            if type=='organizations':
                log_data = {'last_org_repository_id': last_repository_id}
            else:
                log_data = {'last_user_repository_id': last_repository_id}    
            insert_log_data(conn, log_data)
    finally:
        close_connection(conn)