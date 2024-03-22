import os
import sqlite3
import psycopg2

DBMS = os.getenv('DBMS')
if DBMS == 'SQLITE':
    PLACE_HOLDER = "?"
elif DBMS == 'POSTGRES':
    PLACE_HOLDER = "%s"
else:
    raise ValueError("Unsupported DBMS")

def open_connection():
    if DBMS == 'SQLITE':
        conn = sqlite3.connect(os.getenv('DB_PATH'))
    elif DBMS == 'POSTGRES':
        conn = psycopg2.connect(
            dbname=os.getenv('DB_NAME'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
            host=os.getenv('DB_HOST'),
            port=os.getenv('DB_PORT')
        )
    else:
        raise ValueError("Unsupported DBMS")

    return conn

def close_connection(conn):
    conn.commit()
    # Close the database connection
    conn.close()
    
def create_tables(conn):
    cursor = conn.cursor()
    # Create the tags table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS organizations (
        id INTEGER NOT NULL PRIMARY KEY,
        login TEXT,
        node_id TEXT,
        description TEXT
    )
    ''')

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS users (
        id INTEGER NOT NULL PRIMARY KEY,
        login TEXT,        
        node_id TEXT,
        type TEXT
    )
    ''')

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS repositories (
        id INTEGER NOT NULL PRIMARY KEY,
        node_id TEXT,
        name TEXT,
        full_name TEXT,
        private BOOLEAN,
        owner TEXT,
        owner_type TEXT,
        owner_id INTEGER,           
        html_url TEXT,
        description TEXT,
        fork BOOLEAN,
        url TEXT,
        created_at TIMESTAMP,
        updated_at TIMESTAMP,
        pushed_at TIMESTAMP,
        homepage TEXT,
        size INTEGER,
        stargazers_count INTEGER,
        watchers_count INTEGER,
        language TEXT,
        has_issues BOOLEAN,
        has_projects BOOLEAN,
        has_downloads BOOLEAN,
        has_wiki BOOLEAN,
        has_pages BOOLEAN,
        has_discussions BOOLEAN,
        forks_count INTEGER,
        mirror_url TEXT,
        archived BOOLEAN,
        disabled BOOLEAN,
        open_issues_count INTEGER,
        license TEXT,
        allow_forking BOOLEAN,
        is_template BOOLEAN,
        web_commit_signoff_required BOOLEAN,
        topics JSON,
        visibility TEXT,
        forks INTEGER,
        open_issues INTEGER,
        watchers INTEGER,
        default_branch TEXT
    )
    ''')

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS issues (
        id INTEGER NOT NULL PRIMARY KEY,
        url TEXT,
        repository_id INTEGER,
        repository_url TEXT,        
        node_id TEXT,
        number INTEGER,
        title TEXT,
        "user" TEXT,
        labels JSON,
        state TEXT,
        locked BOOLEAN,
        assignee TEXT,
        assignees JSON,
        milestone TEXT,
        comments INTEGER,
        created_at TIMESTAMP WITH TIME ZONE,
        updated_at TIMESTAMP WITH TIME ZONE,
        closed_at TIMESTAMP WITH TIME ZONE,
        author_association TEXT,
        active_lock_reason TEXT,
        body TEXT,
        reactions JSON,
        state_reason TEXT
    )
    ''')

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS comments (
        id INTEGER NOT NULL PRIMARY KEY,
        node_id TEXT,
        url TEXT,
        issue_id INTEGER,
        issue_url TEXT,
        "user" TEXT,
        created_at TIMESTAMP WITH TIME ZONE,
        updated_at TIMESTAMP WITH TIME ZONE,
        author_association TEXT,
        body TEXT,
        reactions JSON
    )
    ''')
   
    conn.commit()

def insert_organization_data(conn, id, login, node_id, description):
    cursor = conn.cursor()
    cursor.execute(f'''
    INSERT INTO organizations (id, login, node_id, description) 
    VALUES ({PLACE_HOLDER}, {PLACE_HOLDER}, {PLACE_HOLDER}, {PLACE_HOLDER})
    ON CONFLICT(id) DO UPDATE SET 
        login = EXCLUDED.login, 
        node_id = EXCLUDED.node_id,
        description = EXCLUDED.description
    ''', 
    (id, login, node_id, description))
    conn.commit()

def insert_user_data(conn, id, login, node_id, type):
    cursor = conn.cursor()
    cursor.execute(f'''
    INSERT INTO users (id, login, node_id, type) 
    VALUES ({PLACE_HOLDER}, {PLACE_HOLDER}, {PLACE_HOLDER}, {PLACE_HOLDER})
    ON CONFLICT(id) DO UPDATE SET 
        login = EXCLUDED.login, 
        node_id = EXCLUDED.node_id, 
        type = EXCLUDED.type
    ''', 
    (id, login, node_id, type))
    conn.commit()

def insert_repository_data(conn, id, node_id, name, full_name, private, owner, owner_type, owner_id, html_url, description, fork, url, created_at, updated_at, pushed_at, homepage, size, stargazers_count, watchers_count, language, has_issues, has_projects, has_downloads, has_wiki, has_pages, has_discussions, forks_count, mirror_url, archived, disabled, open_issues_count, license, allow_forking, is_template, web_commit_signoff_required, topics, visibility, forks, open_issues, watchers, default_branch):
    cursor = conn.cursor()
    cursor.execute(f'''
    INSERT INTO repositories (id, node_id, name, full_name, private, owner, owner_type, owner_id, html_url, description, fork, url, created_at, updated_at, pushed_at, homepage, size, stargazers_count, watchers_count, language, has_issues, has_projects, has_downloads, has_wiki, has_pages, has_discussions, forks_count, mirror_url, archived, disabled, open_issues_count, license, allow_forking, is_template, web_commit_signoff_required, topics, visibility, forks, open_issues, watchers, default_branch) 
    VALUES ({PLACE_HOLDER}, {PLACE_HOLDER}, {PLACE_HOLDER}, {PLACE_HOLDER}, {PLACE_HOLDER}, {PLACE_HOLDER}, {PLACE_HOLDER}, {PLACE_HOLDER}, {PLACE_HOLDER}, {PLACE_HOLDER}, {PLACE_HOLDER}, {PLACE_HOLDER}, {PLACE_HOLDER}, {PLACE_HOLDER}, {PLACE_HOLDER}, {PLACE_HOLDER}, {PLACE_HOLDER}, {PLACE_HOLDER}, {PLACE_HOLDER}, {PLACE_HOLDER}, {PLACE_HOLDER}, {PLACE_HOLDER}, {PLACE_HOLDER}, {PLACE_HOLDER}, {PLACE_HOLDER}, {PLACE_HOLDER}, {PLACE_HOLDER}, {PLACE_HOLDER}, {PLACE_HOLDER}, {PLACE_HOLDER}, {PLACE_HOLDER}, {PLACE_HOLDER}, {PLACE_HOLDER}, {PLACE_HOLDER}, {PLACE_HOLDER}, {PLACE_HOLDER}, {PLACE_HOLDER}, {PLACE_HOLDER}, {PLACE_HOLDER}, {PLACE_HOLDER}, {PLACE_HOLDER})
    ON CONFLICT(id) DO UPDATE SET
        node_id = EXCLUDED.node_id,
        name = EXCLUDED.name,
        full_name = EXCLUDED.full_name,
        private = EXCLUDED.private,
        owner = EXCLUDED.owner,
        owner_type = EXCLUDED.owner_type,
        owner_id = EXCLUDED.owner_id,
        html_url = EXCLUDED.html_url,
        description = EXCLUDED.description,
        fork = EXCLUDED.fork,
        url = EXCLUDED.url,
        created_at = EXCLUDED.created_at,
        updated_at = EXCLUDED.updated_at,
        pushed_at = EXCLUDED.pushed_at,
        homepage = EXCLUDED.homepage,
        size = EXCLUDED.size,
        stargazers_count = EXCLUDED.stargazers_count,
        watchers_count = EXCLUDED.watchers_count,
        language = EXCLUDED.language,
        has_issues = EXCLUDED.has_issues,
        has_projects = EXCLUDED.has_projects,
        has_downloads = EXCLUDED.has_downloads,
        has_wiki = EXCLUDED.has_wiki,
        has_pages = EXCLUDED.has_pages,
        has_discussions = EXCLUDED.has_discussions,
        forks_count = EXCLUDED.forks_count,
        mirror_url = EXCLUDED.mirror_url,
        archived = EXCLUDED.archived,
        disabled = EXCLUDED.disabled,
        open_issues_count = EXCLUDED.open_issues_count,
        license = EXCLUDED.license,
        allow_forking = EXCLUDED.allow_forking,
        is_template = EXCLUDED.is_template,
        web_commit_signoff_required = EXCLUDED.web_commit_signoff_required,
        topics = EXCLUDED.topics,
        visibility = EXCLUDED.visibility,
        forks = EXCLUDED.forks,
        open_issues = EXCLUDED.open_issues,
        watchers = EXCLUDED.watchers,
        default_branch = EXCLUDED.default_branch
    ''', 
    (id, node_id, name, full_name, private, owner, owner_type, owner_id, html_url, description, fork, url, created_at, updated_at, pushed_at, homepage, size, stargazers_count, watchers_count, language, has_issues, has_projects, has_downloads, has_wiki, has_pages, has_discussions, forks_count, mirror_url, archived, disabled, open_issues_count, license, allow_forking, is_template, web_commit_signoff_required, topics, visibility, forks, open_issues, watchers, default_branch))
    conn.commit()

def insert_issue_data(conn, id, url, repository_id, repository_url, node_id, number, title, user, labels, state, locked, assignee, assignees, milestone, comments, created_at, updated_at, closed_at, author_association, active_lock_reason, body, reactions, state_reason):
    cursor = conn.cursor()
    cursor.execute(f'''
    INSERT INTO issues (id, url, repository_id, repository_url, node_id, number, title, user, labels, state, locked, assignee, assignees, milestone, comments, created_at, updated_at, closed_at, author_association, active_lock_reason, body, reactions, state_reason) 
    VALUES ({PLACE_HOLDER}, {PLACE_HOLDER}, {PLACE_HOLDER}, {PLACE_HOLDER}, {PLACE_HOLDER}, {PLACE_HOLDER}, {PLACE_HOLDER}, {PLACE_HOLDER}, {PLACE_HOLDER}, {PLACE_HOLDER}, {PLACE_HOLDER}, {PLACE_HOLDER}, {PLACE_HOLDER}, {PLACE_HOLDER}, {PLACE_HOLDER}, {PLACE_HOLDER}, {PLACE_HOLDER}, {PLACE_HOLDER}, {PLACE_HOLDER}, {PLACE_HOLDER}, {PLACE_HOLDER}, {PLACE_HOLDER}, ?)
    ON CONFLICT(id) DO UPDATE SET 
        url = EXCLUDED.url,
        repository_id = EXCLUDED.repository_id,
        repository_url = EXCLUDED.repository_url,
        node_id = EXCLUDED.node_id,
        number = EXCLUDED.number,
        title = EXCLUDED.title,
        user = EXCLUDED.user,
        labels = EXCLUDED.labels,
        state = EXCLUDED.state,
        locked = EXCLUDED.locked,
        assignee = EXCLUDED.assignee,
        assignees = EXCLUDED.assignees,
        milestone = EXCLUDED.milestone,
        comments = EXCLUDED.comments,
        created_at = EXCLUDED.created_at,
        updated_at = EXCLUDED.updated_at,
        closed_at = EXCLUDED.closed_at,
        author_association = EXCLUDED.author_association,
        active_lock_reason = EXCLUDED.active_lock_reason,
        body = EXCLUDED.body,
        reactions = EXCLUDED.reactions,
        state_reason = EXCLUDED.state_reason
    ''', 
    (id, url, repository_id, repository_url, node_id, number, title, user, labels, state, locked, assignee, assignees, milestone, comments, created_at, updated_at, closed_at, author_association, active_lock_reason, body, reactions, state_reason))
    conn.commit()

def get_max_id(conn, table_name):
    """
    Fetches the maximum id for a given table.
    """
    cursor = conn.cursor()
    cursor.execute(f"SELECT MAX(id) FROM {table_name}")
    result = cursor.fetchone()
    if result:
        return result[0] or 0
    return 0
