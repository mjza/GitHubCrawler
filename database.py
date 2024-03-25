import os
import sqlite3
import psycopg2
import json

DBMS = os.getenv('DBMS')

# Determine the place holder
if DBMS == 'SQLITE':
    PH = "?"
elif DBMS == 'POSTGRES':
    PH = "%s"
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
            avatar_url TEXT,
            gravatar_id TEXT,
            url TEXT,
            html_url TEXT,
            type TEXT,
            site_admin BOOLEAN,
            name TEXT,
            company TEXT,
            blog TEXT,
            location TEXT,
            email TEXT,
            hireable BOOLEAN,
            bio TEXT,
            twitter_username TEXT,
            public_repos INTEGER,
            public_gists INTEGER,
            followers INTEGER,
            following INTEGER,
            created_at TEXT,
            updated_at TEXT,
            error BOOLEAN NOT NULL DEFAULT FALSE
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
        license JSON,
        allow_forking BOOLEAN,
        is_template BOOLEAN,
        web_commit_signoff_required BOOLEAN,
        topics JSON,
        visibility TEXT,
        forks INTEGER,
        open_issues INTEGER,
        watchers INTEGER,
        default_branch TEXT,
        permissions JSON
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

def insert_organization_data(conn, org_data):
    cursor = conn.cursor()
    sql = f'''
    INSERT INTO organizations 
    (id, login, node_id, description) 
    VALUES ({PH}, {PH}, {PH}, {PH})
    ON CONFLICT(id) DO UPDATE SET 
        login = EXCLUDED.login, 
        node_id = EXCLUDED.node_id,
        description = EXCLUDED.description
    '''
    cursor.execute(sql, (
        org_data.get('id'), 
        org_data.get('login'), 
        org_data.get('node_id'), 
        org_data.get('description')
    ))
    conn.commit()

def insert_user_data(conn, user_data):
    cursor = conn.cursor()
    sql = f'''
    INSERT INTO users (
        id, login, node_id, type, avatar_url, gravatar_id, url, html_url,
        site_admin, name, company, blog, location, email, hireable, bio, 
        twitter_username, public_repos, public_gists, followers, following, 
        created_at, updated_at, error
    ) 
    VALUES ({PH}, {PH}, {PH}, {PH}, {PH}, {PH}, {PH}, {PH}, {PH}, {PH}, {PH}, {PH}, {PH}, {PH}, {PH}, {PH}, {PH}, {PH}, {PH}, {PH}, {PH}, {PH}, {PH}, {PH})
    ON CONFLICT(id) DO UPDATE SET 
        login = EXCLUDED.login, 
        node_id = EXCLUDED.node_id, 
        type = EXCLUDED.type,
        avatar_url = EXCLUDED.avatar_url, 
        gravatar_id = EXCLUDED.gravatar_id, 
        url = EXCLUDED.url, 
        html_url = EXCLUDED.html_url,
        site_admin = EXCLUDED.site_admin, 
        name = EXCLUDED.name, 
        company = EXCLUDED.company, 
        blog = EXCLUDED.blog, 
        location = EXCLUDED.location, 
        email = EXCLUDED.email, 
        hireable = EXCLUDED.hireable, 
        bio = EXCLUDED.bio, 
        twitter_username = EXCLUDED.twitter_username, 
        public_repos = EXCLUDED.public_repos, 
        public_gists = EXCLUDED.public_gists, 
        followers = EXCLUDED.followers, 
        following = EXCLUDED.following, 
        created_at = EXCLUDED.created_at, 
        updated_at = EXCLUDED.updated_at,
        error = EXCLUDED.error
    '''
    cursor.execute(sql, (
        user_data.get('id'), user_data.get('login'), user_data.get('node_id'), user_data.get('type'), 
        user_data.get('avatar_url'), user_data.get('gravatar_id'), user_data.get('url'), user_data.get('html_url'), 
        user_data.get('site_admin'), user_data.get('name'), user_data.get('company'), user_data.get('blog'),
        user_data.get('location'), user_data.get('email'), user_data.get('hireable'), user_data.get('bio'), 
        user_data.get('twitter_username'), user_data.get('public_repos'), user_data.get('public_gists'),
        user_data.get('followers'), user_data.get('following'), user_data.get('created_at'), 
        user_data.get('updated_at'), user_data.get('error', False)
    ))
    conn.commit()


def insert_repository_data(conn, repo_data):
    cursor = conn.cursor()
    sql = f'''
    INSERT INTO repositories (
        id, node_id, name, full_name, private, owner, owner_type, owner_id, html_url, description, fork, url, created_at, updated_at, pushed_at, homepage, 
        size, stargazers_count, watchers_count, language, has_issues, has_projects, has_downloads, has_wiki, has_pages, has_discussions, forks_count, 
        mirror_url, archived, disabled, open_issues_count, license, allow_forking, is_template, web_commit_signoff_required, 
        topics, visibility, forks, open_issues, watchers, default_branch, permissions
    ) 
    VALUES ({PH}, {PH}, {PH}, {PH}, {PH}, {PH}, {PH}, {PH}, {PH}, {PH}, {PH}, {PH}, {PH}, {PH}, {PH}, {PH}, {PH}, {PH}, {PH}, {PH}, {PH}, {PH}, {PH}, {PH}, {PH}, {PH}, {PH}, {PH}, {PH}, {PH}, {PH}, {PH}, {PH}, {PH}, {PH}, {PH}, {PH}, {PH}, {PH}, {PH}, {PH}, {PH})
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
        default_branch = EXCLUDED.default_branch,
        permissions = EXCLUDED.permissions
    '''
    cursor.execute(sql, (
        repo_data.get('id'), repo_data.get('node_id'), repo_data.get('name'), repo_data.get('full_name'), repo_data.get('private'), 
        repo_data.get('owner'), repo_data.get('owner_type'), repo_data.get('owner_id'), repo_data.get('html_url'), repo_data.get('description'), 
        repo_data.get('fork'), repo_data.get('url'), repo_data.get('created_at'), repo_data.get('updated_at'), repo_data.get('pushed_at'), 
        repo_data.get('homepage'), repo_data.get('size'), repo_data.get('stargazers_count'), repo_data.get('watchers_count'), 
        repo_data.get('language'), repo_data.get('has_issues'), repo_data.get('has_projects'), repo_data.get('has_downloads'), 
        repo_data.get('has_wiki'), repo_data.get('has_pages'), repo_data.get('has_discussions'), repo_data.get('forks_count'), 
        repo_data.get('mirror_url'), repo_data.get('archived'), repo_data.get('disabled'), repo_data.get('open_issues_count'), 
        json.dumps(repo_data.get('license', {})), repo_data.get('allow_forking'), repo_data.get('is_template'), repo_data.get('web_commit_signoff_required'), 
        json.dumps(repo_data.get('topics', [])), repo_data.get('visibility'), repo_data.get('forks'), repo_data.get('open_issues'), 
        repo_data.get('watchers'), repo_data.get('default_branch'), json.dumps(repo_data.get('permissions', {}))
    ))
    conn.commit()

def insert_issue_data(conn, issue_data):
    cursor = conn.cursor()
    sql = f'''
    INSERT INTO issues 
    (id, url, repository_id, repository_url, node_id, number, title, user, labels, state, locked, assignee, assignees, milestone, comments, created_at, updated_at, closed_at, author_association, active_lock_reason, body, reactions, state_reason) 
    VALUES ({PH}, {PH}, {PH}, {PH}, {PH}, {PH}, {PH}, {PH}, {PH}, {PH}, {PH}, {PH}, {PH}, {PH}, {PH}, {PH}, {PH}, {PH}, {PH}, {PH}, {PH}, {PH}, {PH})
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
    '''
    cursor.execute(sql, (
        issue_data.get('id'), issue_data.get('url'), issue_data.get('repository_id'), issue_data.get('repository_url'), 
        issue_data.get('node_id'), issue_data.get('number'), issue_data.get('title'), issue_data.get('user'), 
        issue_data.get('labels'), issue_data.get('state'), issue_data.get('locked'), issue_data.get('assignee'), 
        issue_data.get('assignees'), issue_data.get('milestone'), issue_data.get('comments'), issue_data.get('created_at'), 
        issue_data.get('updated_at'), issue_data.get('closed_at'), issue_data.get('author_association'), 
        issue_data.get('active_lock_reason'), issue_data.get('body'), issue_data.get('reactions'), issue_data.get('state_reason')
    ))
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

def fetch_users_batch(conn, last_owner_id=0, batch_size=100):
    """
    Fetches a batch of users from the database whose ID is greater than the last maximum owner_id
    found in the repositories table and greater than any previously processed user ID.
    
    Args:
        conn: Database connection object.
        last_owner_id: The maximum owner_id processed in the last batch.
        batch_size: Number of users to fetch per batch.
    
    Returns:
        A list of dictionaries, each representing a user.
    """
    cursor = conn.cursor()

    # First, find the current maximum owner_id in the repositories table
    cursor.execute("SELECT MAX(owner_id) FROM repositories")
    max_owner_id = cursor.fetchone()[0] or 0
    max_id_to_fetch = max(last_owner_id, max_owner_id)

    # Fetch users whose ID is greater than max_id_to_fetch
    sql = f"""
        SELECT id, login, type FROM users 
        WHERE id > {PH} 
        ORDER BY id ASC 
        LIMIT {PH}
        """
    cursor.execute(sql, (max_id_to_fetch, batch_size))

    users = cursor.fetchall()
    return [{'id': user[0], 'login': user[1], 'type': user[2]} for user in users]

def fetch_users_batch(conn, last_owner_id=0, batch_size=100):
    """
    Fetches a batch of users from the database whose ID is greater than the last maximum owner_id
    found in the repositories table and greater than any previously processed user ID.
    
    Args:
        conn: Database connection object.
        last_owner_id: The maximum owner_id processed in the last batch.
        batch_size: Number of users to fetch per batch.
    
    Returns:
        A list of dictionaries, each representing a user.
    """
    cursor = conn.cursor()

    # First, find the current maximum owner_id in the repositories table
    cursor.execute("SELECT MAX(owner_id) FROM repositories")
    max_owner_id = cursor.fetchone()[0] or 0
    max_id_to_fetch = max(last_owner_id, max_owner_id)

    # Fetch users whose ID is greater than max_id_to_fetch
    sql = f"""
        SELECT id, login, type FROM users 
        WHERE id > {PH} 
        ORDER BY id ASC 
        LIMIT {PH}
        """
    cursor.execute(sql, (max_id_to_fetch, batch_size))

    users = cursor.fetchall()
    return [{'id': user[0], 'login': user[1], 'type': user[2]} for user in users]

def fetch_organizations_batch(conn, last_owner_id=0, batch_size=100):
    """
    Fetches a batch of orgs from the database whose ID is greater than the last maximum owner_id
    found in the repositories table and greater than any previously processed user ID.
    
    Args:
        conn: Database connection object.
        last_owner_id: The maximum owner_id processed in the last batch.
        batch_size: Number of orgs to fetch per batch.
    
    Returns:
        A list of dictionaries, each representing a org.
    """
    cursor = conn.cursor()

    # First, find the current maximum owner_id in the repositories table
    cursor.execute("SELECT MAX(owner_id) FROM repositories")
    max_owner_id = cursor.fetchone()[0] or 0
    max_id_to_fetch = max(last_owner_id, max_owner_id)

    # Fetch orgs whose ID is greater than max_id_to_fetch
    sql = f"""
        SELECT id, login FROM organizations 
        WHERE id > {PH} 
        ORDER BY id ASC 
        LIMIT {PH}
        """
    cursor.execute(sql, (max_id_to_fetch, batch_size))

    orgs = cursor.fetchall()
    return [{'id': org[0], 'login': org[1], 'type': 'Organization'} for org in orgs]