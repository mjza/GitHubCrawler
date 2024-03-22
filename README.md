# Github Crawler
A project for extracting issues from GitHub for my Ph.D. research

## Creating the Virtual Environment
First, navigate to your project's root directory in your terminal. Then, create a virtual environment named venv (or another name of your choice) by running:

```
python -m venv venv
```

This command creates a new directory named venv in your project directory, which contains a copy of the Python interpreter, the standard library, and various supporting files.

## Activating the Virtual Environment
Before you can start installing packages, you need to activate the virtual environment. 
Activation will ensure that the Python interpreter and tools within the virtual environment are used in preference to the system-wide Python installation.

1. **On macOS and Linux:**

```
source venv/bin/activate
```

2. **On Windows (cmd.exe):**

```
.\venv\Scripts\activate.bat
```

3. **On Windows (PowerShell) or VSC Terminal:**

```
.\venv\Scripts\Activate.ps1
```

Once activated, your terminal prompt must change to indicate that the virtual environment is active.

## Installing Dependencies
With the virtual environment activated, install the required packages. Ensure you have the following installed in your Python environment. If not, you can install them using pip:

```
pip install requests
pip install bs4
pip install python-dotenv
pip install colored
pip install psycopg2-binary
```

## Configuration
Copy and paste the `template.env` file and rename the new file to `.env`. 
Edit the `.env` file as following:
1. Make a Personal Access Token and copy and paste the token as `PAT_KEY`.
   
    To generate your Github personal access token:
    1. Visit the [Personal Access Tokens](https://github.com/settings/tokens?type=beta) ↗ section on Github
    2. Click on Generate new token
    3. Fill in the token name, expiry and resource owner
    4. Carefully review and grant the necessary fine grained permissions to your token
    5. Use the generated token here
2. Make sure that you sellect the right DBMS type. If you want Sqlite the keep `SQLITE` otherwise if you want Postgres keep `POSTGRES`
3. `DB_PATH` is needed for Sqlite and other `DB_*` attributes are needed for Postgres databases. 

## Run
Run the `main.py` file to run the program. 
```
python main.py
```