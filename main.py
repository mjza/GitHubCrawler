from dotenv import load_dotenv
from colored import fg, attr

def read_integer(prompt_message):
    red = fg('red')
    reset = attr('reset')
    while True:
        user_input = input(prompt_message)
        try:
            return int(user_input)
        except ValueError:
            print(f"{red}Please enter a valid integer.{reset}")

def read_yes_no(prompt_message):
    """
    Prompts the user with the given message and expects a 'Y' or 'N' input.
    Returns True for 'Y' and False for 'N'.
    """
    green = fg('green')
    red = fg('red')
    reset = attr('reset')    
    while True:
        user_input = input(prompt_message + f"{green} (Y/N): {reset}").strip().upper()
        if user_input == 'Y':
            return True
        elif user_input == 'N':
            return False
        else:
            print(f"{red}Invalid input. Please enter 'Y' for Yes or 'N' for No.{reset}")

def main():
    from api import fetch_organizations, fetch_users, fetch_user_repositories
    
    blue = fg('blue')
    green = fg('green')
    reset = attr('reset')
    running = True   

    while running:
        print(f"{blue}Welcome to the Stackoverflow CLI!")
        print("Available commands:")
        print("0. Exit")
        print("1. Fetch Organizations")
        print("2. Fetch Users")
        print("3. Fetch Repos")
        print("4. Fetch Issues")
        print("5. Fetch Comments")
        command = input(f"Enter a command number: {reset}").strip()

        if command == "0":
            print(f"{green}Exiting the application. Goodbye!{reset}")
            running = False
        elif command == "1":
            print(f"{green}Fetching organizations...{reset}")          
            fetch_organizations()
            print(f"{green}Successfully fetched all organizations.{reset}")
        elif command == "2":
            print(f"{green}Fetching users...{reset}")          
            fetch_users()
            print(f"{green}Successfully fetched all users.{reset}")
        elif command == "3":
            print(f"{green}Fetching repositories...{reset}")          
            fetch_user_repositories()
            print(f"{green}Successfully fetched all repositories.{reset}")    
        else:
            print(f"{green}Unknown command number. Please try again.{reset}")  

if __name__ == '__main__':
    # Load environment variables from .env file
    load_dotenv()
    
    # show the main menu
    main()
