import re
import html2text
import requests
from bs4 import BeautifulSoup

def extract_question_details(question_url, headers, answer_count):
    """
    Fetches and extracts details from a given question URL. If the URL is None, returns an error message.
    
    Parameters:
    - question_url: The URL of the question to fetch details for.
    - headers: A dictionary of HTTP headers to use for the request.
    
    Returns:
    - A tuple of (title, body) if successful, with 'Error' as title if question_url is None or any issue arises.
    """
    # Check if the question_url is None
    if question_url is None:
        return "Error", "The question URL was not provided.", None

    try:       
        response = requests.get(question_url, headers=headers)
        soup = BeautifulSoup(response.content, 'html.parser')
        
        title_tag = soup.find('h1')
        title = title_tag.text.strip() if title_tag else "Title not found"
        
        text_maker = html2text.HTML2Text()
        text_maker.ignore_links = False
        text_maker.ignore_images = True
        text_maker.ignore_emphasis = False
        text_maker.ignore_tables = False
        text_maker.mark_code = True
        text_maker.body_width = 0
        
        question_body = soup.find('div', class_='s-prose js-post-body')
        question_marke_down = text_maker.handle(str(question_body)) if question_body else "Question body not found"
        question_marke_down = re.sub(r'^\[code\]\s*$', '```', question_marke_down, flags=re.MULTILINE)
        question_marke_down = re.sub(r'^\[/code\]\s*$', '```', question_marke_down, flags=re.MULTILINE)
        
        extracted_answers = extract_all_answers(soup) if answer_count > 0 else {}
        
        return title, question_marke_down, extracted_answers
    except Exception as e:
        print(f"An error occurred while fetching question details: {str(e)}")
        return "Error", str(e), None

def extract_all_answers(soup):
    """
    Fetches and extracts all answers from a given question URL.
    
    Parameters:
    - soup: BeautifulSoup object parsed from the question page.
    
    Returns:
    - A dictionary with answer IDs as keys and their markdown body as values.
    - Returns {"0": "Error: The question URL was not provided."} if soup is None or any issue arises.
    """
    if soup is None:
        return {"0": "Error: The BeautifulSoup object was not provided."}
    
    try:
        post_layouts = soup.find_all('div', class_='post-layout')
        extracted_answers = {}
        
        for layout in post_layouts:
            vote_cell = layout.find('div', class_='js-voting-container')
            if vote_cell:
                answer_id = vote_cell.get('data-post-id', "0")
                
                text_maker = html2text.HTML2Text()
                text_maker.ignore_links = False
                text_maker.ignore_images = True
                text_maker.ignore_emphasis = False
                text_maker.ignore_tables = False
                text_maker.mark_code = True
                text_maker.body_width = 0
                
                answer_body = layout.find('div', class_='s-prose js-post-body')
                answer_markdown = text_maker.handle(str(answer_body)) if answer_body else "Answer body not found"
                answer_markdown = re.sub(r'^\[code\]\s*$', '```', answer_markdown, flags=re.MULTILINE)
                answer_markdown = re.sub(r'^\[/code\]\s*$', '```', answer_markdown, flags=re.MULTILINE)
                
                extracted_answers[answer_id] = answer_markdown
        
        return extracted_answers if extracted_answers else {"0": "Error: No answers found."}
    except Exception as e:
        print(f"An error occurred while fetching answer details: {str(e)}")
        return {"0": f"Error: {str(e)}"}
