
This module handles all interactions with the language model for the litanai project.


import json
import subprocess
from openai import OpenAI
from config import OPENAI_API_KEY_COMMAND, LLM_MODEL

def get_openai_client():
    """Initializes and returns the OpenAI client."""
    try:
        api_key = subprocess.run(OPENAI_API_KEY_COMMAND, shell=True, stdout=subprocess.PIPE, text=True).stdout.strip()
        return OpenAI(api_key=api_key)
    except Exception as e:
        print(f"Error initializing OpenAI client: {e}")
        return None

def query_openai(prompt, text_to_query):
    """
    Sends a query to the OpenAI API and returns the JSON response.
    """
    client = get_openai_client()
    if not client:
        return None

    full_prompt = f"{prompt}\n\n---\n\n{text_to_query}"
    
    try:
        response = client.chat.completions.create(
            messages=[
                {
                    "role": "user",
                    "content": full_prompt,
                }
            ],
            model=LLM_MODEL,
            response_format={"type": "json_object"},
        )
        return json.loads(response.choices[0].message.content)
    except Exception as e:
        print(f"Error querying OpenAI: {e}")
        return None

