# Import libraries
import requests
from bs4 import BeautifulSoup

# Define target URL (replace with the desired website)
target_url = "https://www.example.com"

# Send an HTTP request and get the HTML content
response = requests.get(target_url)

# Check for successful response
if response.status_code == 200:
  # Parse the HTML content
  soup = BeautifulSoup(response.text, 'html.parser')

  # Print the first paragraph (for demonstration purposes)
  print(soup.find('p').text)
else:
  print("Failed to retrieve the website content.")
