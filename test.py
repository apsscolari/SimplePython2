# APS

import os
from dotenv import load_dotenv

#to trigger secret scanning
user = 'test'
password = 'Password1234'

google_api_token = "AIzaSyAQfxPJiounkhOjODEO5ZieffeBv6yft2Q"
gh_PAT = "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"

# main
if __name__ == '__main__':

    print('hello Github world')

# critical vuln example
user_input = input("Enter filename: ")
with open(user_input, 'r') as file:  # Vulnerable to directory traversal
    content = file.read()
