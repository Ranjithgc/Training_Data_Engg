from slack import WebClient
import os
from dotenv import load_dotenv
from slack.errors import SlackApiError

load_dotenv()

slack_token = os.getenv('ACCESS_TOKEN')

client = WebClient(token=slack_token)
try:
    
    response = client.chat_postEphemeral(channel='practice', text='This is only a test.',user='ranjith.gc')

except SlackApiError as e:
  # You will get a SlackApiError if "ok" is False
  assert e.response["error"] 