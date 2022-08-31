"""Load .env file into os.environ."""
import os
from dotenv import load_dotenv

LOCAL = False

if os.path.isfile('.env' ):
    load_dotenv('.env')
    LOCAL = True
    