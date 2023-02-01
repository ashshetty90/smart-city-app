import os

root_dir = os.path.split(os.path.abspath(os.path.dirname(__file__)))[0]
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.join(root_dir, 'credentials', "google-api-key.json")
