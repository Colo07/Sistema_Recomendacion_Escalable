from dotenv import load_dotenv
import os

load_dotenv()

AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
S3_BUCKET=os.getenv('S3_BUCKET')
MODEL_PATH=os.getenv('MODEL_PATH')
EMBEDDINGS_PATH=os.getenv('EMBEDDINGS_PATH')
PROCCESED_PRODUCTS=os.getenv('PROCCESED_PRODUCTS')
PROCCESED_TRANSACTIONS=os.getenv('PROCCESED_TRANSACTIONS')
PROCCESED_USERS=os.getenv('PROCCESED_USERS')