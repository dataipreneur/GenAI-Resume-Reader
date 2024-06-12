import pandas as pd
from openai import OpenAI
import singlestoredb as db
import os
from pypdf import PdfReader
from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain.document_loaders import PyPDFLoader
import openai


#Saving openai api key as environment variable 
os.environ['OPENAI_API_KEY'] = 'key'
client = OpenAI()


#Function to create openai embeddings
def get_embedding(chunk, model = 'text-embedding-3-large'):
   
    chunk = chunk.replace('\n', " ")
    return client.embeddings.create(input = chunk, model=model).data[0].embedding
    
#Defining the connector
def connector():
    return db.connect(host='#.singlestore.com', port='3333', user='user',
                  password='password', database='database_65de5')


#Creation of path list. The data_copy folder comnsists of different job rols categotries. I selected 2 out of every category for this project.
def path_file_creation(main_folder):
    

    path_list = []

    for dirname, _, filenames in os.walk(main_folder):
        for filename in filenames:
            
            if filename[-3:] == 'pdf':
                directories = dirname.split('/')[-1]
                path_list.append(os.path.join(dirname, filename))
    
    return path_list
   

# PDF To text coverter: Returns PDF loader format and the content
def pdf_to_text(file_path):
    
    
    reader = PdfReader(file_path)
    
    pages = reader.pages

    content = ''
   
    for page in pages:
        
        text = page.extract_text()
        content+=(text)

    loader = PyPDFLoader(file_path)
    doc = loader.load()
    return (content, doc)



# split documents into text and embeddings
def resume_chunk(text):


    text_splitter = RecursiveCharacterTextSplitter(
        chunk_size=1000, 
        chunk_overlap=150

        )
    chunk_list = text_splitter.split_documents(text)
    return chunk_list



#Insert the required values in database   
def insert_vector(chunk, resume_text, file_path, vector):
    
    id = 0
    try:
        mydb = connector()
        mycursor = mydb.cursor()
        sql = 'INSERT INTO myresume2(chunk_name, resume_text, file_path, vector) values (%s, %s, %s, JSON_ARRAY_PACK(%s))'
        mycursor.executemany(sql, [(chunk, resume_text, file_path, str(vector))])
        id = mycursor.lastrowid
        mydb.commit()
    except Exception as e:
        print(e)
        pass
    try:
        mydb.close()
    except:
        pass
    return id



#Read values based on serach query from DB
def read_vectors(vector):

    
    output = []
    try:
        mydb = connector()
        mycursor = mydb.cursor()
        sql = "SELECT dot_product(json_array_pack(%s), vector) as score, resume_text, file_path, chunk_name, JSON_ARRAY_UNPACK(vector) as vector FROM myresume2 order by score desc limit 5"
        mycursor.execute(sql, (str(vector)))
        result = mycursor.fetchall()
        for (score, resume_text, file_path, chunk_name, vector) in result:
            output.append({

                #'vector': vector,
                'score': score,
                #'chunk_name': chunk_name,
                'file_name': file_path.split('/')[-1],
                #'file_path':file_path,
                'resume_text': resume_text

            })
        mydb.close()
    except Exception as e:
        print(e)
    
    return output


#Insertion of values into a DB created in singlestore
def context_creation(chunk_list, resume_text, file_name):


    # Create an array of text to embed
    context_array = []
    for i, row in enumerate(chunk_list):
        chunk = row.page_content
        embedding = get_embedding(chunk)
        id = insert_vector(chunk, resume_text, file_name, embedding)
    return id


#Vector DB creation
# Manual Embedding and insertion into db:
def create_embeddings_db(file_path_list):

    

    embeddings = []

    for i in file_path_list:
        text,doc = pdf_to_text(i)
        chunk_list = resume_chunk(doc)
        id = context_creation(chunk_list, text, i)
    return id



def db_creation(main_folder_path):
  

    #path list generation
    path_list = path_file_creation(main_folder_path)
    id = create_embeddings_db(path_list)

    return id


def vector_search():

    text = 'Power Point'
    vector = get_embedding(text)
    print(read_vectors(vector))
    


if __name__ == "__main__":

    #Following function will read csv and store data with vectors
    #db_creation('/Users/priestleyfernandes/Desktop/HCS_genai_resume_reader/data_copy')

    #Following function will help to search the nearest review using vectors
    vector_search()