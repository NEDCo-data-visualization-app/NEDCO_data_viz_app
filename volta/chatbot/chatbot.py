'''
This chatbot works by taking in the user input, generating code, and then executes the code locally.
'''

import pandas as pd
from dotenv import load_dotenv
import os
from langchain_openai.chat_models import ChatOpenAI
from langchain.schema import HumanMessage

load_dotenv()

api_key = os.getenv("OPENAI_API_KEY")

df = pd.read_csv("/Users/srinandham/Downloads/NEDCO_data_viz_app/uploads/wkfile_shiny.csv") 
print(df["chargedate"].min(), df["chargedate"].max()) #Use this as example operation for verifying LLM output

'''
Working models from OpenRouter: 
nvidia/nemotron-nano-9b-v2:free, 
groq-4q-free..., 
openai/gpt-oss-20b:free
'''


llm = ChatOpenAI(
    model="nvidia/nemotron-nano-9b-v2:free",
    base_url="https://openrouter.ai/api/v1",
    temperature=0
)

prompt_template = """
You are given a pandas dataframe named `df` with columns: {columns}.
Write Python code using ONLY this dataframe to answer the question: {question}
Return the code as plain text wrapped with print(). Do NOT provide explanations.
"""

def generate_code(question):
    prompt_text = prompt_template.format(question=question, columns=", ".join(df.columns))
    response = llm([HumanMessage(content=prompt_text)])
    return response.content if hasattr(response, 'content') else response[0].content

while True:
    question = input("Enter your query: ")
    if question.lower() in ["exit", "quit"]:
        break
    
    code_to_run = generate_code(question)
    print("Generated code:\n", code_to_run)
    
    try:
        local_vars = {"df": df}
        exec(code_to_run, {}, local_vars)
    except Exception as e:
        print("Error executing code:", e)