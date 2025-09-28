"Testing an agent"
import pandas as pd
from langchain_openai.chat_models import ChatOpenAI
from langchain_experimental.agents import create_pandas_dataframe_agent
from langchain_experimental.tools.python.tool import PythonREPLTool

df = pd.read_csv("/Users/srinandham/Downloads/NEDCO_data_viz_app/wkfile_shiny.csv") 

llm = ChatOpenAI(
    model="x-ai/grok-4-fast:free", 
    api_key="key",
    base_url="https://openrouter.ai/api/v1",
    temperature=0
)

tools = [PythonREPLTool()]

agent = create_pandas_dataframe_agent(llm, df, verbose=True, allow_dangerous_code=True, handle_parsing_errors=True)

while True:
    query = "What is total mean across all locations? "
    if query.lower() in ["exit", "quit"]:
        break
    result = agent.invoke(query)
    print("Result:\n", result)