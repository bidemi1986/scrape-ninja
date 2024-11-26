# rag.py 
from langchain_community.document_loaders import JSONLoader
from langchain_text_splitters import RecursiveCharacterTextSplitter
from langchain_openai import OpenAIEmbeddings, ChatOpenAI
from langchain_chroma import Chroma
from langchain.chains import create_retrieval_chain
from langchain.chains.combine_documents import create_stuff_documents_chain
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.documents import Document
from dotenv import load_dotenv
from typing import Generator, Dict, Any, List
import logging

from langchain.vectorstores import Chroma
from langchain.embeddings import OpenAIEmbeddings
import os

def initialize_chroma(persist_directory="./chroma_db"):
    """
    Initialize a persistent Chroma vector store.
    
    Args:
        persist_directory: Path to the directory where Chroma data will be stored.
    
    Returns:
        Chroma: An instance of the Chroma vector store.
    """
    if not os.path.exists(persist_directory):
        os.makedirs(persist_directory, exist_ok=True)

    try:
        return Chroma(
            embedding_function=OpenAIEmbeddings(),
            persist_directory=persist_directory
        )
    except Exception as e:
        logging.error(f"Error initializing Chroma: {e}")
        raise


def process_streaming_documents(data_stream: Generator[Dict[str, Any], None, None], 
                              batch_size: int = 10) -> Generator[Document, None, None]:
    """
    Process streaming documents in batches to manage memory.
    
    Args:
        data_stream: Generator yielding document dictionaries
        batch_size: Number of documents to process at once
    """
    for doc in data_stream:
        try:
            if doc and 'content' in doc and 'url' in doc:
                yield Document(
                    page_content=doc['content'],
                    metadata={"source": doc['url']}
                )
        except Exception as e:
            logging.error(f"Error processing document: {e}")
            continue

def setup_and_run_rag_pipeline(question: str, data: Generator[Dict[str, Any], None, None]):
    """
    Set up and run the RAG pipeline with persistent Chroma vector store support.
    
    Args:
        question: The input question to ask the model.
        data: Generator yielding documents from Firebase.
    
    Returns:
        Dict containing the answer and sources.
    """
    try:
        load_dotenv()
        
        # Initialize Chroma vector store
        vectorstore = initialize_chroma()

        # Process streaming documents
        doc_generator = process_streaming_documents(data)
        
        # Split documents into chunks
        text_splitter = RecursiveCharacterTextSplitter(chunk_size=1000, chunk_overlap=200)
        splits = []
        sources = set()

        current_batch = []
        for doc in doc_generator:
            current_batch.append(doc)
            sources.add(doc.metadata.get('source', 'Unknown'))
            
            if len(current_batch) >= 10:
                batch_splits = text_splitter.split_documents(current_batch)
                splits.extend(batch_splits)
                current_batch = []
        
        # Process remaining documents
        if current_batch:
            batch_splits = text_splitter.split_documents(current_batch)
            splits.extend(batch_splits)
        
        if not splits:
            return {
                "answer": "I don't have enough information to answer this question.",
                "sources": []
            }
            
        logging.info(f"Generated {len(splits)} text splits from the streamed data.")
        
        # Add splits to Chroma vector store
        vectorstore.add_documents(splits)
        vectorstore.persist()  # Save changes to disk
        
        # Set up retrieval
        retriever = vectorstore.as_retriever()
        
        # Define the system prompt
        system_prompt = (
            "You are an assistant for question-answering tasks. "
            "Use the following pieces of retrieved context to answer "
            "the question. If you don't know the answer, say that you don't know."
            "\n\n"
            "{context}"
        )
        
        # Create the prompt template
        prompt = ChatPromptTemplate.from_messages([
            ("system", system_prompt),
            ("human", "{input}"),
        ])
        
        # Create and run the chain
        question_answer_chain = create_stuff_documents_chain(
            ChatOpenAI(model="gpt-4o-mini"), prompt
        )
        rag_chain = create_retrieval_chain(retriever, question_answer_chain)
        
        # Get the answer
        results = rag_chain.invoke({"input": question})
        
        # Get relevant sources
        relevant_docs = retriever.get_relevant_documents(question)
        relevant_sources = [doc.metadata.get('source', 'Unknown') for doc in relevant_docs]
        
        return {
            "answer": results['answer'],
            "sources": list(set(relevant_sources))  # Deduplicate sources
        }
        
    except Exception as e:
        logging.error(f"Error in RAG pipeline: {e}")
        return {
            "answer": "An error occurred while processing your question.",
            "sources": []
        }

if __name__ == "__main__":
    # Example of how to test with a simple generator
    def sample_data_generator():
        yield {"content": "Sample content", "url": "http://example.com"}
    
    result = setup_and_run_rag_pipeline(
        question="What is the content?",
        data=sample_data_generator()
    )
    print(result)

# def setup_and_run_rag_pipeline(question: str, file_path='output.json'):
#     """
#     Set up and run the RAG (Retrieval-Augmented Generation) pipeline.

#     :param question: The input question to ask the model.
#     :return: A dictionary with the generated answer and the associated sources.
#     """

#     load_dotenv()

#     # Initialize the LLM
#     llm = ChatOpenAI(model="gpt-4o-mini")

#     # Load the documents
#     loader = JSONLoader(
#         file_path=file_path,
#         jq_schema='.[].content',
#         text_content=False
#     )

#     docs = loader.load()
#     if not docs:
#         raise ValueError("No documents were loaded from the JSON file. Check the file content or jq_schema.")
#     print(f"Loaded {len(docs)} documents.")

#     # Split documents into chunks
#     text_splitter = RecursiveCharacterTextSplitter(chunk_size=1000, chunk_overlap=200)
#     splits = text_splitter.split_documents(docs)
#     if not splits:
#         raise ValueError("No text splits were generated. Check the content of the documents.")
#     print(f"Generated {len(splits)} text splits.")

#     # Store: Create embeddings and store them in a vector database
#     vectorstore = Chroma.from_documents(documents=splits, embedding=OpenAIEmbeddings(),persist_directory="./chroma_db")

#     # Retrieval
#     retriever = vectorstore.as_retriever()

#     # Define the system prompt
#     system_prompt = (
#         "You are an assistant for question-answering tasks. "
#         "Use the following pieces of retrieved context to answer "
#         "the question. If you don't know the answer, say that you don't know."
#         "\n\n"
#         "{context}"
#     )

#     # Define the chat prompt template
#     prompt = ChatPromptTemplate.from_messages(
#         [
#             ("system", system_prompt),
#             ("human", "{input}"),
#         ]
#     )

#     # Create chains
#     question_answer_chain = create_stuff_documents_chain(llm, prompt)
#     rag_chain = create_retrieval_chain(retriever, question_answer_chain)

#     # Run the pipeline with the input question
#     results = rag_chain.invoke({"input": question})

#     # Collect sources
#     source_documents = retriever.get_relevant_documents(question)  # Retrieve relevant documents
#     sources = [doc.metadata.get('source', 'Unknown') for doc in source_documents]

#     # Return answer and sources
#     return {
#         "answer": results['answer'],
#         "sources": sources
#     }






#  Example usage
# if __name__ == "__main__":
#     answer = setup_and_run_rag_pipeline(
#         json_path='./output.json',
#         question="What's an Agent?",
#     )
#     print(answer)
