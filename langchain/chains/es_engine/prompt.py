# flake8: noqa
from langchain.output_parsers.list import CommaSeparatedListOutputParser
from langchain.prompts.prompt import PromptTemplate

_DEFAULT_TEMPLATE = """Given an input question, first create a syntactically correct Elasticsearch DSL query to run, then look at the results of the query and return the answer. Unless the user specifies in his question a specific number of examples he wishes to obtain, always limit your query to at most {top_k} results.

Never query for all the fields from a specific index, only ask for a the few relevant fields given the question.

Pay attention to use only the field names that you can see in the schema description. Be careful to not query for fields that do not exist. Also, pay attention to which field is in which index.

Use the following format:

Question: "Question here"
DSLQuery: "Elasticsearch DSL query to run"
DSLResult: "Result of the Elasticsearch DSL query"
Answer: "Final answer here"

Only use the indices listed below.

{index_info}

Question: {input}"""

PROMPT = PromptTemplate(
    input_variables=["input", "index_info", "top_k"],
    template=_DEFAULT_TEMPLATE,
)

_DECIDER_TEMPLATE = """Given the below input question and list of potential indices, output a comma-separated list of the index names that may be necessary to answer this question.

Question: {query}

Index Names: {index_names}

Relevant Index Names:"""
DECIDER_PROMPT = PromptTemplate(
    input_variables=["query", "index_names"],
    template=_DECIDER_TEMPLATE,
    output_parser=CommaSeparatedListOutputParser(),
)


_es_dsl_prompt = """You are an Elasticsearch expert. Given an input question, first create a syntactically correct Elasticsearch DSL query to run, then look at the results of the query and return the answer to the input question.
Unless the user specifies in the question a specific number of examples to obtain, query for at most {top_k} results using the "size" parameter in Elasticsearch DSL. You can order the results to return the most informative data in the database using the "sort" parameter.
Never query for all fields from an index. You must query only the fields that are needed to answer the question.
Pay attention to use only the field names you can see in the indices below. Be careful to not query for fields that do not exist. Also, pay attention to which field is in which index.

Use the following format:

Question: "Question here"
DSLQuery: "Elasticsearch DSL Query to run"
DSLResult: "Result of the ElasticsearchDSLQuery"
Answer: "Final answer here"

Only use the following indices:
{index_info}

Question: {input}"""

ES_DSL_PROMPT = PromptTemplate(
    input_variables=["input", "index_info", "top_k"],
    template=_es_dsl_prompt,
)

PROMPTS = {
 "es_dsl": ES_DSL_PROMPT
}
