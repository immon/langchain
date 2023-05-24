# flake8: noqa
"""Tools for interacting with a SQL database."""
from typing import Any, Dict, Optional, Type

from pydantic import BaseModel, Extra, Field, root_validator

import json

from langchain.base_language import BaseLanguageModel
from langchain.callbacks.manager import (
    AsyncCallbackManagerForToolRun,
    CallbackManagerForToolRun,
)
from langchain.chains.llm import LLMChain
from langchain.prompts import PromptTemplate
from langchain.es_engine import ESEngine
from langchain.tools.base import BaseTool
from langchain.tools.esengine.prompt import QUERY_CHECKER


class BaseElasticsearchDatastoreTool(BaseModel):
    """Base tool for interacting with a Elasticsearch datastore."""

    db: ESEngine = Field(exclude=True)

    # Override BaseTool.Config to appease mypy
    # See https://github.com/pydantic/pydantic/issues/4173
    class Config(BaseTool.Config):
        """Configuration for this pydantic object."""

        arbitrary_types_allowed = True
        extra = Extra.forbid


class QueryInputSchema(BaseModel):
    index_name : str = Field(description="should be an index name")
    query : Dict[str, Any] = Field(description="should be a search query")

class QueryElasticsearchDatastoreTool(BaseElasticsearchDatastoreTool, BaseTool):
    """Tool for querying a Elasticsearch datastore."""

    name = "query_es_db"
    description = """
    Input to this tool is an index name to query and detailed and correct Elasticsearch DSL query. The output is a result from the datastore.
    If the query is not correct, an error message will be returned.
    If an error is returned, rewrite the query, check the query, and try again.
    """
    args_schema: Type[BaseModel] = QueryInputSchema

    def _run(
        self,
        index_name: str,
        query: Dict[str, Any],
        run_manager: Optional[CallbackManagerForToolRun] = None,
    ) -> str:
        """Execute the query, return the results or an error message."""
        #body = json.loads(query)    # TODO: catch exception
        return self.db.run(index_name=index_name, body=query)

    async def _arun(
        self,
        index_name: str,
        query: str,
        run_manager: Optional[AsyncCallbackManagerForToolRun] = None,
    ) -> str:
        raise NotImplementedError("QueryESDbTool does not support async")


class InfoElasticsearchDatastoreTool(BaseElasticsearchDatastoreTool, BaseTool):
    """Tool for getting metadata about a Elasticsearch datastore."""

    name = "schema_es_db"
    description = """
    Input to this tool is a comma-separated list of indices, output is the schema and sample documents for those indices.
    Be sure that the indices actually exist by calling list_indices_es_db first!

    Example Input: "index1, index2, index3"
    """

    def _run(
        self,
        table_names: str,
        run_manager: Optional[CallbackManagerForToolRun] = None,
    ) -> str:
        """Get the schema for indices in a comma-separated list."""
        return self.db.get_index_info(table_names.split(", "))

    async def _arun(
        self,
        table_name: str,
        run_manager: Optional[AsyncCallbackManagerForToolRun] = None,
    ) -> str:
        raise NotImplementedError("SchemaEsDbTool does not support async")


class ListElasticsearchDatastoreTool(BaseElasticsearchDatastoreTool, BaseTool):
    """Tool for getting index names."""

    name = "list_indices_es_db"
    description = "Input is an empty string, output is a comma separated list of indices in the datastore."

    def _run(
        self,
        tool_input: str = "",
        run_manager: Optional[CallbackManagerForToolRun] = None,
    ) -> str:
        """Get the schema for a specific index."""
        return ", ".join(self.db.get_usable_index_names())

    async def _arun(
        self,
        tool_input: str = "",
        run_manager: Optional[AsyncCallbackManagerForToolRun] = None,
    ) -> str:
        raise NotImplementedError("ListEsDbTool does not support async")


class QueryCheckerTool(BaseElasticsearchDatastoreTool, BaseTool):
    """Use an LLM to check if a query is correct."""

    template: str = QUERY_CHECKER
    llm: BaseLanguageModel
    llm_chain: LLMChain = Field(init=False)
    name = "query_checker_es_db"
    description = """
    Use this tool to double check if Elasticsearc query is correct before executing it.
    Always use this tool before executing a query with query_es_db!
    """

    @root_validator(pre=True)
    def initialize_llm_chain(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        if "llm_chain" not in values:
            values["llm_chain"] = LLMChain(
                llm=values.get("llm"),
                prompt=PromptTemplate(
                    template=QUERY_CHECKER, input_variables=["query"]
                ),
            )

        if values["llm_chain"].prompt.input_variables != ["query"]:
            raise ValueError(
                "LLM chain for QueryCheckerTool must have input variables ['query']"
            )

        return values

    def _run(
        self,
        query: str,
        run_manager: Optional[CallbackManagerForToolRun] = None,
    ) -> str:
        """Use the LLM to check the query."""
        return self.llm_chain.predict(query=query)

    async def _arun(
        self,
        query: str,
        run_manager: Optional[AsyncCallbackManagerForToolRun] = None,
    ) -> str:
        return await self.llm_chain.apredict(query=query)