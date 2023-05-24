"""Toolkit for interacting with a SQL database."""
from typing import List

from pydantic import Field

from langchain.agents.agent_toolkits.base import BaseToolkit
from langchain.base_language import BaseLanguageModel
from langchain.es_engine import ESEngine
from langchain.tools import BaseTool
from langchain.tools.esengine.tool import (
    InfoElasticsearchDatastoreTool,
    ListElasticsearchDatastoreTool,
    QueryElasticsearchDatastoreTool,
    QueryCheckerTool,
)


class ElasticsearchDatastoreToolkit(BaseToolkit):
    """Toolkit for interacting with SQL databases."""

    db: ESEngine = Field(exclude=True)
    llm: BaseLanguageModel = Field(exclude=True)

    class Config:
        """Configuration for this pydantic object."""

        arbitrary_types_allowed = True

    def get_tools(self) -> List[BaseTool]:
        """Get the tools in the toolkit."""
        return [
            InfoElasticsearchDatastoreTool(db=self.db),
            ListElasticsearchDatastoreTool(db=self.db),
            QueryElasticsearchDatastoreTool(db=self.db),
            # QueryCheckerTool(db=self.db, llm=self.llm), # TODO enable ES query checker
        ]
