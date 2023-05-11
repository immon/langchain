from typing import Any, Dict, Optional

from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan
from pydantic import BaseModel, Extra, Field

class BaseElasticsearchTool(BaseModel):
    """Base tool for interacting with an Elasticsearch index."""

    es: Elasticsearch = Field(exclude=True)

    class Config:
        """Configuration for this pydantic object."""

        arbitrary_types_allowed = True
        extra = Extra.forbid


class SearchElasticsearchTool(BaseElasticsearchTool):
    """Tool for searching an Elasticsearch index."""

    def search(self, index: str, query: str, field: str = "_all"):
        response = self.es.search(index=index, body={
            "query": {
                "match": {field: query}
            }
        })

        return response["hits"]["hits"]


class ListIndicesElasticsearchTool(BaseElasticsearchTool):
    """Tool for listing indices."""

    def list_indices(self):
        indices = self.es.indices.get_alias("*")
        return list(indices.keys())


class GetIndexInfoElasticsearchTool(BaseElasticsearchTool):
    """Tool for getting index metadata."""

    def get_index_info(self, index: str):
        index_info = self.es.indices.get(index)
        return index_info


class DeleteIndexElasticsearchTool(BaseElasticsearchTool):
    """Tool for deleting an index."""

    def delete_index(self, index: str):
        self.es.indices.delete(index=index)


class CreateIndexElasticsearchTool(BaseElasticsearchTool):
    """Tool for creating an index."""

    def create_index(self, index: str, settings: Optional[Dict[str, Any]] = None):
        if settings:
            self.es.indices.create(index=index, body=settings)
        else:
            self.es.indices.create(index=index)


class GetDocumentByIdElasticsearchTool(BaseElasticsearchTool):
    """Tool for getting a document by ID."""

    def get_document_by_id(self, index: str, doc_id: str):
        return self.es.get(index=index, id=doc_id)


class DeleteDocumentByIdElasticsearchTool(BaseElasticsearchTool):
    """Tool for deleting a document by ID."""

    def delete_document_by_id(self, index: str, doc_id: str):
        return self.es.delete(index=index, id=doc_id)


class UpdateDocumentByIdElasticsearchTool(BaseElasticsearchTool):
    """Tool for updating a document by ID."""

    def update_document_by_id(self, index: str, doc_id: str, doc: Dict[str, Any]):
        return self.es.update(index=index, id=doc_id, body={"doc": doc})


class IndexDocumentElasticsearchTool(BaseElasticsearchTool):
    """Tool for indexing a document."""

    def index_document(self, index: str, doc: Dict[str, Any], doc_id: Optional[str] = None):
        return self.es.index(index=index, id=doc_id, body=doc)


