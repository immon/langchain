import json
from typing import Any, Dict, List, Optional

from elasticsearch import Elasticsearch, NotFoundError
from elasticsearch.helpers import scan
from elasticsearch_dsl import Search
from pydantic import BaseModel, Extra, Field

class ElasticSearchDatabase(BaseModel):
    """Elasticsearch database interaction class."""

    es: Elasticsearch = Field(exclude=True)

    class Config:
        arbitrary_types_allowed = True
        extra = Extra.forbid

    def create_index(self, index: str, settings: Optional[Dict[str, Any]] = None) -> None:
        if settings:
            self.es.indices.create(index=index, body=settings)
        else:
            self.es.indices.create(index=index)

    def delete_index(self, index: str) -> None:
        self.es.indices.delete(index=index)

    def get_index_info(self, index: str) -> Dict[str, Any]:
        return self.es.indices.get(index=index)

    def list_indices(self) -> List[str]:
        indices = self.es.indices.get_alias("*")
        return list(indices.keys())

    def search(self, index: str, query: str, field: str = "_all") -> List[Dict[str, Any]]:
        search = Search(using=self.es, index=index).query("match", **{field: query})
        response = search.execute()
        return [hit.to_dict() for hit in response.hits.hits]

    def get_document_by_id(self, index: str, doc_id: str) -> Dict[str, Any]:
        try:
            doc = self.es.get(index=index, id=doc_id)
            return doc["_source"]
        except NotFoundError:
            return None

    def delete_document_by_id(self, index: str, doc_id: str) -> None:
        self.es.delete(index=index, id=doc_id)

    def update_document_by_id(self, index: str, doc_id: str, doc: Dict[str, Any]) -> None:
        self.es.update(index=index, id=doc_id, body={"doc": doc})

    def index_document(self, index: str, doc: Dict[str, Any], doc_id: Optional[str] = None) -> str:
        result = self.es.index(index=index, id=doc_id, body=doc)
        return result["_id"]

    def search_complex(self, index: str, query: Dict[str, Any]) -> List[Dict[str, Any]]:
        response = self.es.search(index=index, body=query)
        return [hit["_source"] for hit in response["hits"]["hits"]]

    def scan_index(self, index: str, query: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        if query is None:
            query = {"query": {"match_all": {}}}

        results = []
        for hit in scan(self.es, index=index, query=query):
            results.append(hit["_source"])

        return results

