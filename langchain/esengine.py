from elasticsearch import Elasticsearch, NotFoundError
from typing import List, Optional

class ESEngine:
    def __init__(self, hosts: List[str], http_auth: Optional[tuple] = None, use_ssl: bool = True):
        self._es = Elasticsearch(hosts, http_auth=http_auth, use_ssl=use_ssl)

    def _format_index(self, index: dict) -> str:
        return f'Name: {index}, Mapping: {self._es.indices.get_mapping(index)}'

    def get_indices_names(self) -> List[str]:
        return list(self._es.indices.get_alias("*").keys())

    @property
    def get_index_info(self, index_name: str) -> str:
        return self._format_index(index_name)

    def _get_sample_fields(self, index_name: str) -> dict:
        result = self._es.search(index=index_name, size=1)
        return result['hits']['hits'][0]['_source'] if result['hits']['hits'] else {}

    def run(self, index_name: str, body: dict) -> dict:
        return self._es.search(index=index_name, body=body)

    def get_index_info_no_throw(self, index_name: str) -> str:
        try:
            return self._format_index(index_name)
        except NotFoundError:
            return f"Error: Index {index_name} not found"

    def run_no_throw(self, index_name: str, body: dict) -> dict:
        try:
            return self.run(index_name, body)
        except Exception as e:
            return {"error": str(e)}

    def get_index_mapping(self, index_name: str) -> dict:
        return self._es.indices.get_mapping(index=index_name)

    def get_index_fields(self, index_name: str) -> List[str]:
        mapping = self.get_index_mapping(index_name)
        return list(mapping[index_name]['mappings']['properties'].keys())

    def get_fields_description(self, index_name: str) -> Optional[dict]:
        try:
            return self._es.get(index='data_descriptions', id=index_name)['_source']
        except NotFoundError:
            return None


