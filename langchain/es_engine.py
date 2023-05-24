# flake8: noqa
"""Tools for interacting with a Elasticsearch datastore."""
from __future__ import annotations

import warnings
from typing import Any, Iterable, List, Optional


from elasticsearch import Elasticsearch, NotFoundError
from typing import List, Optional

def find_fields(mapping, current_path=[]):
    all_fields = {}
    def _find_fields(mapping, current_path=[]):
        for key, value in mapping.items():
            if isinstance(value, dict):
                if "properties" in value:
                    _find_fields(value["properties"], current_path + [key])
                elif "type" in value:
                    all_fields[".".join(current_path + [key])] = value["type"]
                    if "fields" in value:
                        _find_fields(value["fields"], current_path + [key])
    _find_fields(mapping)
    return all_fields
    
class ESEngine:
    """Elasticsearch wrapper around a datastore."""

    def __init__( #self, hosts: List[str], http_auth: Optional[tuple] = None, use_ssl: bool = True):
        self,
        connection: Elasticsearch,
        #metadata: Optional[MetaData] = None,
        ignore_indices: Optional[List[str]] = None,
        include_indices: Optional[List[str]] = None,
        sample_docs_in_index_info: int = 3,
        custom_index_info: Optional[dict] = None,
        datastream_support: bool = True,  # TODO: Should be automatic. Check ES version if it supports datastreams
    ):
        """Create Elasticsearch connection from the URI and optional parameters."""

        self._connection = connection

        if include_indices and ignore_indices:
            raise ValueError("Cannot specify both include_indices and ignore_indices")

        # including datastream support by adding the datastreams as well as indices to the all
        # indices list if datastream_support is True
        self._all_indices = set(
            self._get_index_names()
            + (self._get_datastream_names() if datastream_support else [])
        )

        self._include_indices = set(include_indices) if include_indices else set()
        if self._include_indices:
            missing_indices = self._include_indices - self._all_indices
            if missing_indices:
                raise ValueError(
                    f"include_indices {missing_indices} not found in database"
                )

        self._ignore_indices = set(ignore_indices) if ignore_indices else set()
        if self._ignore_indices:
            missing_indices = self._ignore_indices - self._all_indices
            if missing_indices:
                raise ValueError(
                    f"ignore_indices {missing_indices} not found in database"
                )
        usable_indices = self.get_usable_index_names()
        self._usable_indices = set(usable_indices) if usable_indices else self._all_indices

        if not isinstance(sample_docs_in_index_info, int):
            raise TypeError("sample_docs_in_index_info must be an integer")

        self._sample_docs_in_index_info = sample_docs_in_index_info

        self._custom_index_info = custom_index_info
        if self._custom_index_info:
            if not isinstance(self._custom_index_info, dict):
                raise TypeError(
                    "index_info must be a dictionary with index names as keys and the "
                    "desired index info as values"
                )
            # only keep the indices that are also present in the database
            intersection = set(self._custom_index_info).intersection(self._all_indices)
            self._custom_index_info = dict(
                (index, self._custom_index_info[index])
                for index in self._custom_index_info
                if index in intersection
            )

        #self._metadata = metadata or MetaData()
        ## including view support if view_support = true
        #self._metadata.reflect(
            #views=view_support,
            #bind=self._engine,
            #only=list(self._usable_indices),
            #schema=self._schema,
        #)


    @classmethod
    def from_uri(
        cls, hosts: str | None = None, connection_args: Optional[dict] = None, **kwargs: Any
    ) -> ESEngine:
        """Construct a Elasticsearch engine from URI."""

        _connection_args = connection_args or {}
        connection = Elasticsearch(hosts, **_connection_args)

        return cls(connection, **kwargs)

    def get_usable_index_names(self) -> Iterable[str]:
        """Get names of indices available."""
        if self._include_indices:
            return self._include_indices
        return self._all_indices - self._ignore_indices

    def _get_index_names(self) -> List[str]:
        return list(self._connection.indices.get_alias(index="*", expand_wildcards="open").keys())

    def _get_datastream_names(self) -> List[str]:
        return list(map(lambda info: info.get('name'), self._connection.indices.get_data_stream(name="*").get("data_streams")))

    @property
    def index_info(self, index_name: str) -> str:
        """Information about all indices in Elasticsearch."""
        return self.get_index_info()
    
    def get_index_info(self, index_names: Optional[List[str]] = None) -> str:
        """
    This function retrieves and formats the information about specified indices from the Elasticsearch instance.
    If no indices are specified, it retrieves information for all usable indices.
    For each index, it gets the fields and a sample of documents.
        """

        all_index_names = self.get_usable_index_names()
        if index_names is not None:
            missing_indices = set(index_names).difference(all_index_names)
            if missing_indices:
                raise ValueError(f"index_names {missing_indices} not found in database")
            all_index_names = index_names

        indices_description = []
        for index in all_index_names:
            if self._custom_index_info and index in self._custom_index_info:
                indices_description.append(self._custom_index_info[index])
                continue

            # Dump index name and fields
            index_info = f"Index: {index}\nFields: {self.get_index_fields(index_name=index)}\n"

            has_extra_info = (
                self._sample_docs_in_index_info > 0
            )
            if has_extra_info:
                sample_docs = self._get_sample_docs(index)
                if sample_docs:
                    index_info += "\n/*\nSample documents:\n"
                    index_info += "\n".join(map(lambda d: f"{d}", sample_docs))
                    index_info += "\n*/"
            indices_description.append(index_info)
        final_str = "\n\n".join(indices_description)
        return final_str

    def _get_sample_docs(self, index_name: str) -> list:
        # Search for documents in the specified index
        response = self._connection.search(
            index=index_name,
            body={
                "size": self._sample_docs_in_index_info,
                "query": {"match_all": {}}
                }
                )

        # Extract the documents from the response
        documents = [hit["_source"] for hit in response["hits"]["hits"]]
        return documents

    def run(self, index_name: str, body: dict) -> dict:
        return self._connection.search(index=index_name, body=body)

    '''
    def get_index_info_no_throw(self, index_name: str) -> str:
        try:
            return self._format_index(index_name)
        except NotFoundError:
            return f"Error: Index {index_name} not found"
    '''

    def run_no_throw(self, index_name: str, body: dict) -> dict:
        try:
            return self.run(index_name, body)
        except Exception as e:
            return {"error": str(e)}

    def get_index_mapping(self, index_name: str) -> dict:
        response = self._connection.indices.get_mapping(index=index_name)
        return response

    def get_index_fields(self, index_name: str) -> dict:
        mapping = self.get_index_mapping(index_name)
        try:
            first_name = next(iter(mapping))
        except StopIteration:
            first_name = None

        mapping = mapping.get(first_name, {}).get("mappings", {}).get("properties", {})
        all_fields = find_fields(mapping)
        return all_fields
