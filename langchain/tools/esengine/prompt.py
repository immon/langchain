QUERY_CHECKER = """
{query}
Double check the Elasticsearch query above for common mistakes, including:
- Properly structuring the query using the correct query DSL for Elasticsearch version 8
- Validating the syntax of the query, including correct field names and operators
- Using appropriate filters and aggregations to retrieve the desired results
- Properly configuring the sorting order and pagination
- Taking advantage of relevant Elasticsearch features like full-text search, aggregations and analyzers
- Optimizing the query performance by considering query optimization techniques

If there are any mistakes or if the desired results are not achieved, review and modify the query accordingly. If there are no mistakes, proceed with executing the original query on your Elasticsearch cluster.
"""
