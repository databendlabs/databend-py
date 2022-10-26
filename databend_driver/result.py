class QueryResult(object):
    """
    Stores query result from multiple blocks.
    """

    def __init__(
            self, data_generator,
            with_column_types=False):
        self.data_generator = data_generator
        self.with_column_types = with_column_types

        self.data = []
        self.columns_with_types = []
        self.columns = []

        super(QueryResult, self).__init__()

    def store(self, raw_data: dict):
        self.data = raw_data.get("data")
        fields = raw_data.get("schema")["fields"]
        for field in fields:
            self.columns_with_types.append(field["data_type"]["type"])
            self.columns.append(field["name"])

    def get_result(self):
        """
        :return: stored query result.
        """

        for d in self.data_generator:
            self.store(d)

        data = self.data

        if self.with_column_types:
            return data, self.columns_with_types
        else:
            return data
