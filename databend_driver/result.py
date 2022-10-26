class QueryResult(object):
    """
    Stores query result from multiple response data.
    """

    def __init__(
            self, data_generator, first_data,
            with_column_types=False):
        self.data_generator = data_generator
        self.with_column_types = with_column_types
        self.first_data = first_data
        self.data = []
        self.columns_with_types = []

        super(QueryResult, self).__init__()

    def store(self, raw_data: dict):
        self.data.append(raw_data.get("data"))
        fields = raw_data.get("schema")["fields"]
        for field in fields:
            column_type = (field['name'], field["data_type"]["type"])
            self.columns_with_types.append(column_type)

    def get_result(self):
        """
        :return: stored query result.
        """
        data = []
        self.store(self.first_data)
        for d in self.data_generator:
            self.store(d)

        for rd in self.data:
            for r in rd:
                data.append(tuple(r))

        self.columns_with_types.extend(data)
        if self.with_column_types:
            return self.columns_with_types
        else:
            return data
