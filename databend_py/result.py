import ast
from .datetypes import DatabendDataType


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
        self.data_type_dict_list = []
        self.columns_with_types = []
        self.type_convert = DatabendDataType.type_convert_fn

        super(QueryResult, self).__init__()

    def store(self, raw_data: dict):
        fields = raw_data.get("schema")["fields"]
        type_ls = []
        datas = raw_data.get("data")
        for field in fields:
            column_type = (field['name'], field["data_type"]["type"])
            type_ls.append(field["data_type"]["type"])
            self.columns_with_types.append(column_type)

        for data in datas:
            self.data_type_dict_list.append(dict(zip(data, type_ls)))

    def get_result(self):
        """
        :return: stored query result.
        """
        data = []
        self.store(self.first_data)
        for d in self.data_generator:
            self.store(d)

        for read_data in self.data_type_dict_list:
            tmp_list = []
            for d, t in read_data.items():
                tmp_list.append(self.type_convert(t)(d))
            data.append(tuple(tmp_list))

        if self.with_column_types:
            return self.columns_with_types, data
        else:
            return [], data
