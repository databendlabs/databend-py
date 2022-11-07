from itertools import islice, tee
from databend_py.errors import ServerException


class Helper(object):
    def __int__(self, response):
        self.response = response
        super(Helper, self).__init__()

    def get_result_data(self):
        return self.response['data']

    def get_fields(self):
        return self.response["schema"]["fields"]

    def get_next_uri(self):
        if "next_uri" in self.response:
            return self.response['next_uri']
        return None

    def get_error(self):
        if self.response['error'] is None:
            return None

        # Wrap errno into msg, for result check
        return ServerException(message=self.response['error']['message'],
                               code=self.response['error']['code'])

    def check_error(self):
        error = self.get_error()
        if error:
            raise error


def chunks(seq, n):
    # islice is MUCH slower than slice for lists and tuples.
    if isinstance(seq, (list, tuple)):
        i = 0
        item = seq[i:i + n]
        while item:
            yield list(item)
            i += n
            item = seq[i:i + n]

    else:
        it = iter(seq)
        item = list(islice(it, n))
        while item:
            yield item
            item = list(islice(it, n))


def pairwise(iterable):
    a, b = tee(iterable)
    next(b, None)
    return zip(a, b)


def column_chunks(columns, n):
    for column in columns:
        if not isinstance(column, (list, tuple)):
            raise TypeError(
                'Unsupported column type: {}. list or tuple is expected.'
                    .format(type(column))
            )

    # create chunk generator for every column
    g = [chunks(column, n) for column in columns]

    while True:
        # get next chunk for every column
        item = [next(column, []) for column in g]
        if not any(item):
            break
        yield item


# from paste.deploy.converters
def asbool(obj):
    if isinstance(obj, str):
        obj = obj.strip().lower()
        if obj in ['true', 'yes', 'on', 'y', 't', '1']:
            return True
        elif obj in ['false', 'no', 'off', 'n', 'f', '0']:
            return False
        else:
            raise ValueError('String is not true/false: %r' % obj)
    return bool(obj)
