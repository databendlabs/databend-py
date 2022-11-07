class Error(Exception):
    code = None

    def __init__(self, message=None):
        self.message = message
        super(Error, self).__init__(message)

    def __str__(self):
        message = ' ' + self.message if self.message is not None else ''
        return 'Code: {}.{}'.format(self.code, message)


class ServerException(Error):
    def __init__(self, message, code=None):
        self.message = message
        self.code = code
        super(ServerException, self).__init__(message)

    def __str__(self):
        return 'Code: {}\n{}'.format(self.code, self.message)
