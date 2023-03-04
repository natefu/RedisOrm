class BaseError(Exception):
    def __init__(self):
        self.err_msg = ''


class InvalidInputException(BaseError):
    def __init__(self, message):
        self.err_msg = {
            'message': f'Invalid input {message}'
        }
        Exception.__init__(self, self.err_msg)


class ObjectNotFoundException(BaseError):
    def __init__(self, message):
        self.err_msg = {
            'message': f'Object {message} not found'
        }
        Exception.__init__(self, self.err_msg)


class GetMoreObjectsException(BaseError):
    def __init__(self, message, count):
        self.err_msg = {
            'message': f'input {str(message)} retrieve {count} objects'
        }
        Exception.__init__(self, self.err_msg)


class ValueRequiredException(BaseError):
    def __init__(self, message):
        self.err_msg = {
            'message': f'Value {message} is required'
        }
        Exception.__init__(self, self.err_msg)


class RedisOrmSystemError(BaseError):
    def __init__(self, message):
        self.err_msg = {
            'message': f'Internal Failure: {message}'
        }
        Exception.__init__(self, self.err_msg)


class DuplicatedValueError(BaseError):
    def __init__(self, message):
        self.err_msg = f'{message} should be unique'
        Exception.__init__(self, self.err_msg)
