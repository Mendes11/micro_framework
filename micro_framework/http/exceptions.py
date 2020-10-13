class APIError(Exception):
    status_code = 500
    default_message = 'An unexpected error has occurred'
    default_code = 'internal_error'

    def __init__(self, message=None, code=None):
        if message is None:
            message = self.default_message

        if code is None:
            code = self.default_code

        if not isinstance(message, dict) and not isinstance(message, list):
            data = {'message': message, 'code': code}
        else:
            data = message
        self.data = data


class NotFound(APIError):
    status_code = 404
    default_message = 'Item not found'
    default_code = 'not_found'


class ServiceUnavailable(APIError):
    status_code = 503
    default_message = 'The desired service is unavailable'
    default_code = 'service_unavailable'


class BadRequest(APIError):
    status_code = 400
    default_message = 'An error has occurred while handling your request, ' \
                      'check the data sent.'
