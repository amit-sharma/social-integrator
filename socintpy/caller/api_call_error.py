class APICallError(Exception):
    def __init__(self, error_code, error_message):
        self.error_code = error_code
        self.error_message = error_message

    def __str__(self):
        return repr(self.value)
