class Log:
    def __init__(self, level, printer):
        self._level = level
        self._printer = printer

        def display(title, msg):
            self._printer(title + ' ')
            self._printer(' '.join(map(lambda x: str(x), msg)))
            self._printer('\n')
        def debug(*msg):
            display('DEBUG', msg)
        def warn(*msg):
            display('WARN', msg)
        def error(*msg):
            display('ERROR', msg)

        self.debug = lambda *_: _
        self.warn = lambda *_: _
        self.error = lambda *_: _

        if self._level == 'debug':
            self.debug = debug
            self.warn = warn
            self.error = error
        elif self._level == 'warn':
            self.warn = warn
            self.error = error
        elif self._level == 'error':
            self.error = error
