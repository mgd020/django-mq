from timeit import default_timer


class Timer:
    start_time = None
    end_time = None

    def start(self):
        self.start_time = default_timer()
        self.end_time = None

    def stop(self):
        if self.end_time is None:
            self.end_time = default_timer()

    @property
    def value(self):
        start_time = self.start_time
        if start_time is None:
            return None
        end_time = self.end_time
        if end_time is None:
            end_time = default_timer()
        return end_time - start_time

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.stop()
