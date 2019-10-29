from multiprocessing import Process

from django.core.management.base import BaseCommand
from mq.utils import Timer


class RedisBench:
    q = 'q'

    def __init__(self):
        import redis
        self.r = redis.StrictRedis(host='localhost', port=6379)

    @classmethod
    def setup(cls):
        import redis
        redis.StrictRedis(host='localhost', port=6379).delete(cls.q)

    def push(self, i):
        self.r.rpush(self.q, i)

    def pop(self, timeout):
        return self.r.blpop(self.q, timeout)

    def count(self):
        return self.r.llen(self.q)


class MqBench:
    using = None

    def __init__(self):
        from mq.models import Queue, Empty
        self.Empty = Empty
        self.q = Queue.objects.using(self.using).get(name='q')

    @classmethod
    def setup(cls):
        from mq.models import Queue
        qs = Queue.objects.using(cls.using)
        qs.filter(name='q').delete()
        qs.create(name='q')
        from django.db import connections
        connections[cls.using].close()

    def push(self, i):
        self.q.put(str(i))

    def pop(self, timeout):
        try:
            return self.q.get(timeout=timeout)
        except self.Empty:
            pass

    def count(self):
        return self.q.qsize()


class MysqlBench(MqBench):
    using = 'mysql'


class SqliteBench(MqBench):
    using = 'default'


class PostgresBench(MqBench):
    using = 'postgres'


class Command(BaseCommand):
    types = {
        'mysql': MysqlBench,
        'sqlite': SqliteBench,
        'postgres': PostgresBench,
        'redis': RedisBench,
        # 'rq',
    }

    def add_arguments(self, parser):
        parser.add_argument('--type', required=True, choices=self.types.keys())
        parser.add_argument('--items', default=1, type=int)
        parser.add_argument('--producers', default=1, type=int)
        parser.add_argument('--consumers', default=1, type=int)
        parser.add_argument('--timeout', default=5, type=int)

    def handle(self, **options):
        # setup
        self.types[options['type']].setup()

        # push
        processes = [Process(target=self.producer, args=(options,)) for i in range(options['producers'])]
        with Timer() as push_timer:
            for p in processes:
                p.start()
            for p in processes:
                p.join()

        # pop
        processes = [Process(target=self.consumer, args=(options,)) for i in range(options['consumers'])]
        with Timer() as pop_timer:
            for p in processes:
                p.start()
            for p in processes:
                p.join()

        print('%s,%s' % (push_timer.value, pop_timer.value))

    @classmethod
    def producer(cls, options):
        handler = cls.types[options['type']]()
        for i in range(options['items']):
            handler.push(i)

    @classmethod
    def consumer(cls, options):
        handler = cls.types[options['type']]()
        timeout = options['timeout']
        while handler.count():
            handler.pop(timeout)
