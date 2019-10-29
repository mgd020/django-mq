import random
import time

from django.db.models import Case, IntegerField, When
from django.db.transaction import atomic

from .models import Message
from .utils import Timer


class Empty(Exception):
    pass


class Queue:
    sleep_max = 2.0

    def __init__(self, name=None, priority=False, lifo=False):
        items = Message.objects.all()
        if name:
            items = items.filter(queue=name)
            self.name = name
        else:
            self.name = ''
        if priority:
            items = items.annotate(has_priority=Case(When(priority=None, then=1), default=0, output_field=IntegerField()))
            order_by = ('has_priority', 'priority', 'pk')
            self.priority = True
        else:
            order_by = ('pk',)
            self.priority = False
        if lifo:
            order_by = ('-' + f for f in order_by)
        self.items = items.order_by(*order_by)

    def put(self, item, block=False, timeout=None, priority=None):
        if not isinstance(item, (list, tuple)):
            item = (item,)
        Message.objects.bulk_create([Message(payload=p, priority=priority, queue=self.name) for p in item])

    def get(self, block=True, timeout=None):
        backoff = 2

        if timeout:
            timer = Timer()
            timer.start()

        while True:
            with Timer() as slot_timer:
                # get item with optimistic lock
                pk = self.items.filter(lock=False).values_list('pk', flat=True).first()
                if pk:
                    with atomic():
                        if Message.objects.filter(pk=pk, lock=False).update(lock=True):
                            message = Message.objects.get(pk=pk)
                            message.delete()
                            return message.payload
                elif not block:
                    raise Empty

                # check for timeout
                if timeout:
                    time_left = timeout - timer.value
                    if time_left <= 0:
                        raise Empty

            # binary exponential backoff based on slot time, timeout, and sleep max
            sleep_max = [self.max_sleep, slot_timer.value * (backoff - 1)]
            backoff <<= 1
            if timeout:
                sleep_max.append(time_left - slot_timer.value)
            time.sleep(random.uniform(0, min(sleep_max)))

    def put_nowait(self, item, priority=None):
        self.put(item, False)

    def get_nowait(self):
        return self.get(False)

    def qsize(self):
        return self.items.count()

    def empty(self):
        return not self.items.exists()

    def full(self):
        return False

    def task_done():
        raise NotImplementedError

    def join():
        raise NotImplementedError
