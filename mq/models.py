import random
import time

from django.db import models
from django.db.transaction import atomic
from django.utils.functional import cached_property
from django.db.utils import OperationalError

from .utils import Timer


class Empty(Exception):
    pass


class Full(Exception):
    pass


class Message(models.Model):
    id = models.BigAutoField(primary_key=True, verbose_name='ID')
    payload = models.TextField()
    lock = models.BooleanField(default=False, blank=True, db_index=True)
    queue = models.ForeignKey('Queue', on_delete=models.CASCADE)
    priority = models.IntegerField(null=True, blank=True, db_index=True)


class Queue(models.Model):
    name = models.CharField(max_length=100, unique=True)
    priority = models.BooleanField(blank=True, default=False)
    lifo = models.BooleanField(blank=True, default=False)
    backoff_max = models.FloatField(null=True, blank=True, default=2)

    PRIORITY_CASE = models.Case(models.When(priority=None, then=1), default=0, output_field=models.IntegerField())
    PRIORITY_ORDER = ('has_priority', 'priority', 'pk')
    FIFO_ORDER = ('pk',)

    @cached_property
    def messages(self):
        qs = self.message_set.all()
        if self.priority:
            qs = qs.annotate(has_priority=self.HAS_PRIORITY_CASE)
            order_by = self.PRIORITY_ORDER
        else:
            order_by = self.FIFO_ORDER
        if self.lifo:
            order_by = ('-' + f for f in order_by)
        return qs.order_by(*order_by)

    def put(self, item, block=False, timeout=None, priority=None):
        if not isinstance(item, (list, tuple)):
            item = (item,)

        backoff = 2
        db = self._state.db

        if timeout:
            timer = Timer()
            timer.start()

        while True:
            with Timer() as slot_timer:
                # try put
                try:
                    Message.objects.using(db).bulk_create([Message(payload=p, priority=priority, queue=self) for p in item])
                    return
                except OperationalError:
                    if not block:
                        raise Full

                # check for timeout
                if timeout:
                    time_left = timeout - timer.value
                    if time_left <= 0:
                        raise Empty

            # backoff
            sleep_max = [slot_timer.value * (backoff - 1)]
            backoff <<= 1
            if self.backoff_max is not None:
                sleep_max.append(self.backoff_max)
            if timeout:
                sleep_max.append(time_left - slot_timer.value)
            seconds = random.uniform(0, min(sleep_max))
            if seconds > 0:
                time.sleep(seconds)

    def get(self, block=True, timeout=None):
        backoff = 2
        db = self._state.db

        if timeout:
            timer = Timer()
            timer.start()

        while True:
            with Timer() as slot_timer:
                # get item with optimistic lock
                pk = self.messages.filter(lock=False).values_list('pk', flat=True).first()
                if pk is not None:
                    with atomic():
                        if Message.objects.using(db).filter(pk=pk, lock=False).update(lock=True):
                            message = Message.objects.using(db).get(pk=pk)
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
            sleep_max = [slot_timer.value * (backoff - 1)]
            backoff <<= 1
            if self.backoff_max is not None:
                sleep_max.append(self.backoff_max)
            if timeout:
                sleep_max.append(time_left - slot_timer.value)
            seconds = random.uniform(0, min(sleep_max))
            if seconds > 0:
                time.sleep(seconds)

    def qsize(self):
        return self.message_set.count()

    def empty(self):
        return not self.message_set.exists()
