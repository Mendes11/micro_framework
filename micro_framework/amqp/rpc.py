import logging
import time
from multiprocessing import Pipe, Manager

from micro_framework.amqp.amqp_elements import rpc_reply_queue, rpc_exchange
from micro_framework.amqp.entrypoints import BaseEventListener

logger = logging.getLogger(__name__)


class ListenerReceiver:
    """
    Implements a method to receive from the Reply EventListener's connection
    instance with a timeout possibility.

    When the timeout is reached, it raises TimeoutError.

    This class is usually created by the RPCConnection when it sends a new
    rpc message to the broker and notify the Reply-Listener to send the reply
    to a specific multiprocess.Connection instance.

    """

    def __init__(self, connection):
        self.connection = connection

    def result(self, timeout=None):
        if timeout is not None:
            if self.connection.poll(timeout=timeout):
                return self.connection.recv()
            raise TimeoutError()
        while not self.connection.poll(timeout=.5):
            pass
        return self.connection.recv()


class _CorrelationResultIntent:
    def __init__(self, correlation_id, sender):
        self.correlation_id = correlation_id
        self.sender = sender

    def reply_to_sender(self, reply):
        if not self.sender.closed and self.sender.writable:
            return self.sender.send(reply)
        raise ValueError("Reply sender is closed.")


def listen_to_correlation(correlation_id):
    global _correlations_queue
    print("Listen to: ", correlation_id)
    receiver, sender = Pipe(duplex=False)
    print("Sending to Queue")
    _correlations_queue.put(
        _CorrelationResultIntent(correlation_id, sender)
    )
    print("Put finished")
    return ListenerReceiver(receiver)


class RPCReplyListener(BaseEventListener):
    """
    Listens to any RPCReply for this service
    """
    singleton = True
    internal = True

    async def setup(self):
        global _correlations_queue, manager
        self.mp_manager = Manager()

        self._reply_queue = rpc_reply_queue()
        self.routing_key = self._reply_queue.routing_key
        self.running = False

        self.producer_lock = self.mp_manager.Lock()
        self.replies_lock = self.mp_manager.Lock()

        # This is a shared variable between all processes. It maps the
        # correlation ids and their intents to be returned to the user.
        self.reply_intents = self.mp_manager.dict()
        self.replies = {}
        await super(RPCReplyListener, self).setup()

    async def start(self):
        self.running = True
        await self.runner.spawn_extension(
            self, self.reply_sender
        )
        await super(RPCReplyListener, self).start()

    async def stop(self):
        self.running = False
        _correlations_queue.put(None)
        await super(RPCReplyListener, self).stop()

    async def get_exchange(self):
        return rpc_exchange()

    async def get_queue(self):
        return self._reply_queue

    def reply_sender(self):
        while self.running:
            if not self.replies:
                time.sleep(.1)
                continue
            with self.replies_lock:
                replies = list(self.replies.items())
            for corr_id, reply in replies:
                intent = self.reply_intents.get(corr_id)
                if not intent:
                    continue
                intent.reply_to_sender(reply)

                with self.replies_lock:
                    self.replies.pop(corr_id)
                    self.reply_intents.pop(corr_id)

            time.sleep(.1)

    @property
    def picklable_listener(self):
        return _PicklableReplyListener(self)

    async def call_route(self, entry_id, *args, _meta=None, **kwargs):
        # Any exception here will make the manager requeue the message
        # First we update our correlation_ids dict by emptying the
        # correlations_queue.
        # await self.read_correlations_queue()
        try:
            corr_id = entry_id.properties["correlation_id"]
        except KeyError:
            logger.error("Received a reply with no registered correlation_id")
            return

        body = args[0]
        with self.replies_lock:
            self.replies[corr_id] = body


class _PicklableReplyListener:
    """
    A version of the ReplyListener to be sent to worker processes.
    """

    def __init__(self, reply_listener: RPCReplyListener):
        self.producer_lock = reply_listener.producer_lock
        self.replies_lock = reply_listener.replies_lock
        self.reply_intents = reply_listener.reply_intents
        self.queue = reply_listener.queue

    def register_new_correlation(self, correlation_id):
        receiver, sender = Pipe(duplex=False)
        with self.replies_lock:
            self.reply_intents[correlation_id] = _CorrelationResultIntent(
                correlation_id, sender
            )
        return ListenerReceiver(receiver)
