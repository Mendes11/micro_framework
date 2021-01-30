import logging
from multiprocessing import Manager
from multiprocessing import Pipe


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


m = Manager()
_correlations_queue = m.Queue()
lock = m.Lock()
_correlation_ids = m.dict()


def listen_to_correlation(correlation_id):
    global _correlation_ids
    receiver, sender = Pipe(duplex=False)
    with lock:
        _correlation_ids[correlation_id] = sender
    return ListenerReceiver(receiver)


class RPCReplyListener(BaseEventListener):
    """
    Listens to any RPCReply for this service
    """
    singleton = True
    internal = True

    async def setup(self):
        self._reply_queue = rpc_reply_queue()
        self.routing_key = self._reply_queue.routing_key
        await super(RPCReplyListener, self).setup()

    async def get_exchange(self):
        return rpc_exchange()

    async def get_queue(self):
        return self._reply_queue

    async def read_correlations_queue(self):
        global _correlation_ids
        while not _correlations_queue.empty():
            sender, corr_id = _correlations_queue.get_nowait()
            with lock:
                _correlation_ids[corr_id] = sender

    async def call_route(self, entry_id, *args, _meta=None, **kwargs):
        global _correlation_ids
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
        try:
            if corr_id in _correlation_ids:
                with lock:
                    sender = _correlation_ids.pop(corr_id)
                if not sender.closed and sender.writable:
                    sender.send(body)
                else:
                    logger.error(
                        "Sender is closed or not writable.{}, {}".format(
                            sender.closed, sender.writable)
                    )
            else:
                logger.error(
                    "Correlation id: {} not found on "
                    "_correlation_ids dict.".format(corr_id)
                )
        except Exception:
            logger.exception(
                "Error with ReplyListener when returning an answer from a "
                "RPC call."
            )
