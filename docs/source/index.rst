.. Micro Framework documentation master file, created by
   sphinx-quickstart on Mon May 25 14:16:45 2020.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to Micro Framework's documentation!
===========================================

(yet another) Python Microservices Framework capable of running functions in
threads or processes.

The code structure is inspired by Nameko (https://github.com/nameko/nameko)
but with some different concepts regarding the function call.

Instalation
===========================================
.. code-block::

   pip install micro_framework



Usage
=========================================

To use, simply create a module to start, define your routes and done.

.. note::
   Note that the function is only the path. That is to enable us to do some script import configurations like django.setup() in the worker only.

python main.py

.. code-block:: python

    import logging
    from micro_framework.runner import Runner
    from micro_framework.routes import Route, CallbackRoute
    from micro_framework.amqp.entrypoints import EventListener
    from micro_framework.retry import AsyncBackOff, BackOffData
    from micro_framework.amqp.dependencies import Producer

    config = {
      'AMQP_URI': 'amqp://guest:guest@localhost:5672',
      'MAX_WORKERS': 3,
      'SERVICE_NAME': 'my_service',
      'WORKER_MODE': 'thread'
    }

    logger = logging.getLogger(__name__)
    logging.basicConfig(level=logging.INFO, format='%(message)s')

    routes = [
      CallbackRoute(
         'tasks.test',
         callback_function_path='tasks.test_failure', # Called if error and after the backoff max_retries
         entrypoint=EventListener(
            source_service='my_exchange', event_name='my_routing_key'
         ),
         dependencies={'producer': Producer(), 'retry': BackOffData()},
         backoff=AsyncBackOff(
            max_retries=5, interval=10000, exception_list=None
         ),
      ),
      Route(
         'tasks.test2',
         entrypoint=EventListener(
         source_service='my_service', event_name='event_name',
         ),
      ),
    ]

    if __name__ == '__main__':
       runner = Runner(routes, config)
       runner.start()

tasks.py

.. code-block:: python

   def test(payload, producer):
       # business_logic
       producer('event_name', {})

   def test2(payload):
       print("Called after test function")

   def test_failure(payload, producer, retry):
       print("Called after test failed and the max_retries is reached.")


Components
===========================================

Runner
-------------------------------------------

   It is the component that will start your service. You instantiate it with
   the configuration and the contexts.

Runner Context
-------------------------------------------

Aggregate Routes that share the same workers.

   * In the same Runner (your service), you can have multiple
  contexts that do not share the workers number. That way you can have
  dedicated workers to do some long-running tasks without letting your other
  tasks starve due to no workers available.
  * Each Context will have it's own metric label to differentiate it.


Route
-------------------------------------------

A route acts as a pipeline for the entrypoint payload until reaching the
**Target** function to be executed.

Inside the route you define the behavior of the framework before creating a
worker and after the worker finished the execution. It has a default Worker
class but accepts a custom one.

Inside the route you can add message translators that will be used by the
worker when spawned and the entrypoint to trigger it.

Entrypoint
-------------------------------------------

An entrypoint is the trigger of an external action to the execution of the
route configured to it.

Allowed Entrypoints are:

   - TimeEntrypoint:
      -- Its a timer, that triggers at each specified time. (Not cluster aware)

   - amqp.EventListener:
      -- It's an AMQP queue listener, that deals automatically configures all
      AMQP stuff and can be called either by this Framework **Producer**
      Dependency or by Nameko's EventDispatcher (yes, you can use both!).

      -- .. note::
         The same applies to our Producer allowing to trigger Nameko's
         event_source

   - websocket.WebSocketListener:
      -- Start's a Websocket Server that implements our RPC Manager Class.
         You can call it through our WebSocketClient dependency.


   - amqp.RPCListener:
      -- Allows the **Target** to be called through AMQP protocol.


Worker
-------------------------------------------

A worker is the class responsible for the function call and the
 steps before and after such as message translating using the route
  translators and dependency injection from the given dependencies.


Dependency
-------------------------------------------

It is a class that is Provides usually a callable to be injected into the
**Target**.

To Inject a dependency, you can either declare it in a class that has the
**Target** as a method, or use the @inject decorator in the method/function
directly, so it will be passed as a argument (named as the attribute name in
the @inject)


MessageTranslator
-------------------------------------------

Transforms a payload/message into another payload/message.



Configurations
==============================================

   - AMQP_URI: URI of the broker (When using AMQP Stuff).


   - MAX_WORKERS: Number of workers to spawn simultaneously


   - SERVICE_NAME: The name of the running service, useful for Producer
    dependency which automatically includes the service_name in the dispatch.


   - WORKER_MODE: Runner mode. It can be:
       - thread: Each worker will run in the same process as Threads.
        When stopped, the worker will finish only the current
        running task and ignore the pending ones in the pool

       - greedy_thread: Same as thread but the worker will only finish after
         completing all pending tasks in the pool

       - process: Each worker will run in a different process.

       - greedy_process: Same as process and the same behavior when shutting
        down as greedy_thread.


   - MAX_TASKS_PER_CHILD: (process mode only) Kill the process after handling N
     tasks and then start a new one.


   - WEBSOCKET_IP (default="0.0.0.0"): Address that the WS server will listen
to.


   - WEBSOCKET_PORT (default=8765): PORT that the WS server will listen to.


   - ENABLE_METRICS (default=False): Allows the framework to export prometheus
     metrics in a configured host/port


   - METRICS dict: Configurations to the Metric Server

     - HOST (default="0.0.0.0")
     - PORT (default=8000)

Notes
==============================================
.. note::
   In order to use the AsyncBackoff class, you must install the RabbitMQ
   plugin: **rabbitmq_delayed_message_exchange**
   (https://github.com/rabbitmq/rabbitmq-delayed-message-exchange)

.. include:: modules.rst

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
