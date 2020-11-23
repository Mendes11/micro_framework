from .multiprocess import ProcessSpawner, ProcessPoolSpawner
from .thread import ThreadSpawner, ThreadPoolSpawner


# Greedy Spawner means that the runner will consume all received tasks so far
# when a stop signal is sent.

# The non-greedy spawner will finish the current tasks and skip all pending
# ones. (Entrypoints with acknowledge are good candidates for this)

SPAWNERS = {
    'thread': ThreadSpawner,
    'process': ProcessSpawner,
    'greedy_thread': ThreadPoolSpawner,
    # Will consume all received tasks on stop
    'greedy_process': ProcessPoolSpawner,  # Will consume all received tasks
}
