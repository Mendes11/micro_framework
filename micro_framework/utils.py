from micro_framework.dependencies import RunnerDependency, Dependency


def is_runner_dependency(obj):
    return isinstance(obj, RunnerDependency)

def is_dependency(obj):
    return isinstance(obj, Dependency)
