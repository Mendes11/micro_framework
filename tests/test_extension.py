import pytest

from micro_framework.extensions import Extension
from micro_framework.runner import Runner, RunnerContext

pytestmark = pytest.mark.asyncio


@pytest.fixture
def runner(config, mocker):
    return Runner(contexts=[mocker.MagicMock()], config=config)


@pytest.fixture
async def runner_context_factory(config, mocker, runner):
    async def new_runner_context(routes=None):
        if routes is None:
            routes = [mocker.MagicMock()]
        ctx = RunnerContext(routes=routes)
        ctx.config = config
        await ctx.bind(runner)
        runner.contexts.append(ctx)
        return ctx

    return new_runner_context


class NormalExtension(Extension):
    pass


class ContextSingleton(Extension):
    context_singleton = True


class SingletonExtension(Extension):
    singleton = True


class PicklableExtension(Extension):
    picklable = True



async def test_normal_bind_extension_cloned(runner_context_factory):
    context = await runner_context_factory()
    ext = NormalExtension()
    new_ext = ext.bind(context, None)
    assert ext != new_ext


async def test_normal_bind_extension_has_runner(runner_context_factory):
    context = await runner_context_factory()
    ext = NormalExtension()
    new_ext = ext.bind(context)
    assert new_ext.runner == context


async def test_normal_bind_extension_with_inner_extension_cloned(
        runner_context_factory):
    class ParentExt(Extension):
        inner_ext = NormalExtension()

    context = await runner_context_factory()
    ext = ParentExt()
    new_ext = ext.bind(context)

    assert new_ext.inner_ext != ext.inner_ext


async def test_normal_bind_extension_with_inner_extension_with_runner(
        runner_context_factory):
    class ParentExt(Extension):
        inner_ext = NormalExtension()

    context = await runner_context_factory()
    ext = ParentExt()
    new_ext = ext.bind(context)

    assert new_ext.inner_ext.runner == context


async def test_bind_different_extensions(runner_context_factory):
    class ParentExt(Extension):
        inner_ext = NormalExtension()

    ext1 = ParentExt()
    ext2 = ParentExt()

    ctx = await runner_context_factory()
    new_ext1 = ext1.bind(ctx)
    new_ext2 = ext2.bind(ctx)

    assert new_ext1 != new_ext2
    assert new_ext1.inner_ext != new_ext2.inner_ext


async def test_bind_extension_registered(runner_context_factory):
    class ParentExt(Extension):
        inner_ext = NormalExtension()

    context = await runner_context_factory()
    ext = ParentExt()
    new_ext = ext.bind(context)

    assert context.extra_extensions == [new_ext.inner_ext, new_ext]


async def test_bind_context_singleton(runner_context_factory):
    # We will bind two extensions through different contexts each to assert
    # that between the same context both have the same ContextSingleton()

    class ParentExtension(Extension):
        context_singleton_ext = ContextSingleton()

    ext1 = ParentExtension()
    ext2 = ParentExtension()
    ctx1 = await runner_context_factory()
    ctx2 = await runner_context_factory()

    # Bind both for Context1
    new_ext1_1 = ext1.bind(ctx1)
    new_ext1_2 = ext2.bind(ctx1)
    # Bind both for Context2
    new_ext2_1 = ext1.bind(ctx2)
    new_ext2_2 = ext2.bind(ctx2)

    # Assert all different
    assert len({new_ext1_1, new_ext1_2,  new_ext2_1, new_ext2_2}) == 4

    # Assert ContextSingleton is equal through same Context
    assert new_ext1_1.context_singleton_ext == new_ext1_2.context_singleton_ext
    assert new_ext2_1.context_singleton_ext == new_ext2_2.context_singleton_ext

    # Assert ContextSingleton is different in different Context
    assert new_ext1_1.context_singleton_ext != new_ext2_1.context_singleton_ext
    assert new_ext1_2.context_singleton_ext != new_ext2_2.context_singleton_ext


async def test_bind_singleton(runner_context_factory):
    class ParentExtension(Extension):
        singleton_ext = SingletonExtension()

    ext1 = ParentExtension()
    ext2 = ParentExtension()
    ctx1 = await runner_context_factory()
    ctx2 = await runner_context_factory()

    # Bind both for Context1
    new_ext1_1 = ext1.bind(ctx1)
    new_ext1_2 = ext2.bind(ctx1)
    # Bind both for Context2
    new_ext2_1 = ext1.bind(ctx2)
    new_ext2_2 = ext2.bind(ctx2)

    # Assert all different
    assert len({new_ext1_1, new_ext1_2, new_ext2_1, new_ext2_2}) == 4

    # Assert ContextSingleton is equal through same Context
    assert new_ext1_1.singleton_ext == new_ext1_2.singleton_ext
    assert new_ext2_1.singleton_ext == new_ext2_2.singleton_ext

    # Assert ContextSingleton is also equal in different Context
    assert new_ext1_1.singleton_ext == new_ext2_1.singleton_ext
    assert new_ext1_2.singleton_ext == new_ext2_2.singleton_ext


async def test_bind_picklable_extension(runner_context_factory):
    class ParentExtension(Extension):
        picklable_extension = PicklableExtension()

    ext = ParentExtension()

    ctx = await runner_context_factory()
    new_ext = ext.bind(ctx)

    assert new_ext.picklable_extension != ext.picklable_extension
    assert new_ext.runner == ctx
    assert new_ext.picklable_extension.runner is None
    assert new_ext.picklable_extension.parent is None


async def test_bind_picklable_extension_with_non_picklable_inside(
        runner_context_factory):
    class PicklableParent(PicklableExtension):
        non_picklable = NormalExtension()

    ext = PicklableParent()
    ctx = await runner_context_factory()

    with pytest.raises(TypeError):
        ext.bind(ctx)

async def test_bind_picklable_extension_not_singleton(
        runner_context_factory):
    class InnerPicklable(Extension):
        picklable = True
        singleton = False
        context_singleton = False

    class ParentExtension(NormalExtension):
        inner_ext = InnerPicklable()

    ext1 = ParentExtension()
    ext2 = ParentExtension()
    ctx1 = await runner_context_factory()
    ctx2 = await runner_context_factory()

    # Bind both for Context1
    new_ext1_1 = ext1.bind(ctx1)
    new_ext1_2 = ext2.bind(ctx1)
    # Bind both for Context2
    new_ext2_1 = ext1.bind(ctx2)
    new_ext2_2 = ext2.bind(ctx2)

    # Assert all different
    assert len({new_ext1_1, new_ext1_2, new_ext2_1, new_ext2_2}) == 4

    # Assert Runner is empty for inner_ext
    assert all([ext.inner_ext.runner is None
                for ext in [new_ext1_1, new_ext1_2, new_ext2_1, new_ext2_2]])

    # Assert ContextSingleton is different through same Context
    assert new_ext1_1.inner_ext != new_ext1_2.inner_ext
    assert new_ext2_1.inner_ext != new_ext2_2.inner_ext

    # Assert ContextSingleton is different in different Context
    assert new_ext1_1.inner_ext != new_ext2_1.inner_ext
    assert new_ext1_2.inner_ext != new_ext2_2.inner_ext


async def test_bind_picklable_extension_as_context_singleton(
        runner_context_factory):
    class InnerPicklable(Extension):
        picklable = True
        context_singleton = True

    class ParentExtension(NormalExtension):
        inner_ext = InnerPicklable()

    ext1 = ParentExtension()
    ext2 = ParentExtension()
    ctx1 = await runner_context_factory()
    ctx2 = await runner_context_factory()

    # Bind both for Context1
    new_ext1_1 = ext1.bind(ctx1)
    new_ext1_2 = ext2.bind(ctx1)
    # Bind both for Context2
    new_ext2_1 = ext1.bind(ctx2)
    new_ext2_2 = ext2.bind(ctx2)

    # Assert all different
    assert len({new_ext1_1, new_ext1_2, new_ext2_1, new_ext2_2}) == 4

    # Assert Runner is empty for inner_ext
    assert all([ext.inner_ext.runner is None
                for ext in [new_ext1_1, new_ext1_2, new_ext2_1,new_ext2_2]])

    # Assert ContextSingleton is equal through same Context
    assert new_ext1_1.inner_ext == new_ext1_2.inner_ext
    assert new_ext2_1.inner_ext == new_ext2_2.inner_ext

    # Assert ContextSingleton is different in different Context
    assert new_ext1_1.inner_ext != new_ext2_1.inner_ext
    assert new_ext1_2.inner_ext != new_ext2_2.inner_ext


async def test_bind_picklable_extension_as_singleton(
        runner_context_factory):
    class InnerPicklable(Extension):
        picklable = True
        singleton = True

    class ParentExtension(NormalExtension):
        inner_ext = InnerPicklable()

    ext1 = ParentExtension()
    ext2 = ParentExtension()
    ctx1 = await runner_context_factory()
    ctx2 = await runner_context_factory()

    # Bind both for Context1
    new_ext1_1 = ext1.bind(ctx1)
    new_ext1_2 = ext2.bind(ctx1)
    # Bind both for Context2
    new_ext2_1 = ext1.bind(ctx2)
    new_ext2_2 = ext2.bind(ctx2)

    # Assert all different
    assert len({new_ext1_1, new_ext1_2, new_ext2_1, new_ext2_2}) == 4

    # Assert Runner is empty for inner_ext
    assert all([ext.inner_ext.runner is None
                for ext in [new_ext1_1, new_ext1_2, new_ext2_1, new_ext2_2]])

    # Assert ContextSingleton is equal through same Context
    assert new_ext1_1.inner_ext == new_ext1_2.inner_ext
    assert new_ext2_1.inner_ext == new_ext2_2.inner_ext

    # Assert ContextSingleton is equal in different Context
    assert new_ext1_1.inner_ext == new_ext2_1.inner_ext
    assert new_ext1_2.inner_ext == new_ext2_2.inner_ext
