import asyncio
from event_driven import EventDrivenPipeline


def test_start_returns_after_stop_without_events():
    pipeline = EventDrivenPipeline()

    async def runner():
        task = asyncio.create_task(pipeline.start())
        await asyncio.sleep(0.1)
        await pipeline.stop()
        await asyncio.wait_for(task, timeout=1)
        assert task.done()

    asyncio.run(runner())
