from __future__ import annotations

import asyncio

import pytest

from signal_noise.store.event_bus import EventBus, SignalEvent


def _make_event(name: str = "test_signal", event_type: str = "update") -> SignalEvent:
    return SignalEvent(
        name=name, timestamp="2026-03-01T00:00:00", value=1.0,
        event_type=event_type,
    )


@pytest.mark.asyncio
async def test_publish_subscribe():
    bus = EventBus()
    received: list[SignalEvent] = []

    async def _consumer():
        async for event in bus.subscribe("test_*"):
            received.append(event)
            if len(received) >= 2:
                break

    task = asyncio.create_task(_consumer())
    await asyncio.sleep(0.01)

    await bus.publish(_make_event("test_signal"))
    await bus.publish(_make_event("test_other"))
    await bus.publish(_make_event("no_match"))  # should not match

    await asyncio.wait_for(task, timeout=1.0)
    assert len(received) == 2
    assert received[0].name == "test_signal"
    assert received[1].name == "test_other"


@pytest.mark.asyncio
async def test_comma_separated_pattern():
    bus = EventBus()
    received: list[SignalEvent] = []

    async def _consumer():
        async for event in bus.subscribe("funding_rate_btc,liq_ratio_*"):
            received.append(event)
            if len(received) >= 2:
                break

    task = asyncio.create_task(_consumer())
    await asyncio.sleep(0.01)

    await bus.publish(_make_event("funding_rate_btc"))
    await bus.publish(_make_event("funding_rate_eth"))  # no match
    await bus.publish(_make_event("liq_ratio_btc_1h"))

    await asyncio.wait_for(task, timeout=1.0)
    assert len(received) == 2
    assert received[0].name == "funding_rate_btc"
    assert received[1].name == "liq_ratio_btc_1h"


@pytest.mark.asyncio
async def test_multiple_subscribers():
    bus = EventBus()
    r1: list[SignalEvent] = []
    r2: list[SignalEvent] = []

    async def _consumer(results, pattern):
        async for event in bus.subscribe(pattern):
            results.append(event)
            break

    t1 = asyncio.create_task(_consumer(r1, "*"))
    t2 = asyncio.create_task(_consumer(r2, "funding_*"))
    await asyncio.sleep(0.01)

    assert bus.subscriber_count() == 2
    await bus.publish(_make_event("funding_rate_btc"))

    await asyncio.wait_for(asyncio.gather(t1, t2), timeout=1.0)
    assert len(r1) == 1
    assert len(r2) == 1


@pytest.mark.asyncio
async def test_queue_full_drops():
    bus = EventBus(max_queue_size=1)
    received: list[SignalEvent] = []

    async def _consumer():
        async for event in bus.subscribe("*"):
            received.append(event)
            break

    task = asyncio.create_task(_consumer())
    await asyncio.sleep(0.01)

    # First fills queue, second should be dropped
    n1 = await bus.publish(_make_event("sig1"))
    n2 = await bus.publish(_make_event("sig2"))
    assert n1 == 1
    assert n2 == 0  # dropped

    await asyncio.wait_for(task, timeout=1.0)
    assert len(received) == 1
    assert received[0].name == "sig1"


@pytest.mark.asyncio
async def test_subscriber_cleanup_on_cancel():
    bus = EventBus()

    async def _consumer():
        async for _ in bus.subscribe("*"):
            pass

    task = asyncio.create_task(_consumer())
    await asyncio.sleep(0.01)
    assert bus.subscriber_count() == 1

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert bus.subscriber_count() == 0


@pytest.mark.asyncio
async def test_no_subscribers():
    bus = EventBus()
    n = await bus.publish(_make_event())
    assert n == 0
