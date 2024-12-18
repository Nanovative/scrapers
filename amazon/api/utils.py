import asyncio

event_queue = asyncio.Queue(5)
event_loop_lock = asyncio.Lock()
