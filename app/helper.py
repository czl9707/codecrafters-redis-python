import random
import string
import asyncio
from typing import List, Tuple, Coroutine


# helper functions
def get_random_replication_id() -> str:
    return "".join(
        random.choice(string.ascii_lowercase + string.digits) for _ in range(40)
    )


async def wait_for_n_finish(
    coros: List[Coroutine], n: int, timeout: int = 0
) -> Tuple[List[asyncio.Task], List[asyncio.Task]]:
    n = min(len(coros), n)
    if n == 0:
        return [], []

    pending = [asyncio.create_task(coro) for coro in coros]
    finished = []

    if timeout > 0:
        timeout_task = asyncio.create_task(asyncio.sleep(timeout))

    while True:
        done = [task for task in pending if task.done()]

        for task in done:
            pending.remove(task)
        finished.extend(done)

        if (len(finished) >= n) or (timeout > 0 and timeout_task.done()):
            break
        await asyncio.sleep(0)  # Give back control

    for task in pending:
        task.cancel()
    timeout_task.cancel()

    return finished, pending
