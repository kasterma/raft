"""Trying some async ideas for raft setup"""

from asyncio import Queue, QueueEmpty, QueueFull
import asyncio
import random
import time
from threading import Thread

async def send(q: Queue, label):
    """Send the messages this server sends to other servers"""
    while True:
        m = await q.get()
        print(f"{label}: Sending {m}.")
        q.task_done()

async def recv(q: Queue, label):
    """Receive messages for this server from other servers"""
    while True:
        i = random.randint(0, 1000)
        try:
            q.put_nowait(i)
            print(f"{label}: Put {i} on recv queue.")
            await asyncio.sleep(1)
        except QueueFull:
            print(f"{label}: recv queue is full.")

async def handle(q_send: Queue, q_recv: Queue, label):
    """Handle received messages by sending them"""
    while True:
        try:
            m = q_recv.get_nowait()
        except QueueEmpty:
            print(f"{label}: nothing on queue")
            await asyncio.sleep(0.5)
            continue

        try:
            q_send.put_nowait(m)
        except QueueFull:
            print("sending queue is full, dropped")

async def main(label):
    q_send = Queue()
    q_recv = Queue()

    tasks = [asyncio.create_task(send(q_send, label)),
             asyncio.create_task(recv(q_recv, label)),
             asyncio.create_task(handle(q_send, q_recv, label))]

    await asyncio.sleep(5)

    for task in tasks:
        task.cancel()

    rs = await asyncio.gather(*tasks, return_exceptions=True)
    print(rs, label)

Thread(target=lambda: asyncio.run(main("one"))).start()
Thread(target=lambda: asyncio.run(main("two"))).start()
print("hi")
