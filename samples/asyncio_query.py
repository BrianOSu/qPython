from qpython import qaioconnection
import asyncio
import time


async def kdb_query(kdb_port, query):
    start_time = time.time()
    # create connection object
    q = qaioconnection.QConnection(host='localhost', port=kdb_port)
    # initialize connection
    await q.open()

    print(f"{q} Initialised")
    print(f"IPC version: {q.protocol_version}. Port: {kdb_port} Is connected: {q.is_connected()}\n")

    data = await q.sendSync(query)
    print(f"{q} Returned {data}")

    await q.close()
    print(f"{q} Closed")
    end_time = time.time()
    print(f"{q} Duration:{end_time - start_time}\n")


async def wrapper(kdb_port, work_queue):
    while not work_queue.empty():
        query = await work_queue.get()
        await kdb_query(kdb_port, query)


async def main():
    start_time = time.time()
    work_queue = asyncio.Queue()

    for query in [
        'system"sleep 4"; 1',
        'system"sleep 3"; 2',
        'system"sleep 3"; 3',
        'system"sleep 1"; 4',
    ]:
        await work_queue.put(query)

    # Run the tasks
    await asyncio.gather(
        asyncio.create_task(wrapper(5000, work_queue)),
        asyncio.create_task(wrapper(5001, work_queue)),
    )

    end_time = time.time()
    print(f"Total Duration: {end_time - start_time}")


if __name__ == '__main__':
    asyncio.run(main())
