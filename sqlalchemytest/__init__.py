from datetime import UTC, date, datetime
import gc
import asyncio
import functools
import itertools
import time
from functools import wraps
import threading
from memory_profiler import profile

import psutil

import sqlalchemy
from sqlalchemy.dialects.postgresql import DATERANGE, SMALLINT
from sqlalchemy.dialects.postgresql import Range as SQLRange
from sqlalchemy.ext.asyncio import AsyncAttrs, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, MappedAsDataclass, mapped_column, relationship
from sqlalchemy import ForeignKey, String, bindparam, func, insert, select, text


def sizeof_fmt(num, suffix="B"):
    mb = num / 1024**2
    return f"{mb:.2f} MiB"


async def get_connection():
    engine = create_async_engine("postgresql+asyncpg://postgres:test@localhost:5432/postgres")
    async_session = async_sessionmaker(engine)
    async with engine.begin() as conn:
        await conn.run_sync(BaseModel.metadata.drop_all)
        await conn.run_sync(BaseModel.metadata.create_all)
    return engine, async_session


class BaseModel(AsyncAttrs, MappedAsDataclass, DeclarativeBase):
    __abstract__ = True
    id: Mapped[int] = mapped_column(primary_key=True, nullable=False, autoincrement=True, init=False)
    createdAt: Mapped[datetime] = mapped_column(nullable=False, server_default=func.current_timestamp(), init=False)


class Request(BaseModel):
    __tablename__ = "Request"
    subscription: Mapped[int]
    # End exclusive range [date, date)
    range: Mapped[SQLRange[date]] = mapped_column(DATERANGE)
    issuerId: Mapped[str]


print("generating data")
TEST_DATA = [
    (
        datetime.now(tz=UTC),
        datetime.now(tz=UTC),
    )
    for _ in range(2000 * 1000)  # 2 million records is comparable to the production data size
]
print("done generating data")

gc.disable()


def timeit(func):
    process = psutil.Process()
    stopping = threading.Event()

    prefix = func.__name__

    def get_memory():
        memory = []
        starting_memory = process.memory_info().rss

        while not stopping.is_set():
            memory.append(process.memory_info().rss)
            time.sleep(0.1)

        normalized = [m - starting_memory for m in memory]
        print(f"[{prefix}] min memory: {sizeof_fmt(min(normalized))}")
        print(f"[{prefix}] max memory: {sizeof_fmt(max(normalized))}")

    # @profile
    async def execute_method(func):
        await func

    @wraps(func)
    async def with_benchmarks():
        # try to collect garbage before starting
        for _ in range(5):
            gc.collect()
            time.sleep(1)

        conn, sess = await get_connection()
        starting_time = time.time()

        thread = threading.Thread(target=get_memory)
        thread.start()
        await execute_method(func(sess, TEST_DATA))
        stopping.set()
        ending_time = time.time()

        print(f"[{prefix}] time seconds: {ending_time - starting_time}")
        thread.join()

        async with sess() as session:
            count = await session.execute(select(sqlalchemy.func.count(Request.id)))
            print(f"[{prefix}] record count: {count.scalar()}")

        print("-" * 80)

    return with_benchmarks


@timeit
async def version1(sess, data):
    """add objects one by one, then commit"""
    async with sess() as session:
        for d in data:
            r = SQLRange(d[0], d[1])
            session.add(Request(subscription=1, range=r, issuerId="test"))

        await session.commit()


@timeit
async def version2(sess, data):
    """add objects in batches of 10000, using insert()"""
    async with sess() as session:
        # note that the batch size is limited, postgresql fails with larger batch sizes
        for batch in itertools.batched(data, 10000):
            await session.execute(
                insert(Request).values(
                    [
                        {
                            "subscription": 1,
                            "range": SQLRange(d[0], d[1]),
                            "issuerId": "test",
                        }
                        for d in batch
                    ]
                )
            )

        # commit everything at once
        await session.commit()


@timeit
async def version3(sess, data):
    """add all objects using execute()"""
    async with sess() as session:
        await session.execute(
            Request.__table__.insert(),
            [
                {
                    "subscription": 1,
                    "range": SQLRange(d[0], d[1]),
                    "issuerId": "test",
                }
                for d in data
            ],
        )

        await session.commit()


@timeit
async def version4(sess, data):
    """use a compiled statement and execute(), similar to version3 but with compiled statements"""
    compiled = (
        Request.__table__.insert()
        .values(
            subscription=bindparam("subscription"),
            range=bindparam("range"),
            issuerId=bindparam("issuerId"),
        )
        .compile()
    )

    args = [
        {
            "subscription": 1,
            "range": (d[0], d[1]),
            "issuerId": "test",
        }
        for d in data
    ]

    async with sess() as session:
        await session.execute(text(str(compiled)), args)
        await session.commit()


@timeit
async def version5(sess, data):
    """use a compiled statement and execute(), similar to version4 but with batching to reduce memory usage"""
    compiled = (
        Request.__table__.insert()
        .values(
            subscription=bindparam("subscription"),
            range=bindparam("range"),
            issuerId=bindparam("issuerId"),
        )
        .compile()
    )

    async with sess() as session:
        for args in itertools.batched(data, 10000):
            params = [
                {
                    "subscription": 1,
                    "range": (d[0], d[1]),
                    "issuerId": "test",
                }
                for d in args
            ]
            await session.execute(text(str(compiled)), params)
        await session.commit()


async def main():
    await version1()
    await version2()
    await version3()
    await version4()
    await version5()


if __name__ == "__main__":
    asyncio.run(main())
