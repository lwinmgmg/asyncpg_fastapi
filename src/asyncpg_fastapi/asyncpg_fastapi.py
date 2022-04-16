import typing
from uuid import uuid4
import asyncpg
from fastapi import FastAPI

class ConfigureAsyncpg:
    def __init__(
        self,
        app: FastAPI,
        dsn: str,
        *,
        init_db: typing.Callable = None,  # callable for running sql on init
        pool=None,  # usable on testing
        **options,
    ):
        """This is the entry point to configure an asyncpg pool with fastapi.

        Arguments
            app: The fastapp application that we use to store the pool
                and bind to it's initialitzation events
            dsn: A postgresql desn like postgresql://user:password@postgresql:5432/db
            init_db: Optional callable that receives a db connection,
                for doing an initialitzation of it
            pool: This is used for testing to skip the pool initialitzation
                an just use the SingleConnectionTestingPool
            **options: connection options to directly pass to asyncpg driver
                see: https://magicstack.github.io/asyncpg/current/api/index.html#connection-pools
        """
        self.app = app
        self.dsn = dsn
        self.init_db = init_db
        self.con_opts = options
        self._pool = pool
        self._db_code = f"db{uuid4().hex}"
        self.app.router.add_event_handler("startup", self.on_connect)
        self.app.router.add_event_handler("shutdown", self.on_disconnect)

    async def on_connect(self):
        """handler called during initialitzation of asgi app, that connects to
        the db"""
        # if the pool is comming from outside (tests), don't connect it
        if self._pool:
            setattr(self.app.state, self._db_code, self._pool)
            return
        pool = await asyncpg.create_pool(dsn=self.dsn, **self.con_opts)
        async with pool.acquire() as db:
            await self.init_db(db)
        setattr(self.app.state, self._db_code, pool)

    async def on_disconnect(self):
        # if the pool is comming from outside, don't desconnect it
        # someone else will do (usualy a pytest fixture)
        if self._pool:
            return
        await getattr(self.app.state, self._db_code).close()

    def on_init(self, func):
        self.init_db = func
        return func

    @property
    def pool(self):
        return getattr(self.app.state, self._db_code)

    async def connection(self):
        """
        A ready to use connection Dependency just usable
        on your path functions that gets a connection from the pool
        Example:
            db = configure_asyncpg(app, "dsn://")
            @app.get("/")
            async def get_content(db = Depens(db.connection)):
                await db.fetch("SELECT * from pg_schemas")
        """
        async with self.pool.acquire() as db:
            yield db

    async def transaction(self):
        """
        A ready to use transaction Dependecy just usable on a path function
        Example:
            db = configure_asyncpg(app, "dsn://")
            @app.get("/")
            async def get_content(db = Depens(db.transaction)):
                await db.execute("insert into keys values (1, 2)")
                await db.execute("insert into keys values (1, 2)")
        All view function executed, are wrapped inside a postgresql transaction
        """
        async with self.pool.acquire() as db:
            txn = db.transaction()
            await txn.start()
            try:
                yield db
            except:  # noqa
                await txn.rollback()
                raise
            else:
                await txn.commit()

    atomic = transaction
