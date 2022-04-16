# Asyncpg for fastapi (For multiple databases)

This package is from fastapi_asyncpg package(https://pypi.org/project/fastapi-asyncpg/) whose maintainer is jordic
The original package does not support multiple database to connect. It only allow to connect one database for one application.
In my project, I need to connect multiple readonly databases. So, I modified a little to support multiple databases.
