import contextlib
import os
from dotenv import load_dotenv, find_dotenv
import sqlalchemy
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from typing import AsyncGenerator
import psycopg_pool
# Asumiendo que se requiere la librería 'psycopg' para el driver asíncrono
# Aunque SQLAlchemy maneja esto a través de la URL del dialecto, es bueno saberlo.

class DatabaseManager:
    """
    Gestiona la conexión al pool de la base de datos PostgreSQL de forma asíncrona.
    Implementa un patrón Singleton para asegurar que solo haya una instancia
    de motor y sesión en toda la aplicación.
    """
    _instance = None
    _engine = None
    _async_session: async_sessionmaker[AsyncSession] = None
    _pool_ready = False
    _url_conn = None

    def __new__(self, url_conn: str = None):
        if self._instance is None:
            self._url_conn = url_conn
            self._instance = super(DatabaseManager, self).__new__(self)
        return self._instance

    @staticmethod
    def _load_env():
        """Carga las variables de entorno desde el archivo .env."""
        if not os.path.exists(".env"):
            print("Advertencia: No se encontró el archivo .env. Usando variables de entorno del sistema.")
            return

        load_dotenv(find_dotenv())

    def health_pool(self)-> bool:
        return self._pool_ready

    @classmethod
    async def init_engine(self):
        """Establece la conexión al motor y la sesión."""
        self._load_env()

        control_url: str = f"postgresql://{os.environ["POSTGRES_USER"]}:{os.environ["POSTGRES_PASSWORD"]}@{os.environ["POSTGRES_HOST"]}:{os.environ["PORT"]}/{os.environ["POSTGRES_DB"]}"

        db_url = control_url if not self._url_conn else self._url_conn.replace("+psycopg", "")
        if not db_url:
            raise EnvironmentError(
                "DATABASE_URL no está definida en las variables de entorno o en el .env file."
            )

        pool = psycopg_pool.AsyncConnectionPool(conninfo=db_url,
                                                open=False,
                                                check=psycopg_pool.AsyncConnectionPool.check_connection,
                                                close_returns=True)
        await pool.open()

        self._engine = create_async_engine(
                            db_url,
                            poolclass=sqlalchemy.pool.NullPool
                        )

        self._async_session = async_sessionmaker(
            bind=self._engine,
            autoflush=False,
            class_=AsyncSession,
            expire_on_commit=False
        )
        self._pool_ready = True

    @contextlib.asynccontextmanager
    async def get_session(self) -> AsyncGenerator[AsyncSession]:
        if self._async_session is None:
            raise Exception("DatabaseSessionManager is not initialized")

        session = self._async_session()
        try:
            yield session

            await session.commit()
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()


    @classmethod
    async def close_db_pool(self):
        """Desconecta el motor de la base de datos al cerrar la aplicación."""
        if self._engine:
            print("Cerrando pool de conexiones de la base de datos...")
            await self._engine.dispose()
            self._async_session = None
        self._pool_ready = False
