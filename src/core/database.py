import psycopg2
import logging
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT, ISOLATION_LEVEL_DEFAULT
from typing import List
from src.models.plant import Plant

class DatabaseManager:
    def __init__(self, plant: Plant, connect_to_postgres_db: bool = False):
        self.plant = plant
        self.db_name = plant.db_name
        
        db_to_connect = "postgres" if connect_to_postgres_db else self.db_name
        
        self.conn = None
        self.cursor = None
        self._connect(db_to_connect)

    def _connect(self, db_to_connect: str):
        try:
            self.conn = psycopg2.connect(
                host=self.plant.db_host,
                port=self.plant.db_port,
                dbname=db_to_connect,
                user=self.plant.db_user,
                password=self.plant.db_password
            )
            self.cursor = self.conn.cursor()
        except Exception as e:
            logging.error(f"Error conectando a BD de {self.plant.name}: {e}")
            raise e

    def execute_queries(self, queries: List[str]):
        """Ejecuta una lista de queries con autocommit seguro"""
        try:
            for query in queries:
                if query and query.strip():
                    self.cursor.execute(query)
            self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            logging.error(f"Error ejecutando queries en {self.plant.name}: {e}")
            raise e

    def execute_single_query(self, query: str, params: tuple = None, fetchone=False, fetchall=False):
        """Ejecuta un solo query y opcionalmente retorna resultados"""
        try:
            self.cursor.execute(query, params)
            if fetchone:
                return self.cursor.fetchone()
            if fetchall:
                return self.cursor.fetchall()
            self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            raise e

    def create_database(self):
        """Solo usado en el setup inicial para crear la base de datos vacía."""
        self.cursor.execute("SELECT 1 FROM pg_database WHERE datname = %s;", (self.db_name,))
        if self.cursor.fetchone():
            return # Ya existe
        try:
            self.conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            self.cursor.execute(f'CREATE DATABASE "{self.db_name}";')
        finally:
            self.conn.set_isolation_level(ISOLATION_LEVEL_DEFAULT)

    def close(self):
        if self.cursor: self.cursor.close()
        if self.conn: self.conn.close()