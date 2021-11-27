import sqlite3


CREATE_TABLES = ["""
create table if not exists resorts (
    id integer primary key autoincrement,
    name text not null,
    price real not null
);""",
"""
create table if not exists features (
    id integer primary key autoincrement,
    name text not null
);""",
"""
create table if not exists environments (
    id integer primary key autoincrement,
    name text not null
);""",
"""
create table if not exists resort_features (
    resort_id integer not null,
    feature_id integer not null,
    primary key (resort_id, feature_id),
    foreign key (resort_id) references resorts(id) on delete cascade,
    foreign key (feature_id) references features(id) on delete cascade
);
""",
"""
create table if not exists resort_environments (
    resort_id integer not null,
    environment_id integer not null,
    primary key (resort_id, environment_id),
    foreign key (resort_id) references resorts(id) on delete cascade,
    foreign key (environment_id) references environments(id) on delete cascade
);
"""]


class DataBaseConnection(object):
    __instance = None
    connection = None

    def __init__(self):
        pass

    @classmethod
    def get_connection(cls):
        if not cls.__instance.connection:
            raise ValueError("No connection.")
        return cls.__instance.connection

    @classmethod
    def open_connection(cls, db_file_path: str):
        if cls.__instance.connection:
            # close previous connection
            raise ConnectionError("Close previous connection.")

        # open new connection
        cls.__instance.connection = sqlite3.connect(db_file_path)
        return cls.__instance.connection

    @classmethod
    def close_connection(cls):
        if cls.__instance.connection:
            try:
                cls.__instance.connection.close()
            except Exception:
                pass
            finally:
                cls.__instance.connection = None

    @classmethod
    def get_instance(cls):
        if not cls.__instance:
            cls.__instance = DataBaseConnection()
        return cls.__instance

    @classmethod
    def init_tables(cls):
        con = cls.get_connection()
        with con:
            for statement in CREATE_TABLES:
                con.execute(statement)


if __name__ == "__main__":
    instance = DataBaseConnection.get_instance()
    instance.open_connection("db.db")
    instance.init_tables()
