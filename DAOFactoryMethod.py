import sqlite3
from abc import ABC, abstractmethod

import Environment
import Feature
from SubjectObserver import Subject, Observer, DAOUpdateObserver
from DataBaseConnection import DataBaseConnection
import Resort


class DAO(ABC):
    @abstractmethod
    def get_all(self, dbcon: DataBaseConnection) -> list:
        pass

    @abstractmethod
    def filter(self, dbcon: DataBaseConnection, params: list) -> list:
        pass

    @abstractmethod
    def add(self, dbcon: DataBaseConnection, object_):
        pass

    @abstractmethod
    def remove(self, dbcon: DataBaseConnection, object_):
        pass

    @abstractmethod
    def update(self, dbcon: DataBaseConnection, object_old, object_new):
        pass


class DAOFactory(ABC):
    @abstractmethod
    def create_DAO(self):
        pass


class ResortDAOFactory(DAOFactory):

    _observer: Observer = None

    def __init__(self, observer=None):
        self._observer = observer

    def create_DAO(self) -> DAO:
        return ResortDAO()


class FeatureDAOFactory(DAOFactory):

    _observer: Observer = None

    def __init__(self, observer=None):
        self._observer = observer

    def create_DAO(self) -> DAO:
        dao = FeatureDAO()
        if self._observer:
            dao.attach(observer)
        return dao


class EnvironmentDAOFactory(DAOFactory):

    _observer: Observer = None

    def __init__(self, observer=None):
        self._observer = observer

    def create_DAO(self) -> DAO:
        return EnvironmentDAO()


class ResortDAO(DAO):

    _last_action: dict = None
    _observers: list = list()

    def get_all(self, dbcon: DataBaseConnection) -> list:
        con = dbcon.get_connection()
        statement = """select * from resorts;"""
        all = list()
        with con:
            for row in con.execute(statement):
                all.append(row)
        return all

    def filter(self, dbcon: DataBaseConnection, params: list) -> list:
        con = dbcon.get_connection()
        if any(params):
            base_statement = """select * from resorts where """
            param_statements = [f"{param['column']}{param['op']}:{param['column']}" for param in params]
            final_statement = base_statement + " and ".join(param_statements)
            query_params = {param["column"]: param["value"] for param in params}
            filtered = list()
            with con:
                exec = con.execute(final_statement, query_params)
                for row in exec:  # protected from SQL injection
                    filtered.append(row)
            return filtered
        else:
            return self.get_all(dbcon)

    def add(self, dbcon: DataBaseConnection, resort: Resort.Resort):
        con = dbcon.get_connection()

        bs_resort = """insert into resorts (name, price) values (?, ?)"""
        bs_resort_features = """insert into resort_features (resort_id, feature_id) values (?, ?)"""
        bs_resort_environments = """insert into resort_environments (resort_id, environment_id) values (?, ?)"""

        avail_features = get_all(dbcon, FeatureDAOFactory())
        avail_features_dct = {feature: id_ for id_, feature in avail_features}
        avail_environments = get_all(dbcon, EnvironmentDAOFactory())
        avail_environments_dct = {environment: id_ for id_, environment in avail_environments}

        with con:
            cursor = con.cursor()
            cursor.execute(bs_resort, (resort.name, resort.price))
            resort_id = cursor.lastrowid

            for feature in list(resort.feature_ids):
                try:
                    feature_id = avail_features_dct[feature]
                    con.execute(bs_resort_features, (resort_id, feature_id))
                except KeyError:
                    raise sqlite3.IntegrityError()

            for environment in list(resort.environment_ids):
                try:
                    environment_id = avail_environments_dct[environment]
                    con.execute(bs_resort_environments, (resort_id, environment_id))
                except KeyError:
                    raise sqlite3.IntegrityError()

        self._last_action = {
            "action": "add",
            "object": resort
        }
        self.notify()

    def remove(self, dbcon: DataBaseConnection, object_):
        con = dbcon.get_connection()
        base_statement = """delete from resorts where id=:id"""
        delete_cond = [
            {
                "column": "name",
                "value": object_.name,
                "op": "="
            },
            {
                "column": "price",
                "value": object_.price,
                "op": "="
            }
        ]
        to_delete = self.filter(dbcon, delete_cond)

        with con:
            for td in to_delete:
                con.execute(base_statement, {"id": td[0]})

        self._last_action = {
            "action": "remove",
            "object": object_
        }
        self.notify()

    def update(self, dbcon: DataBaseConnection, object_old: Resort.Resort, object_new: Resort.Resort):
        con = dbcon.get_connection()
        base_statement = """update resorts set name=:name, price=:price where id=:id"""

        update_cond = [
            {
                "column": "name",
                "value": object_old.name,
                "op": "="
            },
            {
                "column": "price",
                "value": object_old.price,
                "op": "="
            }
        ]
        to_update = self.filter(dbcon, update_cond)

        with con:
            for tu in to_update:
                con.execute(base_statement, {
                    "id": tu[0],
                    "name": object_new.name,
                    "price": object_new.price,
                })

        self._last_action = {
            "action": "update",
            "old": object_old,
            "new": object_new
        }
        self.notify()

    def attach(self, observer: Observer) -> None:
        self._observers.append(observer)

    def detach(self, observer: Observer) -> None:
        self._observers.remove(observer)

    def notify(self) -> None:
        for observer in self._observers:
            observer.update(self)


class FeatureDAO(DAO, Subject):

    _last_action: dict = None
    _observers: list = list()

    def get_all(self, dbcon: DataBaseConnection) -> list:
        con = dbcon.get_connection()
        statement = """select * from features;"""
        all = list()
        with con:
            for row in con.execute(statement):
                all.append(row)
        return all

    def filter(self, dbcon: DataBaseConnection, params: list) -> list:
        con = dbcon.get_connection()
        if any(params):
            base_statement = """select * from features where """
            param_statements = [f"{param['column']}{param['op']}:{param['column']}" for param in params]
            final_statement = base_statement + " and ".join(param_statements)
            query_params = {param["column"]: param["value"] for param in params}
            filtered = list()
            with con:
                exec = con.execute(final_statement, query_params)
                for row in exec:  # protected from SQL injection
                    filtered.append(row)
            return filtered
        else:
            return self.get_all(dbcon)

    def add(self, dbcon: DataBaseConnection, feature: Feature.Feature):
        con = dbcon.get_connection()
        base_statement = """insert into features (name) values (?)"""

        with con:
            con.execute(base_statement, (feature.name, ))

        self._last_action = {
            "action": "add",
            "object": feature
        }
        self.notify()

    def remove(self, dbcon: DataBaseConnection, object_):
        con = dbcon.get_connection()
        base_statement = """delete from features where id=:id"""
        delete_cond = [
            {
                "column": "name",
                "value": object_.name,
                "op": "="
            }
        ]
        to_delete = self.filter(dbcon, delete_cond)

        with con:
            for td in to_delete:
                con.execute(base_statement, {"id": td[0]})

        self._last_action = {
            "action": "remove",
            "object": object_
        }
        self.notify()

    def update(self, dbcon: DataBaseConnection, object_old: Feature.Feature, object_new: Feature.Feature):
        con = dbcon.get_connection()
        base_statement = """update features set name=:name where id=:id"""
        update_cond = [
            {
                "column": "name",
                "value": object_old.name,
                "op": "="
            }
        ]
        to_update = self.filter(dbcon, update_cond)

        with con:
            for tu in to_update:
                con.execute(base_statement, {
                    "id": tu[0],
                    "name": object_new.name
                })

        self._last_action = {
            "action": "update",
            "old": object_old,
            "new": object_new
        }
        self.notify()

    def attach(self, observer: Observer) -> None:
        self._observers.append(observer)

    def detach(self, observer: Observer) -> None:
        self._observers.remove(observer)

    def notify(self) -> None:
        for observer in self._observers:
            observer.update(self)


class EnvironmentDAO(DAO):

    _last_action: dict = None
    _observers: list = list()

    def get_all(self, dbcon: DataBaseConnection) -> list:
        con = dbcon.get_connection()
        statement = """select * from environments;"""
        all = list()
        with con:
            for row in con.execute(statement):
                all.append(row)
        return all

    def filter(self, dbcon: DataBaseConnection, params: list) -> list:
        con = dbcon.get_connection()
        if any(params):
            base_statement = """select * from environments where """
            param_statements = [f"{param['column']}{param['op']}:{param['column']}" for param in params]
            final_statement = base_statement + " and ".join(param_statements)
            query_params = {param["column"]: param["value"] for param in params}
            filtered = list()
            with con:
                exec = con.execute(final_statement, query_params)
                for row in exec:  # protected from SQL injection
                    filtered.append(row)
            return filtered
        else:
            return self.get_all(dbcon)

    def add(self, dbcon: DataBaseConnection, environment: Environment.Environment):
        con = dbcon.get_connection()
        base_statement = """insert into environments (name) values (?)"""

        with con:
            con.execute(base_statement, (environment.name, ))

        self._last_action = {
            "action": "add",
            "object": environment
        }
        self.notify()

    def remove(self, dbcon: DataBaseConnection, object_):
        con = dbcon.get_connection()
        base_statement = """delete from environments where id=:id"""
        delete_cond = [
            {
                "column": "name",
                "value": object_.name,
                "op": "="
            }
        ]
        to_delete = self.filter(dbcon, delete_cond)

        with con:
            for td in to_delete:
                con.execute(base_statement, {"id": td[0]})

        self._last_action = {
            "action": "remove",
            "object": object_
        }
        self.notify()

    def update(self, dbcon: DataBaseConnection, object_old: Environment.Environment, object_new: Environment.Environment):
        con = dbcon.get_connection()
        base_statement = """update environments set name=:name where id=:id"""
        update_cond = [
            {
                "column": "name",
                "value": object_old.name,
                "op": "="
            }
        ]
        to_update = self.filter(dbcon, update_cond)

        with con:
            for tu in to_update:
                con.execute(base_statement, {
                    "id": tu[0],
                    "name": object_new.name
                })

            self._last_action = {
                "action": "update",
                "old": object_old,
                "new": object_new
            }
            self.notify()

    def attach(self, observer: Observer) -> None:
        self._observers.append(observer)

    def detach(self, observer: Observer) -> None:
        self._observers.remove(observer)

    def notify(self) -> None:
        for observer in self._observers:
            observer.update(self)


def get_all(dbcon: DataBaseConnection, dao_factory: DAOFactory) -> list:
    return dao_factory.create_DAO().get_all(dbcon)


def filter(dbcon: DataBaseConnection, dao_factory: DAOFactory, params: list) -> list:
    return dao_factory.create_DAO().filter(dbcon, params)


def add(dbcon: DataBaseConnection, dao_factory: DAOFactory, objects: list) -> None:
    for object_ in objects:
        dao_factory.create_DAO().add(dbcon, object_)


def remove(dbcon: DataBaseConnection, dao_factory: DAOFactory, objects: list) -> None:
    for object_ in objects:
        dao_factory.create_DAO().remove(dbcon, object_)


def update(dbcon: DataBaseConnection, dao_factory: DAOFactory, object_old, object_new) -> None:
    dao_factory.create_DAO().update(dbcon, object_old, object_new)


if __name__ == "__main__":
    dbconn = DataBaseConnection.get_instance()
    dbconn.open_connection("db.db")
    dbconn.init_tables()

    # register observers
    observer = DAOUpdateObserver()

    # add data
    environments = [
        Environment.Environment("savannah"),
        Environment.Environment("ocean"),
        Environment.Environment("mountain")
    ]

    features = [
        Feature.Feature("spa"),
        Feature.Feature("golf"),
        Feature.Feature("water_park")
    ]

    add(dbconn, EnvironmentDAOFactory(), environments)
    add(dbconn, FeatureDAOFactory(observer), features)

    resortBuilder = Resort.ResortBuilder()
    resortBuilder.set_name("rixos")
    resortBuilder.set_price(12999.0)
    resortBuilder.add_environments(["ocean", "mountain"])
    resortBuilder.add_feature_ids(["spa", "golf"])
    add(dbconn, ResortDAOFactory(), [resortBuilder.get_object()])

    resortBuilder = Resort.ResortBuilder()
    resortBuilder.set_name("asteria")
    resortBuilder.set_price(7999.0)
    resortBuilder.add_environments(["savannah"])
    resortBuilder.add_feature_ids(["spa", "water_park"])
    add(dbconn, ResortDAOFactory(), [resortBuilder.get_object()])

    all_resorts = get_all(dbconn, ResortDAOFactory())
    print(all_resorts)
    all_features = get_all(dbconn, FeatureDAOFactory())
    print(all_features, "\nFilter:")

    # filter
    params = [
        {
            "column": "price",
            "value": 9000.0,
            "op": "<"
        }
    ]

    f_resorts = filter(dbconn, ResortDAOFactory(), params)
    print(f_resorts, "\nUpdate:")

    update(dbconn, ResortDAOFactory(observer), resortBuilder.get_object(), Resort.Resort("very_expensive_resort", 599999.99))
    update(dbconn, FeatureDAOFactory(), features[1], Feature.Feature("expensive_feature"))

    all_resorts = get_all(dbconn, ResortDAOFactory())
    print(all_resorts)
    all_features = get_all(dbconn, FeatureDAOFactory())
    print(all_features)

    remove(dbconn, ResortDAOFactory(), [resortBuilder.get_object()])
    remove(dbconn, EnvironmentDAOFactory(observer), environments)

    all_resorts = get_all(dbconn, ResortDAOFactory())
    print("\nRemove\n", all_resorts)
    all_features = get_all(dbconn, FeatureDAOFactory())
    print(all_features)
    all_environments = get_all(dbconn, EnvironmentDAOFactory())
    print(all_environments)
