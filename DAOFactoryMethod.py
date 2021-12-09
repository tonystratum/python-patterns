import sqlite3
from abc import ABC, abstractmethod

import Environment
import Feature
from SubjectObserver import Subject, Observer, DAOUpdateObserver
from DataBaseConnection import DataBaseConnection
import Resort
import Memento


class DAO(ABC):
    @abstractmethod
    def get_all(self) -> list:
        pass

    @abstractmethod
    def filter(self, params: list) -> list:
        pass

    @abstractmethod
    def add(self, object_):
        pass

    @abstractmethod
    def remove(self, object_):
        pass

    @abstractmethod
    def update(self, object_old, object_new):
        pass


class DAOFactory(ABC):
    @abstractmethod
    def create_DAO(self):
        pass


class ResortDAOFactory(DAOFactory):
    _observer: Observer = None

    def __init__(self, dbcon: DataBaseConnection = None, observer=None):
        self._dbcon = dbcon
        self._observer = observer

    def create_DAO(self) -> DAO:
        dao = ResortDAO(self._dbcon)
        if self._observer:
            dao.attach(self._observer)
        return dao


class FeatureDAOFactory(DAOFactory):
    _observer: Observer = None

    def __init__(self, dbcon: DataBaseConnection = None, observer=None):
        self._dbcon = dbcon
        self._observer = observer

    def create_DAO(self) -> DAO:
        dao = FeatureDAO(self._dbcon)
        if self._observer:
            dao.attach(self._observer)
        return dao


class EnvironmentDAOFactory(DAOFactory):
    _observer: Observer = None

    def __init__(self, dbcon: DataBaseConnection = None, observer=None):
        self._dbcon = dbcon
        self._observer = observer

    def create_DAO(self) -> DAO:
        dao = EnvironmentDAO(self._dbcon)
        if self._observer:
            dao.attach(self._observer)
        return dao


class ResortDAO(DAO, Subject):
    _last_action: dict = None
    _observers: list = list()
    _dbcon: DataBaseConnection = None

    def __init__(self, dbcon: DataBaseConnection = None):
        self._dbcon = dbcon

    def get_all(self) -> list:
        con = self._dbcon.get_connection()
        statement = """select * from resorts;"""
        all = list()
        with con:
            for row in con.execute(statement):
                all.append(row)
        return all

    def filter(self, params: list) -> list:
        con = self._dbcon.get_connection()
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
            return self.get_all()

    def add(self, resort: Resort.Resort):
        con = self._dbcon.get_connection()

        bs_resort = """insert into resorts (name, price) values (?, ?)"""
        bs_resort_features = """insert into resort_features (resort_id, feature_id) values (?, ?)"""
        bs_resort_environments = """insert into resort_environments (resort_id, environment_id) values (?, ?)"""

        avail_features = get_all(FeatureDAOFactory(self._dbcon))
        avail_features_dct = {feature: id_ for id_, feature in avail_features}
        avail_environments = get_all(EnvironmentDAOFactory(self._dbcon))
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

    def remove(self, object_):
        con = self._dbcon.get_connection()
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
        to_delete = self.filter(delete_cond)

        with con:
            for td in to_delete:
                con.execute(base_statement, {"id": td[0]})

        self._last_action = {
            "action": "remove",
            "object": object_
        }
        self.notify()

    def update(self, object_old: Resort.Resort, object_new: Resort.Resort):
        con = self._dbcon.get_connection()
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
        to_update = self.filter(update_cond)

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

    def save(self) -> Memento.Memento:
        return Memento.ResortDAOMemento(self._last_action)

    def restore(self, memento: Memento.Memento):
        self._last_action = memento.get_state()
        if self._last_action["action"] == "update":
            self.update(self._last_action["new"], self._last_action["old"])
        else:
            raise NotImplementedError


class FeatureDAO(DAO, Subject):
    _last_action: dict = None
    _observers: list = list()
    _dbcon: DataBaseConnection = None

    def __init__(self, dbcon: DataBaseConnection = None):
        self._dbcon = dbcon

    def get_all(self) -> list:
        con = self._dbcon.get_connection()
        statement = """select * from features;"""
        all = list()
        with con:
            for row in con.execute(statement):
                all.append(row)
        return all

    def filter(self, params: list) -> list:
        con = self._dbcon.get_connection()
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
            return self.get_all()

    def add(self, feature: Feature.Feature):
        con = self._dbcon.get_connection()
        base_statement = """insert into features (name) values (?)"""

        with con:
            con.execute(base_statement, (feature.name,))

        self._last_action = {
            "action": "add",
            "object": feature
        }
        self.notify()

    def remove(self, object_):
        con = self._dbcon.get_connection()
        base_statement = """delete from features where id=:id"""
        delete_cond = [
            {
                "column": "name",
                "value": object_.name,
                "op": "="
            }
        ]
        to_delete = self.filter(delete_cond)

        with con:
            for td in to_delete:
                con.execute(base_statement, {"id": td[0]})

        self._last_action = {
            "action": "remove",
            "object": object_
        }
        self.notify()

    def update(self, object_old: Feature.Feature, object_new: Feature.Feature):
        con = self._dbcon.get_connection()
        base_statement = """update features set name=:name where id=:id"""
        update_cond = [
            {
                "column": "name",
                "value": object_old.name,
                "op": "="
            }
        ]
        to_update = self.filter(update_cond)

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


class EnvironmentDAO(DAO, Subject):
    _last_action: dict = None
    _observers: list = list()
    _dbcon: DataBaseConnection = None

    def __init__(self, dbcon: DataBaseConnection = None):
        self._dbcon = dbcon

    def get_all(self) -> list:
        con = self._dbcon.get_connection()
        statement = """select * from environments;"""
        all = list()
        with con:
            for row in con.execute(statement):
                all.append(row)
        return all

    def filter(self, params: list) -> list:
        con = self._dbcon.get_connection()
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
            return self.get_all()

    def add(self, environment: Environment.Environment):
        con = self._dbcon.get_connection()
        base_statement = """insert into environments (name) values (?)"""

        with con:
            con.execute(base_statement, (environment.name,))

        self._last_action = {
            "action": "add",
            "object": environment
        }
        self.notify()

    def remove(self, object_):
        con = self._dbcon.get_connection()
        base_statement = """delete from environments where id=:id"""
        delete_cond = [
            {
                "column": "name",
                "value": object_.name,
                "op": "="
            }
        ]
        to_delete = self.filter(delete_cond)

        with con:
            for td in to_delete:
                con.execute(base_statement, {"id": td[0]})

        self._last_action = {
            "action": "remove",
            "object": object_
        }
        self.notify()

    def update(self, object_old: Environment.Environment, object_new: Environment.Environment):
        con = self._dbcon.get_connection()
        base_statement = """update environments set name=:name where id=:id"""
        update_cond = [
            {
                "column": "name",
                "value": object_old.name,
                "op": "="
            }
        ]
        to_update = self.filter(update_cond)

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


def get_all(dao_factory: DAOFactory) -> list:
    return dao_factory.create_DAO().get_all()


def filter(dao_factory: DAOFactory, params: list) -> list:
    return dao_factory.create_DAO().filter(params)


def add(dao_factory: DAOFactory, objects: list) -> None:
    for object_ in objects:
        dao_factory.create_DAO().add(object_)


def remove(dao_factory: DAOFactory, objects: list) -> None:
    for object_ in objects:
        dao_factory.create_DAO().remove(object_)


def update(dao_factory: DAOFactory, object_old, object_new) -> None:
    dao_factory.create_DAO().update(object_old, object_new)


if __name__ == "__main__":

    PZ1 = False
    PZ2 = False
    PZ3 = True

    dbcon = DataBaseConnection.get_instance()
    dbcon.open_connection("db.db", reinit_file=True)
    dbcon.init_tables()

    # register observers
    observer = None
    if PZ2:
        observer = DAOUpdateObserver()

    # create factories
    environmentDAOFactory = EnvironmentDAOFactory(dbcon, observer)
    resortDAOFactory = ResortDAOFactory(dbcon, observer)
    featureDAOFactory = FeatureDAOFactory(dbcon, observer)

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

    add(environmentDAOFactory, environments)
    add(featureDAOFactory, features)
    print(get_all(environmentDAOFactory))

    resortBuilder = Resort.ResortBuilder()
    resortBuilder.set_name("rixos")
    resortBuilder.set_price(12999.0)
    resortBuilder.add_environments(["ocean", "mountain"])
    resortBuilder.add_feature_ids(["spa", "golf"])
    add(resortDAOFactory, [resortBuilder.get_object()])

    resortBuilder = Resort.ResortBuilder()
    resortBuilder.set_name("asteria")
    resortBuilder.set_price(7999.0)
    resortBuilder.add_environments(["savannah"])
    resortBuilder.add_feature_ids(["spa", "water_park"])
    add(resortDAOFactory, [resortBuilder.get_object()])

    if PZ1:
        all_resorts = get_all(resortDAOFactory)
        print(all_resorts)
        all_features = get_all(resortDAOFactory)
        print(all_features, "\nFilter:")

        # filter
        params = [
            {
                "column": "price",
                "value": 9000.0,
                "op": "<"
            }
        ]

        f_resorts = filter(resortDAOFactory, params)
        print(f_resorts, "\nUpdate:")

        update(resortDAOFactory, resortBuilder.get_object(),
               Resort.Resort("very_expensive_resort", 599999.99))
        update(featureDAOFactory, features[1], Feature.Feature("expensive_feature"))

        all_resorts = get_all(resortDAOFactory)
        print(all_resorts)
        all_features = get_all(featureDAOFactory)
        print(all_features)

        remove(resortDAOFactory, [resortBuilder.get_object()])
        remove(environmentDAOFactory, environments)

        all_resorts = get_all(resortDAOFactory)
        print("\nRemove\n", all_resorts)
        all_features = get_all(featureDAOFactory)
        print(all_features)
        all_environments = get_all(environmentDAOFactory)
        print(all_environments)

    if PZ3:
        all_resorts = get_all(resortDAOFactory)
        print(all_resorts)

        resortDAO = resortDAOFactory.create_DAO()
        result_history = Memento.ResortDAOHistory(resortDAO)
        result_history.backup()

        resortDAO.update(resortBuilder.get_object(), Resort.Resort("very_expensive_resort", 599999.99))
        result_history.backup()

        all_resorts = get_all(resortDAOFactory)
        print(all_resorts)

        resortDAO.update(Resort.Resort("very_expensive_resort", 599999.99), Resort.Resort("update1", 699999.99))
        result_history.backup()

        all_resorts = get_all(resortDAOFactory)
        print(all_resorts)

        resortDAO.update(Resort.Resort("update1", 699999.99), Resort.Resort("update2", 799999.99))
        result_history.backup()

        all_resorts = get_all(resortDAOFactory)
        print(all_resorts)

        result_history.undo()

        all_resorts = get_all(resortDAOFactory)
        print(all_resorts)

        result_history.undo()

        all_resorts = get_all(resortDAOFactory)
        print(all_resorts)

        result_history.undo()

        all_resorts = get_all(resortDAOFactory)
        print(all_resorts)
