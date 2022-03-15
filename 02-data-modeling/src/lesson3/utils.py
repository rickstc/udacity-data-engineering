from cassandra.cluster import Cluster


class CassUtils:
    """
    Utilities for interacting and ensuring consistency within Apache Cassandra
    """
    CASS_HOST = '127.0.0.1'
    DEFAULT_KEYSPACE = "udacity"

    def _list_keyspaces(session):
        keyspaces = set()
        query = session.execute('DESCRIBE keyspaces')
        for row in query.all():
            keyspaces.add(row.keyspace_name)
        return keyspaces

    @classmethod
    def create_keyspace(self, session, keyspace_name=None):
        if keyspace_name is None:
            keyspace_name = self.DEFAULT_KEYSPACE
        if keyspace_name.startswith('system'):
            print("Your chosen keyspace name should not begin with system*")
            return None
        try:
            rep = {'class': 'SimpleStrategy', 'replication_factor': 1}
            session.execute(
                f"CREATE KEYSPACE IF NOT EXISTS {keyspace_name} WITH REPLICATION = {rep}")
            return True
        except Exception as e:
            print(e)
            return False

    @classmethod
    def reset_cluster(self):
        session = self.connect()
        keyspaces = self._list_keyspaces(session)
        for keyspace in keyspaces:
            if not keyspace.startswith('system'):
                session.execute(f'DROP KEYSPACE IF EXISTS {keyspace}')

    @classmethod
    def connect(self):
        cluster = Cluster([self.CASS_HOST])
        session = cluster.connect()
        return session
