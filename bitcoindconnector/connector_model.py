import asyncio
from pybtc import *

class DependsTransaction(Exception):
    def __init__(self, raw_tx_hash):
        self.raw_tx_hash = raw_tx_hash

class Cache():
    def __init__(self, max_size=1000):
        self._store = dict()
        self._keys = set()
        self._max_size = max_size
        self._requests = 0
        self._hit = 0

    def set(self, key, value):
        self._check_limit()
        self._keys.add(key)
        self._store[key] = value

    def _check_limit(self):
        if len(self._keys) >= self._max_size:
            self._store.pop(self._keys.pop())

    def get(self, key):
        self._requests += 1
        try:
            i = self._store[key]
            self._hit += 1
            return i
        except:
            return None

    def pop(self, key):
        self._requests += 1
        try:
            self._keys.remove(key)
            self._hit += 1
            return self._store.pop(key)
        except:
            return None

    def len(self):
        return len(self._store)

    def hitrate(self):
        if self._requests:
            return self._hit / self._requests
        else:
            return 0



def tm(p = None):
    if p is not None:
        return round(time.time() - p, 4)
    return time.time()

async def get_last_block_height(conn):
    """
    :param cur: 
    :return: None or integer
    """
    stmt = await conn.prepare("SELECT height "
                              "FROM connector_block "
                              "ORDER BY id DESC LIMIT 1;")
    h = await stmt.fetchval()
    return h



async def get_last_block_hash(conn):
    """
    :param cur: 
    :return: None or binary hash 
    """
    stmt = await conn.prepare("SELECT hash "
                              "FROM  connector_block "
                              "ORDER BY id DESC LIMIT 1;")
    h = await stmt.fetchval()
    if h is None:
        return None
    return h


async def block_height_by_hash(app, block_hash, conn):
    """
    :param block_hash: binary block hash
    :param cur: 
    :return:  None or integer
    """
    if block_hash == app.last_inserted_block[0]:
        return app.last_inserted_block[1]
    # print("no")
    # print(app.last_inserted_block)
    stmt = await conn.prepare("SELECT height FROM connector_block "
                              "WHERE "
                              "hash = $1 LIMIT 1;")
    h = await stmt.fetchval(block_hash)
    return h

async def load_tx_cache(app, conn):
    """
    :param tx_hash:  binary transaction hash
    :param cur:      db cursor
    :return: None or integer
    """
    stmt = await conn.prepare("SELECT hash, id FROM connector_transaction  "
                              "ORDER BY id DESC LIMIT 50000;")
    rows = await stmt.fetch()
    for row in rows:
        app.tx_cache.set(row["hash"], row["id"])

async def load_block_cache(app, conn):
    """
    :param tx_hash:  binary transaction hash
    :param cur:      db cursor
    :return: None or integer
    """
    stmt = await conn.prepare("SELECT hash, height FROM connector_block  "
                              "ORDER BY id DESC LIMIT 10000;")
    rows = await stmt.fetch()
    for row in rows:
        app.block_cache.set(row["hash"], row["height"])



async def block_id_by_hash(app, block_hash, conn):
    """
    :param block_hash: binary bock hash 
    :param cur: 
    :return: None or integer
    """
    stmt = await conn.prepare("SELECT id FROM connector_block "
                              "WHERE  hash = $1 LIMIT 1;")
    block_id = await stmt.fetchval(block_hash)
    return block_id


async def clear_old_tx(conn, block_exp = 100, unconfirmed_exp = 5):
    """
    :param cur: 
    :param block_exp: number of block depth  
    :param unconfirmed_exp: days
    :return: {"pool": deleted from pool: integer, 
              "blocks": deleted from block: integer}
    """
    pool_count = 0
    block_count = 0
    height = await get_last_block_height(conn)
    if height is not None:
        stmt = await conn.prepare("SELECT id FROM connector_transaction "
                          "WHERE  height < ($1);")
        rows = await stmt.fetch(height - block_exp)
        id_list = [row[0] for row in rows]
        if id_list:
            stmt = await conn.prepare("DELETE FROM connector_transaction "
                              "WHERE id = ANY ($1);")
            await stmt.fetch(id_list)
        block_count = len(id_list)
        stmt = await conn.prepare("SELECT id FROM connector_transaction "
                          "WHERE "
                          "height IS NULL "
                          "AND timestamp < $1;")
        rows = await stmt.fetch(int(time.time()) - 60*60*24*unconfirmed_exp)
        id_list = [row[0] for row in rows]
        if id_list:
            stmt = await conn.prepare("DELETE FROM connector_transaction "
                                      "WHERE id = ANY ($1) "
                                      ";")
            await stmt.fetch(id_list)
        pool_count = len(id_list)
    return {"pool": pool_count, "blocks": block_count}


async def remove_orphan(app, conn):
    """
    :param orphan_height: integer 
    :param cur: 
    :return: 
    """

    stmt = await conn.prepare("UPDATE connector_transaction "
                              "SET height = NULL "
                              "WHERE connector_transaction.height = $1;")
    await stmt.fetch(app.last_block_height)
    stmt = await conn.prepare("DELETE FROM connector_block " 
                              "WHERE height = $1 returning hash;")
    bhash = await stmt.fetch(app.last_block_height)
    app.last_block_height -= 1
    app.block_cache.pop(bhash)


async def get_tx_id_list(app, hash_list, conn):
    """
    :param hash_list: list of binary hashes 
    :param cur: 
    :return: (list, list)
    """
    cached = list()
    for h in hash_list:
        row = app.tx_cache.get(h)
        if row:
            cached.append({"hash": h, "id": row})
    if not hash_list:
        return ([],[])

    tx_id_list = list()
    for row in  cached:
        h = row["hash"]
        if h in hash_list:
            tx_id_list.append(row["id"])
            hash_list.remove(h)
    return (tx_id_list, hash_list)


async def get_last_tx_id(conn):
    """
    """
    stmt = await conn.prepare("SELECT id "
                              "FROM connector_transaction "
                              "ORDER BY id DESC LIMIT 1;")
    tx_id = await stmt.fetchval()
    if not tx_id:
        tx_id = 0
    return tx_id

async def get_last_block_id(conn):
    """
    """
    stmt = await conn.prepare("SELECT id "
                              "FROM connector_block "
                              "ORDER BY id DESC LIMIT 1;")
    tx_id = await stmt.fetchval()
    if not tx_id:
        tx_id = 0
    return tx_id

async def insert_new_block(app, block_hash,
                           height,
                           previous_hash,
                           timestamp, conn):
    """
    :param block_hash: binary hash
    :param height: integer
    :param previous_hash: binary hash
    :param timestamp: integer
    :param cur: 
    :return: 
    """
    app.last_block_id += 1
    block_id = app.last_block_id
    await conn.copy_records_to_table('connector_block',
                                     columns=["id", "hash", "height", "previous_hash", "timestamp"],
                                     records=[(block_id,
                                               block_hash,
                                               height,
                                               previous_hash,
                                               timestamp),])
    app.last_inserted_block = [block_hash, height]

async def update_block_height(app, height, tx_id_list):
    if app.active:
        async with app._db_pool.acquire() as conn:
            stmt = await conn.prepare("UPDATE connector_transaction "
                                      "SET height = $1 "
                                      "WHERE  id  in (select(unnest($2::BIGINT[])));")
            await stmt.fetch(height, tx_id_list)


async def insert_new_tx(app, tx_hash):
    """
    :param tx_hash: binary hash 
    :param cur: 
    :param affected: integer 1
    :return: 
    """
    app.last_tx_id += 1
    tx_id = app.last_tx_id
    app.add_tx_future[tx_hash] = {"insert": asyncio.Future(),
                                  "row": (tx_id, tx_hash, int(time.time()))}
    if not app.tx_batch_active:
        app.loop.create_task(app.tx_batch())
    await  app.add_tx_future[tx_hash]["insert"]
    assert app.add_tx_future[tx_hash]["insert"].result() == True
    app.add_tx_future.pop(tx_hash)
    return tx_id

async def insert_new_tx_batch(batch, conn):
    await conn.copy_records_to_table('connector_transaction',
                                     columns=["id", "hash", "timestamp"],
                                     records=batch)

async def get_missed_tx(hash_list, conn):
    """
    :param hash_list: list of binary hashes 
    :param cur: 
    :return: list of binary hashes
    """
    if not hash_list:
        return hash_list
    stmt = await conn.prepare("SELECT hash FROM connector_transaction "
                              "WHERE hash = ANY($1);")
    rows = await stmt.fetch(hash_list)
    for row in rows:
        h = row[0]
        if h in hash_list:
            hash_list.remove(h)
    return hash_list


async def unconfirmed_count(conn):
    """
    :param cur: 
    :return: integer
    """
    stmt = await conn.prepare("SELECT count(id) FROM connector_transaction "
                              "WHERE height is NULL;")
    count = await stmt.fetchval()
    return count


async def init_db(conn):
    await conn.execute("""
                            CREATE TABLE IF NOT EXISTS connector_block (
                            id BIGINT PRIMARY KEY,
                            height INT4  NOT NULL,
                            hash bytea NOT NULL,
                            previous_hash bytea,
                            timestamp INT4   DEFAULT 0
                            );""")

    await conn.execute("CREATE INDEX IF NOT EXISTS connector_block_hash_block "
                      "ON connector_block USING hash (hash);")

    await conn.execute("""
                            CREATE TABLE IF NOT EXISTS connector_transaction (
                            id BIGINT PRIMARY KEY,
                            height INT4  DEFAULT NULL,
                            hash bytea NOT NULL,
                            timestamp INT4   DEFAULT 0
                            );""")

    await conn.execute("CREATE INDEX IF NOT EXISTS connector_transaction_hash_transaction "
                      "ON connector_transaction USING btree (hash);")


def get_stream(stream):
    if type(stream) != io.BytesIO:
        if type(stream) == str:
            stream = unhexlify(stream)
        if type(stream) == bytes:
            stream = io.BytesIO(stream)
        else:
            raise TypeError
    return stream


def decode_block_tx(block):
    stream = get_stream(block)
    stream.seek(80)
    return {i: Transaction(stream) for i in range(var_int_to_int(read_var_int(stream)))}

