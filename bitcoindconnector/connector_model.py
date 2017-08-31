
from binascii import hexlify, unhexlify
from asyncpg import BitString
import time

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
                              "FROM Block "
                              "ORDER BY id DESC LIMIT 1;")
    h = await stmt.fetchval()
    return h


async def get_last_block_hash(conn):
    """
    :param cur: 
    :return: None or binary hash 
    """
    stmt = await conn.prepare("SELECT hash "
                              "FROM  Block "
                              "ORDER BY id DESC LIMIT 1;")
    h = await stmt.fetchval()
    if h is None:
        return None
    return h


async def block_height_by_hash(block_hash, conn):
    """
    :param block_hash: binary block hash
    :param cur: 
    :return:  None or integer
    """
    stmt = await conn.prepare("SELECT height FROM Block "
                              "WHERE "
                              "hash = $1 LIMIT 1;")
    h = await stmt.fetchval(block_hash)
    return h

async def tx_id_by_hash(tx_hash, conn):
    """
    :param tx_hash:  binary transaction hash
    :param cur:      db cursor
    :return: None or integer
    """
    stmt = await conn.prepare("SELECT id FROM Transaction  "
                              "WHERE hash =$1  LIMIT 1;")
    tx_id = await stmt.fetchval(tx_hash)
    return tx_id


async def block_id_by_hash(block_hash, conn):
    """
    :param block_hash: binary bock hash 
    :param cur: 
    :return: None or integer
    """
    stmt = await conn.prepare("SELECT id FROM Block "
                              "WHERE  hash = $1 LIMIT 1;")
    block_id = await stmt.fetchval(block_hash)
    return block_id


async def clear_old_tx(conn, block_exp = 50, unconfirmed_exp = 5):
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
        stmt = await conn.prepare("SELECT id FROM Transaction "
                          "WHERE  height < ($1) and affected != 'B1';")
        rows = await stmt.fetch(height - block_exp)
        id_list = [row[0] for row in rows]
        if id_list:
            stmt = await conn.prepare("DELETE FROM Transaction "
                              "WHERE id = ANY ($1);")
            await stmt.fetch(id_list)
        block_count = len(id_list)
        stmt = await conn.prepare("SELECT id, affected FROM Transaction "
                          "WHERE "
                          "height IS NULL "
                          "AND timestamp < $1;")
        rows = await stmt.fetch(int(time.time()) - 60*60*24*unconfirmed_exp)
        id_list = [row[0] for row in rows]
        if id_list:
            stmt = await conn.prepare("DELETE FROM Transaction "
                                      "WHERE id = ANY ($1) "
                                      "AND affected = 'B0';")
            await stmt.fetch(id_list)
        pool_count = len(id_list)
    return {"pool": pool_count, "blocks": block_count}


async def remove_orphan(orphan_height, conn):
    """
    :param orphan_height: integer 
    :param cur: 
    :return: 
    """
    stmt = await conn.prepare("UPDATE Transaction "
                              "SET height = NULL "
                              "WHERE Transaction.height = $1;")
    await stmt.fetch(orphan_height)
    stmt = await conn.prepare("DELETE FROM Block " 
                              "WHERE height = $1;")
    await stmt.fetch(orphan_height)



async def get_tx_id_list(hash_list, conn):
    """
    :param hash_list: list of binary hashes 
    :param cur: 
    :return: (list, list)
    """
    if not hash_list:
        return ([],[])
    stmt = await conn.prepare("SELECT hash, id "
                              "FROM Transaction "
                              "WHERE hash = ANY($1);")
    rows = await stmt.fetch(hash_list)
    tx_id_list = list()
    for row in rows:
        h = row[0]
        if h in hash_list:
            tx_id_list.append(row[1])
            hash_list.remove(h)
    return (tx_id_list, hash_list)




async def insert_new_block(block_hash, height,
                           previous_hash,
                           timestamp,
                           tx_id_list, conn):
    """
    :param block_hash: binary hash
    :param height: integer
    :param previous_hash: binary hash
    :param timestamp: integer
    :param tx_id_list: list of integers
    :param cur: 
    :return: 
    """
    async with conn.transaction():
        stmt = await conn.prepare("INSERT INTO Block (hash, height, previous_hash, timestamp) "
                          "VALUES ($1, $2, $3, $4);")
        await stmt.fetch(block_hash, height, previous_hash, timestamp)
        if tx_id_list:
            stmt = await conn.prepare("UPDATE Transaction "
                                      "SET height = $1 "
                                      "WHERE  id = ANY($2);")
            await stmt.fetch(height, tx_id_list)


async def insert_new_tx(tx_hash, conn, affected=0):
    """
    :param tx_hash: binary hash 
    :param cur: 
    :param affected: integer 
    :return: 
    """
    af = BitString('0') if affected else BitString('1')
    stmt = await conn.prepare("INSERT INTO Transaction (hash, affected) "
                     "VALUES ($1, $2) RETURNING id;")
    lastrowid = await stmt.fetchval(tx_hash, af)
    return lastrowid


async def get_missed_tx(hash_list, conn):
    """
    :param hash_list: list of binary hashes 
    :param cur: 
    :return: list of binary hashes
    """
    if not hash_list:
        return hash_list
    stmt = await conn.prepare("SELECT hash FROM Transaction "
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
    stmt = await conn.prepare("SELECT count(id) FROM Transaction "
                              "WHERE height is NULL;")
    count = await stmt.fetchval()
    return count


async def init_db(conn):
    await conn.execute("""
                            CREATE TABLE IF NOT EXISTS Block (
                            id BIGSERIAL PRIMARY KEY,
                            height INT4  NOT NULL,
                            hash bytea NOT NULL,
                            previous_hash bytea,
                            timestamp INT4   DEFAULT 0
                            );""")

    await conn.execute("CREATE INDEX IF NOT EXISTS hash_block "
                      "ON Block USING btree (hash);")

    await conn.execute("""
                            CREATE TABLE IF NOT EXISTS Transaction (
                            id BIGSERIAL PRIMARY KEY,
                            height INT4  DEFAULT NULL,
                            hash bytea NOT NULL,
                            timestamp INT4   DEFAULT 0,
                            affected BIT(1) DEFAULT 'B0'
                            );""")

    await conn.execute("CREATE INDEX IF NOT EXISTS hash_transaction "
                      "ON Transaction USING btree (hash);")

    await conn.execute("""
                        CREATE OR REPLACE FUNCTION set_timestamp_column() 
                        RETURNS TRIGGER AS $$
                        BEGIN
                            select extract(epoch from now()) into NEW.timestamp;
                            RETURN NEW; 
                        END;
                        $$ language 'plpgsql';""")

    await conn.execute("DROP TRIGGER IF EXISTS before_insert_transaction ON Transaction;")

