
from binascii import hexlify, unhexlify
import time

async def get_last_block_height(cur):
    """
    :param cur: 
    :return: None or integer
    """
    await cur.execute("SELECT height "
                      "FROM Block "
                      "ORDER BY id DESC LIMIT 1;")
    row = await cur.fetchone()
    if row is None:
        return None
    return row[0]


async def get_last_block_hash(cur):
    """
    :param cur: 
    :return: None or binary hash 
    """
    await cur.execute("SELECT hash "
                      "FROM  Block "
                      "ORDER BY id DESC LIMIT 1;")
    row = await cur.fetchone()
    if row is None:
        return None
    return row[0].tobytes()


async def block_height_by_hash(block_hash, cur):
    """
    :param block_hash: binary block hash
    :param cur: 
    :return:  None or integer
    """
    await cur.execute("SELECT height FROM Block \
                       WHERE  \
                       hash = %s LIMIT 1;",
                      (block_hash,))
    row = await cur.fetchone()
    if row is None:
        return None
    else:
        return row[0]

async def tx_id_by_hash(tx_hash, cur):
    """
    :param tx_hash:  binary transaction hash
    :param cur:      db cursor
    :return: None or integer
    """
    await cur.execute("SELECT id FROM Transaction  "
                      "WHERE hash =%s  LIMIT 1;",
                      (tx_hash,))
    row = await cur.fetchone()
    if row is None:
        return None
    else:
        return row[0]


async def block_id_by_hash(block_hash, cur):
    """
    :param block_hash: binary bock hash 
    :param cur: 
    :return: None or integer
    """
    await cur.execute("SELECT id FROM Block "
                      "WHERE  hash = %s LIMIT 1;",
                      (block_hash,))
    row = await cur.fetchone()
    if row is None:
        return None
    else:
        return row[0]


async def clear_old_tx(cur, block_exp = 50, unconfirmed_exp = 5):
    """
    :param cur: 
    :param block_exp: number of block depth  
    :param unconfirmed_exp: days
    :return: {"pool": deleted from pool: integer, 
              "blocks": deleted from block: integer}
    """
    pool_count = 0
    block_count = 0
    height = await get_last_block_height(cur)
    if height is not None:
        await cur.execute("SELECT id FROM Transaction "
                          "WHERE  height < (%s) and affected != 'B1';",
                          (height - block_exp,))
        rows = await cur.fetchall()
        id_list = [row[0] for row in rows]
        if id_list:
            await cur.execute("DELETE FROM Transaction "
                              "WHERE id IN (%s)" % ','.join(map(str, id_list)))
            await cur.execute("COMMIT;")
        block_count = len(id_list)
        await cur.execute("SELECT id, affected FROM Transaction "
                          "WHERE "
                          "height IS NULL "
                          "AND timestamp < %s;",
                          (int(time.time()) - 60*60*24*unconfirmed_exp,))
        rows = await cur.fetchall()
        id_list = [row[0] for row in rows]
        if id_list:
            await cur.execute("DELETE FROM Transaction "
                              "WHERE id IN (%s) "
                              "AND affected = 'B0'" % ','.join(map(str, id_list)))
            await cur.execute("commit;")
        pool_count = len(id_list)
    return {"pool": pool_count, "blocks": block_count}


async def remove_orphan(orphan_height, cur):
    """
    :param orphan_height: integer 
    :param cur: 
    :return: 
    """
    await cur.execute("UPDATE Transaction "
                      "SET height = NULL "
                      "WHERE Transaction.height = %s;", (orphan_height,))
    await cur.execute("DELETE FROM Block " 
                      "WHERE height = %s;", (orphan_height,))
    await cur.execute("COMMIT;")


async def get_tx_id_list(hash_list, cur):
    """
    :param hash_list: list of binary hashes 
    :param cur: 
    :return: (list, list)
    """
    if not hash_list:
        return hash_list
    hash_list_str = ','.join("'\\x%s'" % hexlify(r).decode()  for r in hash_list)
    await cur.execute("SELECT hash, id "
                      "FROM Transaction "
                      "WHERE hash IN (%s);" % hash_list_str)
    rows = await cur.fetchall()
    tx_id_list = list()
    for row in rows:
        h = row[0].tobytes()
        if h in hash_list:
            tx_id_list.append(row[1])
            hash_list.remove(h)
    return (tx_id_list, hash_list)




async def insert_new_block(block_hash, height,
                           previous_hash,
                           timestamp,
                           tx_id_list, cur):
    """
    :param block_hash: binary hash
    :param height: integer
    :param previous_hash: binary hash
    :param timestamp: integer
    :param tx_id_list: list of integers
    :param cur: 
    :return: 
    """
    try:
        await cur.execute('START TRANSACTION;')
        await cur.execute("INSERT INTO Block (hash, height, previous_hash, timestamp) "
                          "VALUES (%s, %s, %s, %s);",
                          (block_hash, height, previous_hash, timestamp))
        if tx_id_list:
            await cur.execute("UPDATE Transaction "
                              "SET height = %s "
                              "WHERE  id in (%s);" %
                              (height, ','.join(map(str, tx_id_list))))
        await cur.execute('COMMIT;')
    except Exception:
        await cur.execute('ROLLBACK;')
        raise


async def insert_new_tx(tx_hash, cur, affected=0):
    """
    :param tx_hash: binary hash 
    :param cur: 
    :param affected: integer 
    :return: 
    """
    af = 'B1' if affected else 'B0'
    await cur.execute("INSERT INTO Transaction (hash, affected) "
                      "VALUES (%s, %s) RETURNING id;",
                      (tx_hash, af))
    [lastrowid,] = await cur.fetchone()
    return lastrowid


async def get_missed_tx(hash_list, cur):
    """
    :param hash_list: list of binary hashes 
    :param cur: 
    :return: list of binary hashes
    """
    if not hash_list:
        return hash_list
    hash_list_str = ','.join("'\\x%s'" % hexlify(r).decode()  for r in hash_list)
    await cur.execute("SELECT hash FROM Transaction "
                      "WHERE hash in (%s);" % hash_list_str)
    rows = await cur.fetchall()
    for row in rows:
        h = row[0].tobytes()
        if h in hash_list:
            hash_list.remove(h)
    return hash_list


async def unconfirmed_count(cur):
    """
    :param cur: 
    :return: integer
    """
    await cur.execute("SELECT count(id) FROM Transaction "
                      "WHERE height is NULL")
    [row, ] = await cur.fetchone()
    return row


async def init_db(cur):
    await cur.execute("""
                            CREATE TABLE IF NOT EXISTS Block (
                            id BIGSERIAL PRIMARY KEY,
                            height INT4  NOT NULL,
                            hash bytea NOT NULL,
                            previous_hash bytea,
                            timestamp INT4   DEFAULT 0
                            );""")

    await cur.execute("CREATE INDEX IF NOT EXISTS hash_block "
                      "ON Block USING btree (hash);")

    await cur.execute("""
                            CREATE TABLE IF NOT EXISTS Transaction (
                            id BIGSERIAL PRIMARY KEY,
                            height INT4  DEFAULT NULL,
                            hash bytea NOT NULL,
                            timestamp INT4   DEFAULT 0,
                            affected BIT(1) DEFAULT 'B0'
                            );""")

    await cur.execute("CREATE INDEX IF NOT EXISTS hash_transaction "
                      "ON Transaction USING btree (hash);")

    await cur.execute("""
                        CREATE OR REPLACE FUNCTION set_timestamp_column() 
                        RETURNS TRIGGER AS $$
                        BEGIN
                            select extract(epoch from now()) into NEW.timestamp;
                            RETURN NEW; 
                        END;
                        $$ language 'plpgsql';""")

    await cur.execute("DROP TRIGGER IF EXISTS before_insert_transaction ON Transaction;")
    await cur.execute("COMMIT;")
