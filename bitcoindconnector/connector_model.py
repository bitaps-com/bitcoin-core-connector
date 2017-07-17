
from binascii import hexlify, unhexlify
from zlib import crc32

async def get_last_block_height(cur):
    await cur.execute("SELECT height\
        FROM  Block ORDER BY id DESC LIMIT 1;")
    row = await cur.fetchone()
    if row is None:
        return None
    return row[0]

async def tx_id_by_hash(tx_hash, cur):
    await cur.execute("SELECT id FROM Transaction \
                       WHERE hash_crc32=crc32(%s) \
                       and hash = %s LIMIT 1;",
                      (tx_hash, tx_hash))
    row = await cur.fetchone()
    if row is None:
        return None
    else:
        return row[0]


async def block_id_by_hash(block_hash, cur):
    block_hash = unhexlify(block_hash)
    await cur.execute("SELECT id FROM Block \
                       WHERE  \
                       hash = %s LIMIT 1;",
                      (block_hash,))
    row = await cur.fetchone()
    if row is None:
        return None
    else:
        return row[0]


async def clear_old_tx(cur):
    # неподтвержденные возраст 5 дней
    # в блоках глубже 50 блоков неиспользуемые
    lb_hash = await get_last_block_hash(cur)
    height = await block_height_by_hash(lb_hash, cur)
    await cur.execute("SELECT id FROM Transaction \
                       WHERE  \
                       height < (%s - 50) and affected = 0;",
                      (height,))
    rows = await cur.fetchall()
    id_list = [row[0] for row in rows]
    if id_list:
        await cur.execute("DELETE FROM Transaction \
            WHERE id in (%s)" % ','.join(map(str, id_list)))
        await cur.execute("commit;")
    blocks = len(id_list)
    await cur.execute("SELECT id FROM Transaction \
                       WHERE  \
                       height is NULL and \
                       timestamp < (UNIX_TIMESTAMP() - 432000);")
    rows = await cur.fetchall()
    id_list = [row[0] for row in rows]
    if id_list:
        await cur.execute("DELETE FROM Transaction \
            WHERE id in (%s)" % ','.join(map(str, id_list)))
        await cur.execute("commit;")
    pool = len(id_list)
    return {"pool": pool, "blocks": blocks}


async def block_height_by_hash(block_hash, cur):
    block_hash = unhexlify(block_hash)
    await cur.execute("SELECT height FROM Block \
                       WHERE  \
                       hash = %s LIMIT 1;",
                      (block_hash,))
    row = await cur.fetchone()
    if row is None:
        return None
    else:
        return row[0]


async def get_last_block_hash(cur):
    await cur.execute("SELECT lower(hex(hash))\
        FROM  Block ORDER BY id DESC LIMIT 1;")
    row = await cur.fetchone()
    if row is None:
        return None
    return row[0]


async def get_prev_block_hash(block_hash, cur):
    block_hash = unhexlify(block_hash)
    await cur.execute("SELECT previous_hash FROM Block \
                       WHERE  \
                       hash = %s LIMIT 1;",
                      (block_hash,))
    row = await cur.fetchone()
    if row is None:
        return None
    else:
        return hexlify(row[0])


async def remove_orphan(orphan_height, cur):
    """
       Получить список транзакций блока
       Во всех транзакциях установить высоту блока в NULL
       Удалить запись Block
    """
    await cur.execute("UPDATE Transaction SET height = NULL\
        WHERE Transaction.height = %s;", (orphan_height,))
    # print("orphan height %s " % orphan_height)
    await cur.execute("DELETE FROM Block \
        WHERE height = %s;" % orphan_height)
    await cur.execute("commit;")
    # print(cur.rowcount)


async def get_tx_id_list(hash_list, cur):
    if not hash_list:
        return hash_list
    list_crc32 = ','.join([str(crc32(unhexlify(r)[::-1])) for r in hash_list])
    await cur.execute("SELECT lower(hex(reverse(hash))), id FROM Transaction \
                       WHERE hash_crc32 in (%s);" % list_crc32)
    rows = await cur.fetchall()
    tx_id_list = list()
    for row in rows:
        if row[0] in hash_list:
            tx_id_list.append(row[1])
            hash_list.remove(row[0])
    return (tx_id_list, hash_list)


async def get_lock(str, cur):
    await cur.execute('SELECT GET_LOCK(%s,0);', (str,))
    (lock,) = await cur.fetchone()
    return lock


async def release_lock(str, cur):
    await cur.execute('DO RELEASE_LOCK(%s);', (str,))


async def insert_new_block(block_hash, height,
                           previous_hash, timestamp, tx_id_list, cur):
    try:
        await cur.execute('START TRANSACTION;')
        await cur.execute("INSERT INTO Block (hash, hash_crc32, height, previous_hash,\
                            timestamp)\
                            VALUES (unhex(%s), crc32(unhex(%s)), %s, unhex(%s), %s);",
                          (block_hash, block_hash, height, previous_hash, timestamp))
        await cur.execute("UPDATE Block SET next_hash = unhex(%s) \
                           WHERE  hash_crc32 = crc32(unhex(%s)) and  hash = unhex(%s);",
                          (block_hash, previous_hash, previous_hash))
        if tx_id_list:
            await cur.execute("UPDATE Transaction SET height = %s "
                              "WHERE  id in (%s);" %
                              (height, ','.join(map(str, tx_id_list))))
        await cur.execute('COMMIT;')
    except Exception:
        await cur.execute('ROLLBACK;')
        raise


async def insert_new_tx(tx_hash, cur, affected=0):
    await cur.execute("INSERT INTO Transaction (hash, hash_crc32, timestamp, affected) "
                      "VALUES (%s,crc32(%s), UNIX_TIMESTAMP(), %s);",
                      (tx_hash, tx_hash, affected))
    return cur.lastrowid


async def get_missed_tx(hash_list, cur):
    if not hash_list:
        return hash_list
    list_crc32 = ','.join([str(crc32(unhexlify(r))) for r in hash_list])
    await cur.execute("SELECT lower(hex(hash)) FROM Transaction \
                       WHERE hash_crc32 in (%s);" % list_crc32)
    rows = await cur.fetchall()
    for row in rows:
        if row[0] in hash_list:
            hash_list.remove(row[0])
    return hash_list


async def unconfirmed_count(cur):
    await cur.execute("SELECT count(id) FROM Transaction \
                       WHERE height is NULL")
    [row, ] = await cur.fetchone()
    return row


async def init_db(db, cur):
    await cur.execute("CREATE DATABASE IF NOT EXISTS %s;" % db)
    await cur.execute("USE %s;" % db)
    await cur.execute("""
                        CREATE TABLE IF NOT EXISTS `Block` (
                        `id` INT(11) UNSIGNED NOT NULL AUTO_INCREMENT,
                        `height` INT(11) UNSIGNED NOT NULL,
                        `hash` BINARY(32) NOT NULL,
                        `hash_crc32` INT(10) UNSIGNED NOT NULL,
                        `previous_hash` BINARY(32) NOT NULL,
                        `next_hash` BINARY(32) DEFAULT NULL,
                        `timestamp` INT(11) UNSIGNED NOT NULL DEFAULT 0,
                        PRIMARY KEY (`id`),
                        INDEX (`hash_crc32` ASC)
                        )
                        ENGINE = InnoDB
                        DEFAULT CHARACTER SET = utf8
                        ROW_FORMAT = COMPRESSED
                        KEY_BLOCK_SIZE = 8;""")

    await cur.execute("""
                        CREATE TABLE IF NOT EXISTS `Transaction` (
                        `id` INT(11) UNSIGNED NOT NULL AUTO_INCREMENT,
                        `height` INT(11) UNSIGNED DEFAULT NULL,
                        `hash` BINARY(32) NOT NULL,
                        `hash_crc32` INT(10) UNSIGNED NOT NULL,
                        `timestamp` INT(11) UNSIGNED NOT NULL DEFAULT 0,
                        `affected` tinyint(1) DEFAULT 0,
                        PRIMARY KEY (`id`),
                        INDEX (`hash_crc32` ASC)
                        )
                        ENGINE = InnoDB
                        DEFAULT CHARACTER SET = utf8
                        ROW_FORMAT = COMPRESSED
                        KEY_BLOCK_SIZE = 8;""")
    await cur.execute("SET @@GLOBAL.max_sp_recursion_depth = 10000;")
    await cur.execute("SET @@session.max_sp_recursion_depth = 10000;")
    await cur.execute("SET GLOBAL sql_mode = 'NO_AUTO_CREATE_USER,"
                      "NO_ENGINE_SUBSTITUTION';")
    await cur.execute("SET SESSION sql_mode = 'NO_AUTO_CREATE_USER,"
                      "NO_ENGINE_SUBSTITUTION';")
    await cur.execute("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED;")
    await cur.execute("SET GLOBAL TRANSACTION ISOLATION LEVEL READ COMMITTED;")
