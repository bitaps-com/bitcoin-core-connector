import asyncio
import traceback
import concurrent.futures
import time
import aiojsonrpc
import zmq
import zmq.asyncio
from pybtc import rh2s, s2rh
import struct
import io
from .connector_model import *
from binascii import hexlify, unhexlify
import asyncpg


class Connector:
    def __init__(self,
                 bitcoind_rpc_url,
                 bitcoind_zerromq_url,
                 postgresql_dsn,
                 loop,
                 logger,
                 postgresql_pool_max_size=5,
                 start_block=None,
                 tx_handler=None,
                 block_handler=None,
                 orphan_handler=None,
                 before_block_handler=None,
                 block_received_handler=None,
                 batch_limit=20,
                 rpc_threads_limit=100,
                 block_timeout=300,
                 rpc_timeout=100,
                 external_dublicate_filter=False,
                 external_cache_loader=None,
                 preload=False):
        self.loop = loop
        self.log = logger
        self.postgresql_pool_max_size = postgresql_pool_max_size
        self.rpc_url = bitcoind_rpc_url
        self.zmq_url = bitcoind_zerromq_url
        self.postgresql_dsn = postgresql_dsn
        self.orphan_handler = orphan_handler
        self.block_timeout = block_timeout
        self.tx_handler = tx_handler
        self.block_handler = block_handler
        self.before_block_handler = before_block_handler
        self.block_received_handler = block_received_handler
        self.start_block = start_block
        self.rpc_timeout = rpc_timeout
        self.external_dublicate_filter = external_dublicate_filter
        self.external_cache_loader = external_cache_loader
        self.active = True
        self.active_block = asyncio.Future()
        self.active_block.set_result(True)
        self.total_received_tx = 0
        self.total_received_tx_time = 0
        self.node_last_block = 0
        self.block_dependency_tx = 0

        # dev
        self.preload = preload
        self.block_preload = Cache(max_size=50000)
        self.block_hashes_preload = Cache(max_size=50000)

        self.tx_cache = Cache(max_size=50000)
        self.block_cache = Cache(max_size=10000)
        self.last_tx_id = 0
        self.last_block_id = 0
        self.last_inserted_block = [0,0]
        self.last_block_height = None
        self.batch_limit = batch_limit
        self.block_txs_request = None
        self.sync = False
        self.sync_requested = False
        self.tx_sub = False
        self.block_sub = False
        self.connected = asyncio.Future()
        self.await_tx_list = list()
        self.missed_tx_list = list()
        self.await_tx_future = dict()
        self.await_tx_id_list = list()
        self.add_tx_future = dict()
        self.tx_batch_active = False
        self.sync_tx_lock = list()
        self.get_missed_tx_threads = 0
        self.get_missed_tx_threads_limit = rpc_threads_limit
        self.tx_in_process = set()
        self.zmqContext = False
        self._db_pool = False
        self.tasks = list()
        self.log.info("Bitcoind connector started")
        asyncio.ensure_future(self.start(), loop=self.loop)

    async def start(self):
        try:
            conn = await asyncpg.connect(dsn=self.postgresql_dsn)
            await init_db(conn)
            self.last_tx_id = await get_last_tx_id(conn)
            self.last_block_id = await get_last_block_id(conn)
            self.last_block_height = await get_last_block_height(conn)
            if not self.external_dublicate_filter:
                await load_tx_cache(self, conn)
            elif self.external_cache_loader:
                await self.external_cache_loader(self.tx_cache, conn)
            await load_block_cache(self, conn)
            await conn.close()
            self._db_pool = await asyncpg.create_pool(dsn=self.postgresql_dsn,
                                                      min_size=1,
                                                      max_size=self.postgresql_pool_max_size)
        except Exception as err:
            self.log.error("Bitcoind connector start failed: %s", err)
            self.log.debug(str(traceback.format_exc()))
            await self.stop()
            return
        self.rpc = aiojsonrpc.rpc(self.rpc_url, self.loop, timeout=self.rpc_timeout)
        self.zmqContext = zmq.asyncio.Context()
        self.zmqSubSocket = self.zmqContext.socket(zmq.SUB)
        self.zmqSubSocket.setsockopt_string(zmq.SUBSCRIBE, "hashblock")
        self.zmqSubSocket.setsockopt_string(zmq.SUBSCRIBE, "rawtx")
        self.zmqSubSocket.connect(self.zmq_url)
        self.tasks.append(self.loop.create_task(self.zeromq_handler()))
        self.tasks.append(self.loop.create_task(self.watchdog()))
        self.connected.set_result(True)
        if self.preload:
            print("preload")
            self.loop.create_task(self.preload_block())
            self.loop.create_task(self.preload_block_hashes())
        self.loop.create_task(self.get_last_block())

    async def zeromq_handler(self):
        while True:
            try:
                msg = await self.zmqSubSocket.recv_multipart()
                topic = msg[0]
                body = msg[1]
                if len(msg[-1]) == 4:
                    msg_sequence = struct.unpack('<I', msg[-1])[-1]
                if topic == b"hashblock":
                    hash = hexlify(body).decode()
                    self.log.warning("New block %s" % hash)
                    self.loop.create_task(self._get_block_by_hash(hash))
                elif topic == b"rawtx":
                    try:
                        tx = Transaction(body, format="raw")
                    except:
                        self.log.critical("Transaction decode failed: %s" % body.hex())
                    self.loop.create_task(self._new_transaction(tx))
                if not self.active:
                    break
            except asyncio.CancelledError:
                self.log.warning("zeromq handler terminated")
                break
            except Exception as err:
                self.log.error(str(err))

    async def _new_transaction(self, tx):
        tx_hash = rh2s(tx["txId"])
        ft = self.await_tx_future if tx_hash in self.await_tx_list else None
        if tx_hash in self.tx_in_process:
            return
        self.tx_in_process.add(tx_hash)
        # Check is transaction new
        tx_id = self.tx_cache.get(tx_hash)
        if tx_id is not None:
            self.tx_in_process.remove(tx_hash)
            return
        try:
            # call external handler
            if self.tx_handler:
                tx_id = await self.tx_handler(tx, ft)
            # insert new transaction to dublicate filter
            if not self.external_dublicate_filter:
                tx_id = await insert_new_tx(self, tx["txId"])
            self.tx_cache.set(tx["txId"], tx_id)
            if tx_hash in self.await_tx_list:
                self.await_tx_list.remove(tx_hash)
                self.await_tx_id_list.append(tx_id)
                if not self.await_tx_future[unhexlify(tx_hash)[::-1]].done():
                    self.await_tx_future[unhexlify(tx_hash)[::-1]].set_result(True)
                if not self.await_tx_list:
                    self.block_txs_request.set_result(True)
        except DependsTransaction as err:
            self.block_dependency_tx += 1
            self.loop.create_task(self.wait_tx_then_add(err.raw_tx_hash, tx))
        except Exception as err:
            if tx_hash in self.await_tx_list:
                self.await_tx_list = []
                self.await_tx_id_list = []
                self.block_txs_request.cancel()
                for i in self.await_tx_future:
                    if not self.await_tx_future[i].done():
                        self.await_tx_future[i].cancel()
            self.log.debug("new transaction error %s " % err)
            self.log.debug(str(traceback.format_exc()))
        finally:
            self.tx_in_process.remove(tx_hash)

    async def _new_block(self, block):
        self.block_dependency_tx = 0
        """
        0 Check if block already exist in db
        1 Check parent block in db:
            If no parent
                get last block height from db
                   if last block height >= recent block height 
                       this is orphan ignore it
                   else:
                       remove top block from db and ask block with
                       hrecent block height -1
                       return
        2 add all transactions from block to db
            ask full block from node
            parse txs and add to db in case not exist
        3 call before add block handler^ if this handler rise 
          exception block adding filed
        4 add block to db and commit
        5 after block add handelr 
        6 ask next block
        """
        if not self.active or not self.active_block.done():
            return
        if block is None:
            self.sync = False
            self.log.debug('Block synchronization completed')
            return
        self.active_block = asyncio.Future()

        binary_block_hash = unhexlify(block["hash"])
        binary_previousblock_hash = \
            unhexlify(block["previousblockhash"]) \
            if "previousblockhash" in block else None
        block_height = int(block["height"])
        next_block_height = block_height + 1
        self.log.info("New block %s %s" % (block_height, block["hash"]))
        bt = q = tm()
        try:
            async with self._db_pool.acquire() as con:
                # blockchain position check
                block_exist = self.block_cache.get(binary_block_hash)
                if block_exist is not None:
                    self.log.info("block already exist in db %s" % block["hash"])
                    return
                # Get parent from db
                if binary_previousblock_hash is not None:
                    parent_height = self.block_cache.get(binary_previousblock_hash)
                else:
                    parent_height = None
                # self.log.warning("parent height %s" % parent_height)

                if parent_height is None:
                    # have no mount point in local chain
                    # self.log.warning("last local height %s" % self.last_block_height)
                    if self.last_block_height is not None:
                        if self.last_block_height >= block_height:
                            self.log.critical("bitcoin node out of sync block %s" % block["hash"])
                            return
                        if self.last_block_height+1 == block_height:
                            if self.orphan_handler:
                                tq = tm()
                                await self.orphan_handler(self.last_block_height, con)
                                self.log.info("orphan handler  %s [%s]" % (self.last_block_height, tm(tq)))
                            tq = tm()
                            await remove_orphan(self, con)
                            self.log.info("remove orphan %s [%s]" % (self.last_block_height, tm(tq)))
                        next_block_height -= 2
                        if next_block_height > self.last_block_height:
                            next_block_height = self.last_block_height + 1
                        if self.sync and next_block_height >= self.sync:
                            if self.sync_requested:
                                next_block_height = self.last_block_height + 1
                        else:
                            self.sync = next_block_height
                        return
                    else:
                        if self.start_block is not None and block["height"] != self.start_block:
                            self.log.info("Start from block %s" % self.start_block)
                            next_block_height = self.start_block
                            return
                else:
                    if self.last_block_height + 1 != block_height:
                        if self.orphan_handler:
                            tq = tm()
                            await self.orphan_handler(self.last_block_height, con)
                            self.log.info("orphan handler  %s [%s]" % (self.last_block_height, tm(tq)))
                        await remove_orphan(self, con)
                        next_block_height -= 1
                        self.log.debug("requested %s" % next_block_height)
                        return
                self.log.debug("blockchain position check [%s]" % tm(q))

                # add all block transactions
                q = tm()
                binary_tx_hash_list = [unhexlify(t)[::-1] for t in block["tx"]]
                if block["height"] in (91842, 91880):
                    # BIP30 Fix
                    self.tx_cache.pop(s2rh("d5d27987d2a3dfc724e359870c6644b40e497bdc0589a033220fe15429d88599"))
                    self.tx_cache.pop(s2rh("e3bf3d07d4b0375638d5f1db5255fe07ba2c4cb067cd81b84ee974b6585fb468"))
                tx_id_list, missed = await get_tx_id_list(self, binary_tx_hash_list, con)
                assert len(tx_id_list)+len(missed) == len(block["tx"])
                self.await_tx_id_list = tx_id_list
                if self.before_block_handler:
                    sn = await self.before_block_handler(block,
                                                         missed,
                                                         self.sync_tx_lock,
                                                         self.node_last_block,
                                                         con)
                    if sn and missed:
                        self.await_tx_id_list = self.await_tx_id_list + [0 for i in range(len(missed))]
                        missed = []
                cq = tm()
                missed = [rh2s(t) for t in missed]
                self.log.info("Transactions already exist: %s missed %s [%s]" % (len(tx_id_list), len(missed), tm(q)))
                if missed:
                    self.log.debug("Request missed transactions")
                    self.missed_tx_list = list(missed)
                    self.await_tx_list = missed
                    self.await_tx_future = dict()
                    for i in missed:
                        self.await_tx_future[unhexlify(i)[::-1]] = asyncio.Future()
                    self.block_txs_request = asyncio.Future()
                    if len(missed) / len(block["tx"]) > 0.3:
                        self.loop.create_task(self._get_missed(block["hash"]))
                    else:
                        self.loop.create_task(self._get_missed())
                    try:
                        await asyncio.wait_for(self.block_txs_request, timeout=self.block_timeout)
                    except asyncio.CancelledError:
                        # refresh rpc connection session
                        await self.rpc.close()
                        self.rpc = aiojsonrpc.rpc(self.rpc_url, self.loop, timeout=self.rpc_timeout)
                        raise RuntimeError("block transaction request timeout")
                if len(block["tx"]) != len(self.await_tx_id_list):
                    self.log.error("get block transactions failed")
                    self.log.error(str(self.await_tx_id_list))
                    raise Exception("get block transactions failed")

                tx_count = len(self.await_tx_id_list)
                self.total_received_tx += tx_count
                self.total_received_tx_time += tm(q)
                rate = round(self.total_received_tx/self.total_received_tx_time)
                self.log.info("Transactions received: %s [%s] rate tx/s ->> %s <<" % (tx_count, tm(cq), rate))
                async with con.transaction():
                    if self.block_received_handler:
                        await self.block_received_handler(block, con)
                    # insert new block
                    await insert_new_block(self, binary_block_hash,
                                           block["height"],
                                           binary_previousblock_hash,
                                           block["time"], con)
                    if not self.external_dublicate_filter:
                        self.loop.create_task(update_block_height(self, block["height"],
                                                                  list(self.await_tx_id_list)))
                if self.sync == block["height"]:
                    self.sync += 1
                    next_block_height = self.sync
                # after block added handler
                if self.block_handler:
                    await self.block_handler(block, con)
            self.last_block_height = block["height"]
            self.block_cache.set(binary_block_hash, block["height"])
        except Exception as err:
            if self.await_tx_list:
                self.await_tx_list = []
            self.log.error(str(traceback.format_exc()))
            self.log.error("new block error %s" % str(err))
            next_block_height = None
        finally:
            self.active_block.set_result(True)
            self.log.debug("block  processing completed")
            if next_block_height is not None:
                self.sync_requested = True
                self.loop.create_task(self.get_block_by_height(next_block_height))
            self.log.info("%s block [%s tx/ %s size] (dp %s) processing time %s cache [%s/%s]" %
                          (block["height"],
                           len(block["tx"]),
                           block["size"] / 1000000,
                           self.block_dependency_tx,
                           tm(bt),
                           len(self.block_hashes_preload._store),
                           len(self.block_preload._store)))

    async def wait_tx_then_add(self, raw_tx_hash, tx):
        tx_hash = rh2s(tx["hash"])
        try:
            if not self.await_tx_future[raw_tx_hash].done():
                await self.await_tx_future[raw_tx_hash]
            self.loop.create_task(self._new_transaction(tx))
        except:
            self.tx_in_process.remove(tx_hash)

    async def tx_batch(self):
        if self.tx_batch_active:
            return
        self.tx_batch_active = True
        batch, hash_list = [], set()
        try:
            for f in self.add_tx_future:
                if not self.add_tx_future[f]["insert"].done():
                    batch.append(self.add_tx_future[f]["row"])
                    hash_list.add(f)
            if not batch:
                return
            async with self._db_pool.acquire() as conn:
                await insert_new_tx_batch(batch, conn)
            for f in hash_list:
                self.add_tx_future[f]["insert"].set_result(True)
            self.loop.create_task(self.tx_batch())
        finally:
            self.tx_batch_active = False

    async def _get_missed(self, block_hash=False):
        if block_hash:
            print('request block')
            t = time.time()
            block = self.block_preload.pop(block_hash)
            if not block:
                result = await self.rpc.getblock(block_hash, 0)
            try:
                if not block:
                    block = decode_block_tx(result)
                self.log.info("block downloaded %s" % round(time.time() - t, 4))
                for tx in block:
                    if rh2s(block[tx]["txId"]) in self.missed_tx_list:
                        self.loop.create_task(self._new_transaction(block[tx]))
                return
            except Exception as err:
                self.log.error("_get_missed exception %s " % str(err))
                self.log.error(str(traceback.format_exc()))
                self.await_tx_list = []
                self.block_txs_request.cancel()

        if self.get_missed_tx_threads > self.get_missed_tx_threads_limit:
            return
        self.get_missed_tx_threads += 1
        # start more threads
        if len(self.missed_tx_list) > 1:
            self.loop.create_task(self._get_missed())
        while True:
            if not self.missed_tx_list:
                break
            try:
                batch = list()
                while self.missed_tx_list:
                    batch.append(["getrawtransaction", self.missed_tx_list.pop()])
                    if len(batch) >= self.batch_limit:
                        break
                result = await self.rpc.batch(batch)
                for r in result:
                    try:
                        tx = Transaction(r["result"], format="raw")
                    except:
                        self.log.error("Transaction decode failed: %s" % r["result"])
                        raise Exception("Transaction decode failed")
                    self.loop.create_task(self._new_transaction(tx))
            except Exception as err:
                self.log.error("_get_missed exception %s " % str(err))
                self.log.error(str(traceback.format_exc()))
                self.await_tx_list = []
                self.block_txs_request.cancel()
        self.get_missed_tx_threads -= 1

    async def get_block_by_height(self, height):
        try:
            self.log.debug("get block by height")
            d = await self.rpc.getblockcount()
            if d >= height:
                d = await self.rpc.getblockhash(height)
                await self._get_block_by_hash(d)
            else:
                self.log.debug("block %s not yet exist" % height)
        except Exception as err:
            self.log.error("get last block failed %s" % str(err))

    async def get_last_block(self):
        self.log.debug("check blockchain status")
        try:
            d = await self.rpc.getblockcount()
            self.node_last_block = d
            async with self._db_pool.acquire() as con:
                ld = await get_last_block_height(con)
                if ld is None:
                    ld = 0
                if ld >= d:
                    self.log.debug("blockchain is synchronized")
                else:
                    d = await self.rpc.getbestblockhash()
                    await self._get_block_by_hash(d)
        except Exception as err:
            self.log.error("get last block failed %s" % str(err))

    async def _get_block_by_hash(self, hash):
        if not self.active:
                return
        self.log.debug("get block by hash %s" % hash)
        try:
            block = self.block_hashes_preload.pop(hash)
            if not block:
                block = await self.rpc.getblock(hash)
            assert block["hash"] == hash
            self.loop.create_task(self._new_block(block))
        except Exception:
            self.log.error("get block by hash %s FAILED" % hash)

    async def preload_block_hashes(self):
        while True:
            try:
                async with self._db_pool.acquire() as con:
                    start_height = await  get_last_block_height(con)
                height = start_height + 10
                d = await self.rpc.getblockcount()
                if d > height:
                    while True:
                        height += 1
                        d = await self.rpc.getblockhash(height)
                        ex = self.block_preload.get(d)
                        if not ex:
                            b = await self.rpc.getblock(d)
                            self.block_hashes_preload.set(d, b)
                        if start_height + 15000 < height:
                            break
            except asyncio.CancelledError:
                self.log.info("connector preload_block_hashes terminated")
                break
            except:
                pass
            await asyncio.sleep(10)

    async def preload_block(self):
        while True:
            try:
                async with self._db_pool.acquire() as con:
                    start_height = await  get_last_block_height(con)
                height = start_height + 10
                d = await self.rpc.getblockcount()
                if d > height:
                    while True:
                        height += 1
                        d = await self.rpc.getblockhash(height)
                        ex = self.block_preload.get(d)
                        if not ex:
                            b = await self.rpc.getblock(d, 0)
                            block = decode_block_tx(b)
                            self.block_preload.set(d, block)
                        if start_height + 15000 < height:
                            break
            except asyncio.CancelledError:
                self.log.info("connector preload_block terminated")
                break
            except:
                pass
            await asyncio.sleep(15)

    async def watchdog(self):
        """
        Statistic output
        Check new blocks
        Garbage collection
        """
        while True:
            try:
                counter = 0
                while True:
                    await asyncio.sleep(10)
                    conn = False
                    try:
                        conn = await asyncpg.connect(dsn=self.postgresql_dsn)
                        count = await unconfirmed_count(conn)
                        lb_hash = await get_last_block_hash(conn)
                        height = await block_height_by_hash(self, lb_hash, conn)
                        if counter == 20 and not self.external_dublicate_filter:
                            counter = 0
                            self.log.info("filter tx %s last block %s" %
                                          (count, height))
                            r = await clear_old_tx(conn)
                            if r["pool"]:
                                self.log.warning("cleared from pool %s" % r["pool"])
                            if r["blocks"]:
                                self.log.warning("cleared from blocks %s " % r["blocks"])
                        counter += 1
                    finally:
                        if conn:
                            await conn.close()
                    await self.get_last_block()
            except asyncio.CancelledError:
                self.log.info("connector watchdog terminated")
                break
            except Exception as err:
                self.log.error(str(traceback.format_exc()))
                self.log.error("watchdog error %s " % err)

    async def stop(self):
        self.active = False
        self.log.warning("New block processing restricted")
        self.log.warning("Stopping bitcoind connector")
        for task in self.tasks:
            task.cancel()
        await asyncio.wait(self.tasks)
        if not self.active_block.done():
            self.log.warning("Waiting active block task")
            await self.active_block
        await self.rpc.close()
        self.log.warning("Close db pool")
        if self._db_pool:
            await self._db_pool.close()
        if self.zmqContext:
            self.zmqContext.destroy()
        self.log.debug('Connector ready to shutdown')

