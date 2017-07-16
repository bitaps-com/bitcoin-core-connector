import asyncio
import aiomysql
import traceback
import aiohttp
import time
import aiojsonrpc
import zmq
import zmq.asyncio
import bitcoinlib
import struct
import io
from .connector_model import *


class Connector:

    def __init__(self, loop, logger, config,
                 tx_handler=None, block_handler=None,
                 orphan_handler=None, debug = False,
                 debug_full = False,
                 mysql_pool_size = 50, start_block=None):
        self.loop = loop
        self.log = logger
        self.active = True
        self.active_block = asyncio.Future()
        self.active_block.set_result(True)
        self.session = aiohttp.ClientSession()
        self.start_block = start_block

        def empty(a=None):
            return None

        if debug:
            self.log.info("Connector debug mode")
            if not hasattr(self.log, 'debugI'):
                if debug_full:
                    setattr(self.log, 'debugI', self.log.debug)
                    setattr(self.log, 'debugII', self.log.debug)
                    setattr(self.log, 'debugIII', self.log.debug)
                else:
                    setattr(self.log, 'debugI', empty)
                    setattr(self.log, 'debugII', empty)
                    setattr(self.log, 'debugIII', empty)
        else:
            setattr(self.log, 'debug', empty)
            setattr(self.log, 'debugI', empty)
            setattr(self.log, 'debugII', empty)
            setattr(self.log, 'debugIII', empty)

        self.rpc_url = config["BITCOIND"]["rpc"]
        self.zmq_url = config["BITCOIND"]["zeromq"]
        self.orphan_handler = orphan_handler
        self.batch_limit = 15
        self.tx_handler = tx_handler
        self.block_handler = block_handler
        self.block_txs_request = None
        self.sync = False
        self.sync_requested = False
        self.tx_sub = False
        self.block_sub = False
        self.connected = asyncio.Future()
        self.mysql_pool_size = mysql_pool_size
        self.MYSQL_CONFIG = config["MYSQL"]
        self.log.info("Bitcoind node connector started")
        self.await_tx_list = list()
        self.missed_tx_list = list()
        self.await_tx_id_list = list()
        self.get_missed_tx_threads = 0
        self.get_missed_tx_threads_limit = 100
        self._watchdog = False
        self._zmq_handler = False
        self._mysql_pool_conn = False
        asyncio.ensure_future(self.start(config), loop=self.loop)


    async def init_mysql(self):
        conn = await \
            aiomysql.connect(user=self.MYSQL_CONFIG["user"],
                             password=self.MYSQL_CONFIG["password"],
                             db="",
                             host=self.MYSQL_CONFIG["host"],
                             port=int(self.MYSQL_CONFIG["port"]),
                             loop=self.loop)
        cur = await conn.cursor()
        await init_db(self.MYSQL_CONFIG["database"], cur)
        conn.close()

    async def get_mysql_conn(self):
        conn = await \
            aiomysql.connect(user=self.MYSQL_CONFIG["user"],
                             password=self.MYSQL_CONFIG["password"],
                             db=self.MYSQL_CONFIG["database"],
                             host=self.MYSQL_CONFIG["host"],
                             port=int(self.MYSQL_CONFIG["port"]),
                             loop=self.loop)
        return conn

    async def start(self, config):
        try:
            await self.init_mysql()
            self._mysql_pool_conn = await \
                aiomysql.create_pool(host=self.MYSQL_CONFIG["host"],
                                     port=int(self.MYSQL_CONFIG["port"]),
                                     user=self.MYSQL_CONFIG["user"],
                                     password=self.MYSQL_CONFIG["password"],
                                     db=self.MYSQL_CONFIG["database"],
                                     loop=self.loop,
                                     minsize=20, maxsize=self.mysql_pool_size)
        except Exception as err:
            self.log.error("Start failed")
            self.log.error(str(traceback.format_exc()))
            await self.stop()
            return
        self.rpc = aiojsonrpc.rpc(self.rpc_url, self.loop)
        self.zmqContext = zmq.asyncio.Context()
        self.zmqSubSocket = self.zmqContext.socket(zmq.SUB)
        self.zmqSubSocket.setsockopt_string(zmq.SUBSCRIBE, "hashblock")
        self.zmqSubSocket.setsockopt_string(zmq.SUBSCRIBE, "rawtx")
        self.zmqSubSocket.connect(self.zmq_url)
        self._zmq_handler = self.loop.create_task(self.handle())
        self.loop.create_task(self.get_last_block())
        self._watchdog = self.loop.create_task(self.watchdog())


    async def handle(self) :
        while True:
            try:
                msg = await self.zmqSubSocket.recv_multipart()
                topic = msg[0]
                body = msg[1]
                sequence = "Unknown"
                if len(msg[-1]) == 4:
                  msgSequence = struct.unpack('<I', msg[-1])[-1]
                  sequence = str(msgSequence)
                if topic == b"hashblock":
                    self.log.warning("New block %s" % hexlify(body))
                    self.loop.create_task(self._get_block_by_hash(hexlify(body).decode()))
                elif topic == b"rawtx":
                    try:
                        tx = bitcoinlib.Transaction.deserialize(io.BytesIO(body))
                    except:
                        self.log.error("Transaction decode failed: %s" % hexlify(body).decode())
                    self.loop.create_task(self._new_transaction(tx))
            except Exception as err:
                pass



    async def _new_transaction(self, tx):
        tx_hash = bitcoinlib.rh2s(tx.hash)
        conn = None
        try:
            q = time.time()
            conn = await self._mysql_pool_conn.acquire()
            cur = await conn.cursor()
            lock = await get_lock(tx_hash, cur)
            if not lock:
                raise Exception("AEX %s" % tx_hash)
            # Check is transaction new
            tx_id = await tx_id_by_hash(tx.hash, cur)
            if tx_id is None:
                # print("new")
                await cur.execute('START TRANSACTION;')
                # call external handler
                r = 0
                if self.tx_handler:
                    r = await self.tx_handler(tx, cur)
                if r != 1 and r != 0:
                    raise Exception("Transaction handler response error %s"  % tx_hash)
                tx_id = await insert_new_tx(tx.hash, cur, affected=r)
                self.log.debugII("%s %s" % (tx_hash,
                                round(time.time() - q, 4)))
                await cur.execute('COMMIT;')
            if tx_hash in self.await_tx_list:
                self.await_tx_list.remove(tx_hash)
                self.await_tx_id_list.append(tx_id)
                if not self.await_tx_list:
                    self.block_txs_request.set_result(True)
        except Exception as err:
            self.log.error("new transaction error %s " % err)
            try:
                await cur.execute('ROLLBACK;')
            except Exception:
                pass
            if tx_hash in self.await_tx_list:
                self.await_tx_list = []
                self.await_tx_id_list = []
                self.block_txs_request.cancel()

        finally:
            if conn is not None:
                await release_lock(tx_hash, cur)
                self._mysql_pool_conn.release(conn)


    async def _new_block(self, block):
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
        if not self.active  or not self.active_block.done():
            return
        if block is None:
            self.sync = False
            self.log.debug('Block synchronization completed')
            return
        self.active_block = asyncio.Future()
        conn = None
        lock = False
        block_height = int(block["height"])
        next_block_height = block_height + 1
        try:
            self.log.debug("New block")
            bt = time.time()
            conn = await self._mysql_pool_conn.acquire()
            cur = await conn.cursor()
            self.log.debug("got mysql connection [%s]" % round(time.time()-bt,4))
            q = time.time()
            # blockchain position check
            lock = await get_lock("_new_block", cur)
            if not lock:
                self.log.debugI("can't get lock _new_block")
                next_block_height = None
                return
            self.log.debug("New block %s %s" % (block_height, block["hash"]))
            if "previousblockhash" in block:
                self.log.debugI("Parent hash %s" % block["previousblockhash"])
            # Is this block new?
            tq = time.time()
            block_id = await block_id_by_hash(block["hash"], cur)
            self.log.debug("block id form db %s [%s]" % (block_id, round(time.time()-tq,4)))
            if block_id  is not None:
                self.log.info("block already exist %s" % block["hash"])
                return
            # Get parent from db
            if "previousblockhash" in block:
                parent_height = await block_height_by_hash(block["previousblockhash"], cur)
            else:
                parent_height = None
            self.log.debug("parent height "+str(parent_height))
            if parent_height is None:
                # have no mount point in local chain
                last_block_height = await get_last_block_height(cur)
                self.log.debug("last local height " + str(last_block_height))
                if last_block_height is not None:
                    if last_block_height >= block_height:
                        self.log.info("not mainchain block %s" % block["hash"])
                        return
                    if last_block_height+1 == block_height:
                        if self.orphan_handler:
                            tq = time.time()
                            await self.orphan_handler(last_block_height, cur)
                            self.log.debug("orphan handler  %s [%s]" % (last_block_height, round(time.time() - tq, 4)))
                        tq = time.time()
                        await remove_orphan(last_block_height, cur)
                        self.log.info("remove orphan %s [%s]" % (last_block_height, round(time.time() - tq, 4)))
                    next_block_height -= 2
                    if next_block_height>last_block_height:
                        next_block_height = last_block_height + 1
                    if self.sync and next_block_height >= self.sync:
                        if self.sync_requested:
                            next_block_height = last_block_height + 1
                    else:
                        self.sync = next_block_height
                    self.log.debug("requested %s" % next_block_height)
                    return
                else:
                    if self.start_block is not None and block_height != self.start_block:
                        self.log.info("Start from block %s" % self.start_block)
                        next_block_height = self.start_block
                        return
            self.log.debug("blockchain position check [%s]" % round(time.time()-q,4))
            # add all block transactions
            q = time.time()
            tx_id_list, missed = await get_tx_id_list(list(block["tx"]), cur)
            self.await_tx_id_list = tx_id_list
            self.log.debug("Transactions already exist: %s missed %s [%s]" %
                           (len(tx_id_list), len(missed), round(time.time()-q,4)))
            if missed:
                self.log.debug("Request missed transactions")
                self.missed_tx_list = list(missed)
                self.await_tx_list = missed
                self.block_txs_request = asyncio.Future()
                self.loop.create_task(self._get_missed())
                await asyncio.wait_for(self.block_txs_request, timeout=300)
            if len(block["tx"]) != len(self.await_tx_id_list):
                self.log.error("get block transactions failed")
                raise Exception("get block transactions failed")
            ct = len(self.await_tx_id_list)
            self.log.debug("Transactions received: %s [%s]" % (ct, round(time.time()-q,4)))
            q = time.time()
            await insert_new_block(block["hash"], block["height"],
                                   block["previousblockhash"],
                                   block["time"],
                                   self.await_tx_id_list, cur)
            self.log.debug("added block to db [%s]" % round(time.time()-q,4))
            if self.sync == block["height"]:
                self.sync += 1
                next_block_height = self.sync
            # after block added handler
            q = time.time()
            if self.block_handler:
                await self.block_handler(block, cur)
                self.log.debug("block handler processed [%s]" % round(time.time()-q,4))
            self.log.info("New block %s %s" %(block["height"], block["hash"]))
            self.log.info("%s transactions %s" %(block["height"], ct))
        except Exception as err:
            if self.await_tx_list:
                self.await_tx_list = []
            self.log.error(str(traceback.format_exc()))
            self.log.error("new block error %s" % str(err))
            next_block_height = None
        finally:
            self.active_block.set_result(True)
            if lock:
                await release_lock("_new_block", cur)
                self.log.debugI("block  processing completed")
            if conn:
                self._mysql_pool_conn.release(conn)
            if next_block_height is not None:
                self.sync_requested = True
                self.loop.create_task(self.get_block_by_height(next_block_height))
            self.log.info("%s block processing time %s" %(block["height"], round(time.time() - bt, 4)))

    async def _get_missed(self):
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
                    if len(batch)>=self.batch_limit:
                        break
                # print(tx_hash)
                result = await self.rpc.batch(batch)
                for r in result:
                    d = r["result"]
                    try:
                        tx = bitcoinlib.Transaction.deserialize(io.BytesIO(unhexlify(d)))
                    except:
                        self.log.error("Transaction decode failed: %s" % d)
                    self.loop.create_task(self._new_transaction(tx))
            except Exception as err:
                self.log.error("_get_missed exception %s " % str(err))
                self.log.error(str(traceback.format_exc()))
                self.await_tx_list = []
                self.block_txs_request.cancel()
        self.get_missed_tx_threads -= 1


    async def get_block_by_height(self, height):
        self.log.debug("get block by height")
        d = await self.rpc.getblockcount()
        if d >= height:
            d = await self.rpc.getblockhash(height)
            await self._get_block_by_hash(d)
        else:
            self.log.debug("block %s not yet exist" % height)

    async def get_last_block(self):
        self.log.debug("check blockchain status")
        try:
            d = await self.rpc.getblockcount()
            conn = await self._mysql_pool_conn.acquire()
            cur = await conn.cursor()
            ld = await get_last_block_height(cur)
            if ld >= d:
                self.log.debug("blockchain is synchronized")
            else:
                d = await self.rpc.getbestblockhash()
                await self._get_block_by_hash(d)
        finally:
            self._mysql_pool_conn.release(conn)



    async def _get_block_by_hash(self, hash):
        if not self.active:
                return
        self.log.debug("get block by hash %s" % hash)
        try:
            block = await self.rpc.getblock(hash)
            assert  block["hash"] == hash
            self.loop.create_task(self._new_block(block))
        except Exception:
            self.log.error("get block by hash %s FAILED" % hash)

    async def watchdog(self):
        """
        Statistic output
        Check new blocks
        Garbage collection
        """
        conn = False
        while True:
            try:
                conn = await self.get_mysql_conn()
                cur = await conn.cursor()
                while True:
                    await asyncio.sleep(60)
                    count = await unconfirmed_count(cur)
                    lb_hash = await get_last_block_hash(cur)
                    height = await block_height_by_hash(lb_hash, cur)
                    self.log.info("unconfirmed tx %s last block %s" %
                                  (count, height))
                    r = await clear_old_tx(cur)
                    if r["pool"]:
                        self.log.info("cleared from pool %s not "
                                      "affected tx" % r["pool"])
                    if r["blocks"]:
                        self.log.info("cleared from blocks %s not "
                                      "affected tx" % r["blocks"])
                    await self.get_last_block()
            except asyncio.CancelledError:
                self.log.debug("watchdog terminated")
                break
            except Exception as err:
                self.log.error("watchdog error %s " % err)
            finally:
                if conn:
                    conn.close()

    async def stop(self):
        # close watchdog
        # cancell all subscriptions
        # disable block adding
        # close socket
        # wait until recent block completed
        # close session for rpc
        # close mysql connection pool
        self.log.warning("Stopping bitcoind connector")
        self.log.warning("Kill watchdog")
        if self._watchdog:
            self._watchdog.cancel()
        self.log.warning("Destroy zmq")
        if self._zmq_handler:
            self._zmq_handler.cancel()
        self.log.warning("New block processing restricted")
        self.active = False
        await asyncio.sleep(1)
        if not self.active_block.done():
            self.log.warning("Waiting active block task")
            await self.active_block
        self.session.close()
        self.log.warning("Close mysql pool")
        if self._mysql_pool_conn:
            self._mysql_pool_conn.close()
            await self._mysql_pool_conn.wait_closed()
        self.zmqContext.destroy()
        self.log.debug('Connector ready to shutdown')

