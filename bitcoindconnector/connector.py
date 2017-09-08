import asyncio
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
import asyncpg

class Connector:

    def __init__(self,
                 bitcoind_rpc_url,
                 bitcoind_zerromq_url,
                 postgresql_dsn,
                 loop,
                 logger,
                 start_block=None,
                 tx_handler=None,
                 block_handler=None,
                 orphan_handler=None,
                 db_pool_size=50,
                 debug = False,
                 debug_full = False):
        """
        """
        def empty(a=None):
            return None

        self.loop = loop
        self.log = logger

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

        self.rpc_url = bitcoind_rpc_url
        self.zmq_url = bitcoind_zerromq_url
        self.postgresql_dsn = postgresql_dsn
        self.db_pool_size = db_pool_size
        self.orphan_handler = orphan_handler
        self.tx_handler = tx_handler
        self.block_handler = block_handler
        self.start_block = start_block

        self.session = aiohttp.ClientSession()
        self.active = True
        self.active_block = asyncio.Future()
        self.active_block.set_result(True)
        self.total_received_tx = 0
        self.total_received_tx_time = 0

        self.batch_limit = 20
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
        self.get_missed_tx_threads = 0
        self.get_missed_tx_threads_limit = 50
        self.tx_in_process = set()
        self.zmqContext = False

        self._watchdog = False
        self._zmq_handler = False
        self._db_pool = False
        self.log.info("Bitcoind connector started")
        asyncio.ensure_future(self.start(), loop=self.loop)



    async def start(self):
        try:
            conn = await asyncpg.connect(dsn=self.postgresql_dsn)
            await init_db(conn)
            conn.close()
            self._db_pool = await \
                asyncpg.create_pool(dsn=self.postgresql_dsn,
                                  loop=self.loop,
                                  min_size=10, max_size=self.db_pool_size)
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
                if not self.active:
                    break
            except asyncio.CancelledError:
                self.log.debug("handle terminated")
                break
            except Exception as err:
                self.log.error(str(err))



    async def _new_transaction(self, tx):
        tx_hash = bitcoinlib.rh2s(tx.hash)
        q = tm()
        async with self._db_pool.acquire() as conn:
            if tx_hash in self.await_tx_list:
                ft = self.await_tx_future
            else:
                ft = None
            self.log.debugIII("connection %s %s" % (tx_hash, tm(q)))
            q = tm()
            if tx_hash in self.tx_in_process:
                return
            self.tx_in_process.add(tx_hash)

            # Check is transaction new
            tx_id = await tx_id_by_hash(tx.hash, conn)
            if tx_id is not None:
                self.tx_in_process.remove(tx_hash)
                return

            # start transaction
            tr = conn.transaction()
            await tr.start()
            try:
                # call external handler
                r = 0
                if self.tx_handler:
                    qh = tm()
                    r = await self.tx_handler(tx, ft, conn)
                    self.log.debugII("handler %s %s" % (tx_hash, tm(qh)))
                if r != 1 and r != 0:
                    raise Exception("Transaction handler response error %s"  % tx_hash)

                # insert new transaction
                tx_id = await insert_new_tx(tx.hash, conn, affected=r)
                self.log.debugIII("new tx %s %s" % (tx_hash,tm(q)))
                await tr.commit()

                if tx_hash in self.await_tx_list:
                    self.await_tx_list.remove(tx_hash)
                    self.await_tx_id_list.append(tx_id)
                    self.await_tx_future[unhexlify(tx_hash)[::-1]].set_result(True)
                    if not self.await_tx_list:
                        self.block_txs_request.set_result(True)
            except DependsTransaction as err:
                await tr.rollback()
                self.log.warning("dependency error %s" % bitcoinlib.rh2s(err.raw_tx_hash))
                self.loop.create_task(self.wait_tx_then_add(err.raw_tx_hash, tx))
            except Exception as err:
                if tx_hash in self.await_tx_list:
                    self.await_tx_list = []
                    self.await_tx_id_list = []
                    self.block_txs_request.cancel()
                    for i in self.await_tx_future:
                        if not self.await_tx_future[i].done():
                            self.await_tx_future[i].cancel()
                await tr.rollback()
                self.log.error("new transaction error %s " % err)
                self.log.error(str(traceback.format_exc()))
            finally:
                self.tx_in_process.remove(tx_hash)


    async def wait_tx_then_add(self, raw_tx_hash, tx):
        tx_hash = bitcoinlib.rh2s(tx.hash)
        try:
            self.log.warning("resolve dependency %s" % bitcoinlib.rh2s(raw_tx_hash))
            if not self.await_tx_future[raw_tx_hash].done():
                await self.await_tx_future[raw_tx_hash]
                self.log.warning("dependency resolved %s" % bitcoinlib.rh2s(raw_tx_hash))
            print(len(self.tx_in_process), ' ', len(self.await_tx_future))
            self.loop.create_task(self._new_transaction(tx))
        except Exception as err:
            print(err)
            self.log.debug("dependency failed %s" % bitcoinlib.rh2s(raw_tx_hash))
            self.tx_in_process.remove(tx_hash)

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

        binary_block_hash = unhexlify(block["hash"])
        binary_previousblock_hash = unhexlify(block["previousblockhash"]) \
                                    if "previousblockhash" in block else None
        block_height = int(block["height"])
        next_block_height = block_height + 1
        self.log.info("New block %s %s" % (block_height, block["hash"]))
        bt = q = tm()

        try:
            async with self._db_pool.acquire() as con:
                self.log.debugI("got db connection [%s]" % tm(q))
                # blockchain position check
                q = tm()
                block_id = await block_id_by_hash(binary_block_hash, con)
                if block_id  is not None:
                    self.log.info("block already exist in db %s" % block["hash"])
                    return
                # Get parent from db
                if binary_previousblock_hash is not None:
                    parent_height = await block_height_by_hash(binary_previousblock_hash, con)
                else:
                    parent_height = None
                self.log.debugI("parent height %s" % parent_height)

                if parent_height is None:
                    # have no mount point in local chain
                    last_block_height = await get_last_block_height(con)
                    self.log.debugI("last local height %s" % last_block_height)
                    if last_block_height is not None:
                        if last_block_height >= block_height:
                            self.log.info("not mainchain block %s" % block["hash"])
                            return
                        if last_block_height+1 == block_height:
                            if self.orphan_handler:
                                tq = tm()
                                await self.orphan_handler(last_block_height, con)
                                self.log.debug("orphan handler  %s [%s]" % (last_block_height, tm(tq)))
                            tq = tm()
                            await remove_orphan(last_block_height, con)
                            self.log.info("remove orphan %s [%s]" % (last_block_height, tm(tq)))
                        next_block_height -= 2
                        if next_block_height>last_block_height:
                            next_block_height = last_block_height + 1
                        if self.sync and next_block_height >= self.sync:
                            if self.sync_requested:
                                next_block_height = last_block_height + 1
                        else:
                            self.sync = next_block_height
                        self.log.debugI("requested %s" % next_block_height)
                        return
                    else:
                        if self.start_block is not None and block_height != self.start_block:
                            self.log.info("Start from block %s" % self.start_block)
                            next_block_height = self.start_block
                            return
                self.log.debug("blockchain position check [%s]" % tm(q))

                # add all block transactions
                q = tm()
                binary_tx_hash_list = [unhexlify(t)[::-1] for t in block["tx"]]
                tx_id_list, missed = await get_tx_id_list(binary_tx_hash_list, con)
                missed = [bitcoinlib.rh2s(t) for t in missed]
                self.await_tx_id_list = tx_id_list
                self.log.debug("Transactions already exist: %s missed %s [%s]" % (len(tx_id_list), len(missed), tm(q)))
                if missed:
                    self.log.debug("Request missed transactions")
                    self.missed_tx_list = list(missed)
                    self.await_tx_list = missed
                    self.await_tx_future = dict()
                    for i in missed:
                        self.await_tx_future[unhexlify(i)[::-1]] = asyncio.Future()
                    self.block_txs_request = asyncio.Future()
                    self.loop.create_task(self._get_missed())
                    await asyncio.wait_for(self.block_txs_request, timeout=300)
                if len(block["tx"]) != len(self.await_tx_id_list):

                    self.log.error("get block transactions failed")
                    self.log.error(str(self.await_tx_id_list))

                    raise Exception("get block transactions failed")
                tx_count = len(self.await_tx_id_list)
                self.total_received_tx += tx_count
                self.total_received_tx_time += tm(q)
                rate = round(self.total_received_tx/self.total_received_tx_time)
                self.log.debug("Transactions received: %s [%s] [%s]" % (tx_count, tm(q), rate))

                # insert new block
                q = tm()
                await insert_new_block(binary_block_hash,
                                       block["height"],
                                       binary_previousblock_hash,
                                       block["time"],
                                       self.await_tx_id_list, con)
                self.log.debug("added block to db [%s]" % tm(q))
                if self.sync == block["height"]:
                    self.sync += 1
                    next_block_height = self.sync

                # after block added handler
                q = tm()
                if self.block_handler:
                    await self.block_handler(block, con)
                    self.log.debug("block handler processed [%s]" % round(tm(q),4))
                # self.log.info("New block %s %s" %(block["height"], block["hash"]))
                # self.log.info("%s transactions %s" %(block["height"], tx_count))

        except Exception as err:
            if self.await_tx_list:
                self.await_tx_list = []
            self.log.error(str(traceback.format_exc()))
            self.log.error("new block error %s" % str(err))
            next_block_height = None
        finally:
            self.active_block.set_result(True)
            self.log.debugI("block  processing completed")
            if next_block_height is not None:
                self.sync_requested = True
                self.loop.create_task(self.get_block_by_height(next_block_height))
            self.log.info("%s block [%s tx] processing time %s" %
                          (block["height"], len(block["tx"]), tm(bt)))

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
                result = await self.rpc.batch(batch)
                for r in result:
                    d = r["result"]
                    try:
                        # todo check if we can sent hex string to deserialize
                        print(d)
                        tx = bitcoinlib.Transaction.deserialize(io.BytesIO(unhexlify(d)))
                    except:
                        self.log.error("Transaction decode failed: %s" % d)
                        raise Exception("Transaction decode failed: %s" % d)
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
        d = await self.rpc.getblockcount()
        async with self._db_pool.acquire() as con:
            ld = await get_last_block_height(con)
            if ld is None:
                ld = 0
            if ld >= d:
                self.log.debug("blockchain is synchronized")
            else:
                d = await self.rpc.getbestblockhash()
                await self._get_block_by_hash(d)



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
        while True:
            conn = False
            try:
                conn =  await asyncpg.connect(dsn=self.postgresql_dsn)
                while True:
                    await asyncio.sleep(60)
                    count = await unconfirmed_count(conn)
                    lb_hash = await get_last_block_hash(conn)
                    height = await block_height_by_hash(lb_hash, conn)
                    self.log.info("unconfirmed tx %s last block %s" %
                                  (count, height))
                    r = await clear_old_tx(conn)
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
                self.log.error(str(traceback.format_exc()))
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
        self.log.warning("Stopping bitcoind bitcoindconnector")
        self.log.warning("Kill watchdog")
        if self._watchdog:
            self._watchdog.cancel()
        self.log.warning("Destroy zmq")
        if self._zmq_handler:
            self._zmq_handler.cancel()
            self.log.warning("Cancel zmq handler")
        self.log.warning("New block processing restricted")
        self.active = False
        if not self.active_block.done():
            self.log.warning("Waiting active block task")
            await self.active_block
        self.session.close()
        self.log.warning("Close mysql pool")
        if self._db_pool:
            self._db_pool.close()
        if self.zmqContext:
            self.zmqContext.destroy()
        self.log.debug('Connector ready to shutdown')

