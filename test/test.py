import os,sys,inspect
currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0,parentdir)
import configparser
import bitcoindconnector
import argparse
import asyncio
import sys
import signal
import traceback
import logging
import colorlog
from pythonjsonlogger import jsonlogger
import warnings
import zmq
import uvloop
import pybtc

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

warnings.filterwarnings("ignore")


class App:
    def __init__(self, loop, logger, config, connector_debug, connector_debug_full):
        self.loop = loop
        self.log = logger
        self.connector = False
        self.get_block_receipt_threads_limit = 100
        self.get_block_receipt_threads = 0
        self.confirmation_threads_limit = 20
        self.confirmation_threads = 0
        self.config = config
        self.mysql_pool = False
        self.confirm_target = 20
        self.ttc = 0
        self.rpc = False
        self.log.info("test state server init ...")
        signal.signal(signal.SIGINT, self.terminate)
        signal.signal(signal.SIGTERM, self.terminate)
        asyncio.ensure_future(self.start(config, connector_debug, connector_debug_full), loop=self.loop)

    async def start(self, config, connector_debug, connector_debug_full):
        # init database
        try:
            # self.log.info("Init mysql pool ")
            # await self.init_mysql()
            # self.mysql_pool = await \
            #     aiomysql.create_pool(host=self.MYSQL_CONFIG["host"],
            #                          port=int(self.MYSQL_CONFIG["port"]),
            #                          user=self.MYSQL_CONFIG["user"],
            #                          password=self.MYSQL_CONFIG["password"],
            #                          db=self.MYSQL_CONFIG["database"],
            #                          loop=self.loop,
            #                          minsize=10, maxsize=60)
            dsn = config['POSTGRESQL']["dsn"].replace(',',' ')
            pool_threads = config['POSTGRESQL']["pool_threads"]
            zeromq = config['BITCOIND']["zeromq"]
            rpc = config['BITCOIND']["rpc"]

            self.connector = bitcoindconnector.Connector(rpc,
                                                         zeromq,
                                                         dsn,
                                                         loop,
                                                         self.log,
                                                         tx_handler=self.new_transaction_handler,
                                                         start_block=1)
            # await self.bitcoindconnector.connected
            # self.bitcoindconnector.subscribe_blocks()
            # self.bitcoindconnector.subscribe_transactions()
            # await self.bitcoindconnector.get_last_block()
            # if "RPC" in config:
            #     self.rpc = await rpc.start(config["RPC"]["host"], config["RPC"]["port"], self)
            #     self.log.info("RPC outpoint %s:%s" % (config["RPC"]["host"],  config["RPC"]["port"]))
            #

            # self.console = Console(self.bitcoindconnector, self)
            # await self.bitcoindconnector.connected
            # self.bitcoindconnector.subscribe_blocks()
            # self.bitcoindconnector.subscribe_transactions()
            # await self.bitcoindconnector.get_last_block()
        except Exception as err:
            self.log.error("Start failed")
            self.log.error(str(traceback.format_exc()))
            # self.terminate(None,None)

    # async def init_mysql(self):
    #     conn = await \
    #         aiomysql.connect(user=self.MYSQL_CONFIG["user"],
    #                          password=self.MYSQL_CONFIG["password"],
    #                          db="",
    #                          host=self.MYSQL_CONFIG["host"],
    #                          port=int(self.MYSQL_CONFIG["port"]),
    #                          loop=self.loop)
    #     db = self.MYSQL_CONFIG["database"]
    #     cur = await conn.cursor()
    #     await cur.execute("CREATE DATABASE IF NOT EXISTS %s;" % db)
    #     await cur.execute("USE %s;" % db)
    #     try:
    #         pass
    #         # await model.init_db(self.MYSQL_CONFIG["database"],  self.config,  cur)
    #     except:
    #         self.log.error(str(traceback.format_exc()))
    #     conn.close()

    async def orphan_block_handler(self, orphan_hash, cur):
        self.log.warning("handler remove orphan %s" % orphan_hash)

    async def new_block_handler(self, data, cur):
        self.log.warning("handler new block %s" % str(data["hash"]))

    async def new_transaction_handler(self, data, ft):
        # pass
        # self.log.debug("tx_handler:")
        assert data["rawTx"] == data.serialize(hex=False)
        # print(pybtc.rh2s(data["txId"]))

    def _exc(self, a, b, c):
        return

    def terminate(self, a, b):
        self.loop.create_task(self.terminate_coroutine())

    async def terminate_coroutine(self):
        sys.excepthook = self._exc
        self.log.error('Stop request received')
        if self.connector:
            self.log.warning("Stop node bitcoindconnector")
            await self.connector.stop()
        self.log.info("Test server stopped")
        self.loop.stop()


def init(loop, argv):
    parser = argparse.ArgumentParser(description="test server  v 0.0.1")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("-c", "--config", help = "config file", type=str, nargs=1, metavar=('PATH',))
    group.add_argument("-l", "--log", help = "log file", type=str, nargs=1, metavar=('PATH',))
    parser.add_argument("-v", "--verbose", help="increase output verbosity", action="count", default=0)
    parser.add_argument("--json", help="json formatted logs", action='store_true')
    args = parser.parse_args()
    config_file = "test.cnf"
    log_file = "test.log"
    log_level = logging.WARNING
    logger = colorlog.getLogger('hss')
    if args.config is not None:
        config_file = args.config
    config = configparser.ConfigParser()
    config.read(config_file)
    connector_debug = False
    connector_debug_full = False
    if args.log is None:
        if "LOG" in config.sections():
            if "log_file" in config['LOG']:
                log_file = config['LOG']["log_file"]
            if "log_level" in config['LOG']:
                if config['LOG']["log_level"] == "info":
                    log_level = logging.INFO
                elif config['LOG']["log_level"] == "info":
                    log_level = logging.INFO
                elif config['LOG']["log_level"] == "debug":
                    log_level = logging.DEBUG
                elif config['LOG']["log_level"] == "debugI":
                    log_level = logging.DEBUG
                    connector_debug = True
                elif config['LOG']["log_level"] == "debugII":
                    log_level = logging.DEBUG
                    connector_debug = True
                    connector_debug_full = True
    else:
        log_file = args.log
    if args.verbose == 0:
        log_level = logging.WARNING
    elif args.verbose == 1:
        log_level = logging.INFO
    elif args.verbose > 1:
        log_level = logging.DEBUG
        if args.verbose > 3:
            connector_debug = True
        if args.verbose > 4:
            connector_debug = True
            connector_debug_full = True
    if log_level == logging.WARNING and "LOG" in config.sections():
        if "log_level" in config['LOG']:
            if config['LOG']["log_level"] == "info":
                log_level = logging.INFO
            elif config['LOG']["log_level"] == "debug":
                log_level = logging.DEBUG

    if args.json:
        logger = logging.getLogger()
        logHandler = logging.StreamHandler()
        formatter = jsonlogger.JsonFormatter('%(created)s %(asctime)s %(levelname)s %(message)s %(module)s %(lineno)d)')
        logHandler.setFormatter(formatter)
        logger.addHandler(logHandler)
        logger.setLevel(log_level)
    else:
        logger.setLevel(log_level)
        logger.debug("test")
        fh = logging.FileHandler(log_file)
        fh.setLevel(log_level)
        ch = logging.StreamHandler()
        ch.setLevel(log_level)
        formatter = colorlog.ColoredFormatter('%(log_color)s%(asctime)s %(levelname)s: %(message)s (%(module)s:%(lineno)d)')
        fh.setFormatter(formatter)
        ch.setFormatter(formatter)
        logger.addHandler(fh)
        logger.addHandler(ch)


    logger.setLevel(log_level)
    logger.info("Start")



    loop = asyncio.get_event_loop()



    app = App(loop, logger, config, connector_debug, connector_debug_full)
    return app


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    app = init(loop, sys.argv[1:])
    loop.run_forever()
    pending = asyncio.Task.all_tasks()
    loop.run_until_complete(asyncio.gather(*pending))
    loop.close()


