import time
import pywintypes
from threading import Thread, Event
import copy

import sys
sys.coinit_flags = 0

from pythoncom import CoInitializeEx, CoUninitialize, COINIT_MULTITHREADED, PumpWaitingMessages
from win32com.client import Dispatch, WithEvents, constants

import datetime
import logging

import numpy


class Exchange:
    def __init__(self):
        self.connection = None
        self.listen_portfo = []
        self.listen_bidask = []
        self.instrum = {}
        self.client = Dispatch("SmartCOM3.StServer.1")
        self._connected = Event()
        self._termination = Event()
        self._disconnected = Event()
        self._disconnected.set()
        self._thread = Thread(target=self._worker)
        self._thread.start()
        self.constants = constants
        self._legalDisconnect = False

    def terminate(self):
        self._termination.set()
        self._thread.join()

    def __del__(self):
        self.terminate()

    def connect(self, connection=None, timeout=None):
        if connection:
            self.connection = copy.deepcopy(connection)
        if self._connected.isSet():
            return True
        self.client.connect(**self.connection)
        if self._connected.wait(timeout):
            for p in self.listen_portfo:
                self.client.ListenPortfolio(p)
            for s in self.listen_bidask:
                self.client.ListenBidAsks(s)
            for sym, instr in self.instrum.items():
                instr.waitBidAsk()
            return True
        return False

    def disconnect(self, timeout=None):
        self._legalDisconnect = True
        if self._disconnected.isSet():
            return True
        for p in self.listen_portfo:
            self.client.CancelPortfolio(p)
        for s in self.listen_bidask:
            self.client.CancelBidAsks(s)
        self.client.disconnect()
        if self._disconnected.wait(timeout):
            for sym, instr in self.instrum.items():
                instr.clearBidAsk()
            return True
        return False

    def addInstrum(self, symbol, portfolio):
        if symbol not in self.listen_bidask:
            self.listen_bidask.append(symbol)
            if portfolio not in self.listen_portfo:
                self.listen_portfo.append(portfolio)
            self.instrum[symbol] = Instrument(self, symbol, portfolio)
            return self.instrum[symbol]

    def _worker(self):
        CoInitializeEx(COINIT_MULTITHREADED)
        handler = WithEvents(self.client, EventHandler)
        handler.setExchange(self)
        while not self._termination.isSet():
            PumpWaitingMessages()
            self._termination.wait(0.001)
        CoUninitialize()


class Instrument:
    cookie = 0
    buy = 0
    sell = 0

    def __init__(self, exchange, symbol, portfolio):
        self.ex = exchange
        self.constants = exchange.constants
        self.symbol = symbol
        self.portfolio = portfolio
        self.clearBidAsk()
        self.ba_rows = 0
        self.balance = 0.
        self.amount_balance = 0
        self.amountBalanceNull = Event()
        self.amountBalanceNull.set()

    def placeLimitOrder(self, price, amount, action):
        self.ex.client.PlaceOrder(
            self.portfolio, self.symbol, action, self.constants.StOrder_Type_Limit, self.constants.StOrder_Validity_Gtc,
            price, amount, 0, Instrument.cookie)
        Instrument.cookie += 1

    def getMinuteBars(self, number, timeTo):
        self.bars = {}
        self.gotAllBars = Event()
        self.barCounter = 0
        self.ex.client.GetBars(self.symbol,
                               self.constants.StBarInterval_1Min,
                               pywintypes.Time(time.mktime(timeTo.timetuple())), number)
        self.gotAllBars.wait()
        return self.bars

    def updateBidAsk(self, row, nrows, bid, bid_amount, ask, ask_amount):
        self.bid[row][0] = bid
        self.bid[row][1] = bid_amount
        self.ask[row][0] = ask
        self.ask[row][1] = ask_amount
        self.ba_rows = nrows

    def addBar(self, row, nrows, tm, openp, highp, lowp, closep, volume,
               open_int):
        self.bars[datetime.datetime.fromtimestamp(int(tm))] = closep
        self.barCounter += 1
        if self.barCounter >= nrows:
            self.gotAllBars.set()

    def addTrade(self, price, amount):
        self.balance += price * amount
        self.amount_balance += amount
        if self.amount_balance == 0:
            self.amountBalanceNull.set()
        else:
            self.amountBalanceNull.clear()
        logging.debug('Trade for %s, amount %g, price %g' % (self.symbol, amount, price))

    def getBalance(self):
        return self.balance

    def setState(self, balance, amount_balance):
        self.balance = balance
        self.amount_balance = amount_balance
        if self.amount_balance == 0:
            self.amountBalanceNull.set()
        else:
            self.amountBalanceNull.clear()

    def getState(self):
        return {'balance': self.balance, 'amount_balance': self.amount_balance}

    def clearBidAsk(self):
        self.bid = numpy.zeros((100, 2))
        self.ask = numpy.zeros((100, 2))

    def waitBidAsk(self):
        while self.bid[0][0] * self.ask[0][0] == 0:
            time.sleep(0.2)


class EventHandler:

    def setExchange(self, ex):
        self.ex = ex

    def OnSetPortfolio(self, portfolio, cash, leverage, comission, saldo):
        pass

    def OnDisconnected(self, reason):
        self.ex._disconnected.set()
        self.ex._connected.clear()
        logging.info('Disconnected: %s' % reason.encode('utf_8_sig'))
        if not self.ex._legalDisconnect:
            pass
        self.ex._legalDisconnect = False

    def OnAddPortfolio(self, row, nrows, portfolioName, portfolioExch, portfolioStatus):
        pass

    def OnAddSymbol(
            self, row, nrows, symbol, short_name, long_name, type, decimals, lot_size, punkt, step, sec_ext_id,
            sec_exch_name, expiry_date, days_before_expiry, strike):
        pass

    def OnUpdatePosition(self, portfolio, symbol, avprice, amount, planned):
        pass

    def OnAddTick(self, symbol, datetime, price, volume, tradeno, action):
        pass

    def OnSetMyTrade(self, row, nrows, portfolio, symbol, datetime, price, volume, tradeno, buysell, orderno):
        pass

    def OnSetMyOrder(
            self, row, nrows, portfolio, symbol, state, action, type, validity, price, amount, stop, filled, datetime, id,
            no, cookie):
        pass

    def OnOrderMoveFailed(self, orderid):
        pass

    def OnOrderMoveSucceeded(self, orderid):
        pass

    def OnOrderSucceeded(self, cookie, orderid):
        pass

    def OnUpdateBidAsk(self, symbol, row, nrows, bid, bidsize, ask, asksize):
        self.ex.instrum[symbol].updateBidAsk(row, nrows, bid, bidsize, ask, asksize)

    def OnUpdateOrder(
            self, portfolio, symbol, state, action, type, validity, price, amount, stop, filled, datetime, orderid, orderno,
            status_mask, cookie):
        pass

    def OnAddTrade(self, portfolio, symbol, orderid, price, amount, datetime, tradeno):
        if datetime >= self.connectionTime:
            self.ex.instrum[symbol].addTrade(price, amount)

    def OnSetSubscribtionCheckReult(self, result):
        pass

    def OnOrderFailed(self, cookie, orderid, reason):
        logging.info('Order failed')

    def OnConnected(self):
        self.ex._connected.set()
        self.ex._disconnected.clear()
        self.ex._legalDisconnect = False
        self.connectionTime = pywintypes.Time(time.time())
        logging.info('Connected')

    def OnSetMyClosePos(
            self, row, nrows, portfolio, symbol, amount, price_buy, price_sell, postime, order_open, order_close):
        pass

    def OnAddBar(self, row, nrows, symbol, interval, datetime, open, high, low, close, volume, open_int):
        self.ex.instrum[symbol].addBar(row, nrows, datetime, open, high, low, close, volume, open_int)

    def OnOrderCancelSucceeded(self, orderid):
        pass

    def OnOrderCancelFailed(self, orderid):
        pass

    def OnUpdateQuote(
            self, symbol, datetime, open, high, low, close, last, volume, size, bid, ask, bidsize, asksize, open_int, go_buy,
            go_sell, go_base, go_base_backed, high_limit, low_limit, trading_status, volat, theor_price):
        pass

    def OnAddTickHistory(self, row, nrows, symbol, datetime, price, volume, tradeno, action):
        pass
