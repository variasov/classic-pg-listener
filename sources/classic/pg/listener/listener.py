from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass, field
import logging
import time
from typing import Callable, TypeAlias, Optional

import psycopg
from psycopg import Connection, Notify

from classic.actors import Actor, Stop


ConnectionFactory: TypeAlias = Callable[[], Connection]
Receiver: TypeAlias = Callable[[Notify], None]


@dataclass
class AddCallback:
    callback: Receiver
    channel: str
    future: Future = field(default_factory=Future)


@dataclass
class RemoveCallback:
    channel: str
    future: Future = field(default_factory=Future)


class Listener(Actor):
    """
    Класс, умеющий запускать указанные коллбеки при приеме уведомлений
    из соответствующих каналов в PostgreSQL.

    Для использования необходимо передать функцию-фабрику,
    порождающую соединение. Соединение должно быть с autocommit=True.

    Для добавления коллбеков используется метод .add_callback(). В него
    следует передать коллбек и название канала. Коллбек будет вызван при приеме
    уведомления в соответствующем канале.

    Методом .remove_callback() можно убрать коллбек, если он больше не нужен.

    Например:

    >>> from classic.pg.listener import Listener
    ... from psycopg import connect, Notify
    ...
    ... listener = Listener(
    ...     lambda: connect(
    ...         user='test',
    ...         password='test',
    ...         host='127.0.0.1',
    ...         port='5432',
    ...         dbname='test',
    ...         autocommit=True,
    ...     )
    ... )
    ...
    ... def some_task(notify: Notify):
    ...     print(notify)
    ...
    ... listener.add_callback(some_task, 'some_channel')
    ... listener.run()


    Может работать в двух режимах:
    - "Блокирующий режим". При нем исполнение происходит в текущем потоке.
      Для этого используется блокирующий метод .run()

    - "Фоновый режим". При нем запускается внутренний поток для обслуживания
      соединения с базой и приема сообщений. В этом режиме класс похож на
      класс Thread из модуля threading. Метод .start() запускает фоновый поток,
      в котором происходит установка и обслуживание соединения, метод .stop()
      используется для запроса остановки фонового потока, методом .join() можно
      дождаться остановки внутреннего потока.

    Пример использования в фоновом режиме:
    >>> from classic.pg.listener import Listener
    ... from psycopg import connect, Notify
    ...
    ... listener = Listener(
    ...     lambda: connect(
    ...         user='test',
    ...         password='test',
    ...         host='127.0.0.1',
    ...         port='5432',
    ...         dbname='test',
    ...         autocommit=True,
    ...     )
    ... )
    ...
    ... def some_task(notify: Notify):
    ...     print(notify)
    ...
    ... listener.add_callback(some_task, 'some_channel')
    ... listener.start()
    ...
    ... print('Основной поток все еще свободен!')
    ...
    ... listener.stop()
    ... listener.join()  # Дожидается остановки

    Коллбеки, назначенные каналам, по умолчанию выполняются во внутреннем
    ThreadPoolExecutor. Параметром max_workers в конструкторе можно повлиять
    на размер пула. При max_workers исполнение будет происходить без пула,
    в том же потоке, что и обслуживает соединение. Таким образом, при запуске
    через метод .run() и значении max_workers=0 все, и обслуживание соединения,
    и исполнение коллбека будет происходить в одном потоке.

    Методы .add_callback() и .remove_callback() выполняют свою работу не сразу.
    Оба метода возвращают future, в результате которого будет None,
    если добавление произошло успешно, либо выкинет исключение,
    произошедшее в потоке, обслуживающем слушателя.
    """

    _connection_factory: ConnectionFactory
    _connection: Optional[Connection]
    _callbacks: dict[str, Receiver]
    _executor: Optional[ThreadPoolExecutor]
    _logger: logging.Logger
    _notifies_renews_count: int | None

    def __init__(
            self,
            connection_factory: ConnectionFactory,
            max_workers: int = 1,
            logger: Optional[logging.Logger] = None,
            ping_period: float = 60.0,
    ) -> None:
        super().__init__()
        self._connection_factory = connection_factory
        self._logger = logger or logging.getLogger('classic.pg.listener')
        self._connection = None
        self._callbacks = {}
        if max_workers:
            self._executor = ThreadPoolExecutor(max_workers=max_workers)
        else:
            self._executor = None

        self._notifies = None
        self._time_since_last_activity = None
        self._ping_period = ping_period

    def _loop(self) -> None:
        self._try_to_connect()

        if self._connection:
            try:
                notify = next(self._notifies)
            except psycopg.OperationalError:
                self._disconnect()
            except StopIteration:  # Timeout, notify not received
                self._ping_if_needed()
                self._renew_notifies()
            except KeyboardInterrupt:
                self._stop()
            else:
                self._on_notify(notify)

        while not self._inbox.empty():
            message = self._inbox.get(block=False)
            if isinstance(message, Stop):
                self._stop()
            elif isinstance(message, AddCallback):
                self._add_callback(message)
            elif isinstance(message, RemoveCallback):
                self._remove_callback(message)

    def _after_loop(self):
        self._disconnect()
        if self._executor:
            self._executor.shutdown(wait=False, cancel_futures=True)

    def _try_to_connect(self) -> None:
        if self._connection:
            return

        try:
            self._connection: Connection = self._connection_factory()
            self._connection.autocommit = True
            self._subscribe_all()
            self._notifies_renews_count = 0
            self._renew_notifies()
            self._logger.info('Connected with server')
        except psycopg.OperationalError:
            self._disconnect()

    def _disconnect(self) -> None:
        try:
            self._connection.close()
            self._logger.info('Connection closed.')
        except Exception:
            self._logger.debug('Can\'t disconnect, connection is None')
        finally:
            self._connection = None


    def _ping_if_needed(self) -> None:
        if not self._ping_period:
            return

        if not self._time_since_last_activity:
            self._time_since_last_activity = time.time()
            return

        # Time since last ping > ping period
        if time.time() - self._time_since_last_activity >= self._ping_period:
            self._time_since_last_activity = time.time()
            try:
                self._connection.execute('SELECT 1')
                self._logger.debug('Ping succeeded')
            except psycopg.OperationalError:
                self._logger.error('Ping failed')
                self._disconnect()
                return

    def _renew_notifies(self):
        self._notifies = self._connection.notifies(timeout=self.timeout)

    def _subscribe_all(self) -> None:
        for channel in self._callbacks.keys():
            self._subscribe(channel)

    def _subscribe(self, channel: str) -> None:
        self._connection.execute(f'LISTEN {channel}')
        self._logger.info('Started listening on %s', channel)

    def _on_notify(self, notify: Notify) -> None:
        self._logger.debug(
            'Notify received from %s and pid %s',
            notify.channel,
            notify.pid,
        )
        self._time_since_last_activity = time.time()
        callback = self._callbacks[notify.channel]
        if self._executor:
            self._executor.submit(callback, notify)
        else:
            callback(notify)

    def add_callback(self, callback: Receiver, channel: str) -> Future:
        """
        Запрашивает добавление коллбека в слушателя на указанном канале.
        Добавление происходит не сразу. Метод возвращает future, в результате
        которого будет None, если добавление произошло успешно, либо выкинет
        исключение, произошедшее в потоке, обслуживающем слушателя.
        """
        task = AddCallback(callback, channel)
        self._inbox.put(task)
        return task.future

    def _add_callback(self, message: AddCallback) -> None:
        self._callbacks[message.channel] = message.callback
        self._subscribe(message.channel)
        message.future.set_result(None)

    def remove_callback(self, channel: str) -> Future:
        """
        Удаление вех коллбеков на канале.
        Удаление происходит не сразу. Метод возвращает future, в результате
        которого будет None, если добавление произошло успешно, либо выкинет
        исключение, произошедшее в потоке, обслуживающем слушателя.
        """
        task = RemoveCallback(channel)
        self._inbox.put(task)
        return task.future

    def _remove_callback(self, message: RemoveCallback) -> None:
        self._callbacks.pop(message.channel)
        self._unsubscribe(message.channel)
        message.future.set_result(None)

    def _unsubscribe_all(self) -> None:
        for channel in self._callbacks.keys():
            self._unsubscribe(channel)

    def _unsubscribe(self, channel: str) -> None:
        self._connection.execute(f'UNLISTEN {channel}')
        logging.info('Stopped listening on %s', channel)
