import logging
import threading

from classic.pg.listener import Listener

from psycopg import connect, Notify, Connection


def new_connection() -> Connection:
    return connect(
        user='test',
        password='test',
        host='127.0.0.1',
        port='5432',
        dbname='test',
        autocommit=True,
    )


def test_listener():
    logging.basicConfig(level=logging.INFO)

    sync = threading.Event()
    result = None

    def task(notify: Notify) -> None:
        logging.info(notify)
        nonlocal result
        result = (notify.channel, notify.payload)
        sync.set()

    listener = Listener(new_connection)
    listener.add_callback(task, 'test')
    listener.start()

    # TODO: понять, как тестить без задержек
    import time
    time.sleep(1)

    conn = new_connection()
    conn.execute("NOTIFY test, 'test'")

    sync.wait()
    assert result == ('test', 'test')

    listener.remove_callback('test')

    result = None
    conn = new_connection()
    conn.execute("NOTIFY test, 'test'")

    import time
    time.sleep(1)

    assert result is None

    listener.stop()
