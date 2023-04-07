import asyncio
import functools
import time

from app_logger import timer_logger as logger


def show_run_time(func):
    @functools.wraps(func)
    def swap(*args, **kwargs):
        fn = func.__name__
        c = time.time()
        rst = func(*args, **kwargs)
        logger.info(f"{fn} - {kwargs}: Total time {str(time.time() - c)} secs.")
        return rst

    return swap


def timeit(method):
    @functools.wraps(method)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = method(*args, **kwargs)
        end_time = time.time()
        print(f"{method.__name__} => {(end_time - start_time)} s")

        return result

    return wrapper


def async_timeit(func):
    async def process(func, *args, **params):
        if asyncio.iscoroutinefunction(func):
            print('this function is a coroutine: {}'.format(func.__name__))
            return await func(*args, **params)
        else:
            print('this is not a coroutine')
            return func(*args, **params)

    async def helper(*args, **params):
        print('{}.time'.format(func.__name__))
        start = time.time()
        result = await process(func, *args, **params)

        # Test normal function route...
        # result = await process(lambda *a, **p: print(*a, **p), *args, **params)

        print('>>>', time.time() - start)
        return result

    return helper
