"""Utility functions that can be used by various stat checkers."""

import random
import string
import threading
import time


class SysExitThread(threading.Thread):
    """Pass exception from thread run to the parent."""

    def __init__(self, group=None, target=None, name=None, args=(), kwargs=None, *, daemon=None):
        """Monitorable thread."""
        super().__init__(group=group, target=target, name=name, args=args, kwargs=kwargs, daemon=daemon)
        self._exc = None

    def run(self):
        """Run thread and catch exception and store it."""
        try:
            super().run()
        except Exception as exc:
            self._exc = exc

    def join(self, timeout=None):
        """Allow for join and re-raise exception if one occurs."""
        super().join(timeout=timeout)
        if self._exc:
            raise self._exc


def random_word(length: int):
    """Get a random ascii word that is as long as the provided length."""
    letters = string.ascii_lowercase
    return "".join(random.choice(letters) for _ in range(length))  # noqa S311


def random_int(num_digits: int):
    """Get a random integer of the provided length (not using 0 to prevent leading zeros)."""
    return int("".join(str(random.choice([1, 2, 3, 4, 5, 6, 7, 8, 9])) for _ in range(num_digits)))  # noqa S311


def has_timed_out(start_time: float, timeout: float) -> bool:
    """Return true if a timeout should occur."""
    cur_time = time.time()
    duration = cur_time - start_time
    return duration > timeout
