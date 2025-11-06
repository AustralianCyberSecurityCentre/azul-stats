"""Testcases for utils."""

import unittest

from azul_stats.utils import random_int, random_word


class TestUtils(unittest.TestCase):
    def test_random_word(self):
        self.assertEqual(len(random_word(10)), 10)
        self.assertEqual(len(random_word(8)), 8)
        self.assertEqual(len(random_word(100)), 100)
        self.assertIsInstance(random_word(100), str)

    def test_random_int(self):
        self.assertGreaterEqual(random_int(2), 10)
        self.assertGreaterEqual(random_int(3), 100)
        self.assertIsInstance(random_int(2), int)
