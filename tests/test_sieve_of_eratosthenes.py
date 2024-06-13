import unittest
from ..src.sieve_of_eratosthenes import SieveOfEratosthenes


class TestSieveOfEratosthenes(unittest.TestCase):
    def test_sieve_initialization(self):
        sieve = SieveOfEratosthenes(10)
        self.assertEqual(sieve.get_limit(), 10)
        self.assertEqual(sieve.get_primes(), [2, 3, 5, 7])

    def test_get_primes(self):
        sieve = SieveOfEratosthenes(20)
        self.assertEqual(sieve.get_primes(), [2, 3, 5, 7, 11, 13, 17, 19])

    def test_get_primes_in_range(self):
        sieve = SieveOfEratosthenes(30)
        self.assertEqual(sieve.get_primes_in_range(10, 20), [11, 13, 17, 19])

    def test_get_primes_in_range_invalid(self):
        sieve = SieveOfEratosthenes(30)
        with self.assertRaises(ValueError):
            sieve.get_primes_in_range(20, 10)

    def test_get_nth_prime_in_range(self):
        sieve = SieveOfEratosthenes(50)
        self.assertEqual(sieve.get_nth_prime_in_range(10, 30, 1), 11)
        self.assertEqual(sieve.get_nth_prime_in_range(10, 30, 2), 13)
        self.assertEqual(sieve.get_nth_prime_in_range(10, 30, 3), 17)
        self.assertEqual(sieve.get_nth_prime_in_range(10, 30, 4), 19)
        self.assertEqual(sieve.get_nth_prime_in_range(10, 30, 5), 23)

    def test_get_nth_prime_in_range_invalid(self):
        sieve = SieveOfEratosthenes(50)
        with self.assertRaises(ValueError):
            sieve.get_nth_prime_in_range(10, 30, 0)
        with self.assertRaises(ValueError):
            sieve.get_nth_prime_in_range(10, 30, 100)
        with self.assertRaises(ValueError):
            sieve.get_nth_prime_in_range(30, 10, 1)

    def test_get_nth_prime_out_of_range(self):
        sieve = SieveOfEratosthenes(30)
        with self.assertRaises(ValueError):
            sieve.get_nth_prime_in_range(10, 20, 6)

    def test_get_nth_prime_large_range(self):
        sieve = SieveOfEratosthenes(1000)
        self.assertEqual(sieve.get_nth_prime_in_range(500, 1000, 1), 503)
        self.assertEqual(sieve.get_nth_prime_in_range(500, 1000, 50), 829)
        self.assertEqual(sieve.get_nth_prime_in_range(500, 1000, 60), 907)


if __name__ == "__main__":
    unittest.main()
