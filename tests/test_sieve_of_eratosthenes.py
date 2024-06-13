import pytest
from ..src.sieve_of_eratosthenes import SieveOfEratosthenes


def test_sieve_initialization():
    sieve = SieveOfEratosthenes(10)
    assert sieve.limit == 10
    assert sieve.primes == [2, 3, 5, 7]


def test_get_primes():
    sieve = SieveOfEratosthenes(20)
    assert sieve.get_primes() == [2, 3, 5, 7, 11, 13, 17, 19]


def test_get_primes_in_range():
    sieve = SieveOfEratosthenes(30)
    assert sieve.get_primes_in_range(10, 20) == [11, 13, 17, 19]


def test_get_primes_in_range_invalid():
    sieve = SieveOfEratosthenes(30)
    with pytest.raises(ValueError):
        sieve.get_primes_in_range(20, 10)


def test_get_nth_prime_in_range():
    sieve = SieveOfEratosthenes(50)
    assert sieve.get_nth_prime_in_range(10, 30, 1) == 11
    assert sieve.get_nth_prime_in_range(10, 30, 2) == 13
    assert sieve.get_nth_prime_in_range(10, 30, 3) == 17
    assert sieve.get_nth_prime_in_range(10, 30, 4) == 19
    assert sieve.get_nth_prime_in_range(10, 30, 5) == 23


def test_get_nth_prime_in_range_invalid():
    sieve = SieveOfEratosthenes(50)
    with pytest.raises(ValueError):
        sieve.get_nth_prime_in_range(10, 30, 0)
    with pytest.raises(ValueError):
        sieve.get_nth_prime_in_range(10, 30, 100)
    with pytest.raises(ValueError):
        sieve.get_nth_prime_in_range(30, 10, 1)


def test_get_nth_prime_out_of_range():
    sieve = SieveOfEratosthenes(30)
    with pytest.raises(ValueError):
        sieve.get_nth_prime_in_range(10, 20, 6)


def test_get_nth_prime_large_range():
    sieve = SieveOfEratosthenes(1000)
    assert sieve.get_nth_prime_in_range(500, 1000, 1) == 503
    assert sieve.get_nth_prime_in_range(500, 1000, 50) == 829
    assert sieve.get_nth_prime_in_range(500, 1000, 60) == 907
