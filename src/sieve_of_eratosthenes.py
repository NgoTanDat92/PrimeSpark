class SieveOfEratosthenes:
    def __init__(self, limit: int) -> None:
        """
        Initializes the Sieve of Eratosthenes with a specified limit.

        :param limit: The upper limit for finding prime numbers.
        """
        self.limit: int = limit
        self.primes: list[int] = self._sieve_of_eratosthenes(limit)

    def _sieve_of_eratosthenes(self, limit: int) -> list[int]:
        """
        Applies the Sieve of Eratosthenes algorithm to find all primes up to a given limit.

        :param limit: The upper limit for finding prime numbers.
        :return: A list of prime numbers up to the limit.
        """
        primes = [True] * (limit + 1)
        primes[0] = primes[1] = False
        for i in range(2, int(limit**0.5) + 1):
            if primes[i]:
                for j in range(i * i, limit + 1, i):
                    primes[j] = False
        return [i for i, is_prime in enumerate(primes) if is_prime]

    def get_limit(self) -> int:
        """
        Gets the limit for the sieve.

        :return: The limit value.
        """
        return self.limit

    def get_primes(self) -> list[int]:
        """
        Gets the list of prime numbers up to the limit.

        :return: A list of prime numbers.
        """
        return self.primes

    def get_primes_in_range(self, start: int, end: int) -> list[int]:
        """
        Gets the list of prime numbers within a specified range.

        :param start: The start of the range.
        :param end: The end of the range.
        :return: A list of prime numbers within the specified range.
        :raises ValueError: If start is greater than end.
        """
        if start > end:
            raise ValueError("Start must be less than or equal to End.")
        return [p for p in self.get_primes() if start <= p <= end]

    def get_nth_prime_in_range(self, start: int, end: int, nth: int) -> list[int]:
        """
        Gets the nth prime number within a specified range.

        :param start: The start of the range.
        :param end: The end of the range.
        :param nth: The ordinal position of the prime number to retrieve.
        :return: The nth prime number within the specified range.
        :raises ValueError: If nth is not within the valid range or if start is greater than end.
        """
        primes_in_range = self.get_primes_in_range(start, end)
        if nth <= 0 or nth > len(primes_in_range):
            raise ValueError(
                "Invalid value for nth. It must be between 1 and the number of primes in the range."
            )
        return primes_in_range[nth - 1]
