class SieveOfEratosthenes:
    def __init__(self, limit):
        self.limit = limit
        self.primes = self._sieve_of_eratosthenes(limit)

    def _sieve_of_eratosthenes(self, limit):
        primes = [True] * (limit + 1)
        primes[0] = primes[1] = False
        for i in range(2, int(limit**0.5) + 1):
            if primes[i]:
                for j in range(i * i, limit + 1, i):
                    primes[j] = False
        return [i for i, is_prime in enumerate(primes) if is_prime]

    def get_primes(self):
        return self.primes

    def get_primes_in_range(self, start, end):
        if start > end:
            raise ValueError("Start must be less than or equal to End.")
        return [p for p in self.primes if start <= p <= end]

    def get_nth_prime_in_range(self, start, end, nth):
        primes_in_range = self.get_primes_in_range(start, end)
        if nth <= 0 or nth > len(primes_in_range):
            raise ValueError(
                "Invalid value for nth. It must be between 1 and the number of primes in the range."
            )
        return primes_in_range[nth - 1]
