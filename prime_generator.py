from pyspark import SparkConf, SparkContext
import os
import argparse
from src.sieve import SieveOfEratosthenes
from src.sieve import SieveOfAtkin


def get_sieve_class(sieve_name: str):
    if sieve_name == "eratosthenes":
        return SieveOfEratosthenes
    elif sieve_name == "atkin":
        return SieveOfAtkin
    else:
        raise ValueError(f"Unknown sieve method: {sieve_name}")


def get_primes_in_range(
    sc: SparkContext, start: int, end: int, sieve_class
) -> list[int]:
    if start > end:
        raise ValueError("Start must be less than or equal to End.")

    broadcast_end = sc.broadcast(end)

    sieve = sieve_class(broadcast_end.value)
    primes_up_to_end = sieve.get_primes()

    primes_rdd = sc.parallelize(primes_up_to_end)
    primes_in_range = primes_rdd.filter(lambda x: start <= x <= end).collect()

    return primes_in_range


def generate_primes_in_range(
    sc: SparkContext, start: int, end: int, output_dir: str, sieve_class
) -> None:
    try:
        primes_in_range = get_primes_in_range(sc, start, end, sieve_class)

        output_file = f"output_primes_{start}_{end}.txt"
        output_path = os.path.join(output_dir, output_file)

        print("hello")

        os.makedirs(output_dir, exist_ok=True)

        with open(output_path, "w") as f:
            f.write(
                f"There are {len(primes_in_range)} prime numbers in range {start} to {end}:\n"
            )
            f.write("\n".join(map(str, primes_in_range)))

        print(
            f"There are {len(primes_in_range)} prime numbers in range {start} to {end}, these are written to {output_path}"
        )

    except Exception as e:
        print(f"Error: {e}")


def get_nth_prime_in_range(
    sc: SparkContext, start: int, end: int, nth: int, output_dir: str, sieve_class
) -> None:
    try:
        primes_in_range = get_primes_in_range(sc, start, end, sieve_class)

        if nth <= 0 or nth > len(primes_in_range):
            raise ValueError(
                "Invalid value for nth. It must be between 1 and the number of primes in the range."
            )
        nth_prime = primes_in_range[nth - 1]

        nth_prime_file = f"nth_prime_{nth}_from_{start}_to_{end}.txt"
        nth_prime_path = os.path.join(output_dir, nth_prime_file)

        os.makedirs(output_dir, exist_ok=True)

        with open(nth_prime_path, "w") as f:
            f.write(
                f"There are {len(primes_in_range)} in range {start} to {end}. The {nth} prime number is: {nth_prime}\n"
            )
        print(
            f"There are {len(primes_in_range)} in range {start} to {end}. The {nth} prime number is: {nth_prime}, written to {nth_prime_path}"
        )

    except Exception as e:
        print(f"Error: {e}")


def main(
    method: str, start: int, end: int, nth: int, output_dir: str, sieve: str
) -> None:
    conf = SparkConf().setAppName("PrimeNumberGenerator")
    sc = SparkContext(conf=conf)

    sieve_class = get_sieve_class(sieve)

    try:
        if method == "range":
            generate_primes_in_range(sc, start, end, output_dir, sieve_class)
        elif method == "nth":
            if nth is None:
                raise ValueError("Parameter nth must be provided for method 'nth'")
            get_nth_prime_in_range(sc, start, end, nth, output_dir, sieve_class)
        else:
            raise ValueError(
                "Invalid method. Use 'range' to generate primes in a range or 'nth' to get the nth prime number."
            )
    finally:
        sc.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Calculate prime numbers within a range or find the nth prime number using PySpark."
    )
    parser.add_argument(
        "method",
        type=str,
        choices=["range", "nth"],
        help="Method to use: 'range' or 'nth'",
    )
    parser.add_argument("start", type=int, help="Start of the range (inclusive)")
    parser.add_argument("end", type=int, help="End of the range (inclusive)")
    parser.add_argument(
        "--nth",
        type=int,
        help="Find the nth prime number in the range (only for 'nth' method)",
        default=None,
    )
    parser.add_argument("output_dir", help="Output directory for storing results")
    parser.add_argument(
        "--sieve",
        type=str,
        choices=["eratosthenes", "atkin"],
        required=True,
        help="Sieve method to use: 'eratosthenes' or 'atkin'",
    )
    args = parser.parse_args()

    main(args.method, args.start, args.end, args.nth, args.output_dir, args.sieve)
