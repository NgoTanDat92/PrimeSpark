from pyspark import SparkConf, SparkContext
import os
import argparse
from src.sieve_of_eratosthenes import SieveOfEratosthenes


def get_primes_in_range(sc, start, end):
    if start > end:
        raise ValueError("Start must be less than or equal to End.")

    broadcast_end = sc.broadcast(end)

    sieve = SieveOfEratosthenes(broadcast_end.value)
    primes_up_to_end = sieve.get_primes()

    primes_rdd = sc.parallelize(primes_up_to_end)
    primes_in_range = primes_rdd.filter(lambda x: start <= x <= end).collect()

    return primes_in_range


def generate_primes_in_range(sc, start, end, output_dir):
    try:
        primes_in_range = get_primes_in_range(sc, start, end)

        output_file = f"output_primes_{start}_{end}.txt"
        output_path = os.path.join(output_dir, output_file)

        with open(output_path, "w") as f:
            f.write(f"Prime numbers in range {start} to {end}:\n")
            f.write("\n".join(map(str, primes_in_range)))

        print(f"Prime numbers in range {start} to {end} written to {output_path}")

    except Exception as e:
        print(f"Error: {e}")


def get_nth_prime_in_range(sc, start, end, nth, output_dir):
    try:
        primes_in_range = get_primes_in_range(sc, start, end)

        if nth <= 0 or nth > len(primes_in_range):
            raise ValueError(
                "Invalid value for nth. It must be between 1 and the number of primes in the range."
            )
        nth_prime = primes_in_range[nth - 1]

        nth_prime_file = f"nth_prime_{nth}_from_{start}_to_{end}.txt"
        nth_prime_path = os.path.join(output_dir, nth_prime_file)

        with open(nth_prime_path, "w") as f:
            f.write(
                f"The {nth} prime number in range {start} to {end} is: {nth_prime}\n"
            )
        print(f"The {nth} prime number in range {start} to {end} is: {nth_prime}")
        print(f"The {nth} prime number written to {nth_prime_path}")

    except Exception as e:
        print(f"Error: {e}")


def main(method, start, end, nth, output_dir):
    conf = SparkConf().setAppName("PrimeNumberGenerator")
    sc = SparkContext(conf=conf)

    try:
        if method == "range":
            generate_primes_in_range(sc, start, end, output_dir)
        elif method == "nth":
            if nth is None:
                raise ValueError("Parameter nth must be provided for method 'nth'")
            get_nth_prime_in_range(sc, start, end, nth, output_dir)
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
    args = parser.parse_args()

    main(args.method, args.start, args.end, args.nth, args.output_dir)
