from pyspark import SparkConf, SparkContext
import os
import argparse


def sieve_of_eratosthenes(limit):
    # Implementing the sieve of Eratosthenes to find all primes up to 'limit'
    primes = [True] * (limit + 1)
    primes[0] = primes[1] = False
    for i in range(2, int(limit**0.5) + 1):
        if primes[i]:
            for j in range(i * i, limit + 1, i):
                primes[j] = False
    return [i for i, is_prime in enumerate(primes) if is_prime]


def main(start, end, output_dir):
    conf = SparkConf().setAppName("PrimeNumberGenerator")
    sc = SparkContext(conf=conf)

    try:
        # Validate input range
        if start > end:
            raise ValueError("Start must be less than or equal to End.")

        # Broadcast the upper limit to all nodes
        broadcast_end = sc.broadcast(end)

        # Generate primes up to 'end' using the sieve
        primes_up_to_end = sieve_of_eratosthenes(broadcast_end.value)

        # Filter primes in the specified range
        primes_rdd = sc.parallelize(primes_up_to_end)
        primes_in_range = primes_rdd.filter(lambda x: start <= x <= end).collect()

        # Output file path
        output_file = f"output_primes_{start}_{end}.txt"
        output_path = os.path.join(output_dir, output_file)

        # Write primes to file
        with open(output_path, "w") as f:
            f.write(f"Prime numbers in range {start} to {end}:\n")
            f.write("\n".join(map(str, primes_in_range)))

        # Print success message
        print(f"Prime numbers in range {start} to {end} written to {output_path}")

    except Exception as e:
        print(f"Error: {e}")

    finally:
        sc.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Calculate prime numbers within a range using PySpark."
    )
    parser.add_argument("start", type=int, help="Start of the range (inclusive)")
    parser.add_argument("end", type=int, help="End of the range (inclusive)")
    parser.add_argument("output_dir", help="Output directory for storing results")
    args = parser.parse_args()

    main(args.start, args.end, args.output_dir)
