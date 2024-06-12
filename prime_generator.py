from pyspark import SparkConf, SparkContext
import os
import argparse


def sieve_of_eratosthenes(limit):
    # Create the sieve of numbers from 2 to limit
    primes = [True] * (limit + 1)
    # 0 and 1 are not prime numbers
    primes[0] = primes[1] = False
    # evaluate numbers in range from 2 to sqrt of limit
    # whether they are prime numbers
    for i in range(2, int(limit**0.5) + 1):
        if primes[i]:
            # if i is a prime number then all the multiples of it
            # are composite numbers
            for j in range(i * i, limit + 1, i):
                primes[j] = False
    # collect all prime numbers up to limit
    return [i for i, is_prime in enumerate(primes) if is_prime]


def main(start, end, output_dir):
    # Initialize Spark context
    conf = SparkConf().setAppName("PrimeNumberGenerator")
    sc = SparkContext(conf=conf)

    # Broadcast the upper limit to all nodes
    broadcast_end = sc.broadcast(end)

    # Generate prime numbers up to the end value using the Sieve of Eratosthenes
    primes_up_to_end = sieve_of_eratosthenes(broadcast_end.value)

    # Filter primes within the range [start, end] using PySpark
    primes_rdd = sc.parallelize(primes_up_to_end)
    primes_in_range = primes_rdd.filter(lambda x: x >= start and x <= end).collect()

    # Print the prime numbers
    print(f"Prime numbers in range {start} to {end}: {primes_in_range}")

    # Create the output directory if it doesn't exist
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Generate the output file name pattern
    output_file = f"output_primes_{start}_{end}.txt"
    output_path = os.path.join(output_dir, output_file)

    # Write the prime numbers to the output file
    with open(output_path, "w") as f:
        f.write(f"Prime numbers in range {start} to {end}:\n")
        f.write("\n".join(map(str, primes_in_range)))

    # Stop the Spark context
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
