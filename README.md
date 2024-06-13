## Prime Number Generator using PySpark

This project is a Prime Number Generator that uses PySpark to calculate prime numbers within a given range or to find the nth prime number within a specified range. The program uses the Sieve of Eratosthenes algorithm to generate prime numbers up to a specified limit and then filters the prime numbers within the desired range. The results are saved to an output directory specified by the user.

### Features

- Generates prime numbers using the Sieve of Eratosthenes algorithm.
- Utilizes PySpark to distribute the computation and filter the primes in the specified range.
- Saves the prime numbers within the range to an output file in the specified directory.
- Finds the nth prime number within a specified range.

## Prerequisites

Before running this project, ensure you have the following installed:

- Python 3.9+
- Apache Spark
- PySpark

## Usage

### Running the Script

The script supports two modes of operation: generating primes within a range and finding the nth prime within a range.

#### Generating Primes in a Range

To generate prime numbers within a specified range, use the following command:

```bash
spark-submit prime_generator.py range <start> <end> <output_dir>
```

For example, to find prime numbers between 10 and 100 and save the results to the `primes` directory:

```bash
spark-submit prime_generator.py range 10 100 primes
```

#### Finding the nth Prime in a Range

To find the nth prime number within a specified range, use the following command:

```bash
spark-submit prime_generator.py nth <start> <end> --nth <nth> <output_dir>
```

For example, to find the 5th prime number between 10 and 100 and save the result to the `primes` directory:

```bash
spark-submit prime_generator.py nth 10 100 --nth 5 primes
```

### Example Output

If the command is successful, the script will print the prime numbers or the nth prime number in the specified range to the console and save them in an output file in the specified output directory.

Example content of `output_primes_10_100.txt`:

```
Prime numbers in range 10 to 100:
11
13
17
19
23
29
31
37
41
43
47
53
59
61
67
71
73
79
83
89
97
```

Example content of `nth_prime_5_from_10_to_100.txt`:

```
The 5th prime number in range 10 to 100 is: 23
```

## License

This project is licensed under the MIT License. See the `LICENSE` file for more details.

## Contributing

Contributions are welcome! Please fork the repository and submit a pull request for any improvements or bug fixes.
