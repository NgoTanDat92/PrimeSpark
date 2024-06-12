
## Prime Number Generator using PySpark

This project is a Prime Number Generator that uses PySpark to calculate prime numbers within a given range. The program uses the Sieve of Eratosthenes algorithm to generate prime numbers up to a specified limit and then filters the prime numbers within the desired range. The results are saved to an output directory specified by the user.

### Features

- Generates prime numbers using the Sieve of Eratosthenes algorithm.
- Utilizes PySpark to distribute the computation and filter the primes in the specified range.
- Saves the prime numbers within the range to an output file in the specified directory.

## Prerequisites

Before running this project, ensure you have the following installed:

- Python 3.9+
- Apache Spark
- PySpark

## Usage

### Running the Script

To run the script, use the following command:

```sh
spark-submit prime_number_generator.py <start> <end> <output_dir>
```

For example, to find prime numbers between 10 and 100 and save the results to the `primes` directory:

```sh
spark-submit prime_generator.py 10 100 primes
```

### Example Output

If the command is successful, the script will print the prime numbers in the specified range to the console and save them in a file named `output_primes_<start>_<end>.txt` in the specified output directory.

Example content of `output_primes_1_10.txt`:

```
Prime numbers in range 10 to 100:
2
3
5
7
```

## License

This project is licensed under the MIT License. See the `LICENSE` file for more details.

## Contributing

Contributions are welcome! Please fork the repository and submit a pull request for any improvements or bug fixes.

