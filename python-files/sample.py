# Sample Python File
# This file demonstrates basic Python programming concepts

def fibonacci(n):
    """
    Generate Fibonacci sequence up to n numbers.
    
    Args:
        n: Number of Fibonacci numbers to generate
        
    Returns:
        List of Fibonacci numbers
    """
    if n <= 0:
        return []
    elif n == 1:
        return [0]
    
    sequence = [0, 1]
    while len(sequence) < n:
        next_num = sequence[-1] + sequence[-2]
        sequence.append(next_num)
    
    return sequence


def is_prime(num):
    """Check if a number is prime."""
    if num < 2:
        return False
    for i in range(2, int(num ** 0.5) + 1):
        if num % i == 0:
            return False
    return True


class DataProcessor:
    """A class for processing data."""
    
    def __init__(self, data):
        self.data = data
    
    def calculate_mean(self):
        """Calculate the mean of the data."""
        if not self.data:
            return 0
        return sum(self.data) / len(self.data)
    
    def calculate_median(self):
        """Calculate the median of the data."""
        if not self.data:
            return 0
        sorted_data = sorted(self.data)
        n = len(sorted_data)
        mid = n // 2
        if n % 2 == 0:
            return (sorted_data[mid - 1] + sorted_data[mid]) / 2
        return sorted_data[mid]


if __name__ == "__main__":
    # Test Fibonacci
    print("Fibonacci sequence:", fibonacci(10))
    
    # Test prime check
    primes = [n for n in range(2, 50) if is_prime(n)]
    print("Prime numbers up to 50:", primes)
    
    # Test DataProcessor
    processor = DataProcessor([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
    print(f"Mean: {processor.calculate_mean()}")
    print(f"Median: {processor.calculate_median()}")
