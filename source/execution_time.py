import time

def timing(func):
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        execution_time = end_time - start_time
        print(f"Execution time: {execution_time} seconds")
        return result
    return wrapper

if __name__ == "__main__":
    @timing
    def test_timing():
        time.sleep(1)
        print("test")

    test_timing()