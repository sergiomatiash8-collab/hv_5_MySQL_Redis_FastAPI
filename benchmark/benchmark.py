import time
import requests

BASE_URL = "http://127.0.0.1:8000"

ENDPOINTS = [
    f"{BASE_URL}/campaign/387/performance",
    f"{BASE_URL}/advertiser/5/spending",
    f"{BASE_URL}/user/617250/engagements",
]

RUNS = 5


def measure(url: str, runs: int) -> list[float]:
    times = []
    for _ in range(runs):
        start = time.perf_counter()
        requests.get(url)
        elapsed = (time.perf_counter() - start) * 1000
        times.append(round(elapsed, 2))
    return times


def run_benchmark():
    print("\n" + "=" * 60)
    print("BENCHMARK — Redis cache vs MySQL direct")
    print("=" * 60)

    for url in ENDPOINTS:
        endpoint = url.replace(BASE_URL, "")
        print(f"\nEndpoint: {endpoint}")

        # перший запит — завжди іде в MySQL (cold start)
        cold = measure(url, 1)
        print(f"  Cold start (MySQL):  {cold[0]} ms")

        # наступні запити — з кешу Redis
        cached = measure(url, RUNS)
        avg_cached = round(sum(cached) / len(cached), 2)
        print(f"  Cached (Redis) x{RUNS}: {cached}")
        print(f"  Average cached:      {avg_cached} ms")
        print(f"  Speedup:             {round(cold[0] / avg_cached, 1)}x faster")

    print("\n" + "=" * 60)


if __name__ == "__main__":
    run_benchmark()