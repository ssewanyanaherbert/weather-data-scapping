import multiprocessing
import time
import sys
from producer.weather_producer import produce_weather_data
from consumer.weather_consumer import consume_weather_data


def start_process(target, name):
    """Create and start a named process."""
    p = multiprocessing.Process(target=target, name=name)
    p.start()
    print(f"✅ {name} started (pid={p.pid})")
    return p


def shutdown(producer_process, consumer_process):
    """Gracefully terminate both processes."""
    print("\n🛑 Shutting down pipeline...")
    for p in (producer_process, consumer_process):
        if p.is_alive():
            p.terminate()
            p.join(timeout=5)
            if p.is_alive():                # still alive after 5s → force kill
                print(f"⚠️  Force-killing {p.name}")
                p.kill()
                p.join()
    print("👋 Pipeline stopped cleanly.")
    sys.exit(0)


def start_pipeline():
    """Run producer and consumer concurrently with auto-restart."""
    print("🚀 Starting Weather Data Pipeline...")

    producer_process = start_process(produce_weather_data, "WeatherProducer")
    consumer_process = start_process(consume_weather_data, "WeatherConsumer")

    print("⏳ Pipeline running — press Ctrl+C to stop.\n")

    try:
        while True:
            time.sleep(5)

            if not producer_process.is_alive():
                print("❌ WeatherProducer died — restarting...")
                producer_process = start_process(produce_weather_data, "WeatherProducer")

            if not consumer_process.is_alive():
                print("❌ WeatherConsumer died — restarting...")
                consumer_process = start_process(consume_weather_data, "WeatherConsumer")

    except KeyboardInterrupt:
        shutdown(producer_process, consumer_process)


if __name__ == "__main__":
    multiprocessing.freeze_support()   # needed for Windows .exe packaging
    start_pipeline()