import multiprocessing
import time
import signal
import sys
from producer.weather_producer import produce_weather_data
from consumer.weather_consumer import consume_weather_data

def signal_handler(sig, frame):
    """Handle Ctrl+C gracefully."""
    print("\n🛑 Shutting down pipeline...")
    sys.exit(0)

def start_pipeline():
    """Run producer and consumer concurrently."""
    print("🚀 Starting Weather Data Pipeline...")
    
    producer_process = multiprocessing.Process(target=produce_weather_data, name="WeatherProducer")
    consumer_process = multiprocessing.Process(target=consume_weather_data, name="WeatherConsumer")

    print("📡 Starting producer...")
    producer_process.start()
    
    print("🎧 Starting consumer...")
    consumer_process.start()

    print("✅ Pipeline running! Press Ctrl+C to stop.")
    
    try:
        # Monitor processes
        while True:
            if not producer_process.is_alive():
                print("❌ Producer died, restarting...")
                producer_process = multiprocessing.Process(target=produce_weather_data)
                producer_process.start()
                
            if not consumer_process.is_alive():
                print("❌ Consumer died, restarting...")
                consumer_process = multiprocessing.Process(target=consume_weather_data)
                consumer_process.start()
                
            time.sleep(5)  # Check every 5 seconds
            
    except KeyboardInterrupt:
        print("\n🛑 Stopping pipeline...")
        producer_process.terminate()
        consumer_process.terminate()
        producer_process.join()
        consumer_process.join()
