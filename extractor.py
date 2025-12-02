import redis
import os
import sys
import time
import json
import signal
from dotenv import load_dotenv
from bs4 import BeautifulSoup
from multiprocessing import Process

# Load environment
load_dotenv()

redis_pool = redis.ConnectionPool(
    host=os.getenv('REDIS_HOST', 'localhost'),
    port=6379,
    decode_responses=True,
    socket_keepalive=True,
    socket_connect_timeout=5,
    max_connections=50
)

r = redis.Redis(connection_pool=redis_pool, retry_on_timeout=True)
should_stop = False

def signal_handler(sig, frame):
    global should_stop
    print(f"Shutdown signal received")
    should_stop = True

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


def extract_title(html):
    """Extract page title from HTML"""
    try:
        soup = BeautifulSoup(html, 'html.parser')
        
        # Try to find title tag
        title_tag = soup.find('title')
        if title_tag and title_tag.string:
            return title_tag.string.strip()
        
        # Fallback: try h1
        h1_tag = soup.find('h1')
        if h1_tag:
            return h1_tag.get_text().strip()
        
        return "No title found"
    
    except Exception as e:
        # Note: worker_id not available in this function, use generic message
        return "Error extracting title"
    
def extractor_worker():
    worker_id = os.getpid()
    print(f"📝 [Extractor-{worker_id}] Started and waiting for jobs...")
    
    try:
        # Test Redis connection
        r.ping()
    except redis.ConnectionError as e:
        print(f"❌ [Explorer-{worker_id}] Cannot connect to Redis: {e}")
        return
    
    urls_processed = 0  # Reset for each job
    while not should_stop:
        try:
            result = r.brpop('exploit_queue', timeout=1)
            if not result:
                continue

            # Parse queue item
            queue_name, item_json = result
            item = json.loads(item_json)
            url = item['url']
            html = item['html']

            print(f"📝 [Extractor-{worker_id}] Extracting: {url}")

            # Extract title
            title = extract_title(html)
            result_data = {
                'url': url,
                'title': title,
                'timestamp': str(time.time()),  # Convert to string for Redis Stream
                'worker_id': str(worker_id)
            }

            pipe = r.pipeline()
            pipe.xadd('results_stream', result_data)
            pipe.incr('urls_extracted')
            pipe.execute()

            urls_processed += 1
                    
            if urls_processed % 10 == 0:
                print(f"📊 [Extractor-{worker_id}] Extracted {urls_processed} titles")

        except redis.ConnectionError as e:
            print(f"⚠️  [Extractor-{worker_id}] Redis connection lost: {e}")
            time.sleep(2)  # Wait before retry
            continue
        
        except json.JSONDecodeError as e:
            print(f"⚠️  [Extractor-{worker_id}] Invalid JSON in queue: {e}")
            continue
        
        except Exception as e:
            print(f"❌ [Extractor-{worker_id}] Unexpected error: {e}")
            continue
        
        if r.get('stop_signal') == '1':
            print(f"🛑 [Extractor-{worker_id}] Job stopped after extracting {urls_processed} titles")
            break
        
if __name__ == '__main__':
    

    pubsub = r.pubsub()
    pubsub.subscribe('scraper_control')

    active_processes = []

    print("🎯 Extractor Node - Waiting for client to start scraping...")

    while not should_stop:

        message = pubsub.get_message()
        if message and message['type'] == 'message':
            print(message)
            try: 
                data = json.loads(message['data'])
                command = data['command']
                print(command)
                if command == 'start':
                    num_workers = int(r.get('num_extractors') or 4)
                    print(f"\n🚀 Starting {num_workers} extractor workers...")
                    for _ in range(num_workers):
                        p = Process(target=extractor_worker)
                        p.start()
                        active_processes.append(p)

                    r.lpush('extractor_ack', json.dumps({
                        'status': 'ready',
                        'num_workers': num_workers,
                        'timestamp': time.time()
                    }))
                    
                    p: Process
                    for p in active_processes:
                        if p.is_alive():
                            p.join()
                    active_processes.clear()
                elif command == 'stop':
                        print("\n🛑 Stopping all extractor workers...")
                        for p in active_processes:
                            if p.is_alive():
                                p.terminate()
                                p.join(timeout=5)
                        active_processes.clear()
                        print("✅ All extractor workers stopped")
            except KeyboardInterrupt:
                print("\n⚠️  Shutting down extractor node...")
                for p in active_processes:
                    if p.is_alive():
                        p.terminate()
                        p.join(timeout=2)
            except Exception as e:
                pass
            finally:
                for p in active_processes:
                    if p.is_alive():
                        p.terminate()
                        p.join(timeout=2)
                active_processes.clear()
        
        time.sleep(0.1)  # Check for messages every 100ms
