from multiprocessing import Process
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup
from dotenv import load_dotenv
import redis
import os
import time
import json
import signal

import requests

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

def main():

    pubsub = r.pubsub()
    pubsub.subscribe('scraper_control')

    active_processes = []

    print("🎯 Explorer Node - Waiting for client to start scraping...")
    while not should_stop:

        message = pubsub.get_message()
        if message and message['type'] == 'message':
            print(message)
            try: 
                data = json.loads(message['data'])
                command = data['command']
                print(command)
                if command == 'start':
                    num_workers = int(r.get('num_explorers') or 4)
                    print(f"\n🚀 Starting {num_workers} explorer workers...")
                    for _ in range(num_workers):
                        p = Process(target=explorer_worker)
                        p.start()
                        active_processes.append(p)

                    r.lpush('explorer_ack', json.dumps({
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
                        print("\n🛑 Stopping all explorer workers...")
                        for p in active_processes:
                            if p.is_alive():
                                p.terminate()
                                p.join(timeout=5)
                        active_processes.clear()
                        print("✅ All explorer workers stopped")
            except KeyboardInterrupt:
                print("\n⚠️  Shutting down explorer node...")
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

def explorer_worker():
    worker_id = os.getpid()
    root_url = r.get('root_url')
    print(f"🔍 [Explorer-{worker_id}] Started and waiting for jobs...")
    
    try:
        # Test Redis connection
        r.ping()
    except redis.ConnectionError as e:
        print(f"❌ [Explorer-{worker_id}] Cannot connect to Redis: {e}")
        return
    
    while not should_stop:

        urls_visited = 0

        result = r.brpop('explored_queue', timeout=1)
        if not result:
            continue

        # Parse queue item
        queue_name, item_json = result
        item = json.loads(item_json)
        url = item['url']

        # Deduplication
        if r.sadd('visited_urls', url) == 0:
            continue  # Already processed by another worker

        print(f"🔍 [Explorer-{worker_id}] Exploring: {url}")

        html, new_links = explore_url(worker_id, url)
        pipe = r.pipeline()
        new_links_count = 0

        for link in new_links:
            if r.sismember('visited_urls', link):
                continue

            new_item = json.dumps({'url': link, 'html': None})
            pipe.lpush('explored_queue', new_item)
            new_links_count += 1
        
        exploit_item = json.dumps({'url': url, 'html': html})
        pipe.lpush('exploit_queue', exploit_item)
        pipe.incr('urls_found')
        
        pipe.execute()
        urls_visited += 1

        if urls_visited % 10 == 0:
            print(f"📊 [Explorer-{worker_id}] Visited {urls_visited} URLs, discovered {new_links_count} new links")
        
        if r.get('stop_signal') == '1':
            print(f"🛑 [Explorer-{worker_id}] Job stopped after processing {urls_visited} URLs")
            break

def explore_url(worker_id, url):
    """Fetch URL and discover new links"""
    try:
        # Fetch HTML
        response = requests.get(url, timeout=10, allow_redirects=True)
        response.raise_for_status()
        html = response.text
        
        # Parse and find links
        soup = BeautifulSoup(html, 'html.parser')
        links = []
        
        for anchor in soup.find_all('a', href=True):
            href = anchor['href']
            absolute_url = urljoin(url, href)
            
            # Validate URL
            if is_valid_url(absolute_url, url):
                links.append(absolute_url)
        
        return html, links
    
    except requests.RequestException as e:
        print(f"⚠️  [Explorer-{worker_id}] Error fetching {url}: {e}")
        return None, []
    except Exception as e:
        print(f"⚠️  [Explorer-{worker_id}] Unexpected error: {e}")
        return None, []

def is_valid_url(url, base_url):
    """Check if URL is valid and belongs to same domain"""
    try:
        parsed = urlparse(url)
        base_parsed = urlparse(base_url)
        
        # Must have a scheme (reject bare domains like www.dlsu.edu.ph)
        if not parsed.scheme:
            return False
        
        # Must be http/https
        if parsed.scheme not in ['http', 'https']:
            return False
        
        # Must have a network location
        if not parsed.netloc:
            return False
        
        # Must be same domain
        if parsed.netloc != base_parsed.netloc:
            return False
        
        # Skip common non-html files
        skip_extensions = ['.pdf', '.jpg', '.png', '.gif', '.zip', '.mp4', '.css', '.js']
        if any(url.lower().endswith(ext) for ext in skip_extensions):
            return False
        
        return True
    except:
        return False

if __name__ == '__main__':
    main()
