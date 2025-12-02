import os
import redis
import sys
import json
import time
import threading

r = redis.Redis(
    host=os.getenv('REDIS_HOST', 'localhost'),
    port=6379,
    decode_responses=True,
    socket_keepalive=True,
    socket_connect_timeout=5,
)

def main():
    global scraper_running
    if len(sys.argv) == 5:
        _, root_url, duration_minutes, num_explorers, num_extractors = sys.argv
        duration_minutes = int(duration_minutes)
        num_explorers = int(num_explorers)
        num_extractors = int(num_extractors)
    else:
        print("Usage: python scraper.py <root_url> <duration_minutes> <num_explorers> <num_extractors>")
        print("Example: python scraper.py https://www.dlsu.edu.ph/ 1 16 1")
        sys.exit(1)

    print_header()
    print_config(root_url, duration_minutes * 60, num_explorers, num_extractors)

    # Initialize input
    print("🔄 Initializing Redis...")
    r.flushdb()
    r.delete('explorer_ack')
    r.delete('extractor_ack')
    r.set('num_explorers', num_explorers)
    r.set('num_extractors', num_extractors)
    r.set('stop_signal', '0')
    r.set('duration', duration_minutes * 60)
    r.set('urls_found', 0)
    r.set('urls_extracted', 0)
    r.set('root_url', root_url)
    
    print("📡 Signaling worker nodes to start...")
    r.publish('scraper_control', json.dumps({'command': 'start'}))
    
    print("⏳ Waiting for explorer acknowledgment...", end='', flush=True)
    explorer_ack = r.brpop('explorer_ack', timeout=10)  # Wait up to 10s
    if not explorer_ack:
        print("\n❌ Explorer workers failed to start (timeout)")
        sys.exit(1)
    print(" ✅")
    
    print("⏳ Waiting for extractor acknowledgment...", end='', flush=True)
    extractor_ack = r.brpop('extractor_ack', timeout=10)
    if not extractor_ack:
        print("\n❌ Extractor workers failed to start (timeout)")
        sys.exit(1)
    print(" ✅")

    print("🌱 Seeding initial URL and starting scraper...")
    r.set('start_time', time.time())
    initial_item = json.dumps({'url': root_url, 'html': None})
    r.lpush('explored_queue', initial_item)
    
    print("\n" + "=" * 80)
    print("🚀 SCRAPER RUNNING")
    print("=" * 80 + "\n")

    scraper_running = True
    monitor_thread = threading.Thread(target=monitor_scraper, daemon=True)
    monitor_thread.start()

    broadcast_status()


def monitor_scraper():
    global scraper_running
    try:
        start_time = float(r.get('start_time') or time.time())
        duration = int(r.get('duration'))
        
        while scraper_running:
            elapsed = time.time() - start_time
            remaining = max(0, duration - elapsed)
            
            if remaining <= 0:
                print("⏰ Time's up! Sending stop signal...")
                r.set('stop_signal', '1')
                scraper_running = False
                
                # Collect and save results
                collect_results()
                break
            
            time.sleep(1)
    
    except Exception as e:
        print(f"Monitor error: {e}")
        scraper_running = False

def broadcast_status():
    global scraper_running
    
    try:
        while scraper_running:
            start_time = float(r.get('start_time') or time.time())
            duration = int(r.get('duration') or 60)
            
            elapsed = time.time() - start_time
            remaining = max(0, duration - elapsed)
            
            # Get metrics
            urls_found = int(r.get('urls_found') or 0)
            urls_extracted = int(r.get('urls_extracted') or 0)
            visited_count = r.scard('visited_urls')
            explore_queue_len = r.llen('explored_queue')
            exploit_queue_len = r.llen('exploit_queue')
            
            # Calculate throughput
            throughput = urls_extracted / elapsed if elapsed > 0 else 0
            
            # Print status line
            print_status_line(
                elapsed, remaining,
                urls_found, urls_extracted, visited_count,
                explore_queue_len, exploit_queue_len,
                throughput
            )
            
            time.sleep(0.5)  # Update twice per second
    
    except KeyboardInterrupt:
        print("\n\n⚠️  Manual stop requested")
        scraper_running = False
        r.set('stop_signal', '1')
        r.publish('scraper_control', json.dumps({'command': 'stop'}))
    
    except Exception as e:
        print(f"\n❌ Broadcast error: {e}")
        r.set('stop_signal', '1')
        scraper_running = False

def collect_results():
    """Wait for workers to finish and save results to file"""
    print("\n🔄 Collecting results...")

    
    # Collect final stats
    print("\n" + "=" * 80)
    print("📈 FINAL RESULTS")
    print("=" * 80)
    
    start_time = float(r.get('start_time') or time.time())
    elapsed = time.time() - start_time
    urls_found = int(r.get('urls_found') or 0)
    urls_extracted = int(r.get('urls_extracted') or 0)
    visited_count = r.scard('visited_urls')
    
    throughput = urls_extracted / elapsed if elapsed > 0 else 0
    extraction_rate = (urls_extracted / urls_found * 100) if urls_found > 0 else 0
    
    print(f"\n⏱️  Duration:         {elapsed:.2f}s ({elapsed/60:.2f} minutes)")
    print(f"🔍 URLs Found:       {urls_found}")
    print(f"📝 URLs Extracted:   {urls_extracted}")
    print(f"🌐 Unique Visited:   {visited_count}")
    print(f"⚡ Throughput:       {throughput:.2f} URLs/s")
    print(f"📊 Extraction Rate:  {extraction_rate:.1f}%")
    
    # Get configuration
    num_explorers = int(r.get('num_explorers') or 0)
    num_extractors = int(r.get('num_extractors') or 0)
    root_url = r.get('root_url') or ''
    duration_minutes = int(r.get('duration') or 0) / 60
    
    # Get ALL results from stream
    print("\n📥 Retrieving extracted data from Redis...")
    results_stream = r.xrange('results_stream')
    extracted_data = [{'url': data['url'], 'title': data['title']} for _, data in results_stream]
    print(f"✅ Retrieved {len(extracted_data)} extracted pages")
    
    # Create output directory
    os.makedirs('out', exist_ok=True)
    
    timestamp = int(time.time())
    
    print("\n💾 Saving results...")
    
    # 1. Save CSV file
    csv_filename = f'out/output_exp{num_explorers}_ext_{num_extractors}_{timestamp}.csv'
    try:
        import csv
        with open(csv_filename, 'w', newline='', encoding='utf-8') as f:
            if extracted_data:
                writer = csv.DictWriter(f, fieldnames=['urls', 'title'])
                writer.writeheader()
                for item in extracted_data:
                    writer.writerow({'urls': item['url'], 'title': item['title']})
        print(f"   ✅ CSV:     {csv_filename}")
    except Exception as e:
        print(f"   ❌ CSV failed: {e}")
    
    # 2. Save summary TXT file
    txt_filename = f'out/summary_exp{num_explorers}_ext_{num_extractors}_{timestamp}.txt'
    try:
        with open(txt_filename, 'w', encoding='utf-8') as f:
            f.write(f'Number of URLs found: {urls_found}\n')
            f.write(f'Number of pages extracted: {urls_extracted}\n')
            f.write(f'Throughput: {throughput:.2f}\n')
            f.write(f'Extraction rate: {extraction_rate:.2f}%\n')
            f.write(f'\nURLs accessed:\n')
            for item in extracted_data:
                f.write(f"{item['url']}\n")
        print(f"   ✅ Summary: {txt_filename}")
    except Exception as e:
        print(f"   ❌ Summary failed: {e}")
    
    # 3. Save JSON file
    json_filename = f'out/results_{timestamp}.json'
    results = {
        'duration': elapsed,
        'urls_found': urls_found,
        'urls_extracted': urls_extracted,
        'unique_visited': visited_count,
        'throughput': throughput,
        'extraction_rate': extraction_rate,
        'timestamp': time.strftime("%Y-%m-%d %H:%M:%S"),
        'config': {
            'root_url': root_url,
            'duration': duration_minutes,
            'num_explorers': num_explorers,
            'num_extractors': num_extractors
        },
        'sample_data': extracted_data[:100]  # First 100 samples
    }
    
    try:
        with open(json_filename, 'w', encoding='utf-8') as f:
            json.dump(results, f, indent=2)
        print(f"   ✅ JSON:    {json_filename}")
    except Exception as e:
        print(f"   ❌ JSON failed: {e}")
    
    print("\n" + "=" * 80)
    print("✅ SCRAPER COMPLETED SUCCESSFULLY")
    print("=" * 80 + "\n")


def clear_line():
    """Clear current line in terminal"""
    sys.stdout.write('\r\033[K')
    sys.stdout.flush()

def print_status_line(elapsed, remaining, urls_found, urls_extracted, visited, explore_queue, exploit_queue, throughput):
    """Print single-line status update"""
    clear_line()
    status = (
        f"⏱️  {elapsed:.1f}s / {remaining:.1f}s remaining  |  "
        f"🔍 Found: {urls_found}  |  "
        f"📝 Extracted: {urls_extracted}  |  "
        f"🌐 Visited: {visited}  |  "
        f"⚡ {throughput:.2f} URLs/s  |  "
        f"📥 Queues: [{explore_queue}, {exploit_queue}]"
    )
    sys.stdout.write(status)
    sys.stdout.flush()

def print_header():
    """Print dashboard header"""
    print("\n" + "=" * 80)
    print("🖥️  DISTRIBUTED WEB SCRAPER - TERMINAL DASHBOARD")
    print("=" * 80 + "\n")

def print_config(root_url, duration, num_explorers, num_extractors):
    """Print configuration"""
    print("📋 Configuration:")
    print(f"   Root URL:        {root_url}")
    print(f"   Duration:        {duration / 60:.1f} minutes")
    print(f"   Explorers:       {num_explorers}")
    print(f"   Extractors:      {num_extractors}")
    print("\n" + "-" * 80 + "\n")

if __name__ == '__main__':
    main()
