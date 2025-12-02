import threading
from flask import Flask, jsonify, render_template, request
from flask_socketio import SocketIO, emit
from dotenv import load_dotenv
import redis
import os
import json
import time

load_dotenv()

app = Flask(__name__)
app.config['TEMPLATES_AUTO_RELOAD'] = True
app.config['SECRET_KEY'] = os.getenv('SECRET_KEY', 'dev-secret-key-change-in-production')
socketio = SocketIO(app, cors_allowed_origins="*")

r = redis.Redis(
    host=os.getenv('REDIS_HOST', 'localhost'),
    port=6379,
    decode_responses=True,
    socket_keepalive=True,
    socket_connect_timeout=5,
)

scraper_running = False
pubsub = r.pubsub()
pubsub.subscribe(['explorer_ready', 'extractor_ready'])

def collect_results():
    """Wait for workers to finish and save results to file"""
    print("\n🔄 Collecting results...")
    
    # Collect final stats
    print("\n📈 Final Results:")
    print("=" * 60)
    
    start_time = float(r.get('start_time') or time.time())
    elapsed = time.time() - start_time
    urls_found = int(r.get('urls_found') or 0)
    urls_extracted = int(r.get('urls_extracted') or 0)
    visited_count = r.scard('visited_urls')
    
    print(f"   Duration:       {elapsed:.2f}s")
    print(f"   URLs Found:     {urls_found}")
    print(f"   URLs Extracted: {urls_extracted}")
    print(f"   Unique Visited: {visited_count}")
    print(f"   Throughput:     {urls_extracted/elapsed:.2f} URLs/s" if elapsed > 0 else "   Throughput:     0.00 URLs/s")
    
    # Get configuration
    num_explorers = int(r.get('num_explorers') or 0)
    num_extractors = int(r.get('num_extractors') or 0)
    root_url = r.get('root_url') or ''
    duration_minutes = int(r.get('duration') or 0) / 60
    
    # Get ALL results from stream
    results_stream = r.xrange('results_stream')
    extracted_data = [{'url': data['url'], 'title': data['title']} for _, data in results_stream]
    
    # Create output directory if it doesn't exist
    os.makedirs('out', exist_ok=True)
    
    timestamp = time.time()
    
    # 1. Save CSV file (like your old parallel scraper)
    csv_filename = f'out/output_exp{num_explorers}_ext_{num_extractors}_{timestamp}.csv'
    try:
        import csv
        with open(csv_filename, 'w', newline='', encoding='utf-8') as f:
            if extracted_data:
                writer = csv.DictWriter(f, fieldnames=['urls', 'title'])
                writer.writeheader()
                for item in extracted_data:
                    writer.writerow({'urls': item['url'], 'title': item['title']})
        print(f"\n💾 CSV saved to {csv_filename}")
    except Exception as e:
        print(f"⚠️  Failed to save CSV: {e}")
    
    # 2. Save summary TXT file (like your old parallel scraper)
    txt_filename = f'out/summary_exp{num_explorers}_ext_{num_extractors}_{timestamp}.txt'
    try:
        with open(txt_filename, 'w', encoding='utf-8') as f:
            f.write(f'Number of URLs found: {urls_found}\n')
            f.write(f'Number of pages extracted: {urls_extracted}\n')
            f.write(f'Throughput: {urls_extracted / elapsed if elapsed > 0 else 0}\n')
            f.write(f'Extraction rate: {(urls_extracted / urls_found) if urls_found > 0 else 0}\n')
            f.write(f'\nURLs accessed:\n')
            for item in extracted_data:
                f.write(f"{item['url']}\n")
        print(f"💾 Summary saved to {txt_filename}")
    except Exception as e:
        print(f"⚠️  Failed to save summary: {e}")
    
    # 3. Save JSON file (for detailed analysis)
    json_filename = f'out/results_{int(timestamp)}.json'
    results = {
        'duration': elapsed,
        'urls_found': urls_found,
        'urls_extracted': urls_extracted,
        'unique_visited': visited_count,
        'throughput': urls_extracted / elapsed if elapsed > 0 else 0,
        'extraction_rate': (urls_extracted / urls_found) if urls_found > 0 else 0,
        'timestamp': time.strftime("%Y-%m-%d %H:%M:%S"),
        'config': {
            'root_url': root_url,
            'duration': duration_minutes,
            'num_explorers': num_explorers,
            'num_extractors': num_extractors
        },
        'extracted_data': extracted_data[:100]  # First 100 for JSON
    }
    
    try:
        with open(json_filename, 'w', encoding='utf-8') as f:
            json.dump(results, f, indent=2)
        print(f"💾 JSON saved to {json_filename}")
    except Exception as e:
        print(f"⚠️  Failed to save JSON: {e}")
    
    print("=" * 60 + "\n")
    
    # Return results for WebSocket emission
    return {
        'csv_filename': csv_filename,
        'txt_filename': txt_filename,
        'json_filename': json_filename,
        'results': results
    }

def monitor_scraper():
    """Background thread to monitor and auto-stop scraper"""
    global scraper_running
    
    try:
        start_time = float(r.get('start_time') or time.time())
        duration = int(r.get('duration') or 60)
        
        while scraper_running:
            elapsed = time.time() - start_time
            remaining = max(0, duration - elapsed)
            print('REMAINING', remaining)
            
            if remaining <= 0:
                print("⏰ Time's up! Sending stop signal...")
                r.set('stop_signal', '1')
                scraper_running = False
                
                # Wait a bit for workers to finish processing
                
                # Collect and save results
                results_data = collect_results()
                
                # Emit results via WebSocket
                socketio.emit('scraper_completed', results_data)
                break
            
            time.sleep(1)
    
    except Exception as e:
        print(f"Monitor error: {e}")
        scraper_running = False

def broadcast_status():
    global scraper_running

    while scraper_running:
        
        start_time = float(r.get('start_time'))
        duration = int(r.get('duration'))
        elapsed = time.time() - start_time
        remaining = max(0, duration - elapsed)
        
        urls_found = int(r.get('urls_found'))
        urls_extracted = int(r.get('urls_extracted'))
        visited_count = r.scard('visited_urls')
        explored_queue_len = r.llen('explored_queue')
        exploit_queue_len = r.llen('exploit_queue')

        throughput = urls_extracted / elapsed if elapsed > 0 else 0
        data = {
            'running': scraper_running and remaining > 0,
            'elapsed': round(elapsed, 1),
            'remaining': round(remaining, 1),
            'urls_found': urls_found,
            'urls_extracted': urls_extracted,
            'visited_count': visited_count,
            'explored_queue_len': explored_queue_len,
            'exploit_queue_len': exploit_queue_len,
            'throughput': round(throughput, 2),
            'progress': min(100, (elapsed / duration * 100)) if duration > 0 else 0
        }
        socketio.emit('status_update', data)
        time.sleep(0.1)


@app.route('/')
def index():
    """Main dashboard page"""
    return render_template('index.html')

@app.route('/start', methods=['POST'])
def start_scraper():
    global scraper_running
    data = request.json
    root_url = data.get('root_url', 'https://www.dlsu.edu.ph/')
    duration = int(data.get('duration', 60))  # Duration in minutes
    num_explorers = int(data.get('num_explorers', 4))
    num_extractors = int(data.get('num_extractors', 1))

    # Initialize input
    r.flushdb()
    r.delete('explorer_ack')
    r.delete('extractor_ack')
    r.set('num_explorers', num_explorers)
    r.set('num_extractors', num_extractors)
    r.set('stop_signal', '0')
    r.set('duration', duration * 60)
    r.set('urls_found', 0)
    r.set('urls_extracted', 0)
    r.set('root_url', root_url)
    
    r.publish('scraper_control', json.dumps({'command': 'start'}))
    
    explorer_ack = r.brpop('explorer_ack', timeout=10)  # Wait up to 10s
    # extractor_ack = r.brpop('extractor_ack', timeout=10)
    
    r.set('start_time', time.time())
    initial_item = json.dumps({'url': root_url, 'html': None})
    r.lpush('explored_queue', initial_item)

    scraper_running = True
    monitor_thread = threading.Thread(target=monitor_scraper, daemon=True)
    monitor_thread.start()

    broadcast_thread = threading.Thread(target=broadcast_status, daemon=True)
    broadcast_thread.start()

    return jsonify({
            'success': True,
            'message': f'Scraper started! Explorers: {num_explorers}, Extractors: {num_extractors}, Duration: {duration}m',
            'config': {
                'root_url': root_url,
                'duration': duration,
                'num_explorers': num_explorers,
                'num_extractors': num_extractors
            }
        })

@app.route('/stop', methods=['POST'])
def stop_scraper():
    """Stop the distributed scraper"""
    global scraper_running
    
    try:
        r.set('stop_signal', '1')
        scraper_running = False
        
        # Publish stop command to worker nodes via Pub/Sub
        r.publish('scraper_control', json.dumps({'command': 'stop'}))
        
        # Emit via WebSocket
        socketio.emit('scraper_stopped', {'message': 'Stop signal sent'})
        
        return jsonify({'success': True, 'message': 'Stop signal sent'})
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500


socketio.run(app, host='0.0.0.0', port=5000, debug=True)
