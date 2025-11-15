import json
import time
import re
import subprocess
from flask import Flask, render_template, jsonify
from flask_cors import CORS
import threading

app = Flask(__name__)
CORS(app)

metrics = {
    'total_orders': 0,
    'running_average': 0.0,
    'success_count': 0,
    'retry_count': 0,
    'dlq_count': 0,
    'recent_prices': [],
    'total_price_sum': 0.0,
    'last_updated': None
}

class LogParser:
    def __init__(self):
        self.running = True
        
    def parse_consumer_logs(self):
        while self.running:
            try:
                result = subprocess.run([
                    'docker', 'logs', '--tail', '100', 'order-consumer'
                ], capture_output=True, text=True, timeout=10)
                
                if result.returncode == 0:
                    self.process_logs(result.stdout)
                
            except Exception as e:
                print(f"Error reading logs: {e}")
            
            time.sleep(3)  

    def process_logs(self, log_output):
        lines = log_output.split('\n')
        
        current_total = 0
        current_success = 0
        current_retry = 0
        current_dlq = 0
        prices = []
        total_sum = 0.0
        
        for line in lines:
            if not line.strip():
                continue
                
            if 'Processed order:' in line:
                current_total += 1
                current_success += 1
                
                price_match = re.search(r'Price: \$([\d.]+)', line)
                if price_match:
                    price = float(price_match.group(1))
                    prices.append(price)
                    total_sum += price
            
            avg_match = re.search(r'Running average price: \$([\d.]+)', line)
            if avg_match:
                metrics['running_average'] = float(avg_match.group(1))
            
            if 'Sent to retry topic' in line:
                current_retry += 1
            
            if 'Sent to DLQ' in line:
                current_dlq += 1
        
        if current_total > 0:
            metrics['total_orders'] = current_total
            metrics['success_count'] = current_success
            metrics['retry_count'] = current_retry
            metrics['dlq_count'] = current_dlq
            metrics['total_price_sum'] = total_sum
            
            metrics['recent_prices'] = prices[-20:]  
            
            metrics['last_updated'] = time.time()

    def stop(self):
        self.running = False

log_parser = LogParser()
thread = threading.Thread(target=log_parser.parse_consumer_logs)
thread.daemon = True
thread.start()

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/health')
def health_check():
    return jsonify({
        'status': 'healthy',
        'timestamp': time.time(),
        'message': 'Dashboard is running and parsing logs'
    })

@app.route('/metrics')
def get_metrics():
    total_processed = metrics['success_count'] + metrics['dlq_count']
    success_rate = (metrics['success_count'] / total_processed * 100) if total_processed > 0 else 0
    
    return jsonify({
        'total_orders': metrics['total_orders'],
        'running_average': round(metrics['running_average'], 2),
        'success_count': metrics['success_count'],
        'retry_count': metrics['retry_count'],
        'dlq_count': metrics['dlq_count'],
        'recent_prices': metrics['recent_prices'],
        'success_rate': round(success_rate, 1),
        'last_updated': metrics['last_updated']
    })

if __name__ == '__main__':
    print("🚀 Starting Kafka Order Dashboard (Log Parser Mode)...")
    app.run(host='0.0.0.0', port=5000, debug=False)
