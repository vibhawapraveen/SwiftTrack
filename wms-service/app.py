"""
SwiftLogistics - Warehouse Management System (WMS)
TCP/IP Messaging Protocol with REST API Bridge

This is a mock implementation of the WMS that tracks packages within the warehouse,
from receipt to vehicle loading. It uses a proprietary TCP/IP messaging protocol
for real-time updates.
"""

import os
import json
import uuid
import socket
import threading
from datetime import datetime
from flask import Flask, request, jsonify
import psycopg2
from psycopg2.extras import RealDictCursor
import pika

app = Flask(__name__)

# TCP Server configuration
TCP_HOST = '0.0.0.0'
TCP_PORT = 9000

# Connected TCP clients
tcp_clients = []
tcp_lock = threading.Lock()

# Package statuses
PACKAGE_STATUSES = {
    'RECEIVED': 'Package received at warehouse',
    'PROCESSING': 'Package being processed',
    'READY': 'Package ready for dispatch',
    'LOADING': 'Package being loaded to vehicle',
    'LOADED': 'Package loaded on vehicle',
    'IN_TRANSIT': 'Package in transit',
    'DELIVERED': 'Package delivered',
    'FAILED': 'Delivery failed'
}

# In-memory package storage (simulating real-time warehouse state)
packages = {}
warehouse_locations = {}

# Initialize warehouse locations
for zone in ['A', 'B', 'C', 'D']:
    for rack in range(1, 6):
        for shelf in range(1, 4):
            location = f"{zone}{rack}-{shelf}"
            warehouse_locations[location] = {'occupied': False, 'package_id': None}

def get_db_connection():
    """Get database connection"""
    return psycopg2.connect(
        os.environ.get('DATABASE_URL', 'postgresql://swiftuser:swiftpass@localhost:5432/swiftlogistics'),
        cursor_factory=RealDictCursor
    )

def get_available_location():
    """Get first available warehouse location"""
    for location, data in warehouse_locations.items():
        if not data['occupied']:
            return location
    return None

# RabbitMQ connection
rabbitmq_channel = None
rabbitmq_connection = None

def connect_rabbitmq():
    """Connect to RabbitMQ"""
    global rabbitmq_channel, rabbitmq_connection
    max_retries = 10
    retries = 0
    
    while retries < max_retries:
        try:
            url = os.environ.get('RABBITMQ_URL', 'amqp://swift:logistics123@localhost:5672/')
            parameters = pika.URLParameters(url)
            rabbitmq_connection = pika.BlockingConnection(parameters)
            rabbitmq_channel = rabbitmq_connection.channel()
            
            rabbitmq_channel.queue_declare(queue='order_events', durable=True)
            rabbitmq_channel.queue_declare(queue='wms_events', durable=True)
            rabbitmq_channel.queue_declare(queue='package_events', durable=True)
            
            print("Connected to RabbitMQ")
            return True
        except Exception as e:
            retries += 1
            print(f"RabbitMQ connection attempt {retries}/{max_retries} failed: {e}")
            import time
            time.sleep(5)
    
    print("Failed to connect to RabbitMQ after all retries")
    return False

def publish_event(event_type, data):
    """Publish event to RabbitMQ"""
    global rabbitmq_channel, rabbitmq_connection
    try:
        if rabbitmq_channel is None or rabbitmq_connection.is_closed:
            connect_rabbitmq()
        
        message = json.dumps({
            'event_type': event_type,
            'timestamp': datetime.now().isoformat(),
            'data': data
        })
        
        rabbitmq_channel.basic_publish(
            exchange='',
            routing_key='wms_events',
            body=message,
            properties=pika.BasicProperties(delivery_mode=2)
        )
        print(f"Published event: {event_type}")
    except Exception as e:
        print(f"Failed to publish event: {e}")

def broadcast_tcp_message(message):
    """Broadcast message to all connected TCP clients"""
    with tcp_lock:
        for client in tcp_clients[:]:
            try:
                client.send((json.dumps(message) + '\n').encode())
            except Exception as e:
                print(f"Failed to send to client: {e}")
                tcp_clients.remove(client)

def handle_tcp_client(client_socket, address):
    """Handle individual TCP client connection"""
    print(f"New TCP client connected: {address}")
    with tcp_lock:
        tcp_clients.append(client_socket)
    
    try:
        while True:
            data = client_socket.recv(4096)
            if not data:
                break
            
            try:
                message = json.loads(data.decode().strip())
                response = process_tcp_message(message)
                client_socket.send((json.dumps(response) + '\n').encode())
            except json.JSONDecodeError:
                client_socket.send(json.dumps({
                    'success': False,
                    'error': 'Invalid JSON message'
                }).encode() + b'\n')
    except Exception as e:
        print(f"TCP client error: {e}")
    finally:
        with tcp_lock:
            if client_socket in tcp_clients:
                tcp_clients.remove(client_socket)
        client_socket.close()
        print(f"TCP client disconnected: {address}")

def process_tcp_message(message):
    """Process incoming TCP message using proprietary protocol"""
    msg_type = message.get('type')
    
    if msg_type == 'REGISTER_PACKAGE':
        return register_package(message.get('data', {}))
    elif msg_type == 'UPDATE_STATUS':
        return update_package_status(message.get('package_id'), message.get('status'))
    elif msg_type == 'GET_PACKAGE':
        return get_package(message.get('package_id'))
    elif msg_type == 'GET_ORDER_PACKAGES':
        return get_order_packages(message.get('order_id'))
    elif msg_type == 'SCAN_BARCODE':
        return scan_barcode(message.get('barcode'))
    elif msg_type == 'LOAD_TO_VEHICLE':
        return load_to_vehicle(message.get('package_id'), message.get('driver_id'))
    elif msg_type == 'PING':
        return {'success': True, 'type': 'PONG', 'timestamp': datetime.now().isoformat()}
    else:
        return {'success': False, 'error': f'Unknown message type: {msg_type}'}

def register_package(data):
    """Register a new package in the warehouse"""
    try:
        package_id = f"PKG{datetime.now().strftime('%Y%m%d%H%M%S')}{str(uuid.uuid4())[:4].upper()}"
        barcode = f"BC{str(uuid.uuid4())[:8].upper()}"
        location = get_available_location() or "A1-1"
        
        package = {
            'package_id': package_id,
            'order_id': data.get('order_id'),
            'barcode': barcode,
            'warehouse_location': location,
            'status': 'RECEIVED',
            'received_at': datetime.now().isoformat(),
            'processed_at': None,
            'loaded_at': None,
            'delivered_at': None
        }
        
        packages[package_id] = package
        
        # Update warehouse location
        if location in warehouse_locations:
            warehouse_locations[location] = {'occupied': True, 'package_id': package_id}
        
        # Save to database
        try:
            conn = get_db_connection()
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO packages (package_id, order_id, barcode, warehouse_location, status)
                VALUES (%s, %s, %s, %s, %s)
            """, (package_id, data.get('order_id'), barcode, location, 'received'))
            conn.commit()
            cur.close()
            conn.close()
        except Exception as e:
            print(f"Database error: {e}")
        
        # Broadcast to TCP clients
        broadcast_tcp_message({
            'type': 'PACKAGE_REGISTERED',
            'data': package
        })
        
        # Publish event
        publish_event('PACKAGE_RECEIVED', package)
        
        return {
            'success': True,
            'type': 'PACKAGE_REGISTERED',
            'data': package
        }
    except Exception as e:
        return {'success': False, 'error': str(e)}

def update_package_status(package_id, new_status):
    """Update package status"""
    try:
        if package_id not in packages:
            return {'success': False, 'error': 'Package not found'}
        
        package = packages[package_id]
        old_status = package['status']
        package['status'] = new_status
        
        # Update timestamps
        if new_status == 'PROCESSING':
            package['processed_at'] = datetime.now().isoformat()
        elif new_status in ['LOADED', 'IN_TRANSIT']:
            package['loaded_at'] = datetime.now().isoformat()
        elif new_status == 'DELIVERED':
            package['delivered_at'] = datetime.now().isoformat()
            # Free up warehouse location
            location = package['warehouse_location']
            if location in warehouse_locations:
                warehouse_locations[location] = {'occupied': False, 'package_id': None}
        
        # Update database
        try:
            conn = get_db_connection()
            cur = conn.cursor()
            cur.execute("""
                UPDATE packages SET status = %s, processed_at = %s, loaded_at = %s, delivered_at = %s
                WHERE package_id = %s
            """, (new_status.lower(), package.get('processed_at'), package.get('loaded_at'),
                  package.get('delivered_at'), package_id))
            conn.commit()
            cur.close()
            conn.close()
        except Exception as e:
            print(f"Database error: {e}")
        
        # Broadcast to TCP clients
        broadcast_tcp_message({
            'type': 'STATUS_UPDATED',
            'data': {
                'package_id': package_id,
                'old_status': old_status,
                'new_status': new_status,
                'timestamp': datetime.now().isoformat()
            }
        })
        
        # Publish event
        publish_event('PACKAGE_STATUS_UPDATED', {
            'package_id': package_id,
            'order_id': package['order_id'],
            'old_status': old_status,
            'new_status': new_status
        })
        
        return {
            'success': True,
            'type': 'STATUS_UPDATED',
            'data': package
        }
    except Exception as e:
        return {'success': False, 'error': str(e)}

def get_package(package_id):
    """Get package information"""
    if package_id in packages:
        return {
            'success': True,
            'type': 'PACKAGE_INFO',
            'data': packages[package_id]
        }
    return {'success': False, 'error': 'Package not found'}

def get_order_packages(order_id):
    """Get all packages for an order"""
    order_packages = [p for p in packages.values() if p.get('order_id') == order_id]
    return {
        'success': True,
        'type': 'ORDER_PACKAGES',
        'data': order_packages
    }

def scan_barcode(barcode):
    """Scan barcode and return package info"""
    for package in packages.values():
        if package.get('barcode') == barcode:
            return {
                'success': True,
                'type': 'BARCODE_SCAN_RESULT',
                'data': package
            }
    return {'success': False, 'error': 'Barcode not found'}

def load_to_vehicle(package_id, driver_id):
    """Load package to delivery vehicle"""
    if package_id not in packages:
        return {'success': False, 'error': 'Package not found'}
    
    package = packages[package_id]
    package['status'] = 'LOADED'
    package['loaded_at'] = datetime.now().isoformat()
    package['driver_id'] = driver_id
    
    # Free warehouse location
    location = package['warehouse_location']
    if location in warehouse_locations:
        warehouse_locations[location] = {'occupied': False, 'package_id': None}
    
    # Broadcast and publish
    broadcast_tcp_message({
        'type': 'PACKAGE_LOADED',
        'data': {
            'package_id': package_id,
            'driver_id': driver_id,
            'timestamp': datetime.now().isoformat()
        }
    })
    
    publish_event('PACKAGE_LOADED', {
        'package_id': package_id,
        'order_id': package['order_id'],
        'driver_id': driver_id
    })
    
    return {
        'success': True,
        'type': 'PACKAGE_LOADED',
        'data': package
    }

def start_tcp_server():
    """Start TCP server in a separate thread"""
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((TCP_HOST, TCP_PORT))
    server.listen(5)
    print(f"TCP Server listening on {TCP_HOST}:{TCP_PORT}")
    
    while True:
        client_socket, address = server.accept()
        client_thread = threading.Thread(target=handle_tcp_client, args=(client_socket, address))
        client_thread.daemon = True
        client_thread.start()

def consume_order_events():
    """Consume order events from RabbitMQ"""
    def callback(ch, method, properties, body):
        try:
            event = json.loads(body)
            print(f"Received event: {event.get('event_type')}")
            
            if event.get('event_type') == 'ORDER_CREATED':
                # Auto-register package when order is created
                data = event.get('data', {})
                result = register_package({
                    'order_id': data.get('order_id')
                })
                
                # Update transaction log
                if result.get('success'):
                    publish_event('WMS_PROCESSING_COMPLETE', {
                        'order_id': data.get('order_id'),
                        'transaction_id': data.get('transaction_id'),
                        'package_id': result['data']['package_id']
                    })
        except Exception as e:
            print(f"Error processing event: {e}")
    
    while True:
        try:
            if connect_rabbitmq():
                rabbitmq_channel.basic_consume(
                    queue='order_events',
                    on_message_callback=callback,
                    auto_ack=True
                )
                print("Starting to consume order events...")
                rabbitmq_channel.start_consuming()
        except Exception as e:
            print(f"RabbitMQ consumer error: {e}")
            import time
            time.sleep(5)

# REST API Endpoints for API Gateway communication

@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({
        'status': 'healthy',
        'service': 'WMS',
        'tcp_port': TCP_PORT,
        'timestamp': datetime.now().isoformat()
    })

@app.route('/api/packages', methods=['POST'])
def create_package():
    """REST endpoint to register a package"""
    data = request.json
    result = register_package(data)
    return jsonify(result)

@app.route('/api/packages/<package_id>', methods=['GET'])
def get_package_rest(package_id):
    """REST endpoint to get package info"""
    result = get_package(package_id)
    if result.get('success'):
        return jsonify(result)
    return jsonify(result), 404

@app.route('/api/packages/<package_id>/status', methods=['PUT'])
def update_status_rest(package_id):
    """REST endpoint to update package status"""
    data = request.json
    result = update_package_status(package_id, data.get('status'))
    return jsonify(result)

@app.route('/api/packages/order/<order_id>', methods=['GET'])
def get_order_packages_rest(order_id):
    """REST endpoint to get packages for an order"""
    result = get_order_packages(order_id)
    return jsonify(result)

@app.route('/api/packages/<package_id>/load', methods=['POST'])
def load_package_rest(package_id):
    """REST endpoint to load package to vehicle"""
    data = request.json
    result = load_to_vehicle(package_id, data.get('driver_id'))
    return jsonify(result)

@app.route('/api/packages/scan/<barcode>', methods=['GET'])
def scan_barcode_rest(barcode):
    """REST endpoint to scan barcode"""
    result = scan_barcode(barcode)
    if result.get('success'):
        return jsonify(result)
    return jsonify(result), 404

@app.route('/api/warehouse/locations', methods=['GET'])
def get_warehouse_locations():
    """Get all warehouse locations and their status"""
    return jsonify({
        'success': True,
        'locations': warehouse_locations
    })

@app.route('/api/packages', methods=['GET'])
def get_all_packages():
    """Get all packages in the warehouse"""
    return jsonify({
        'success': True,
        'packages': list(packages.values())
    })

if __name__ == '__main__':
    # Start TCP server in background thread
    tcp_thread = threading.Thread(target=start_tcp_server)
    tcp_thread.daemon = True
    tcp_thread.start()
    
    # Start RabbitMQ consumer in background thread
    consumer_thread = threading.Thread(target=consume_order_events)
    consumer_thread.daemon = True
    consumer_thread.start()
    
    print(f"Starting WMS Service on port 8003 (HTTP) and {TCP_PORT} (TCP)...")
    app.run(host='0.0.0.0', port=8003, debug=False, threaded=True)
