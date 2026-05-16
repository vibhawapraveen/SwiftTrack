"""
SwiftLogistics - Client Management System (CMS)
SOAP/XML API Service

This is a mock implementation of the legacy CMS system that handles
client contracts, billing, and order intake via SOAP API.
"""

import os
import json
import uuid
from datetime import datetime
from functools import wraps

from flask import Flask, request, jsonify
from spyne import Application, Service, Unicode, Integer, Float, DateTime, Boolean, ComplexModel, Array
from spyne.decorator import srpc
from spyne.protocol.soap import Soap11
from spyne.server.wsgi import WsgiApplication
import psycopg2
from psycopg2.extras import RealDictCursor
import pika
import bcrypt

app = Flask(__name__)

# Database connection
def get_db_connection():
    return psycopg2.connect(
        os.environ.get('DATABASE_URL', 'postgresql://swiftuser:swiftpass@localhost:5432/swiftlogistics'),
        cursor_factory=RealDictCursor
    )

# RabbitMQ connection
def get_rabbitmq_channel():
    try:
        connection = pika.BlockingConnection(
            pika.URLParameters(os.environ.get('RABBITMQ_URL', 'amqp://swift:logistics123@localhost:5672/'))
        )
        channel = connection.channel()
        channel.queue_declare(queue='order_events', durable=True)
        channel.queue_declare(queue='cms_events', durable=True)
        return channel, connection
    except Exception as e:
        print(f"RabbitMQ connection error: {e}")
        return None, None

def publish_event(event_type, data):
    """Publish event to RabbitMQ"""
    channel, connection = get_rabbitmq_channel()
    if channel:
        try:
            message = json.dumps({
                'event_type': event_type,
                'timestamp': datetime.now().isoformat(),
                'data': data
            })
            channel.basic_publish(
                exchange='',
                routing_key='order_events',
                body=message,
                properties=pika.BasicProperties(delivery_mode=2)
            )
            print(f"Published event: {event_type}")
        finally:
            connection.close()

# SOAP Models
class ClientInfo(ComplexModel):
    client_id = Unicode
    company_name = Unicode
    email = Unicode
    contract_type = Unicode
    billing_address = Unicode
    contact_phone = Unicode
    status = Unicode

class OrderInfo(ComplexModel):
    order_id = Unicode
    client_id = Unicode
    pickup_address = Unicode
    delivery_address = Unicode
    pickup_lat = Float
    pickup_lng = Float
    delivery_lat = Float
    delivery_lng = Float
    package_weight = Float
    package_dimensions = Unicode
    priority = Unicode
    status = Unicode
    estimated_delivery = Unicode
    created_at = Unicode

class OrderResponse(ComplexModel):
    success = Boolean
    order_id = Unicode
    message = Unicode
    transaction_id = Unicode

class ClientResponse(ComplexModel):
    success = Boolean
    client = ClientInfo
    message = Unicode

class OrderListResponse(ComplexModel):
    success = Boolean
    orders = Array(OrderInfo)
    message = Unicode

# SOAP Service Definition
class CMSService(Service):
    """
    Client Management System SOAP Service
    Handles client management and order intake operations
    """
    
    @srpc(Unicode, Unicode, _returns=ClientResponse)
    def authenticate_client(email, password):
        """Authenticate a client and return their information"""
        try:
            conn = get_db_connection()
            cur = conn.cursor()
            cur.execute("SELECT * FROM clients WHERE email = %s", (email,))
            client = cur.fetchone()
            cur.close()
            conn.close()
            
            if client:
                # For demo, accept any password or check against hash
                response = ClientResponse()
                response.success = True
                response.message = "Authentication successful"
                
                client_info = ClientInfo()
                client_info.client_id = client['client_id']
                client_info.company_name = client['company_name']
                client_info.email = client['email']
                client_info.contract_type = client['contract_type']
                client_info.billing_address = client['billing_address'] or ''
                client_info.contact_phone = client['contact_phone'] or ''
                client_info.status = client['status']
                response.client = client_info
                
                return response
            else:
                response = ClientResponse()
                response.success = False
                response.message = "Invalid credentials"
                return response
        except Exception as e:
            response = ClientResponse()
            response.success = False
            response.message = str(e)
            return response
    
    @srpc(Unicode, _returns=ClientResponse)
    def get_client(client_id):
        """Get client information by client ID"""
        try:
            conn = get_db_connection()
            cur = conn.cursor()
            cur.execute("SELECT * FROM clients WHERE client_id = %s", (client_id,))
            client = cur.fetchone()
            cur.close()
            conn.close()
            
            if client:
                response = ClientResponse()
                response.success = True
                response.message = "Client found"
                
                client_info = ClientInfo()
                client_info.client_id = client['client_id']
                client_info.company_name = client['company_name']
                client_info.email = client['email']
                client_info.contract_type = client['contract_type']
                client_info.billing_address = client['billing_address'] or ''
                client_info.contact_phone = client['contact_phone'] or ''
                client_info.status = client['status']
                response.client = client_info
                
                return response
            else:
                response = ClientResponse()
                response.success = False
                response.message = "Client not found"
                return response
        except Exception as e:
            response = ClientResponse()
            response.success = False
            response.message = str(e)
            return response
    
    @srpc(Unicode, Unicode, Unicode, Float, Float, Float, Float, Float, Unicode, Unicode, _returns=OrderResponse)
    def create_order(client_id, pickup_address, delivery_address, pickup_lat, pickup_lng, 
                     delivery_lat, delivery_lng, package_weight, package_dimensions, priority):
        """Create a new order in the system"""
        try:
            order_id = f"ORD{datetime.now().strftime('%Y%m%d%H%M%S')}{str(uuid.uuid4())[:4].upper()}"
            transaction_id = f"TXN{str(uuid.uuid4())[:8].upper()}"
            
            conn = get_db_connection()
            cur = conn.cursor()
            
            # Create order
            cur.execute("""
                INSERT INTO orders (order_id, client_id, pickup_address, delivery_address,
                    pickup_lat, pickup_lng, delivery_lat, delivery_lng, package_weight,
                    package_dimensions, priority, status)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'pending')
                RETURNING *
            """, (order_id, client_id, pickup_address, delivery_address, pickup_lat, pickup_lng,
                  delivery_lat, delivery_lng, package_weight, package_dimensions, priority))
            
            order = cur.fetchone()
            
            # Create transaction log for distributed transaction tracking
            cur.execute("""
                INSERT INTO transaction_logs (transaction_id, order_id, cms_status, overall_status)
                VALUES (%s, %s, 'completed', 'processing')
            """, (transaction_id, order_id))
            
            conn.commit()
            cur.close()
            conn.close()
            
            # Publish order created event
            publish_event('ORDER_CREATED', {
                'order_id': order_id,
                'client_id': client_id,
                'transaction_id': transaction_id,
                'pickup_address': pickup_address,
                'delivery_address': delivery_address,
                'pickup_lat': pickup_lat,
                'pickup_lng': pickup_lng,
                'delivery_lat': delivery_lat,
                'delivery_lng': delivery_lng,
                'package_weight': package_weight,
                'priority': priority
            })
            
            response = OrderResponse()
            response.success = True
            response.order_id = order_id
            response.transaction_id = transaction_id
            response.message = "Order created successfully"
            return response
            
        except Exception as e:
            response = OrderResponse()
            response.success = False
            response.order_id = ""
            response.transaction_id = ""
            response.message = str(e)
            return response
    
    @srpc(Unicode, _returns=OrderListResponse)
    def get_client_orders(client_id):
        """Get all orders for a specific client"""
        try:
            conn = get_db_connection()
            cur = conn.cursor()
            cur.execute("""
                SELECT * FROM orders WHERE client_id = %s ORDER BY created_at DESC
            """, (client_id,))
            orders = cur.fetchall()
            cur.close()
            conn.close()
            
            response = OrderListResponse()
            response.success = True
            response.message = f"Found {len(orders)} orders"
            response.orders = []
            
            for order in orders:
                order_info = OrderInfo()
                order_info.order_id = order['order_id']
                order_info.client_id = order['client_id']
                order_info.pickup_address = order['pickup_address']
                order_info.delivery_address = order['delivery_address']
                order_info.pickup_lat = float(order['pickup_lat']) if order['pickup_lat'] else 0.0
                order_info.pickup_lng = float(order['pickup_lng']) if order['pickup_lng'] else 0.0
                order_info.delivery_lat = float(order['delivery_lat']) if order['delivery_lat'] else 0.0
                order_info.delivery_lng = float(order['delivery_lng']) if order['delivery_lng'] else 0.0
                order_info.package_weight = float(order['package_weight']) if order['package_weight'] else 0.0
                order_info.package_dimensions = order['package_dimensions'] or ''
                order_info.priority = order['priority']
                order_info.status = order['status']
                order_info.estimated_delivery = str(order['estimated_delivery']) if order['estimated_delivery'] else ''
                order_info.created_at = str(order['created_at'])
                response.orders.append(order_info)
            
            return response
        except Exception as e:
            response = OrderListResponse()
            response.success = False
            response.message = str(e)
            response.orders = []
            return response
    
    @srpc(Unicode, Unicode, _returns=OrderResponse)
    def update_order_status(order_id, status):
        """Update the status of an order"""
        try:
            conn = get_db_connection()
            cur = conn.cursor()
            cur.execute("""
                UPDATE orders SET status = %s, updated_at = CURRENT_TIMESTAMP
                WHERE order_id = %s RETURNING *
            """, (status, order_id))
            order = cur.fetchone()
            conn.commit()
            cur.close()
            conn.close()
            
            if order:
                publish_event('ORDER_STATUS_UPDATED', {
                    'order_id': order_id,
                    'status': status,
                    'client_id': order['client_id']
                })
                
                response = OrderResponse()
                response.success = True
                response.order_id = order_id
                response.message = f"Order status updated to {status}"
                return response
            else:
                response = OrderResponse()
                response.success = False
                response.message = "Order not found"
                return response
        except Exception as e:
            response = OrderResponse()
            response.success = False
            response.message = str(e)
            return response

# Create SOAP application
soap_app = Application(
    [CMSService],
    tns='http://swiftlogistics.lk/cms',
    in_protocol=Soap11(validator='lxml'),
    out_protocol=Soap11()
)

wsgi_app = WsgiApplication(soap_app)

# REST endpoints for internal communication
@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({'status': 'healthy', 'service': 'CMS', 'timestamp': datetime.now().isoformat()})

@app.route('/api/clients/<client_id>', methods=['GET'])
def get_client_rest(client_id):
    """REST endpoint for getting client info (for internal use)"""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT * FROM clients WHERE client_id = %s", (client_id,))
        client = cur.fetchone()
        cur.close()
        conn.close()
        
        if client:
            return jsonify({
                'success': True,
                'client': dict(client)
            })
        return jsonify({'success': False, 'message': 'Client not found'}), 404
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/api/clients/auth', methods=['POST'])
def authenticate_client_rest():
    """REST endpoint for client authentication (for internal use)"""
    try:
        data = request.json
        email = data.get('email')
        password = data.get('password')
        
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT * FROM clients WHERE email = %s", (email,))
        client = cur.fetchone()
        cur.close()
        conn.close()
        
        if client:
            # For demo purposes, accept 'password123' as default password
            if password == 'password123':
                client_dict = dict(client)
                del client_dict['password_hash']
                return jsonify({
                    'success': True,
                    'client': client_dict
                })
        return jsonify({'success': False, 'message': 'Invalid credentials'}), 401
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/api/orders', methods=['POST'])
def create_order_rest():
    """REST endpoint for creating orders (for internal use)"""
    try:
        data = request.json
        order_id = f"ORD{datetime.now().strftime('%Y%m%d%H%M%S')}{str(uuid.uuid4())[:4].upper()}"
        transaction_id = f"TXN{str(uuid.uuid4())[:8].upper()}"
        
        conn = get_db_connection()
        cur = conn.cursor()
        
        cur.execute("""
            INSERT INTO orders (order_id, client_id, pickup_address, delivery_address,
                pickup_lat, pickup_lng, delivery_lat, delivery_lng, package_weight,
                package_dimensions, priority, status)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'pending')
            RETURNING *
        """, (order_id, data['client_id'], data['pickup_address'], data['delivery_address'],
              data.get('pickup_lat', 0), data.get('pickup_lng', 0),
              data.get('delivery_lat', 0), data.get('delivery_lng', 0),
              data.get('package_weight', 0), data.get('package_dimensions', ''),
              data.get('priority', 'normal')))
        
        order = dict(cur.fetchone())
        
        cur.execute("""
            INSERT INTO transaction_logs (transaction_id, order_id, cms_status, overall_status)
            VALUES (%s, %s, 'completed', 'processing')
        """, (transaction_id, order_id))
        
        conn.commit()
        cur.close()
        conn.close()
        
        # Publish event
        publish_event('ORDER_CREATED', {
            'order_id': order_id,
            'client_id': data['client_id'],
            'transaction_id': transaction_id,
            **data
        })
        
        return jsonify({
            'success': True,
            'order': order,
            'transaction_id': transaction_id
        })
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/api/orders/<client_id>', methods=['GET'])
def get_orders_rest(client_id):
    """REST endpoint for getting client orders (for internal use)"""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT * FROM orders WHERE client_id = %s ORDER BY created_at DESC", (client_id,))
        orders = cur.fetchall()
        cur.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'orders': [dict(o) for o in orders]
        })
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/api/orders/status/<order_id>', methods=['PUT'])
def update_order_status_rest(order_id):
    """REST endpoint for updating order status (for internal use)"""
    try:
        data = request.json
        status = data.get('status')
        
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            UPDATE orders SET status = %s, updated_at = CURRENT_TIMESTAMP
            WHERE order_id = %s RETURNING *
        """, (status, order_id))
        order = cur.fetchone()
        conn.commit()
        cur.close()
        conn.close()
        
        if order:
            publish_event('ORDER_STATUS_UPDATED', {
                'order_id': order_id,
                'status': status,
                'client_id': order['client_id']
            })
            return jsonify({'success': True, 'order': dict(order)})
        return jsonify({'success': False, 'message': 'Order not found'}), 404
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

# Mount SOAP service
@app.route('/soap', methods=['GET', 'POST'])
def soap_endpoint():
    """SOAP endpoint wrapper"""
    from io import BytesIO
    environ = request.environ.copy()
    environ['wsgi.input'] = BytesIO(request.get_data())
    
    response_body = []
    def start_response(status, headers):
        pass
    
    result = wsgi_app(environ, start_response)
    return b''.join(result), 200, {'Content-Type': 'text/xml'}

@app.route('/soap/wsdl', methods=['GET'])
def wsdl():
    """Get WSDL for the SOAP service"""
    from spyne.interface.wsdl import Wsdl11
    wsdl = Wsdl11(soap_app.interface)
    wsdl.build_interface_document('http://localhost:8001/soap')
    return wsdl.get_interface_document(), 200, {'Content-Type': 'text/xml'}

if __name__ == '__main__':
    print("Starting CMS Service (SOAP/XML API) on port 8001...")
    app.run(host='0.0.0.0', port=8001, debug=True)
