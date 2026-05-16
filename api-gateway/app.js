/**
 * SwiftLogistics - API Gateway
 * Central gateway for all client requests with WebSocket support
 * 
 * This gateway:
 * 1. Routes requests to appropriate backend services (CMS, ROS, WMS)
 * 2. Handles protocol translation (SOAP <-> REST <-> TCP)
 * 3. Provides WebSocket connections for real-time updates
 * 4. Manages authentication and authorization
 * 5. Handles distributed transaction orchestration
 */

const express = require('express');
const cors = require('cors');
const bodyParser = require('body-parser');
const http = require('http');
const WebSocket = require('ws');
const amqp = require('amqplib');
const axios = require('axios');
const jwt = require('jsonwebtoken');
const { v4: uuidv4 } = require('uuid');
const net = require('net');

const app = express();
const PORT = 3000;

// Configuration
const config = {
    cms: process.env.CMS_URL || 'http://localhost:8001',
    ros: process.env.ROS_URL || 'http://localhost:8002',
    wms: process.env.WMS_URL || 'http://localhost:8003',
    wmsTcp: process.env.WMS_TCP_PORT || 9000,
    rabbitmq: process.env.RABBITMQ_URL || 'amqp://swift:logistics123@localhost:5672/',
    jwtSecret: process.env.JWT_SECRET || 'swiftlogistics-secret-key-2026'
};

app.use(cors());
app.use(bodyParser.json());

// Create HTTP server
const server = http.createServer(app);

// WebSocket server for real-time updates
const wss = new WebSocket.Server({ server });

// Store connected WebSocket clients
const wsClients = {
    clients: new Map(),      // Client portal connections
    drivers: new Map()       // Driver app connections
};

// RabbitMQ connection
let rabbitmqChannel = null;
let rabbitmqConnection = null;

// Transaction management store
const transactions = new Map();

// Connect to RabbitMQ
async function connectRabbitMQ() {
    const maxRetries = 15;
    let retries = 0;
    
    while (retries < maxRetries) {
        try {
            rabbitmqConnection = await amqp.connect(config.rabbitmq);
            rabbitmqChannel = await rabbitmqConnection.createChannel();
            
            await rabbitmqChannel.assertQueue('order_events', { durable: true });
            await rabbitmqChannel.assertQueue('wms_events', { durable: true });
            await rabbitmqChannel.assertQueue('route_events', { durable: true });
            await rabbitmqChannel.assertQueue('notification_events', { durable: true });
            
            // Consume events from all queues and broadcast to WebSocket clients
            setupEventConsumers();
            
            console.log('Connected to RabbitMQ');
            return;
        } catch (error) {
            retries++;
            console.log(`RabbitMQ connection attempt ${retries}/${maxRetries}:`, error.message);
            await new Promise(resolve => setTimeout(resolve, 3000));
        }
    }
    console.error('Failed to connect to RabbitMQ');
}

// Setup event consumers for all queues
function setupEventConsumers() {
    // WMS Events
    rabbitmqChannel.consume('wms_events', (msg) => {
        if (msg) {
            const event = JSON.parse(msg.content.toString());
            handleEvent('wms', event);
            rabbitmqChannel.ack(msg);
        }
    });
    
    // Route Events
    rabbitmqChannel.consume('route_events', (msg) => {
        if (msg) {
            const event = JSON.parse(msg.content.toString());
            handleEvent('route', event);
            rabbitmqChannel.ack(msg);
        }
    });
    
    console.log('Event consumers setup complete');
}

// Handle incoming events and broadcast to relevant clients
function handleEvent(source, event) {
    console.log(`Event from ${source}:`, event.event_type);
    
    const notification = {
        type: 'notification',
        source: source,
        event_type: event.event_type,
        data: event.data,
        timestamp: event.timestamp || new Date().toISOString()
    };
    
    // Determine which clients should receive this notification
    if (event.data) {
        // Notify relevant client
        if (event.data.client_id) {
            notifyClient(event.data.client_id, notification);
        }
        
        // Notify relevant driver
        if (event.data.driver_id) {
            notifyDriver(event.data.driver_id, notification);
        }
        
        // Update transaction status
        if (event.data.transaction_id) {
            updateTransactionStatus(event.data.transaction_id, source, event.event_type);
        }
    }
    
    // Broadcast to all connected clients for general updates
    broadcastToAll(notification);
}

// Notify specific client
function notifyClient(clientId, notification) {
    const client = wsClients.clients.get(clientId);
    if (client && client.readyState === WebSocket.OPEN) {
        client.send(JSON.stringify(notification));
    }
}

// Notify specific driver
function notifyDriver(driverId, notification) {
    const driver = wsClients.drivers.get(driverId);
    if (driver && driver.readyState === WebSocket.OPEN) {
        driver.send(JSON.stringify(notification));
    }
}

// Broadcast to all connected clients
function broadcastToAll(notification) {
    wss.clients.forEach(client => {
        if (client.readyState === WebSocket.OPEN) {
            client.send(JSON.stringify(notification));
        }
    });
}

// Transaction management
function createTransaction(orderId) {
    const transactionId = `TXN${uuidv4().substring(0, 8).toUpperCase()}`;
    transactions.set(transactionId, {
        id: transactionId,
        orderId: orderId,
        status: 'pending',
        steps: {
            cms: 'pending',
            ros: 'pending',
            wms: 'pending'
        },
        createdAt: new Date().toISOString(),
        completedAt: null
    });
    return transactionId;
}

function updateTransactionStatus(transactionId, service, eventType) {
    const transaction = transactions.get(transactionId);
    if (transaction) {
        if (eventType.includes('COMPLETE') || eventType.includes('CREATED')) {
            transaction.steps[service] = 'completed';
        } else if (eventType.includes('FAIL') || eventType.includes('ERROR')) {
            transaction.steps[service] = 'failed';
        }
        
        // Check if all steps completed
        const allCompleted = Object.values(transaction.steps).every(s => s === 'completed');
        const anyFailed = Object.values(transaction.steps).some(s => s === 'failed');
        
        if (allCompleted) {
            transaction.status = 'completed';
            transaction.completedAt = new Date().toISOString();
        } else if (anyFailed) {
            transaction.status = 'failed';
            // Trigger compensation logic
            handleTransactionFailure(transaction);
        }
    }
}

function handleTransactionFailure(transaction) {
    console.log('Transaction failed, initiating compensation:', transaction.id);
    // In a real system, this would trigger rollback operations
    // For this prototype, we log the failure for demonstration
    broadcastToAll({
        type: 'transaction_failed',
        transaction_id: transaction.id,
        order_id: transaction.orderId,
        failed_steps: Object.entries(transaction.steps)
            .filter(([_, status]) => status === 'failed')
            .map(([step]) => step)
    });
}

// WebSocket connection handler
wss.on('connection', (ws, req) => {
    console.log('New WebSocket connection');
    
    ws.on('message', (message) => {
        try {
            const data = JSON.parse(message);
            handleWebSocketMessage(ws, data);
        } catch (error) {
            ws.send(JSON.stringify({ error: 'Invalid message format' }));
        }
    });
    
    ws.on('close', () => {
        // Remove from client lists
        for (const [id, client] of wsClients.clients) {
            if (client === ws) {
                wsClients.clients.delete(id);
                console.log(`Client ${id} disconnected`);
                break;
            }
        }
        for (const [id, driver] of wsClients.drivers) {
            if (driver === ws) {
                wsClients.drivers.delete(id);
                console.log(`Driver ${id} disconnected`);
                break;
            }
        }
    });
    
    // Send welcome message
    ws.send(JSON.stringify({
        type: 'connected',
        message: 'Connected to SwiftLogistics real-time updates',
        timestamp: new Date().toISOString()
    }));
});

// Handle WebSocket messages
function handleWebSocketMessage(ws, data) {
    switch (data.type) {
        case 'register_client':
            wsClients.clients.set(data.client_id, ws);
            ws.send(JSON.stringify({
                type: 'registered',
                role: 'client',
                client_id: data.client_id
            }));
            console.log(`Client ${data.client_id} registered for updates`);
            break;
            
        case 'register_driver':
            wsClients.drivers.set(data.driver_id, ws);
            ws.send(JSON.stringify({
                type: 'registered',
                role: 'driver',
                driver_id: data.driver_id
            }));
            console.log(`Driver ${data.driver_id} registered for updates`);
            break;
            
        case 'subscribe_order':
            // Subscribe to updates for a specific order
            ws.orderId = data.order_id;
            ws.send(JSON.stringify({
                type: 'subscribed',
                order_id: data.order_id
            }));
            break;
            
        case 'ping':
            ws.send(JSON.stringify({ type: 'pong', timestamp: new Date().toISOString() }));
            break;
    }
}

// JWT Authentication middleware
function authenticateToken(req, res, next) {
    const authHeader = req.headers['authorization'];
    const token = authHeader && authHeader.split(' ')[1];
    
    if (!token) {
        return res.status(401).json({ success: false, message: 'Authentication required' });
    }
    
    jwt.verify(token, config.jwtSecret, (err, user) => {
        if (err) {
            return res.status(403).json({ success: false, message: 'Invalid token' });
        }
        req.user = user;
        next();
    });
}

// Health check endpoint
app.get('/health', async (req, res) => {
    const services = {};
    
    try {
        const cmsHealth = await axios.get(`${config.cms}/health`, { timeout: 2000 });
        services.cms = cmsHealth.data.status;
    } catch {
        services.cms = 'unhealthy';
    }
    
    try {
        const rosHealth = await axios.get(`${config.ros}/health`, { timeout: 2000 });
        services.ros = rosHealth.data.status;
    } catch {
        services.ros = 'unhealthy';
    }
    
    try {
        const wmsHealth = await axios.get(`${config.wms}/health`, { timeout: 2000 });
        services.wms = wmsHealth.data.status;
    } catch {
        services.wms = 'unhealthy';
    }
    
    services.rabbitmq = rabbitmqChannel ? 'healthy' : 'unhealthy';
    
    res.json({
        status: 'healthy',
        service: 'API Gateway',
        timestamp: new Date().toISOString(),
        services: services
    });
});

// ==================== Authentication Endpoints ====================

// Client login
app.post('/api/auth/client/login', async (req, res) => {
    try {
        const { email, password } = req.body;
        
        // Call CMS for authentication
        const response = await axios.post(`${config.cms}/api/clients/auth`, {
            email,
            password
        });
        
        if (response.data.success) {
            const client = response.data.client;
            const token = jwt.sign(
                { id: client.client_id, email: client.email, role: 'client' },
                config.jwtSecret,
                { expiresIn: '24h' }
            );
            
            res.json({
                success: true,
                token,
                client: {
                    id: client.client_id,
                    company_name: client.company_name,
                    email: client.email,
                    contract_type: client.contract_type
                }
            });
        } else {
            res.status(401).json({ success: false, message: 'Invalid credentials' });
        }
    } catch (error) {
        console.error('Auth error:', error.message);
        res.status(500).json({ success: false, message: 'Authentication service unavailable' });
    }
});

// Driver login
app.post('/api/auth/driver/login', async (req, res) => {
    try {
        const { email, password } = req.body;
        
        // For demo, use hardcoded drivers
        const drivers = {
            'kasun@swiftlogistics.lk': { id: 'DRV001', name: 'Kasun Perera', vehicle: 'Van WP-KA-1234' },
            'nimal@swiftlogistics.lk': { id: 'DRV002', name: 'Nimal Silva', vehicle: 'Motorcycle WP-NB-5678' },
            'samantha@swiftlogistics.lk': { id: 'DRV003', name: 'Samantha Fernando', vehicle: 'Truck WP-SC-9012' }
        };
        
        if (drivers[email] && password === 'password123') {
            const driver = drivers[email];
            const token = jwt.sign(
                { id: driver.id, email, name: driver.name, role: 'driver' },
                config.jwtSecret,
                { expiresIn: '24h' }
            );
            
            res.json({
                success: true,
                token,
                driver: {
                    id: driver.id,
                    name: driver.name,
                    email,
                    vehicle: driver.vehicle
                }
            });
        } else {
            res.status(401).json({ success: false, message: 'Invalid credentials' });
        }
    } catch (error) {
        res.status(500).json({ success: false, message: error.message });
    }
});

// ==================== Order Endpoints ====================

// Create new order (orchestrates CMS, WMS, ROS)
app.post('/api/orders', authenticateToken, async (req, res) => {
    try {
        const orderData = {
            client_id: req.user.id,
            ...req.body
        };
        
        // Create transaction for tracking
        const transactionId = createTransaction(null);
        
        // Step 1: Create order in CMS
        const cmsResponse = await axios.post(`${config.cms}/api/orders`, orderData);
        
        if (cmsResponse.data.success) {
            const order = cmsResponse.data.order;
            
            // Update transaction with order ID
            const transaction = transactions.get(transactionId);
            if (transaction) {
                transaction.orderId = order.order_id;
                transaction.steps.cms = 'completed';
            }
            
            // WMS and ROS processing happens asynchronously via RabbitMQ
            // The event published by CMS triggers WMS and ROS processing
            
            res.json({
                success: true,
                order: order,
                transaction_id: transactionId,
                message: 'Order submitted successfully. Processing in background.'
            });
            
            // Notify client via WebSocket
            notifyClient(req.user.id, {
                type: 'order_created',
                order_id: order.order_id,
                transaction_id: transactionId
            });
        } else {
            res.status(400).json({ success: false, message: 'Failed to create order' });
        }
    } catch (error) {
        console.error('Order creation error:', error.message);
        res.status(500).json({ success: false, message: error.message });
    }
});

// Get client orders
app.get('/api/orders', authenticateToken, async (req, res) => {
    try {
        const response = await axios.get(`${config.cms}/api/orders/${req.user.id}`);
        res.json(response.data);
    } catch (error) {
        res.status(500).json({ success: false, message: error.message });
    }
});

// Get single order with tracking info
app.get('/api/orders/:orderId', authenticateToken, async (req, res) => {
    try {
        const orderId = req.params.orderId;
        
        // Get order from CMS
        const cmsResponse = await axios.get(`${config.cms}/api/orders/${req.user.id}`);
        const order = cmsResponse.data.orders?.find(o => o.order_id === orderId);
        
        if (!order) {
            return res.status(404).json({ success: false, message: 'Order not found' });
        }
        
        // Get package info from WMS
        let packageInfo = null;
        try {
            const wmsResponse = await axios.get(`${config.wms}/api/packages/order/${orderId}`);
            packageInfo = wmsResponse.data.data;
        } catch (e) {
            console.log('WMS package info not available');
        }
        
        // Get route info from ROS
        let routeInfo = null;
        try {
            const rosResponse = await axios.get(`${config.ros}/api/routes`);
            const routes = rosResponse.data.routes || [];
            for (const route of routes) {
                const stop = route.stops?.find(s => s.order_id === orderId);
                if (stop) {
                    routeInfo = { route, stop };
                    break;
                }
            }
        } catch (e) {
            console.log('ROS route info not available');
        }
        
        res.json({
            success: true,
            order: order,
            package: packageInfo,
            route: routeInfo
        });
    } catch (error) {
        res.status(500).json({ success: false, message: error.message });
    }
});

// Update order status
app.put('/api/orders/:orderId/status', authenticateToken, async (req, res) => {
    try {
        const { status } = req.body;
        const response = await axios.put(
            `${config.cms}/api/orders/status/${req.params.orderId}`,
            { status }
        );
        
        // Also update package status in WMS if needed
        if (status === 'delivered' || status === 'failed') {
            try {
                const wmsResponse = await axios.get(`${config.wms}/api/packages/order/${req.params.orderId}`);
                const packages = wmsResponse.data.data || [];
                for (const pkg of packages) {
                    await axios.put(`${config.wms}/api/packages/${pkg.package_id}/status`, {
                        status: status.toUpperCase()
                    });
                }
            } catch (e) {
                console.log('Failed to update WMS package status');
            }
        }
        
        res.json(response.data);
    } catch (error) {
        res.status(500).json({ success: false, message: error.message });
    }
});

// ==================== Driver Endpoints ====================

// Get driver's route for today
app.get('/api/driver/route/today', authenticateToken, async (req, res) => {
    try {
        if (req.user.role !== 'driver') {
            return res.status(403).json({ success: false, message: 'Access denied' });
        }
        
        const response = await axios.get(`${config.ros}/api/routes/driver/${req.user.id}/today`);
        res.json(response.data);
    } catch (error) {
        res.status(500).json({ success: false, message: error.message });
    }
});

// Get all driver routes
app.get('/api/driver/routes', authenticateToken, async (req, res) => {
    try {
        if (req.user.role !== 'driver') {
            return res.status(403).json({ success: false, message: 'Access denied' });
        }
        
        const response = await axios.get(`${config.ros}/api/routes/driver/${req.user.id}`);
        res.json(response.data);
    } catch (error) {
        res.status(500).json({ success: false, message: error.message });
    }
});

// Update delivery status (mark as delivered/failed)
app.post('/api/driver/delivery/:orderId', authenticateToken, async (req, res) => {
    try {
        if (req.user.role !== 'driver') {
            return res.status(403).json({ success: false, message: 'Access denied' });
        }
        
        const { status, reason, signature, photo } = req.body;
        const orderId = req.params.orderId;
        
        // Update order status in CMS
        await axios.put(`${config.cms}/api/orders/status/${orderId}`, { status });
        
        // Update package status in WMS
        try {
            const wmsResponse = await axios.get(`${config.wms}/api/packages/order/${orderId}`);
            const packages = wmsResponse.data.data || [];
            for (const pkg of packages) {
                await axios.put(`${config.wms}/api/packages/${pkg.package_id}/status`, {
                    status: status.toUpperCase()
                });
            }
        } catch (e) {
            console.log('WMS update failed');
        }
        
        // Update route stop status in ROS
        try {
            const routesResponse = await axios.get(`${config.ros}/api/routes/driver/${req.user.id}`);
            const routes = routesResponse.data.routes || [];
            for (const route of routes) {
                const stop = route.stops?.find(s => s.order_id === orderId);
                if (stop) {
                    await axios.put(`${config.ros}/api/routes/${route.route_id}/stops/${orderId}`, {
                        status: status,
                        actual_arrival: new Date().toISOString()
                    });
                    break;
                }
            }
        } catch (e) {
            console.log('ROS update failed');
        }
        
        // Broadcast delivery update
        broadcastToAll({
            type: 'delivery_update',
            order_id: orderId,
            driver_id: req.user.id,
            status: status,
            reason: reason,
            timestamp: new Date().toISOString()
        });
        
        res.json({
            success: true,
            message: `Delivery marked as ${status}`,
            order_id: orderId
        });
    } catch (error) {
        res.status(500).json({ success: false, message: error.message });
    }
});

// Update driver location
app.post('/api/driver/location', authenticateToken, async (req, res) => {
    try {
        if (req.user.role !== 'driver') {
            return res.status(403).json({ success: false, message: 'Access denied' });
        }
        
        const { lat, lng } = req.body;
        
        // Broadcast location update
        broadcastToAll({
            type: 'driver_location',
            driver_id: req.user.id,
            location: { lat, lng },
            timestamp: new Date().toISOString()
        });
        
        res.json({ success: true });
    } catch (error) {
        res.status(500).json({ success: false, message: error.message });
    }
});

// ==================== Tracking Endpoints ====================

// Track order by ID (public endpoint with order ID)
app.get('/api/track/:orderId', async (req, res) => {
    try {
        const orderId = req.params.orderId;
        
        // Get package info from WMS
        let packageInfo = null;
        try {
            const wmsResponse = await axios.get(`${config.wms}/api/packages/order/${orderId}`);
            packageInfo = wmsResponse.data.data;
        } catch (e) {
            console.log('WMS not available');
        }
        
        // Get route info from ROS
        let routeInfo = null;
        try {
            const rosResponse = await axios.get(`${config.ros}/api/routes`);
            const routes = rosResponse.data.routes || [];
            for (const route of routes) {
                const stop = route.stops?.find(s => s.order_id === orderId);
                if (stop) {
                    routeInfo = {
                        driver_id: route.driver_id,
                        estimated_arrival: stop.estimated_arrival,
                        sequence: stop.sequence,
                        total_stops: route.stops.length,
                        stop_status: stop.status
                    };
                    break;
                }
            }
        } catch (e) {
            console.log('ROS not available');
        }
        
        res.json({
            success: true,
            order_id: orderId,
            package: packageInfo,
            route: routeInfo,
            tracking_url: `/track/${orderId}`
        });
    } catch (error) {
        res.status(500).json({ success: false, message: error.message });
    }
});

// ==================== Admin/System Endpoints ====================

// Get all routes (for monitoring)
app.get('/api/admin/routes', async (req, res) => {
    try {
        const response = await axios.get(`${config.ros}/api/routes`);
        res.json(response.data);
    } catch (error) {
        res.status(500).json({ success: false, message: error.message });
    }
});

// Get warehouse status
app.get('/api/admin/warehouse', async (req, res) => {
    try {
        const [packages, locations] = await Promise.all([
            axios.get(`${config.wms}/api/packages`),
            axios.get(`${config.wms}/api/warehouse/locations`)
        ]);
        
        res.json({
            success: true,
            packages: packages.data,
            locations: locations.data
        });
    } catch (error) {
        res.status(500).json({ success: false, message: error.message });
    }
});

// Get transaction status
app.get('/api/transactions/:transactionId', async (req, res) => {
    const transaction = transactions.get(req.params.transactionId);
    if (transaction) {
        res.json({ success: true, transaction });
    } else {
        res.status(404).json({ success: false, message: 'Transaction not found' });
    }
});

// Start server
server.listen(PORT, '0.0.0.0', () => {
    console.log(`API Gateway running on port ${PORT}`);
    console.log(`WebSocket server running on ws://localhost:${PORT}`);
    connectRabbitMQ();
});

// Graceful shutdown
process.on('SIGINT', async () => {
    console.log('Shutting down API Gateway...');
    if (rabbitmqConnection) {
        await rabbitmqConnection.close();
    }
    server.close();
    process.exit(0);
});
