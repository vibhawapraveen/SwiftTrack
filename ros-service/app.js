/**
 * SwiftLogistics - Route Optimization System (ROS)
 * RESTful API Service
 * 
 * This is a mock implementation of a cloud-based route optimization service
 * that generates efficient delivery routes based on delivery addresses.
 */

const express = require('express');
const cors = require('cors');
const bodyParser = require('body-parser');
const amqp = require('amqplib');
const { v4: uuidv4 } = require('uuid');

const app = express();
const PORT = 8002;

app.use(cors());
app.use(bodyParser.json());

// In-memory storage for routes (simulating cloud storage)
const routes = new Map();
const vehicles = new Map();

// Initialize mock vehicles
vehicles.set('DRV001', { id: 'DRV001', type: 'Van', capacity: 50, available: true });
vehicles.set('DRV002', { id: 'DRV002', type: 'Motorcycle', capacity: 10, available: true });
vehicles.set('DRV003', { id: 'DRV003', type: 'Truck', capacity: 100, available: false });

let channel = null;
let connection = null;

// RabbitMQ connection
async function connectRabbitMQ() {
    const maxRetries = 10;
    let retries = 0;
    
    while (retries < maxRetries) {
        try {
            const url = process.env.RABBITMQ_URL || 'amqp://swift:logistics123@localhost:5672/';
            connection = await amqp.connect(url);
            channel = await connection.createChannel();
            
            await channel.assertQueue('order_events', { durable: true });
            await channel.assertQueue('route_events', { durable: true });
            
            // Consume order events
            channel.consume('order_events', async (msg) => {
                if (msg) {
                    const event = JSON.parse(msg.content.toString());
                    console.log('Received event:', event.event_type);
                    
                    if (event.event_type === 'ORDER_CREATED') {
                        // Auto-optimize route when order is created
                        await handleNewOrder(event.data);
                    }
                    
                    channel.ack(msg);
                }
            });
            
            console.log('Connected to RabbitMQ');
            return;
        } catch (error) {
            retries++;
            console.log(`RabbitMQ connection attempt ${retries}/${maxRetries} failed:`, error.message);
            await new Promise(resolve => setTimeout(resolve, 5000));
        }
    }
    console.error('Failed to connect to RabbitMQ after all retries');
}

// Publish event to RabbitMQ
async function publishEvent(eventType, data) {
    if (channel) {
        const message = JSON.stringify({
            event_type: eventType,
            timestamp: new Date().toISOString(),
            data: data
        });
        
        channel.sendToQueue('route_events', Buffer.from(message), { persistent: true });
        console.log('Published event:', eventType);
    }
}

// Handle new order and optimize route
async function handleNewOrder(orderData) {
    try {
        // Find or create today's route for an available driver
        const today = new Date().toISOString().split('T')[0];
        let existingRoute = null;
        
        for (const [routeId, route] of routes) {
            if (route.date === today && route.status === 'planned') {
                existingRoute = route;
                break;
            }
        }
        
        if (existingRoute) {
            // Add stop to existing route
            existingRoute.stops.push({
                order_id: orderData.order_id,
                address: orderData.delivery_address,
                lat: orderData.delivery_lat,
                lng: orderData.delivery_lng,
                sequence: existingRoute.stops.length + 1,
                status: 'pending',
                estimated_arrival: calculateETA(existingRoute.stops.length)
            });
            
            // Re-optimize route
            optimizeRouteStops(existingRoute);
            
            await publishEvent('ROUTE_UPDATED', {
                route_id: existingRoute.route_id,
                driver_id: existingRoute.driver_id,
                order_id: orderData.order_id
            });
        } else {
            // Create new route
            const routeId = `RTE${Date.now()}${uuidv4().substring(0, 4).toUpperCase()}`;
            const newRoute = {
                route_id: routeId,
                driver_id: 'DRV001',
                date: today,
                status: 'planned',
                stops: [{
                    order_id: orderData.order_id,
                    address: orderData.delivery_address,
                    lat: orderData.delivery_lat,
                    lng: orderData.delivery_lng,
                    sequence: 1,
                    status: 'pending',
                    estimated_arrival: calculateETA(0)
                }],
                total_distance: 0,
                estimated_duration: 0,
                created_at: new Date().toISOString()
            };
            
            optimizeRouteStops(newRoute);
            routes.set(routeId, newRoute);
            
            await publishEvent('ROUTE_CREATED', {
                route_id: routeId,
                driver_id: newRoute.driver_id,
                order_id: orderData.order_id
            });
        }
        
        // Update transaction log
        await publishEvent('ROS_PROCESSING_COMPLETE', {
            order_id: orderData.order_id,
            transaction_id: orderData.transaction_id
        });
        
    } catch (error) {
        console.error('Error handling new order:', error);
    }
}

// Calculate ETA based on stop position
function calculateETA(position) {
    const baseTime = new Date();
    baseTime.setHours(8, 0, 0, 0); // Start at 8 AM
    baseTime.setMinutes(baseTime.getMinutes() + (position * 30)); // 30 min per stop
    return baseTime.toISOString();
}

// Mock route optimization using nearest neighbor algorithm
function optimizeRouteStops(route) {
    if (route.stops.length < 2) {
        route.total_distance = 5; // Base distance in km
        route.estimated_duration = 30; // Base duration in minutes
        return;
    }
    
    // Simple nearest neighbor optimization (mock)
    const optimized = [];
    const remaining = [...route.stops];
    let current = remaining.shift();
    optimized.push(current);
    
    while (remaining.length > 0) {
        let nearestIdx = 0;
        let nearestDist = Infinity;
        
        for (let i = 0; i < remaining.length; i++) {
            const dist = calculateDistance(
                current.lat || 6.9271, current.lng || 79.8612,
                remaining[i].lat || 6.9271, remaining[i].lng || 79.8612
            );
            if (dist < nearestDist) {
                nearestDist = dist;
                nearestIdx = i;
            }
        }
        
        current = remaining.splice(nearestIdx, 1)[0];
        optimized.push(current);
    }
    
    // Update sequences and ETAs
    optimized.forEach((stop, idx) => {
        stop.sequence = idx + 1;
        stop.estimated_arrival = calculateETA(idx);
    });
    
    route.stops = optimized;
    route.total_distance = calculateTotalDistance(optimized);
    route.estimated_duration = optimized.length * 30;
}

// Haversine formula for distance calculation
function calculateDistance(lat1, lng1, lat2, lng2) {
    const R = 6371; // Earth's radius in km
    const dLat = toRad(lat2 - lat1);
    const dLng = toRad(lng2 - lng1);
    const a = Math.sin(dLat/2) * Math.sin(dLat/2) +
              Math.cos(toRad(lat1)) * Math.cos(toRad(lat2)) *
              Math.sin(dLng/2) * Math.sin(dLng/2);
    const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));
    return R * c;
}

function toRad(deg) {
    return deg * (Math.PI / 180);
}

function calculateTotalDistance(stops) {
    let total = 5; // Base distance from warehouse
    for (let i = 0; i < stops.length - 1; i++) {
        total += calculateDistance(
            stops[i].lat || 6.9271, stops[i].lng || 79.8612,
            stops[i+1].lat || 6.9271, stops[i+1].lng || 79.8612
        );
    }
    return Math.round(total * 10) / 10;
}

// REST API Endpoints

// Health check
app.get('/health', (req, res) => {
    res.json({
        status: 'healthy',
        service: 'ROS',
        timestamp: new Date().toISOString()
    });
});

// Get all available vehicles
app.get('/api/vehicles', (req, res) => {
    const vehicleList = Array.from(vehicles.values());
    res.json({
        success: true,
        vehicles: vehicleList
    });
});

// Get available vehicles
app.get('/api/vehicles/available', (req, res) => {
    const available = Array.from(vehicles.values()).filter(v => v.available);
    res.json({
        success: true,
        vehicles: available
    });
});

// Create optimized route
app.post('/api/routes/optimize', async (req, res) => {
    try {
        const { driver_id, delivery_points, date } = req.body;
        
        const routeId = `RTE${Date.now()}${uuidv4().substring(0, 4).toUpperCase()}`;
        
        const stops = delivery_points.map((point, idx) => ({
            order_id: point.order_id,
            address: point.address,
            lat: point.lat,
            lng: point.lng,
            sequence: idx + 1,
            status: 'pending',
            estimated_arrival: calculateETA(idx)
        }));
        
        const route = {
            route_id: routeId,
            driver_id: driver_id,
            date: date || new Date().toISOString().split('T')[0],
            status: 'planned',
            stops: stops,
            total_distance: 0,
            estimated_duration: 0,
            created_at: new Date().toISOString()
        };
        
        optimizeRouteStops(route);
        routes.set(routeId, route);
        
        await publishEvent('ROUTE_CREATED', { route });
        
        res.json({
            success: true,
            route: route
        });
    } catch (error) {
        res.status(500).json({
            success: false,
            message: error.message
        });
    }
});

// Get route by ID
app.get('/api/routes/:routeId', (req, res) => {
    const route = routes.get(req.params.routeId);
    if (route) {
        res.json({ success: true, route });
    } else {
        res.status(404).json({ success: false, message: 'Route not found' });
    }
});

// Get routes for a driver
app.get('/api/routes/driver/:driverId', (req, res) => {
    const driverRoutes = Array.from(routes.values())
        .filter(r => r.driver_id === req.params.driverId);
    res.json({
        success: true,
        routes: driverRoutes
    });
});

// Get today's route for a driver
app.get('/api/routes/driver/:driverId/today', (req, res) => {
    const today = new Date().toISOString().split('T')[0];
    const todayRoute = Array.from(routes.values())
        .find(r => r.driver_id === req.params.driverId && r.date === today);
    
    if (todayRoute) {
        res.json({ success: true, route: todayRoute });
    } else {
        res.json({ success: true, route: null, message: 'No route for today' });
    }
});

// Add delivery point to existing route
app.post('/api/routes/:routeId/stops', async (req, res) => {
    try {
        const route = routes.get(req.params.routeId);
        if (!route) {
            return res.status(404).json({ success: false, message: 'Route not found' });
        }
        
        const { order_id, address, lat, lng, priority } = req.body;
        
        route.stops.push({
            order_id,
            address,
            lat,
            lng,
            sequence: route.stops.length + 1,
            status: 'pending',
            priority: priority || 'normal',
            estimated_arrival: calculateETA(route.stops.length)
        });
        
        // Re-optimize with new stop
        optimizeRouteStops(route);
        
        await publishEvent('ROUTE_UPDATED', {
            route_id: route.route_id,
            driver_id: route.driver_id,
            action: 'stop_added',
            order_id
        });
        
        res.json({ success: true, route });
    } catch (error) {
        res.status(500).json({ success: false, message: error.message });
    }
});

// Update stop status
app.put('/api/routes/:routeId/stops/:orderId', async (req, res) => {
    try {
        const route = routes.get(req.params.routeId);
        if (!route) {
            return res.status(404).json({ success: false, message: 'Route not found' });
        }
        
        const stop = route.stops.find(s => s.order_id === req.params.orderId);
        if (!stop) {
            return res.status(404).json({ success: false, message: 'Stop not found' });
        }
        
        const { status, actual_arrival } = req.body;
        stop.status = status;
        if (actual_arrival) {
            stop.actual_arrival = actual_arrival;
        }
        
        await publishEvent('STOP_STATUS_UPDATED', {
            route_id: route.route_id,
            order_id: req.params.orderId,
            status
        });
        
        res.json({ success: true, stop, route });
    } catch (error) {
        res.status(500).json({ success: false, message: error.message });
    }
});

// Re-optimize a route
app.post('/api/routes/:routeId/reoptimize', async (req, res) => {
    try {
        const route = routes.get(req.params.routeId);
        if (!route) {
            return res.status(404).json({ success: false, message: 'Route not found' });
        }
        
        // Only re-optimize pending stops
        const pendingStops = route.stops.filter(s => s.status === 'pending');
        const completedStops = route.stops.filter(s => s.status !== 'pending');
        
        route.stops = completedStops;
        
        // Add pending stops back and re-optimize
        pendingStops.forEach(stop => {
            route.stops.push(stop);
        });
        
        optimizeRouteStops(route);
        
        await publishEvent('ROUTE_REOPTIMIZED', {
            route_id: route.route_id,
            driver_id: route.driver_id
        });
        
        res.json({ success: true, route });
    } catch (error) {
        res.status(500).json({ success: false, message: error.message });
    }
});

// Get all routes
app.get('/api/routes', (req, res) => {
    const allRoutes = Array.from(routes.values());
    res.json({
        success: true,
        routes: allRoutes
    });
});

// Start server
app.listen(PORT, '0.0.0.0', () => {
    console.log(`ROS Service (REST API) running on port ${PORT}`);
    connectRabbitMQ();
});

// Graceful shutdown
process.on('SIGINT', async () => {
    console.log('Shutting down ROS service...');
    if (connection) {
        await connection.close();
    }
    process.exit(0);
});
