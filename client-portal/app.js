/**
 * SwiftTrack Client Portal - JavaScript
 * Handles authentication, order management, and real-time updates
 */

// Configuration
const API_URL = window.location.hostname === 'localhost' ? 'http://localhost:3000' : '';
const WS_URL = window.location.hostname === 'localhost' 
    ? 'ws://localhost:3000' 
    : `ws://${window.location.host}`;

// State
let token = localStorage.getItem('swifttrack_token');
let client = JSON.parse(localStorage.getItem('swifttrack_client') || 'null');
let orders = [];
let ws = null;

// DOM Elements
const loginPage = document.getElementById('login-page');
const dashboardPage = document.getElementById('dashboard-page');
const loginForm = document.getElementById('login-form');
const orderModal = document.getElementById('order-modal');
const detailsModal = document.getElementById('details-modal');
const orderForm = document.getElementById('order-form');
const ordersTableBody = document.getElementById('orders-table-body');
const notificationsList = document.getElementById('notifications-list');
const connectionStatus = document.getElementById('connection-status');

// Initialize
document.addEventListener('DOMContentLoaded', () => {
    if (token && client) {
        showDashboard();
    } else {
        showLogin();
    }
    
    setupEventListeners();
});

// Event Listeners
function setupEventListeners() {
    // Login form
    loginForm.addEventListener('submit', handleLogin);
    
    // Logout
    document.getElementById('logout-btn').addEventListener('click', handleLogout);
    
    // New order button
    document.getElementById('new-order-btn').addEventListener('click', () => {
        orderModal.classList.add('active');
    });
    
    // Close modals
    document.getElementById('close-modal').addEventListener('click', () => {
        orderModal.classList.remove('active');
    });
    
    document.getElementById('cancel-order').addEventListener('click', () => {
        orderModal.classList.remove('active');
    });
    
    document.getElementById('close-details').addEventListener('click', () => {
        detailsModal.classList.remove('active');
    });
    
    // Order form
    orderForm.addEventListener('submit', handleCreateOrder);
    
    // Refresh button
    document.getElementById('refresh-btn').addEventListener('click', loadOrders);
    
    // Close modals on outside click
    window.addEventListener('click', (e) => {
        if (e.target === orderModal) orderModal.classList.remove('active');
        if (e.target === detailsModal) detailsModal.classList.remove('active');
    });
}

// Show login page
function showLogin() {
    loginPage.classList.add('active');
    dashboardPage.classList.remove('active');
}

// Show dashboard
function showDashboard() {
    loginPage.classList.remove('active');
    dashboardPage.classList.add('active');
    
    // Update user info
    document.getElementById('company-name').textContent = client.company_name || client.email;
    
    // Connect WebSocket
    connectWebSocket();
    
    // Load orders
    loadOrders();
}

// Handle login
async function handleLogin(e) {
    e.preventDefault();
    
    const email = document.getElementById('email').value;
    const password = document.getElementById('password').value;
    
    try {
        const response = await fetch(`${API_URL}/api/auth/client/login`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ email, password })
        });
        
        const data = await response.json();
        
        if (data.success) {
            token = data.token;
            client = data.client;
            
            localStorage.setItem('swifttrack_token', token);
            localStorage.setItem('swifttrack_client', JSON.stringify(client));
            
            showToast('success', 'Login Successful', `Welcome back, ${client.company_name}!`);
            showDashboard();
        } else {
            showToast('error', 'Login Failed', data.message || 'Invalid credentials');
        }
    } catch (error) {
        console.error('Login error:', error);
        showToast('error', 'Connection Error', 'Unable to connect to server');
    }
}

// Handle logout
function handleLogout() {
    token = null;
    client = null;
    localStorage.removeItem('swifttrack_token');
    localStorage.removeItem('swifttrack_client');
    
    if (ws) {
        ws.close();
        ws = null;
    }
    
    showLogin();
    showToast('info', 'Logged Out', 'You have been logged out successfully');
}

// Connect WebSocket
function connectWebSocket() {
    if (ws) {
        ws.close();
    }
    
    ws = new WebSocket(WS_URL);
    
    ws.onopen = () => {
        console.log('WebSocket connected');
        updateConnectionStatus(true);
        
        // Register as client
        ws.send(JSON.stringify({
            type: 'register_client',
            client_id: client.id
        }));
    };
    
    ws.onmessage = (event) => {
        const data = JSON.parse(event.data);
        handleWebSocketMessage(data);
    };
    
    ws.onclose = () => {
        console.log('WebSocket disconnected');
        updateConnectionStatus(false);
        
        // Reconnect after 5 seconds
        setTimeout(() => {
            if (token) connectWebSocket();
        }, 5000);
    };
    
    ws.onerror = (error) => {
        console.error('WebSocket error:', error);
        updateConnectionStatus(false);
    };
}

// Update connection status UI
function updateConnectionStatus(connected) {
    const statusEl = document.getElementById('connection-status');
    if (connected) {
        statusEl.className = 'connection-status connected';
        statusEl.innerHTML = '<i class="fas fa-circle"></i><span>Connected</span>';
    } else {
        statusEl.className = 'connection-status disconnected';
        statusEl.innerHTML = '<i class="fas fa-circle"></i><span>Disconnected</span>';
    }
}

// Handle WebSocket messages
function handleWebSocketMessage(data) {
    console.log('WebSocket message:', data);
    
    switch (data.type) {
        case 'connected':
            addNotification('info', 'Connected to real-time updates');
            break;
            
        case 'registered':
            addNotification('success', `Registered as ${data.role}`);
            break;
            
        case 'order_created':
            addNotification('success', `Order ${data.order_id} created successfully`);
            loadOrders();
            break;
            
        case 'notification':
            handleNotification(data);
            break;
            
        case 'delivery_update':
            addNotification('info', `Order ${data.order_id} status: ${data.status}`);
            loadOrders();
            break;
    }
}

// Handle notifications from backend
function handleNotification(notification) {
    const eventType = notification.event_type;
    const eventData = notification.data || {};
    
    let message = '';
    let type = 'info';
    
    switch (eventType) {
        case 'ORDER_CREATED':
            message = `New order ${eventData.order_id} submitted`;
            type = 'success';
            break;
            
        case 'ORDER_STATUS_UPDATED':
            message = `Order ${eventData.order_id} status: ${eventData.status}`;
            type = 'info';
            break;
            
        case 'PACKAGE_RECEIVED':
            message = `Package ${eventData.package_id} received at warehouse`;
            type = 'info';
            break;
            
        case 'PACKAGE_STATUS_UPDATED':
            message = `Package status: ${eventData.new_status}`;
            type = 'info';
            break;
            
        case 'ROUTE_CREATED':
            message = `Route created for delivery`;
            type = 'success';
            break;
            
        case 'ROUTE_UPDATED':
            message = `Route updated with new stops`;
            type = 'info';
            break;
            
        default:
            message = `Event: ${eventType}`;
    }
    
    addNotification(type, message);
    
    // Refresh orders for relevant events
    if (eventType.includes('ORDER') || eventType.includes('PACKAGE') || eventType.includes('ROUTE')) {
        loadOrders();
    }
}

// Add notification to the list
function addNotification(type, message) {
    const iconClass = type === 'success' ? 'fa-check-circle' : 
                      type === 'error' ? 'fa-exclamation-circle' : 
                      type === 'warning' ? 'fa-exclamation-triangle' : 'fa-info-circle';
    
    const notification = document.createElement('div');
    notification.className = `notification-item ${type}`;
    notification.innerHTML = `
        <i class="fas ${iconClass}"></i>
        <span>${message}</span>
        <small>${new Date().toLocaleTimeString()}</small>
    `;
    
    // Remove "waiting" message if present
    const waitingMsg = notificationsList.querySelector('.notification-item:first-child');
    if (waitingMsg && waitingMsg.textContent.includes('Waiting')) {
        waitingMsg.remove();
    }
    
    notificationsList.insertBefore(notification, notificationsList.firstChild);
    
    // Keep only last 20 notifications
    while (notificationsList.children.length > 20) {
        notificationsList.removeChild(notificationsList.lastChild);
    }
}

// Load orders
async function loadOrders() {
    try {
        const response = await fetch(`${API_URL}/api/orders`, {
            headers: {
                'Authorization': `Bearer ${token}`
            }
        });
        
        const data = await response.json();
        
        if (data.success) {
            orders = data.orders || [];
            renderOrders();
            updateStats();
        }
    } catch (error) {
        console.error('Load orders error:', error);
        showToast('error', 'Error', 'Failed to load orders');
    }
}

// Render orders table
function renderOrders() {
    if (orders.length === 0) {
        ordersTableBody.innerHTML = `
            <tr>
                <td colspan="7" class="empty-state">
                    <i class="fas fa-inbox"></i>
                    <p>No orders yet. Create your first order!</p>
                </td>
            </tr>
        `;
        return;
    }
    
    ordersTableBody.innerHTML = orders.map(order => `
        <tr>
            <td><strong>${order.order_id}</strong></td>
            <td>${truncateText(order.pickup_address, 30)}</td>
            <td>${truncateText(order.delivery_address, 30)}</td>
            <td><span class="priority-badge priority-${order.priority}">${order.priority}</span></td>
            <td><span class="status-badge status-${order.status}">${formatStatus(order.status)}</span></td>
            <td>${formatDate(order.created_at)}</td>
            <td>
                <button class="btn btn-outline btn-sm" onclick="viewOrderDetails('${order.order_id}')">
                    <i class="fas fa-eye"></i>
                </button>
            </td>
        </tr>
    `).join('');
}

// Update stats
function updateStats() {
    document.getElementById('total-orders').textContent = orders.length;
    document.getElementById('pending-orders').textContent = orders.filter(o => 
        o.status === 'pending' || o.status === 'processing'
    ).length;
    document.getElementById('in-transit-orders').textContent = orders.filter(o => 
        o.status === 'in_transit' || o.status === 'in-transit' || o.status === 'loaded'
    ).length;
    document.getElementById('delivered-orders').textContent = orders.filter(o => 
        o.status === 'delivered'
    ).length;
}

// Handle create order
async function handleCreateOrder(e) {
    e.preventDefault();
    
    const orderData = {
        pickup_address: document.getElementById('pickup-address').value,
        delivery_address: document.getElementById('delivery-address').value,
        pickup_lat: parseFloat(document.getElementById('pickup-lat').value) || 6.9271,
        pickup_lng: parseFloat(document.getElementById('pickup-lng').value) || 79.8612,
        delivery_lat: parseFloat(document.getElementById('delivery-lat').value) || 6.9344,
        delivery_lng: parseFloat(document.getElementById('delivery-lng').value) || 79.8428,
        package_weight: parseFloat(document.getElementById('package-weight').value) || 1.0,
        package_dimensions: document.getElementById('package-dimensions').value || '30x20x15',
        priority: document.getElementById('priority').value
    };
    
    try {
        const response = await fetch(`${API_URL}/api/orders`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'Authorization': `Bearer ${token}`
            },
            body: JSON.stringify(orderData)
        });
        
        const data = await response.json();
        
        if (data.success) {
            showToast('success', 'Order Created', `Order ${data.order.order_id} submitted successfully!`);
            orderModal.classList.remove('active');
            orderForm.reset();
            loadOrders();
        } else {
            showToast('error', 'Error', data.message || 'Failed to create order');
        }
    } catch (error) {
        console.error('Create order error:', error);
        showToast('error', 'Connection Error', 'Failed to create order');
    }
}

// View order details
async function viewOrderDetails(orderId) {
    try {
        const response = await fetch(`${API_URL}/api/orders/${orderId}`, {
            headers: {
                'Authorization': `Bearer ${token}`
            }
        });
        
        const data = await response.json();
        
        if (data.success) {
            renderOrderDetails(data);
            detailsModal.classList.add('active');
        }
    } catch (error) {
        console.error('Get order details error:', error);
        
        // Fallback to local data
        const order = orders.find(o => o.order_id === orderId);
        if (order) {
            renderOrderDetails({ order });
            detailsModal.classList.add('active');
        }
    }
}

// Render order details
function renderOrderDetails(data) {
    const order = data.order;
    const packageInfo = data.package;
    const routeInfo = data.route;
    
    let html = `
        <div class="detail-section">
            <h3>Order Information</h3>
            <div class="detail-row">
                <label>Order ID:</label>
                <span><strong>${order.order_id}</strong></span>
            </div>
            <div class="detail-row">
                <label>Status:</label>
                <span><span class="status-badge status-${order.status}">${formatStatus(order.status)}</span></span>
            </div>
            <div class="detail-row">
                <label>Priority:</label>
                <span><span class="priority-badge priority-${order.priority}">${order.priority}</span></span>
            </div>
            <div class="detail-row">
                <label>Created:</label>
                <span>${formatDate(order.created_at)}</span>
            </div>
        </div>
        
        <div class="detail-section">
            <h3>Addresses</h3>
            <div class="detail-row">
                <label>Pickup:</label>
                <span>${order.pickup_address}</span>
            </div>
            <div class="detail-row">
                <label>Delivery:</label>
                <span>${order.delivery_address}</span>
            </div>
        </div>
        
        <div class="detail-section">
            <h3>Package Details</h3>
            <div class="detail-row">
                <label>Weight:</label>
                <span>${order.package_weight || '-'} kg</span>
            </div>
            <div class="detail-row">
                <label>Dimensions:</label>
                <span>${order.package_dimensions || '-'}</span>
            </div>
        </div>
    `;
    
    // Package tracking info
    if (packageInfo && packageInfo.length > 0) {
        const pkg = packageInfo[0];
        html += `
            <div class="detail-section">
                <h3>Warehouse Status</h3>
                <div class="detail-row">
                    <label>Package ID:</label>
                    <span>${pkg.package_id}</span>
                </div>
                <div class="detail-row">
                    <label>Barcode:</label>
                    <span>${pkg.barcode}</span>
                </div>
                <div class="detail-row">
                    <label>Location:</label>
                    <span>${pkg.warehouse_location}</span>
                </div>
                <div class="detail-row">
                    <label>Status:</label>
                    <span><span class="status-badge status-${pkg.status.toLowerCase()}">${pkg.status}</span></span>
                </div>
            </div>
        `;
    }
    
    // Route info
    if (routeInfo) {
        html += `
            <div class="detail-section">
                <h3>Delivery Route</h3>
                <div class="detail-row">
                    <label>Driver:</label>
                    <span>${routeInfo.driver_id || routeInfo.route?.driver_id || '-'}</span>
                </div>
                <div class="detail-row">
                    <label>ETA:</label>
                    <span>${routeInfo.estimated_arrival ? formatDate(routeInfo.estimated_arrival) : '-'}</span>
                </div>
                <div class="detail-row">
                    <label>Stop:</label>
                    <span>${routeInfo.sequence || '-'} of ${routeInfo.total_stops || '-'}</span>
                </div>
            </div>
        `;
    }
    
    // Tracking timeline
    html += `
        <div class="detail-section">
            <h3>Tracking Timeline</h3>
            <div class="tracking-timeline">
                ${getTrackingTimeline(order, packageInfo, routeInfo)}
            </div>
        </div>
    `;
    
    document.getElementById('order-details-content').innerHTML = html;
}

// Get tracking timeline HTML
function getTrackingTimeline(order, packageInfo, routeInfo) {
    const steps = [
        { status: 'Order Placed', completed: true, time: order.created_at },
        { status: 'Processing', completed: ['processing', 'ready', 'loaded', 'in_transit', 'delivered'].includes(order.status) },
        { status: 'Ready for Pickup', completed: ['ready', 'loaded', 'in_transit', 'delivered'].includes(order.status) },
        { status: 'In Transit', completed: ['in_transit', 'delivered'].includes(order.status) },
        { status: 'Delivered', completed: order.status === 'delivered' }
    ];
    
    return steps.map((step, index) => `
        <div class="timeline-item ${step.completed ? 'completed' : ''} ${index === steps.findIndex(s => !s.completed) - 1 ? 'active' : ''}">
            <h4>${step.status}</h4>
            <p>${step.time ? formatDate(step.time) : step.completed ? 'Completed' : 'Pending'}</p>
        </div>
    `).join('');
}

// Show toast notification
function showToast(type, title, message) {
    const container = document.getElementById('toast-container');
    
    const iconClass = type === 'success' ? 'fa-check-circle' : 
                      type === 'error' ? 'fa-exclamation-circle' : 'fa-info-circle';
    
    const toast = document.createElement('div');
    toast.className = `toast ${type}`;
    toast.innerHTML = `
        <i class="fas ${iconClass}"></i>
        <div class="toast-content">
            <strong>${title}</strong>
            <p>${message}</p>
        </div>
    `;
    
    container.appendChild(toast);
    
    // Remove after 5 seconds
    setTimeout(() => {
        toast.style.animation = 'slideIn 0.3s ease reverse';
        setTimeout(() => toast.remove(), 300);
    }, 5000);
}

// Utility functions
function truncateText(text, maxLength) {
    if (!text) return '-';
    return text.length > maxLength ? text.substring(0, maxLength) + '...' : text;
}

function formatDate(dateString) {
    if (!dateString) return '-';
    const date = new Date(dateString);
    return date.toLocaleString('en-US', {
        month: 'short',
        day: 'numeric',
        hour: '2-digit',
        minute: '2-digit'
    });
}

function formatStatus(status) {
    if (!status) return '-';
    return status.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase());
}

// Make viewOrderDetails available globally
window.viewOrderDetails = viewOrderDetails;
