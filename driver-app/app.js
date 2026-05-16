/**
 * SwiftTrack Driver App - JavaScript
 * Handles driver authentication, route management, delivery updates, and real-time notifications
 */

// Configuration
const API_URL = window.location.hostname === 'localhost' ? 'http://localhost:3000' : '';
const WS_URL = window.location.hostname === 'localhost' 
    ? 'ws://localhost:3000' 
    : `ws://${window.location.host}`;

// State
let token = localStorage.getItem('swifttrack_driver_token');
let driver = JSON.parse(localStorage.getItem('swifttrack_driver') || 'null');
let todayRoute = null;
let currentDelivery = null;
let ws = null;
let signatureCanvas = null;
let signatureCtx = null;
let isDrawing = false;

// DOM Elements
const loginPage = document.getElementById('login-page');
const dashboardPage = document.getElementById('dashboard-page');
const loginForm = document.getElementById('login-form');
const deliveryModal = document.getElementById('delivery-modal');
const proofModal = document.getElementById('proof-modal');
const failedModal = document.getElementById('failed-modal');
const deliveryList = document.getElementById('delivery-list');
const notificationsList = document.getElementById('notifications-list');
const connectionStatus = document.getElementById('connection-status');

// Initialize
document.addEventListener('DOMContentLoaded', () => {
    if (token && driver) {
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
    
    // Refresh route
    document.getElementById('refresh-route').addEventListener('click', loadTodayRoute);
    
    // Close modals
    document.getElementById('close-delivery-modal').addEventListener('click', () => {
        deliveryModal.classList.remove('active');
    });
    
    document.getElementById('close-proof-modal').addEventListener('click', () => {
        proofModal.classList.remove('active');
    });
    
    document.getElementById('close-failed-modal').addEventListener('click', () => {
        failedModal.classList.remove('active');
    });
    
    // Delivery actions
    document.getElementById('delivery-complete-btn').addEventListener('click', () => {
        deliveryModal.classList.remove('active');
        showProofModal();
    });
    
    document.getElementById('delivery-failed-btn').addEventListener('click', () => {
        deliveryModal.classList.remove('active');
        showFailedModal();
    });
    
    // Proof modal
    document.getElementById('cancel-proof').addEventListener('click', () => {
        proofModal.classList.remove('active');
    });
    
    document.getElementById('submit-proof').addEventListener('click', handleDeliveryComplete);
    document.getElementById('clear-signature').addEventListener('click', clearSignature);
    
    // Failed modal
    document.getElementById('cancel-failed').addEventListener('click', () => {
        failedModal.classList.remove('active');
    });
    
    document.getElementById('submit-failed').addEventListener('click', handleDeliveryFailed);
    
    // Setup signature pad
    setupSignaturePad();
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
    
    // Update driver info
    updateDriverInfo();
    
    // Connect WebSocket
    connectWebSocket();
    
    // Load today's route
    loadTodayRoute();
}

// Update driver info display
function updateDriverInfo() {
    document.getElementById('driver-name').textContent = driver.name;
    document.getElementById('driver-full-name').textContent = driver.name;
    document.getElementById('driver-vehicle').innerHTML = `<i class="fas fa-car"></i> ${driver.vehicle}`;
}

// Handle login
async function handleLogin(e) {
    e.preventDefault();
    
    const email = document.getElementById('email').value;
    const password = document.getElementById('password').value;
    
    try {
        const response = await fetch(`${API_URL}/api/auth/driver/login`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ email, password })
        });
        
        const data = await response.json();
        
        if (data.success) {
            token = data.token;
            driver = data.driver;
            
            localStorage.setItem('swifttrack_driver_token', token);
            localStorage.setItem('swifttrack_driver', JSON.stringify(driver));
            
            showToast('success', 'Login Successful', `Welcome, ${driver.name}!`);
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
    driver = null;
    localStorage.removeItem('swifttrack_driver_token');
    localStorage.removeItem('swifttrack_driver');
    
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
        
        // Register as driver
        ws.send(JSON.stringify({
            type: 'register_driver',
            driver_id: driver.id
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

// Update connection status
function updateConnectionStatus(connected) {
    const statusEl = document.getElementById('connection-status');
    if (connected) {
        statusEl.className = 'connection-status';
        statusEl.innerHTML = '<i class="fas fa-circle"></i>';
    } else {
        statusEl.className = 'connection-status disconnected';
        statusEl.innerHTML = '<i class="fas fa-circle"></i>';
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
            addNotification('success', 'Ready to receive updates');
            break;
            
        case 'notification':
            handleNotification(data);
            break;
            
        case 'delivery_update':
            if (data.driver_id === driver.id) {
                addNotification('info', `Delivery ${data.order_id}: ${data.status}`);
                loadTodayRoute();
            }
            break;
    }
}

// Handle notifications
function handleNotification(notification) {
    const eventType = notification.event_type;
    const eventData = notification.data || {};
    
    let message = '';
    let type = 'info';
    
    switch (eventType) {
        case 'ROUTE_CREATED':
            message = 'New route assigned to you!';
            type = 'success';
            loadTodayRoute();
            break;
            
        case 'ROUTE_UPDATED':
            message = 'Your route has been updated';
            type = 'warning';
            loadTodayRoute();
            break;
            
        case 'ORDER_CREATED':
            if (eventData.driver_id === driver.id) {
                message = 'New delivery added to your route';
                type = 'info';
                loadTodayRoute();
            }
            break;
            
        default:
            message = `Update: ${eventType}`;
    }
    
    if (message) {
        addNotification(type, message);
    }
}

// Add notification to list
function addNotification(type, message) {
    const iconClass = type === 'success' ? 'fa-check-circle' : 
                      type === 'error' ? 'fa-exclamation-circle' : 
                      type === 'warning' ? 'fa-exclamation-triangle' : 'fa-info-circle';
    
    const notification = document.createElement('div');
    notification.className = `notification-item ${type}`;
    notification.innerHTML = `
        <i class="fas ${iconClass}"></i>
        <span>${message}</span>
    `;
    
    // Remove waiting message
    const waitingMsg = notificationsList.querySelector('.notification-item:first-child');
    if (waitingMsg && waitingMsg.textContent.includes('Waiting')) {
        waitingMsg.remove();
    }
    
    notificationsList.insertBefore(notification, notificationsList.firstChild);
    
    // Keep only last 10 notifications
    while (notificationsList.children.length > 10) {
        notificationsList.removeChild(notificationsList.lastChild);
    }
}

// Load today's route
async function loadTodayRoute() {
    try {
        const response = await fetch(`${API_URL}/api/driver/route/today`, {
            headers: {
                'Authorization': `Bearer ${token}`
            }
        });
        
        const data = await response.json();
        
        if (data.success && data.route) {
            todayRoute = data.route;
            renderRoute();
        } else {
            todayRoute = null;
            renderEmptyRoute();
        }
        
        updateStats();
    } catch (error) {
        console.error('Load route error:', error);
        showToast('error', 'Error', 'Failed to load route');
    }
}

// Render route
function renderRoute() {
    if (!todayRoute || !todayRoute.stops || todayRoute.stops.length === 0) {
        renderEmptyRoute();
        return;
    }
    
    // Update route info
    document.getElementById('route-distance').textContent = todayRoute.total_distance || 0;
    document.getElementById('route-duration').textContent = todayRoute.estimated_duration || 0;
    document.getElementById('route-packages').textContent = todayRoute.stops.length;
    
    // Render delivery items
    deliveryList.innerHTML = todayRoute.stops.map((stop, index) => {
        const isCompleted = stop.status === 'delivered';
        const isFailed = stop.status === 'failed';
        const statusClass = isCompleted ? 'completed' : isFailed ? 'failed' : '';
        
        return `
            <div class="delivery-item" onclick="showDeliveryDetails('${stop.order_id}')">
                <div class="delivery-sequence ${statusClass}">${index + 1}</div>
                <div class="delivery-details">
                    <h4>
                        ${stop.order_id}
                        ${stop.priority && stop.priority !== 'normal' ? 
                            `<span class="priority-badge priority-${stop.priority}">${stop.priority}</span>` : ''}
                    </h4>
                    <p>${stop.address || 'Address not available'}</p>
                    ${stop.estimated_arrival ? `
                        <div class="delivery-eta">
                            <i class="fas fa-clock"></i> ETA: ${formatTime(stop.estimated_arrival)}
                        </div>
                    ` : ''}
                </div>
                ${!isCompleted && !isFailed ? `
                    <div class="delivery-actions">
                        <button class="delivery-action-btn navigate" onclick="event.stopPropagation(); navigateToDelivery('${stop.lat}', '${stop.lng}')">
                            <i class="fas fa-directions"></i>
                        </button>
                    </div>
                ` : `
                    <span class="status-badge status-${stop.status}">${stop.status}</span>
                `}
            </div>
        `;
    }).join('');
}

// Render empty route
function renderEmptyRoute() {
    document.getElementById('route-distance').textContent = '0';
    document.getElementById('route-duration').textContent = '0';
    document.getElementById('route-packages').textContent = '0';
    
    deliveryList.innerHTML = `
        <div class="empty-state">
            <i class="fas fa-route"></i>
            <p>No deliveries scheduled for today</p>
        </div>
    `;
}

// Update stats
function updateStats() {
    if (!todayRoute || !todayRoute.stops) {
        document.getElementById('total-stops').textContent = '0';
        document.getElementById('pending-stops').textContent = '0';
        document.getElementById('completed-stops').textContent = '0';
        return;
    }
    
    const total = todayRoute.stops.length;
    const completed = todayRoute.stops.filter(s => s.status === 'delivered' || s.status === 'failed').length;
    const pending = total - completed;
    
    document.getElementById('total-stops').textContent = total;
    document.getElementById('pending-stops').textContent = pending;
    document.getElementById('completed-stops').textContent = completed;
}

// Show delivery details
function showDeliveryDetails(orderId) {
    const stop = todayRoute?.stops?.find(s => s.order_id === orderId);
    if (!stop) return;
    
    currentDelivery = stop;
    
    const detailsHtml = `
        <div class="delivery-detail-row">
            <label>Order ID:</label>
            <span><strong>${stop.order_id}</strong></span>
        </div>
        <div class="delivery-detail-row">
            <label>Address:</label>
            <span>${stop.address || 'Not available'}</span>
        </div>
        <div class="delivery-detail-row">
            <label>Status:</label>
            <span><span class="status-badge status-${stop.status || 'pending'}">${stop.status || 'Pending'}</span></span>
        </div>
        <div class="delivery-detail-row">
            <label>Sequence:</label>
            <span>#${stop.sequence} of ${todayRoute.stops.length}</span>
        </div>
        ${stop.estimated_arrival ? `
            <div class="delivery-detail-row">
                <label>ETA:</label>
                <span>${formatTime(stop.estimated_arrival)}</span>
            </div>
        ` : ''}
        ${stop.priority ? `
            <div class="delivery-detail-row">
                <label>Priority:</label>
                <span><span class="priority-badge priority-${stop.priority}">${stop.priority}</span></span>
            </div>
        ` : ''}
    `;
    
    document.getElementById('delivery-details').innerHTML = detailsHtml;
    
    // Show/hide action buttons based on status
    const completeBtn = document.getElementById('delivery-complete-btn');
    const failedBtn = document.getElementById('delivery-failed-btn');
    
    if (stop.status === 'delivered' || stop.status === 'failed') {
        completeBtn.style.display = 'none';
        failedBtn.style.display = 'none';
    } else {
        completeBtn.style.display = 'flex';
        failedBtn.style.display = 'flex';
    }
    
    deliveryModal.classList.add('active');
}

// Navigate to delivery
function navigateToDelivery(lat, lng) {
    const url = `https://www.google.com/maps/dir/?api=1&destination=${lat},${lng}`;
    window.open(url, '_blank');
}

// Show proof of delivery modal
function showProofModal() {
    proofModal.classList.add('active');
    setupSignaturePad();
}

// Show failed delivery modal
function showFailedModal() {
    failedModal.classList.add('active');
}

// Setup signature pad
function setupSignaturePad() {
    signatureCanvas = document.getElementById('signature-pad');
    if (!signatureCanvas) return;
    
    signatureCtx = signatureCanvas.getContext('2d');
    
    // Clear canvas
    signatureCtx.fillStyle = '#ffffff';
    signatureCtx.fillRect(0, 0, signatureCanvas.width, signatureCanvas.height);
    
    // Set up drawing
    signatureCanvas.addEventListener('mousedown', startDrawing);
    signatureCanvas.addEventListener('mousemove', draw);
    signatureCanvas.addEventListener('mouseup', stopDrawing);
    signatureCanvas.addEventListener('mouseout', stopDrawing);
    
    // Touch events
    signatureCanvas.addEventListener('touchstart', handleTouchStart);
    signatureCanvas.addEventListener('touchmove', handleTouchMove);
    signatureCanvas.addEventListener('touchend', stopDrawing);
}

function startDrawing(e) {
    isDrawing = true;
    signatureCtx.beginPath();
    signatureCtx.moveTo(e.offsetX, e.offsetY);
}

function draw(e) {
    if (!isDrawing) return;
    signatureCtx.lineTo(e.offsetX, e.offsetY);
    signatureCtx.strokeStyle = '#000';
    signatureCtx.lineWidth = 2;
    signatureCtx.stroke();
}

function stopDrawing() {
    isDrawing = false;
}

function handleTouchStart(e) {
    e.preventDefault();
    const touch = e.touches[0];
    const rect = signatureCanvas.getBoundingClientRect();
    const x = touch.clientX - rect.left;
    const y = touch.clientY - rect.top;
    
    isDrawing = true;
    signatureCtx.beginPath();
    signatureCtx.moveTo(x, y);
}

function handleTouchMove(e) {
    e.preventDefault();
    if (!isDrawing) return;
    
    const touch = e.touches[0];
    const rect = signatureCanvas.getBoundingClientRect();
    const x = touch.clientX - rect.left;
    const y = touch.clientY - rect.top;
    
    signatureCtx.lineTo(x, y);
    signatureCtx.strokeStyle = '#000';
    signatureCtx.lineWidth = 2;
    signatureCtx.stroke();
}

function clearSignature() {
    if (signatureCtx && signatureCanvas) {
        signatureCtx.fillStyle = '#ffffff';
        signatureCtx.fillRect(0, 0, signatureCanvas.width, signatureCanvas.height);
    }
}

// Handle delivery complete
async function handleDeliveryComplete() {
    if (!currentDelivery) return;
    
    const recipientName = document.getElementById('recipient-name').value;
    const notes = document.getElementById('delivery-notes').value;
    const signatureData = signatureCanvas ? signatureCanvas.toDataURL() : null;
    
    try {
        const response = await fetch(`${API_URL}/api/driver/delivery/${currentDelivery.order_id}`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'Authorization': `Bearer ${token}`
            },
            body: JSON.stringify({
                status: 'delivered',
                recipient: recipientName,
                signature: signatureData,
                notes: notes
            })
        });
        
        const data = await response.json();
        
        if (data.success) {
            showToast('success', 'Delivery Complete', `Order ${currentDelivery.order_id} marked as delivered`);
            proofModal.classList.remove('active');
            
            // Clear form
            document.getElementById('recipient-name').value = '';
            document.getElementById('delivery-notes').value = '';
            clearSignature();
            
            // Reload route
            loadTodayRoute();
        } else {
            showToast('error', 'Error', data.message || 'Failed to update delivery');
        }
    } catch (error) {
        console.error('Delivery complete error:', error);
        showToast('error', 'Connection Error', 'Failed to update delivery');
    }
}

// Handle delivery failed
async function handleDeliveryFailed() {
    if (!currentDelivery) return;
    
    const reason = document.getElementById('failure-reason').value;
    const notes = document.getElementById('failure-notes').value;
    
    if (!reason) {
        showToast('warning', 'Required', 'Please select a failure reason');
        return;
    }
    
    try {
        const response = await fetch(`${API_URL}/api/driver/delivery/${currentDelivery.order_id}`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'Authorization': `Bearer ${token}`
            },
            body: JSON.stringify({
                status: 'failed',
                reason: reason,
                notes: notes
            })
        });
        
        const data = await response.json();
        
        if (data.success) {
            showToast('info', 'Delivery Failed', `Order ${currentDelivery.order_id} marked as failed`);
            failedModal.classList.remove('active');
            
            // Clear form
            document.getElementById('failure-reason').value = '';
            document.getElementById('failure-notes').value = '';
            
            // Reload route
            loadTodayRoute();
        } else {
            showToast('error', 'Error', data.message || 'Failed to update delivery');
        }
    } catch (error) {
        console.error('Delivery failed error:', error);
        showToast('error', 'Connection Error', 'Failed to update delivery');
    }
}

// Show toast notification
function showToast(type, title, message) {
    const container = document.getElementById('toast-container');
    
    const iconClass = type === 'success' ? 'fa-check-circle' : 
                      type === 'error' ? 'fa-exclamation-circle' : 
                      type === 'warning' ? 'fa-exclamation-triangle' : 'fa-info-circle';
    
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
    
    // Remove after 4 seconds
    setTimeout(() => {
        toast.style.animation = 'slideUp 0.3s ease reverse';
        setTimeout(() => toast.remove(), 300);
    }, 4000);
}

// Utility functions
function formatTime(dateString) {
    if (!dateString) return '-';
    const date = new Date(dateString);
    return date.toLocaleTimeString('en-US', {
        hour: '2-digit',
        minute: '2-digit'
    });
}

// Make functions available globally
window.showDeliveryDetails = showDeliveryDetails;
window.navigateToDelivery = navigateToDelivery;
