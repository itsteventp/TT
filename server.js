import express from 'express';
import { createServer } from 'http';
import { Server } from 'socket.io';
import path from 'path';
import { fileURLToPath } from 'url';
import { TikTokLiveClient, EventType } from 'piratetok-live-js';

// A real, modern Chrome user agent to pair with the client hint headers
const FIXED_UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36';

// Intercept global fetch to bypass TikTok's bot detection by mimicking standard browser headers.
// This resolves the common "ttwid: no ttwid cookie in response" error.
console.log("Global fetch interceptor registered!");
const originalFetch = globalThis.fetch;
globalThis.fetch = async function(url, options) {
    const urlString = typeof url === 'string' ? url : (url && url.url ? url.url : '');
    console.log(`[Fetch Interceptor] Request URL: ${urlString}`);
    if (urlString.includes('tiktok.com')) {
        console.log(`[Fetch Interceptor] Intercepting TikTok request: ${urlString}`);
        options = options || {};
        options.headers = options.headers || {};
        
        // Force the fixed User-Agent to match client hints perfectly
        options.headers['User-Agent'] = FIXED_UA;
        options.headers['user-agent'] = FIXED_UA;
        
        options.headers['Accept-Language'] = 'en-US,en;q=0.9';
        options.headers['Sec-Ch-Ua'] = '"Not/A)Brand";v="8", "Chromium";v="126", "Google Chrome";v="126"';
        options.headers['Sec-Ch-Ua-Mobile'] = '?0';
        options.headers['Sec-Ch-Ua-Platform'] = '"Windows"';
        
        if (!urlString.includes('/api-live/') && !urlString.includes('/api/')) {
            options.headers['Accept'] = 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7';
            options.headers['Sec-Fetch-Dest'] = 'document';
            options.headers['Sec-Fetch-Mode'] = 'navigate';
            options.headers['Sec-Fetch-Site'] = 'none';
            options.headers['Sec-Fetch-User'] = '?1';
            options.headers['Upgrade-Insecure-Requests'] = '1';
        } else {
            options.headers['Accept'] = '*/*';
            options.headers['Sec-Fetch-Dest'] = 'empty';
            options.headers['Sec-Fetch-Mode'] = 'cors';
            options.headers['Sec-Fetch-Site'] = 'same-site';
        }
        console.log(`[Fetch Interceptor] Modified options headers:`, JSON.stringify(options.headers, null, 2));
    }
    
    try {
        const response = await originalFetch(url, options);
        if (urlString.includes('tiktok.com')) {
            console.log(`[Fetch Interceptor] TikTok response status: ${response.status}`);
            console.log(`[Fetch Interceptor] Set-Cookie headers:`, response.headers.getSetCookie?.() || response.headers.get('set-cookie'));
        }
        return response;
    } catch (err) {
        if (urlString.includes('tiktok.com')) {
            console.error(`[Fetch Interceptor] TikTok request failed with error:`, err);
        }
        throw err;
    }
};

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();
const server = createServer(app);
const io = new Server(server, {
    cors: {
        origin: '*',
        methods: ['GET', 'POST']
    }
});

const PORT = process.env.PORT || 3000;

// Serve static files
app.use(express.static(__dirname));

// State Management
let giftQueue = [];
let tiktokConnection = null;
let activeUsername = null;
let isMockConnection = false;
let mockIntervals = [];
let ownerSocketId = null;
let connectionState = 'offline'; // 'offline', 'connecting', 'connected', 'error'
let connectionError = null;

// Broadcast status to all sockets, checking owner lock individually
function broadcastStatus() {
    for (const [id, s] of io.sockets.sockets) {
        s.emit('status_change', {
            status: connectionState,
            username: activeUsername,
            error: connectionError,
            isLocked: ownerSocketId !== null && ownerSocketId !== id
        });
    }
}

// Helper to generate unique IDs
function generateId() {
    return Date.now().toString() + '-' + Math.random().toString(36).substring(2, 9);
}

// Clean and extract profile picture URL
function cleanProfilePic(data) {
    // For piratetok-live-js:
    if (data.user?.avatarThumb?.urlList?.[0]) return data.user.avatarThumb.urlList[0];
    if (data.user?.avatarMedium?.urlList?.[0]) return data.user.avatarMedium.urlList[0];
    if (data.user?.avatarLarge?.urlList?.[0]) return data.user.avatarLarge.urlList[0];

    // Fallbacks:
    if (data.profilePictureUrl) return data.profilePictureUrl;
    if (data.user?.profilePictureUrl) return data.user.profilePictureUrl;
    if (data.user?.profilePictureUrls && data.user.profilePictureUrls.length > 0) {
        return data.user.profilePictureUrls[0];
    }
    return '';
}

// Clean and extract gift icon URL
function cleanGiftIcon(data) {
    // For piratetok-live-js:
    if (data.gift?.image?.urlList?.[0]) return data.gift.image.urlList[0];

    // Fallbacks:
    if (data.extendedGiftInfo?.image?.url) return data.extendedGiftInfo.image.url;
    if (data.giftIconUrl) return data.giftIconUrl;
    if (data.gift?.icon?.url_list && data.gift.icon.url_list.length > 0) {
        return data.gift.icon.url_list[0];
    }
    return '';
}

// Gracefully disconnect and clean up mocks
function disconnectTikTok() {
    mockIntervals.forEach(clearInterval);
    mockIntervals = [];
    isMockConnection = false;

    if (tiktokConnection) {
        try {
            tiktokConnection.disconnect();
            console.log(`Disconnected from TikTok Live client: ${activeUsername}`);
        } catch (err) {
            console.error('Error disconnecting from TikTok Live connection:', err);
        }
        tiktokConnection = null;
    }
    activeUsername = null;
    connectionState = 'offline';
    connectionError = null;
}

// Mock Stream Simulation logic (runs if username starts with "mock")
function startMockStream(socket) {
    isMockConnection = true;
    activeUsername = 'MockStream_Live';
    connectionState = 'connected';
    console.log('Starting mock TikTok Live stream simulation...');
    broadcastStatus();

    const mockUsers = [
        { uniqueId: 'aurora_borealis', nickname: 'Aurora ✨', profilePic: 'https://images.unsplash.com/photo-1544005313-94ddf0286df2?auto=format&fit=crop&w=120&h=120&q=80' },
        { uniqueId: 'cyber_runner', nickname: 'Cyber Runner', profilePic: 'https://images.unsplash.com/photo-1507003211169-0a1dd7228f2d?auto=format&fit=crop&w=120&h=120&q=80' },
        { uniqueId: 'pixel_artist', nickname: 'Pixel Artistry 🎨', profilePic: 'https://images.unsplash.com/photo-1534528741775-53994a69daeb?auto=format&fit=crop&w=120&h=120&q=80' },
        { uniqueId: 'chef_mario', nickname: 'Chef Mario 🍕', profilePic: 'https://images.unsplash.com/photo-1500648767791-00dcc994a43e?auto=format&fit=crop&w=120&h=120&q=80' }
    ];

    const mockGifts = [
        { giftName: 'Rose', giftIcon: 'https://cdn-useast1a.tiktokcdn.com/obj/webcast-va/7063462002598363910.png' },
        { giftName: 'TikTok', giftIcon: 'https://cdn-useast1a.tiktokcdn.com/obj/webcast-va/7063462104595204870.png' },
        { giftName: 'Ice Cream', giftIcon: 'https://cdn-useast1a.tiktokcdn.com/obj/webcast-va/7063462203598363910.png' },
        { giftName: 'Double Heart', giftIcon: 'https://cdn-useast1a.tiktokcdn.com/obj/webcast-va/7063462305598363910.png' }
    ];

    const mockComments = [
        'Wow, outstanding livestream! 🔥',
        'Can you see my chat?',
        'Greetings from Austin! 🤠',
        'Amazing! Best stream ever.',
        'Keep going, you got this!',
        'This dashboard is incredibly fast!',
        'Show us the dashboard on screen!',
        'Crazy gifts incoming!'
    ];

    let step = 0;
    const intervalId = setInterval(() => {
        if (!isMockConnection) return;

        step++;
        // Cycle user so they interact sequentially
        const user = mockUsers[step % mockUsers.length];

        // 35% chance of sending a gift, 65% chance of sending chat
        if (Math.random() < 0.35) {
            const gift = mockGifts[Math.floor(Math.random() * mockGifts.length)];
            const isStreak = Math.random() > 0.3;
            // Simulated streak count
            const repeatCount = isStreak ? Math.floor(Math.random() * 4) + 1 : 1;

            handleGiftEvent({
                uniqueId: user.uniqueId,
                nickname: user.nickname,
                profilePictureUrl: user.profilePic,
                giftName: gift.giftName,
                giftIconUrl: gift.giftIcon,
                repeatCount: repeatCount
            });
        } else {
            const comment = mockComments[Math.floor(Math.random() * mockComments.length)];
            handleChatEvent({
                uniqueId: user.uniqueId,
                comment: comment
            });
        }
    }, 2500);

    mockIntervals.push(intervalId);
}

// Core Data Capture: Gift Event
function handleGiftEvent(data) {
    const uniqueId = data.user?.uniqueId || data.uniqueId;
    const nickname = data.user?.nickname || data.nickname || uniqueId;
    const profilePic = cleanProfilePic(data);
    const giftName = data.gift?.name || data.giftName || 'Gift';
    const giftIcon = cleanGiftIcon(data);
    const repeatCount = data.repeatCount || 1;

    if (!uniqueId) return;

    // Check if user has an active, unfinished container in the queue
    let existing = giftQueue.find(item => item.username === uniqueId && !item.done);
    
    if (existing) {
        // Increment their gift count and append gift info if it has changed
        if (existing._lastGiftName !== giftName) {
            // Append name and update icon
            existing.giftName = existing.giftName + ' + ' + giftName;
            existing.giftIcon = giftIcon;
            existing.giftCount += repeatCount;
            existing._lastGiftName = giftName;
            existing._lastRepeatCount = repeatCount;
        } else {
            // Same gift: Calculate cumulative difference to prevent double counting streaks
            let diff = 0;
            if (repeatCount > (existing._lastRepeatCount || 0)) {
                diff = repeatCount - (existing._lastRepeatCount || 0);
            } else {
                // Streak resets or new streak of same gift
                diff = repeatCount;
            }
            existing.giftCount += diff;
            existing._lastRepeatCount = repeatCount;
        }

        io.emit('update_gift', {
            id: existing.id,
            giftName: existing.giftName,
            giftIcon: existing.giftIcon,
            giftCount: existing.giftCount
        });
        console.log(`Updated Gift for ${uniqueId}: Count=${existing.giftCount}, Gifts=${existing.giftName}`);
    } else {
        // Push a new object to the queue
        const newGift = {
            id: generateId(),
            username: uniqueId,
            nickname: nickname,
            profilePic: profilePic,
            giftName: giftName,
            giftIcon: giftIcon,
            giftCount: repeatCount,
            messages: [],
            done: false,
            _lastGiftName: giftName,
            _lastRepeatCount: repeatCount
        };
        giftQueue.push(newGift);
        io.emit('new_gift', newGift);
        console.log(`New Gift Container created for ${uniqueId}: Gift=${giftName}, Count=${repeatCount}`);
    }
}

// Core Data Capture: Chat Event
function handleChatEvent(data) {
    const uniqueId = data.user?.uniqueId || data.uniqueId;
    const comment = data.content || data.comment;

    if (!uniqueId || !comment) return;

    // Check if user has an active, unfinished container in the queue
    let existing = giftQueue.find(item => item.username === uniqueId && !item.done);
    if (existing) {
        existing.messages.push(comment);
        io.emit('incoming_message', {
            id: existing.id,
            message: comment
        });
        console.log(`Message from ${uniqueId} appended: "${comment}"`);
    }
}

// Socket.io Real-time syncing
io.on('connection', (socket) => {
    console.log(`Client socket connected: ${socket.id}`);

    // Sync state immediately upon load
    socket.emit('sync_queue', giftQueue.filter(item => !item.done));
    
    // Sync current status and whether locked for this specific socket
    socket.emit('status_change', {
        status: connectionState,
        username: activeUsername,
        error: connectionError,
        isLocked: ownerSocketId !== null && ownerSocketId !== socket.id
    });

    // Handle Start Stream toggle event
    socket.on('start_stream', async (data) => {
        const username = typeof data === 'object' ? data.username : data;
        const language = typeof data === 'object' ? (data.language || 'en') : 'en';

        if (!username) {
            socket.emit('status_change', { 
                status: 'error', 
                error: 'Username cannot be blank.',
                isLocked: ownerSocketId !== null && ownerSocketId !== socket.id
            });
            return;
        }

        // Lock Guard
        if (ownerSocketId !== null && ownerSocketId !== socket.id) {
            socket.emit('status_change', { 
                status: 'error', 
                error: 'Another user is managing the live connection.',
                isLocked: true
            });
            return;
        }

        disconnectTikTok();
        activeUsername = username;
        ownerSocketId = socket.id;

        // Route to mock simulation if specified
        if (username.toLowerCase() === 'mock' || username.toLowerCase().startsWith('mock_')) {
            isMockConnection = true;
            activeUsername = 'MockStream_Live';
            connectionState = 'connected';
            broadcastStatus();
            startMockStream(socket);
            return;
        }

        console.log(`Starting real direct TikTok Live connection for: ${username} (Language: ${language})`);
        connectionState = 'connecting';
        broadcastStatus();

        tiktokConnection = new TikTokLiveClient(username)
            .userAgent(FIXED_UA)
            .language(language)
            .region(language === 'es' ? 'ES' : 'US');

        // Bind webcast listeners using EventType constants
        tiktokConnection.on(EventType.gift, (data) => {
            handleGiftEvent(data);
        });

        tiktokConnection.on(EventType.chat, (data) => {
            handleChatEvent(data);
        });

        tiktokConnection.on(EventType.disconnected, () => {
            console.log(`TikTok Connection disconnected for user: ${username}`);
            disconnectTikTok();
            ownerSocketId = null;
            broadcastStatus();
        });

        tiktokConnection.on(EventType.reconnecting, () => {
            console.log(`TikTok Connection reconnecting for user: ${username}`);
            connectionState = 'connecting';
            broadcastStatus();
        });

        tiktokConnection.on(EventType.connected, () => {
            console.log(`Connected directly to live stream of: ${username}`);
            connectionState = 'connected';
            broadcastStatus();
        });

        // Connect asynchronously so we don't block the socket handler
        tiktokConnection.connect().catch((err) => {
            console.error(`TikTok direct connection failed:`, err);
            disconnectTikTok();
            ownerSocketId = null;
            connectionState = 'error';
            connectionError = err.message || 'Failed to connect directly';
            broadcastStatus();
        });
    });

    // Handle Stop Stream event
    socket.on('stop_stream', () => {
        if (ownerSocketId !== null && ownerSocketId !== socket.id) return;
        console.log('Stopping active stream tracking.');
        disconnectTikTok();
        ownerSocketId = null;
        broadcastStatus();
    });

    // Handle Dismiss Container event
    socket.on('dismiss_container', (id) => {
        if (ownerSocketId !== null && ownerSocketId !== socket.id) return;
        let existing = giftQueue.find(item => item.id === id);
        if (existing) {
            existing.done = true;
            console.log(`Marked container ${id} as done/resolved.`);
            // Sync other dashboard browsers
            socket.broadcast.emit('container_dismissed', id);
        }
    });

    // Helper: Clear Active Queue
    socket.on('clear_queue', () => {
        if (ownerSocketId !== null && ownerSocketId !== socket.id) return;
        giftQueue = [];
        console.log('Cleared all active queue items.');
        io.emit('queue_cleared');
    });

    socket.on('disconnect', () => {
        console.log(`Client socket disconnected: ${socket.id}`);
        if (socket.id === ownerSocketId) {
            console.log('Owner disconnected. Stopping live connection and releasing lock.');
            disconnectTikTok();
            ownerSocketId = null;
            broadcastStatus();
        }
    });
});

server.listen(PORT, () => {
    console.log(`Express Server is live on http://localhost:${PORT}`);
});
