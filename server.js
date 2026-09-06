const WebSocket = require('ws');
const https = require('https');
const wss = new WebSocket.Server({ port: process.env.PORT || 8080 });

const rooms = {};

// Native Server-Side Search (Bypasses Browser CORS completely)
function searchYouTubeBackend(query, callback) {
    const url = 'https://www.youtube.com/results?search_query=' + encodeURIComponent(query);
    
    https.get(url, (res) => {
        let data = '';
        res.on('data', chunk => data += chunk);
        res.on('end', () => {
            const results = [];
            // Highly robust Regex to scrape the raw YouTube HTML for the top result
            const regex = /"videoRenderer":\{"videoId":"([^"]+)".*?"title":\{"runs":\[\{"text":"(.*?)"\}\]/g;
            let match;
            
            while ((match = regex.exec(data)) !== null && results.length < 1) { // Grab only the absolute top result
                results.push({
                    videoId: match[1],
                    title: match[2].replace(/\\u0026/g, '&').replace(/\\"/g, '"'),
                    author: 'YouTube'
                });
            }
            callback(results);
        });
    }).on('error', () => {
        callback([]);
    });
}

// Function to safely broadcast peer lists to all members in a specific room
function broadcastPeerList(roomCode) {
    const room = rooms[roomCode]; 
    if (!room) return;
    
    if (room.host && room.host.readyState !== WebSocket.OPEN) room.host = null;
    room.peers = room.peers.filter(client => client && client.readyState === WebSocket.OPEN);

    const peersData = [];
    
    if (room.host) {
        peersData.push({ 
            name: room.host.clientName || 'Host', 
            accountId: room.host.accountId || 'HOST', 
            role: 'HOST' 
        });
    }
    
    room.peers.forEach(p => {
        if (!peersData.some(existing => existing.accountId === p.accountId)) {
            peersData.push({ 
                name: p.clientName || 'Member', 
                accountId: p.accountId || 'MEMBER', 
                role: 'MEMBER' 
            });
        }
    });

    const targets = [room.host, ...room.peers].filter(client => client && client.readyState === WebSocket.OPEN);
    targets.forEach(client => {
        client.send(JSON.stringify({ type: 'PEER_LIST', peers: peersData, count: peersData.length }));
    });
}

wss.on('connection', (ws) => {
    let currentRoom = null;

    ws.on('message', (message) => {
        let data;
        try { data = JSON.parse(message); } catch (e) { return; }

        if (data.type === 'PING') {
            ws.send(JSON.stringify({ type: 'PONG' }));
            return;
        }

        // Intercept search request and execute it locally on the server
        if (data.type === 'SEARCH_YOUTUBE') {
            searchYouTubeBackend(data.query, (results) => {
                if (ws.readyState === WebSocket.OPEN) {
                    ws.send(JSON.stringify({ type: 'SEARCH_RESULTS', results: results }));
                }
            });
            return;
        }

        if (data.type === 'HOST_ROOM') {
            currentRoom = data.code;
            ws.clientName = data.name;
            ws.accountId = data.accountId;
            
            if (!rooms[currentRoom]) rooms[currentRoom] = { host: ws, peers: [] };
            else rooms[currentRoom].host = ws;
            
            ws.send(JSON.stringify({ type: 'ROOM_HOSTED_SUCCESS' }));
            broadcastPeerList(currentRoom);
        } 
        else if (data.type === 'JOIN_ROOM') {
            const roomCode = data.code;
            if (!rooms[roomCode]) {
                ws.send(JSON.stringify({ type: 'ROOM_NOT_FOUND' }));
                return;
            }
            currentRoom = roomCode;
            ws.clientName = data.name;
            ws.accountId = data.accountId;
            
            rooms[roomCode].peers = rooms[roomCode].peers.filter(p => p.accountId !== data.accountId);
            rooms[roomCode].peers.push(ws);
            
            ws.send(JSON.stringify({ type: 'ROOM_JOIN_SUCCESS', code: roomCode }));
            broadcastPeerList(roomCode);
        } 
        else if (data.type === 'ROOM_CLOSED') {
            if (currentRoom && rooms[currentRoom] && rooms[currentRoom].host === ws) {
                const room = rooms[currentRoom];
                [room.host, ...room.peers].forEach(client => {
                    if (client && client.readyState === WebSocket.OPEN) client.send(JSON.stringify({ type: 'ROOM_CLOSED' }));
                });
                delete rooms[currentRoom];
                currentRoom = null;
            }
        } 
        else {
            if (currentRoom && rooms[currentRoom]) {
                const room = rooms[currentRoom];
                const isSenderHost = (room.host === ws);
                
                const hostOnlyTypes = ['PLAY_YOUTUBE', 'CONTROL', 'VOLUME', 'SYNC_TIME'];
                if (hostOnlyTypes.includes(data.type) && !isSenderHost) return; 

                const targets = [room.host, ...room.peers].filter(client => client && client !== ws && client.readyState === WebSocket.OPEN);
                targets.forEach(client => client.send(JSON.stringify(data)));
            }
        }
    });

    ws.on('close', () => {
        if (currentRoom && rooms[currentRoom]) {
            const room = rooms[currentRoom];
            if (room.host === ws) {
                room.host = null; 
                setTimeout(() => {
                    if (rooms[currentRoom] && rooms[currentRoom].host === null) {
                        const targets = [...rooms[currentRoom].peers].filter(client => client && client.readyState === WebSocket.OPEN);
                        targets.forEach(client => client.send(JSON.stringify({ type: 'ROOM_CLOSED' })));
                        delete rooms[currentRoom];
                    }
                }, 10000);
            } else {
                room.peers = room.peers.filter(client => client !== ws && client.readyState === WebSocket.OPEN);
                broadcastPeerList(currentRoom);
            }
        }
    });
});

console.log('SyncBeat Backend Running...');

