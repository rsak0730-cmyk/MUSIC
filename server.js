const WebSocket = require('ws');
const https = require('https');
const wss = new WebSocket.Server({ port: process.env.PORT || 8080 });

const rooms = {};

// Native Server-Side Search (Bypasses Browser CORS)
function searchYouTubeBackend(query, callback) {
    const postData = JSON.stringify({
        context: {
            client: {
                clientName: "WEB",
                clientVersion: "2.20230515.00.00"
            }
        },
        query: query
    });

    const options = {
        hostname: 'www.youtube.com',
        path: '/youtubei/v1/search',
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
            'Content-Length': Buffer.byteLength(postData),
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
    };

    const req = https.request(options, (res) => {
        let data = '';
        res.on('data', chunk => data += chunk);
        res.on('end', () => {
            try {
                const parsed = JSON.parse(data);
                const contents = parsed?.contents?.twoColumnSearchResultsRenderer?.primaryContents?.sectionListRenderer?.contents[0]?.itemSectionRenderer?.contents || [];
                
                const results = [];
                for (let item of contents) {
                    if (item.videoRenderer && item.videoRenderer.videoId) {
                        const v = item.videoRenderer;
                        results.push({
                            videoId: v.videoId,
                            title: v.title?.runs?.[0]?.text || "Unknown Title",
                            author: v.ownerText?.runs?.[0]?.text || v.shortBylineText?.runs?.[0]?.text || "YouTube"
                        });
                        if (results.length >= 1) break; // Auto-select top result
                    }
                }
                callback(results);
            } catch (e) {
                callback([]);
            }
        });
    });

    req.on('error', () => callback([]));
    req.write(postData);
    req.end();
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
                
                // Enforce RBAC rules
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
                }, 15000);
            } else {
                room.peers = room.peers.filter(client => client !== ws && client.readyState === WebSocket.OPEN);
                broadcastPeerList(currentRoom);
            }
        }
    });
});

function broadcastPeerList(roomCode) {
    const room = rooms[roomCode]; // Fixed scoped variable error
    if (!room) return;
    if (room.host && room.host.readyState !== WebSocket.OPEN) room.host = null;
    room.peers = room.peers.filter(client => client && client.readyState === WebSocket.OPEN);

    const peersData = [];
    if (room.host) peersData.push({ name: room.host.clientName || 'Host', accountId: room.host.accountId || '', role: 'HOST' });
    room.peers.forEach(p => {
        if (!peersData.some(existing => existing.accountId === p.accountId)) {
            peersData.push({ name: p.clientName || 'Member', accountId: p.accountId || '', role: 'MEMBER' });
        }
    });

    const targets = [room.host, ...room.peers].filter(client => client && client.readyState === WebSocket.OPEN);
    targets.forEach(client => {
        client.send(JSON.stringify({ type: 'PEER_LIST', peers: peersData, count: peersData.length }));
    });
}

console.log('SyncBeat Backend Running...');

