const WebSocket = require('ws');
const wss = new WebSocket.Server({ port: process.env.PORT || 8080 });

const rooms = {};

// Broadcasts the live peer list to everyone in the room
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
    
    const seenAccounts = new Set();
    room.peers.forEach(p => {
        if (!seenAccounts.has(p.accountId)) {
            seenAccounts.add(p.accountId);
            peersData.push({ 
                name: p.clientName || 'Member', 
                accountId: p.accountId || 'MEMBER', 
                role: 'MEMBER' 
            });
        }
    });

    const targets = [room.host, ...room.peers].filter(client => client && client.readyState === WebSocket.OPEN);
    const payload = JSON.stringify({ type: 'PEER_LIST', peers: peersData, count: peersData.length });
    
    targets.forEach(client => client.send(payload));
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
                
                // Cloud Sync Rule: Only Host can alter playback state
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

console.log('SyncBeat Core Relay Backend Running...');

