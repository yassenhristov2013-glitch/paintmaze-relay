// Paint Maze relay for hosting online (Render.com, free). Same job and messages as relay/relay.gd:
// it hands out 4-letter lobby codes and passes messages between the players of a lobby.
// The host's game runs the match; this only forwards.
//
// Text (JSON):  {"op":"host"} -> {"op":"hosted","code","id":1}
//               {"op":"join","code"} -> {"op":"joined","code","id"}, host gets {"op":"peer_joined","id"}
//               {"op":"lock"} (host) -> nobody else can join;  {"op":"ping"} keeps the connection alive
//               {"op":"max","n":20} (host) -> how many players this lobby takes (default 4, at most MAX_ROOM; FFA)
// Binary:       [to][data...] -> forwarded as [from][data...] to player "to" (0 = everyone else)
// Leaving: the host gets {"op":"peer_left","id"}; if the host leaves, everyone gets {"op":"host_left"}.

const http = require("http");
const { WebSocketServer } = require("ws");

const PORT = process.env.PORT || 9080;
const MAX_PLAYERS = 4; // a new lobby takes this many (the host can raise it with "max", for FFA)
const MAX_ROOM = 20;
const CODE_CHARS = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"; // no 0/O or 1/I

const rooms = new Map(); // code -> {peers: Map(id -> ws), locked, next, made, max}

// Opening the address in a browser shows this (handy to wake the server up before a game).
const server = http.createServer((req, res) => {
  res.writeHead(200, { "Content-Type": "text/plain" });
  res.end("Paint Maze relay is running. Open lobbies: " + rooms.size + "\n");
});

const wss = new WebSocketServer({ server, maxPayload: 1 << 20 });

function sendJson(ws, data) {
  if (ws.readyState === ws.OPEN) ws.send(JSON.stringify(data));
}

function newCode() {
  for (;;) {
    let code = "";
    for (let i = 0; i < 4; i++) code += CODE_CHARS[Math.floor(Math.random() * CODE_CHARS.length)];
    if (!rooms.has(code)) return code;
  }
}

function newestOpenRoom() {
  let best = "";
  let bestT = -1;
  for (const [code, room] of rooms) {
    if (!room.locked && room.made > bestT) {
      best = code;
      bestT = room.made;
    }
  }
  return best;
}

wss.on("connection", (ws) => {
  ws.info = { room: "", id: 0 };
  ws.isAlive = true;
  ws.on("pong", () => (ws.isAlive = true));

  ws.on("message", (data, isBinary) => {
    const info = ws.info;
    if (!isBinary) {
      let msg;
      try {
        msg = JSON.parse(data.toString());
      } catch {
        return;
      }
      if (typeof msg !== "object" || msg === null) return;
      if (msg.op === "host" && info.room === "") {
        const code = newCode();
        rooms.set(code, { peers: new Map([[1, ws]]), locked: false, next: 2, made: Date.now(), max: MAX_PLAYERS });
        info.room = code;
        info.id = 1;
        sendJson(ws, { op: "hosted", code, id: 1 });
        console.log("lobby", code, "made,", rooms.size, "open");
      } else if (msg.op === "join" && info.room === "") {
        let code = String(msg.code || "").trim().toUpperCase();
        if (code === "*") code = newestOpenRoom();
        const room = rooms.get(code);
        if (!room) return sendJson(ws, { op: "error", msg: "No game with that code." });
        if (room.locked) return sendJson(ws, { op: "error", msg: "That game has already started." });
        if (room.peers.size >= room.max) return sendJson(ws, { op: "error", msg: "That game is full." });
        const id = room.next++;
        room.peers.set(id, ws);
        info.room = code;
        info.id = id;
        sendJson(ws, { op: "joined", code, id });
        sendJson(room.peers.get(1), { op: "peer_joined", id });
        console.log("player", id, "joined", code);
      } else if (msg.op === "max" && info.id === 1 && rooms.has(info.room)) {
        const n = Math.floor(Number(msg.n)) || MAX_PLAYERS;
        rooms.get(info.room).max = Math.min(Math.max(n, 1), MAX_ROOM);
      } else if (msg.op === "lock" && info.id === 1 && rooms.has(info.room)) {
        rooms.get(info.room).locked = true;
      }
      return;
    }
    // Binary: [to][data] -> [from][data]
    const room = rooms.get(info.room);
    if (!room || data.length < 1) return;
    const to = data[0];
    const out = Buffer.from(data);
    out[0] = info.id;
    if (to === 0) {
      for (const [id, peer] of room.peers) {
        if (id !== info.id && peer.readyState === peer.OPEN) peer.send(out, { binary: true });
      }
    } else {
      const peer = room.peers.get(to);
      if (peer && peer.readyState === peer.OPEN) peer.send(out, { binary: true });
    }
  });

  ws.on("close", () => {
    const info = ws.info;
    const room = rooms.get(info.room);
    if (!room) return;
    room.peers.delete(info.id);
    if (info.id === 1) {
      for (const peer of room.peers.values()) sendJson(peer, { op: "host_left" });
      rooms.delete(info.room);
      console.log("lobby", info.room, "closed (host left)");
    } else {
      const host = room.peers.get(1);
      if (host) sendJson(host, { op: "peer_left", id: info.id });
      console.log("player", info.id, "left", info.room);
    }
  });
});

// Drops connections that stopped answering (a closed laptop, lost Wi-Fi), so lobbies don't hang around.
setInterval(() => {
  for (const ws of wss.clients) {
    if (!ws.isAlive) {
      ws.terminate();
      continue;
    }
    ws.isAlive = false;
    ws.ping();
  }
}, 30000);

server.listen(PORT, () => console.log("Paint Maze relay listening on port", PORT));
