// ============================================================
// Cloudflare Worker + Durable Object
// كل "غرفة اجتماع" بتبقى Durable Object واحد بيستقبل اتصالات
// WebSocket من كل المشاركين، وبيوزع أي رسالة أو تحديث حالة
// على الباقي فوراً (Real-time، مش polling).
// ============================================================

export class Room {
  constructor(state, env) {
    this.state = state;
    this.sessions = new Map(); // ws -> {id, name, speak, listen}
  }

  async fetch(request) {
    const upgradeHeader = request.headers.get("Upgrade");
    if (!upgradeHeader || upgradeHeader.toLowerCase() !== "websocket") {
      return new Response("لازم تتصل عبر WebSocket على /room/<room-id>", { status: 426 });
    }

    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);
    server.accept();
    this.handleSession(server);

    return new Response(null, { status: 101, webSocket: client });
  }

  handleSession(ws) {
    ws.addEventListener("message", (msg) => {
      let data;
      try {
        data = JSON.parse(msg.data);
      } catch (e) {
        return;
      }

      if (data.type === "presence") {
        this.sessions.set(ws, {
          id: data.id,
          name: data.name,
          speak: data.speak,
          listen: data.listen,
        });
        this.broadcastRoster();
      } else if (data.type === "message") {
        this.broadcastToOthers(data, ws);
      } else if (data.type === "leave") {
        this.sessions.delete(ws);
        this.broadcastRoster();
      }
    });

    const cleanup = () => {
      this.sessions.delete(ws);
      this.broadcastRoster();
    };
    ws.addEventListener("close", cleanup);
    ws.addEventListener("error", cleanup);
  }

  broadcastToOthers(data, sender) {
    const payload = JSON.stringify(data);
    for (const ws of this.sessions.keys()) {
      if (ws !== sender) {
        try { ws.send(payload); } catch (e) { /* اتصال مقطوع، هيتنضف عند الـ close */ }
      }
    }
  }

  broadcastRoster() {
    const participants = Array.from(this.sessions.values());
    const payload = JSON.stringify({ type: "roster", participants });
    for (const ws of this.sessions.keys()) {
      try { ws.send(payload); } catch (e) {}
    }
  }
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const match = url.pathname.match(/^\/room\/([a-zA-Z0-9_-]+)$/);

    if (!match) {
      return new Response(
        "استخدم المسار /room/<room-id> مع اتصال WebSocket. مثال: wss://your-worker.workers.dev/room/meeting123",
        { status: 400 }
      );
    }

    const roomId = match[1];
    const id = env.ROOMS.idFromName(roomId);
    const stub = env.ROOMS.get(id);
    return stub.fetch(request);
  },
};
