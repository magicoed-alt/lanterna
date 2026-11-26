/* Luzes da Plateia – rede em tempo real (MQTT leve por WebSocket, sem bibliotecas)
   Eduardo Braz · Digital Magic */
(function () {
  var enc = new TextEncoder(), dec = new TextDecoder();

  function strBytes(s) {
    var b = enc.encode(s), o = new Uint8Array(b.length + 2);
    o[0] = b.length >> 8; o[1] = b.length & 255; o.set(b, 2); return o;
  }
  function lenBytes(n) {
    var a = [];
    do { var d = n % 128; n = Math.floor(n / 128); if (n > 0) d |= 128; a.push(d); } while (n > 0);
    return a;
  }
  function pacote(tipo, partes) {
    var total = 0; partes.forEach(function (p) { total += p.length; });
    var lb = lenBytes(total), o = new Uint8Array(1 + lb.length + total);
    o[0] = tipo; o.set(lb, 1);
    var i = 1 + lb.length; partes.forEach(function (p) { o.set(p, i); i += p.length; });
    return o;
  }
  function juntar(a, b) { var o = new Uint8Array(a.length + b.length); o.set(a, 0); o.set(b, a.length); return o; }

  function Conexao(url, aoMsg, aoEstado) {
    this.url = url; this.aoMsg = aoMsg; this.aoEstado = aoEstado || function () {};
    this.topicos = []; this.ok = false; this.buf = new Uint8Array(0);
    this.espera = 800; this.pid = 1; this.ws = null;
    this.id = 'ed' + Math.random().toString(36).slice(2, 12);
    this.abrir();
  }
  Conexao.prototype.abrir = function () {
    var s = this, ws;
    clearTimeout(s.tRe);
    try { ws = new WebSocket(s.url, ['mqtt']); } catch (e) { s.reabrir(); return; }
    s.ws = ws; ws.binaryType = 'arraybuffer'; s.buf = new Uint8Array(0);
    ws.onopen = function () {
      if (ws !== s.ws) return;
      // CONNECT: MQTT 3.1.1, sessão limpa, keepalive 20 s
      var vh = juntar(strBytes('MQTT'), new Uint8Array([4, 2, 0, 20]));
      s.enviar(pacote(0x10, [vh, strBytes(s.id)]));
    };
    ws.onmessage = function (e) {
      if (ws !== s.ws) return;
      s.ultRx = Date.now();
      s.buf = juntar(s.buf, new Uint8Array(e.data)); s.ler();
    };
    ws.onclose = ws.onerror = function () { if (ws === s.ws) s.cair(); };
    clearTimeout(s.tConn);
    s.tConn = setTimeout(function () { if (!s.ok && ws === s.ws) s.cair(); }, 8000);
  };
  Conexao.prototype.cair = function () {
    var s = this;
    if (s.ok) { s.ok = false; s.aoEstado(false); }
    clearInterval(s.tPing); clearTimeout(s.tConn);
    var w = s.ws; s.ws = null;
    if (w) { w.onclose = w.onerror = w.onmessage = w.onopen = null; try { w.close(); } catch (e) {} }
    s.reabrir();
  };
  Conexao.prototype.reabrir = function () {
    var s = this; clearTimeout(s.tRe);
    s.tRe = setTimeout(function () { s.abrir(); }, s.espera);
    s.espera = Math.min(s.espera * 2, 8000);
  };
  // chamado quando a página volta a aparecer: reconecta na hora se caiu
  Conexao.prototype.acordar = function (forcar) {
    if (forcar || !this.ws || this.ws.readyState > 1 || Date.now() - (this.ultRx || 0) > 20000) {
      this.espera = 300; this.cair(); clearTimeout(this.tRe); this.abrir();
    }
  };
  Conexao.prototype.ler = function () {
    var s = this;
    while (s.buf.length >= 2) {
      var mult = 1, val = 0, i = 1, b;
      do {
        if (i >= s.buf.length) return;
        b = s.buf[i++]; val += (b & 127) * mult; mult *= 128;
        if (i > 5) { s.cair(); return; }
      } while (b & 128);
      if (s.buf.length < i + val) return;
      var tipo = s.buf[0], corpo = s.buf.slice(i, i + val);
      s.buf = s.buf.slice(i + val);
      s.tratar(tipo, corpo);
    }
  };
  Conexao.prototype.tratar = function (tipo, c) {
    var s = this, t = tipo >> 4;
    if (t === 2) { // CONNACK
      clearTimeout(s.tConn);
      if (c[1] !== 0) { s.cair(); return; }
      s.ok = true; s.espera = 800; s.aoEstado(true);
      s.topicos.forEach(function (tp) { s.mandarInscricao(tp); });
      clearInterval(s.tPing);
      s.ultRx = Date.now();
      s.tPing = setInterval(function () {
        if (Date.now() - s.ultRx > 20000) { s.espera = 300; s.cair(); return; }
        s.enviar(new Uint8Array([0xC0, 0]));
      }, 8000);
    } else if (t === 3) { // PUBLISH
      var qos = (tipo >> 1) & 3, ret = tipo & 1;
      var tl = (c[0] << 8) | c[1];
      var topico = dec.decode(c.slice(2, 2 + tl)), p = 2 + tl;
      if (qos > 0) { s.enviar(new Uint8Array([0x40, 2, c[p], c[p + 1]])); p += 2; }
      var txt = dec.decode(c.slice(p));
      try { s.aoMsg(topico, txt, !!ret); } catch (e) { console.error(e); }
    }
  };
  Conexao.prototype.enviar = function (b) {
    try { if (this.ws && this.ws.readyState === 1) { this.ws.send(b); return true; } } catch (e) {}
    return false;
  };
  Conexao.prototype.mandarInscricao = function (tp) {
    var id = (this.pid++ & 0xffff) || 1; this.pid = id + 1;
    this.enviar(pacote(0x82, [new Uint8Array([id >> 8, id & 255]), strBytes(tp), new Uint8Array([0])]));
  };
  Conexao.prototype.inscrever = function (tp) {
    if (this.topicos.indexOf(tp) < 0) this.topicos.push(tp);
    if (this.ok) this.mandarInscricao(tp);
  };
  Conexao.prototype.publicar = function (tp, txt, reter) {
    if (!this.ok) return false;
    return this.enviar(pacote(reter ? 0x31 : 0x30, [strBytes(tp), enc.encode(txt)]));
  };

  // Vários servidores ao mesmo tempo: se um cair, os outros continuam.
  window.SERVIDORES_PADRAO = [
    'wss://broker.emqx.io:8084/mqtt',
    'wss://broker.hivemq.com:8884/mqtt',
    'wss://test.mosquitto.org:8081/mqtt'
  ];
  window.Rede = function (servidores, aoMsg, aoEstado) {
    var cons = servidores.map(function (u, i) {
      return new Conexao(u, function (tp, txt, ret) { aoMsg(tp, txt, ret, i); },
        function (ok) { if (aoEstado) aoEstado(i, ok); });
    });
    return {
      conexoes: cons,
      inscrever: function (tp) { cons.forEach(function (c) { c.inscrever(tp); }); },
      publicar: function (tp, txt, reter) {
        var n = 0; cons.forEach(function (c) { if (c.publicar(tp, txt, reter)) n++; }); return n;
      },
      acordar: function (forcar) { cons.forEach(function (c) { c.acordar(forcar); }); },
      quantas: function () { return cons.filter(function (c) { return c.ok; }).length; }
    };
  };

  // Ao voltar do segundo plano, o iPhone pode ter matado a conexão sem avisar: reconecta na hora.
  window.aoVoltar = function (rede) {
    var saiu = 0;
    document.addEventListener('visibilitychange', function () {
      if (document.visibilityState === 'hidden') { saiu = Date.now(); return; }
      rede.acordar(saiu && Date.now() - saiu > 2000);
    });
    window.addEventListener('pageshow', function (e) { if (e.persisted) rede.acordar(true); });
    window.addEventListener('online', function () { rede.acordar(true); });
  };

  // Gesto padrão dos apps do Eduardo: dois dedos arrastando para baixo abre o menu.
  window.gestoDoisDedos = function (abrir) {
    var y0 = null;
    document.addEventListener('touchstart', function (e) {
      if (e.touches.length === 2) y0 = (e.touches[0].clientY + e.touches[1].clientY) / 2; else if (e.touches.length > 2) y0 = null;
    }, { passive: true });
    document.addEventListener('touchmove', function (e) {
      if (y0 === null || e.touches.length !== 2) return;
      var y = (e.touches[0].clientY + e.touches[1].clientY) / 2;
      if (y - y0 > 70) { y0 = null; abrir(); }
    }, { passive: true });
    document.addEventListener('touchend', function (e) { if (e.touches.length < 2) y0 = null; }, { passive: true });
  };
})();
