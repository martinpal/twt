package main

import (
  "encoding/hex"
  "flag"
  "fmt"
  log "github.com/sirupsen/logrus"
  "io"
  "net"
  "net/http"
  "os"
  "path"
  pool "github.com/silenceper/pool"
  proto "github.com/golang/protobuf/proto"
  "runtime"
  "strconv"
  "strings"
  "sync"
  "time"
)

const Proxyid = 0
const ChunkSize = 1280

var app *App

type Handler func (http.ResponseWriter, *http.Request)

type Connection struct {
  Connection net.Conn
  LastSeqIn uint64                                // last sequence number we have used for the last incoming chunk
  NextSeqOut uint64                               // next sequence number to be sent out
  MessageQueue map[uint64]ProxyComm               // queue of messages with too high sequence numbers
}

type App struct {
  ListenPort int
  PeerHost string
  PeerPort int
  Ping bool
  DefaultRoute Handler
  ConnectionPool pool.Pool
  LocalConnectionMutex sync.Mutex
  LastLocalConnection uint64
  LocalConnections map[uint64]Connection
}

func NewApp(f Handler, listenPort int, peerHost string, peerPort int, poolInit int, poolCap int, ping bool) *App {
  app := &App{
    ListenPort: listenPort,
    PeerHost: peerHost,
    PeerPort: peerPort,
    Ping: ping,
    DefaultRoute: f,
    LocalConnections: make(map[uint64]Connection),
  }

  log.Debug("Creating connection pool")

  pingFunc := func(v interface{}) error { return nil }
  if ping {
    pingFunc = func(v interface{}) error {
      pingMessage := &ProxyComm {
        Mt: ProxyComm_PING,
        Proxy: Proxyid,
      }
      sendProtobufToConn(v.(net.Conn), pingMessage)
      return nil
    }
  }
  factory := func() (interface{}, error) {
    log.Tracef("Connecting to %s:%d", app.PeerHost, app.PeerPort)
    conn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", app.PeerHost, app.PeerPort))
    if err == nil {
      pingFunc(conn)
    } else {
      log.Errorf("Error connecting to %s:%d", app.PeerHost, app.PeerPort)
    }
    return conn, err
  }
  close := func(v interface{}) error { return v.(net.Conn).Close() }
  poolConfig := &pool.Config{
    InitialCap: poolInit,
    MaxIdle:    poolCap,
    MaxCap:     poolCap,
    Factory:    factory,
    Close:      close,
    Ping:       pingFunc,
//    IdleTimeout: 15 * 60 * time.Second,
  }
  p, err := pool.NewChannelPool(poolConfig)
  if err != nil {
    fmt.Println("err=", err)
  }
  if p == nil {
    log.Infof("p= %#v\n", p)
    return nil
  }
  app.ConnectionPool = p
  log.Debugf("Current conn pool len: %d\n", app.ConnectionPool.Len())

  return app
}

func (a *App) ServeHTTP(w http.ResponseWriter, r *http.Request) {
  a.DefaultRoute(w, r)
}

func Hijack (w http.ResponseWriter, r *http.Request) {
  parsedHost:= strings.Split(r.Host, ":")
  host := parsedHost[0]
  port, _ := strconv.Atoi(parsedHost[1])
  log.Infof("Method %s, Host %s:%d\n", r.Method, host, port)
  w.WriteHeader(http.StatusOK)
  hj, ok := w.(http.Hijacker)
  if !ok {
    http.Error(w, "webserver doesn't support hijacking", http.StatusInternalServerError)
    return
  }
  conn, bufrw, err := hj.Hijack()
  if err != nil {
    http.Error(w, err.Error(), http.StatusInternalServerError)
    return
  }
  app.LocalConnectionMutex.Lock()
  thisConnection := app.LastLocalConnection
  app.LastLocalConnection++
  app.LocalConnections[thisConnection] = Connection { Connection: conn, LastSeqIn: 0, MessageQueue: make(map[uint64]ProxyComm) }
  app.LocalConnectionMutex.Unlock()

  connectMessage := &ProxyComm {
    Mt: ProxyComm_OPEN_CONN,
    Proxy: Proxyid,
    Connection: thisConnection,
    Seq: 0,
    Address: host,
    Port: uint32(port),
  }
  sendProtobuf(connectMessage)

  for {
    b := make([]byte, ChunkSize)
    n, err := bufrw.Read(b)
    if err != nil {
      log.Infof("Error reading local connection: %v, going to send CLOSE_CONN_S message to remote end", err)
      app.LocalConnectionMutex.Lock()
      connRecord := app.LocalConnections[thisConnection]
      connRecord.LastSeqIn++
      app.LocalConnections[thisConnection] = connRecord
      delete(app.LocalConnections, thisConnection)
      app.LocalConnectionMutex.Unlock()
      closeMessage := &ProxyComm {
        Mt: ProxyComm_CLOSE_CONN_S,
        Proxy: Proxyid,
        Connection: thisConnection,
        Seq: connRecord.LastSeqIn,
      }
      sendProtobuf(closeMessage)
      return
    }
    if n > 0 {
      log.Tracef("Read %d bytes, hexdump", n)
      log.Tracef("%s", hex.Dump(b[:n]))
      app.LocalConnectionMutex.Lock()
      connRecord := app.LocalConnections[thisConnection]
      connRecord.LastSeqIn++
      app.LocalConnections[thisConnection] = connRecord
      app.LocalConnectionMutex.Unlock()
      dataMessage := &ProxyComm {
        Mt: ProxyComm_DATA_UP,
        Proxy: Proxyid,
        Connection: thisConnection,
        Seq: connRecord.LastSeqIn,
        Data: b[:n],
      }
      sendProtobuf(dataMessage)
    }
  }
  log.Infof("Close hijacked connection %4d", thisConnection)
}

func sendProtobufToConn(conn net.Conn, message *ProxyComm) {
  log.Tracef("Marshalling message %v", message)
  data, err := proto.Marshal(message)
  if err != nil {
    log.Fatal("marshaling error: ", err)
  }
  length := len(data)
  size := make([]byte, 2)
  size[0] = byte(length & 255)
  size[1] = byte((length & 65535) >> 8)
  log.Tracef("Length %d 0x%x, bytes 0x%x 0x%x", length, length, size[0], size[1])
//  log.Tracef("%s", hex.Dump(data))
  B := make([]byte, 0, 2 + length)
  B = append(B, size...)
  B = append(B, data...)
  conn.Write(B)
}

func sendProtobuf(message *ProxyComm) {
  v, err := app.ConnectionPool.Get()
  if err != nil {
    log.Fatalf("Error getting connection from pool: %v", err)
  }
  log.Tracef("Current conn pool len: %d\n", app.ConnectionPool.Len())
  sendProtobufToConn(v.(net.Conn), message)
  app.ConnectionPool.Put(v)
  log.Tracef("Current conn pool len: %d\n", app.ConnectionPool.Len())
}

// protobuf server
var remoteConnectionMutex sync.Mutex
var remoteConnections map[uint64]Connection

func handleConnection(conn net.Conn) {
  for {
    l := make([]byte, 2)
    n, err := conn.Read(l)
    if err != nil {
      log.Infof("Error reading frame length: %v", err)
      return
    }
    length := int(l[1]) * 256 + int(l[0])
    log.Tracef("Expecting protobuf message long %d bytes", length)
    B := make([]byte, 0, length)
    b := make([]byte, length)
    for len(B) != length {
      n, err = conn.Read(b)
      if err != nil {
        log.Infof("Error reading data: %v", err)
        if err != io.EOF {
          fmt.Println("read error:", err)
        }
        break
      }
      B = append(B, b[:n]...)
      b = make([]byte, cap(B)-len(B))
    }
    if len(B) == length {
      log.Tracef("Protobuf read %d bytes, hexdump", len(B))
      log.Tracef("%s", hex.Dump(B))
      message := &ProxyComm{}
      if err := proto.Unmarshal(B, message); err != nil {
        log.Warnf("Erroneous message hexdump length(%d): %s", length, hex.Dump(B))
        log.Errorf("Failed to parse message length(%d): %v", length, err)
        conn.Close()
        return
      }
      handleProxycommMessage(message)
    } else {
      log.Errorf("Error receiving protobuf message, expected %4d, got %4d", length, len(B))
    }
  }
  return
}

func handleProxycommMessage(message *ProxyComm) {
  log.Tracef("Received message: %v", message)
  if message.Mt == ProxyComm_PING {
    log.Debugf("Received PING message from proxy %d", message.Proxy)
    return
  }
  log.Tracef("Processing queueing logic for connection %d", message.Connection)
  var mutex *sync.Mutex
  var connections *map[uint64]Connection
  switch message.Mt {
    case ProxyComm_OPEN_CONN:
      mutex = &remoteConnectionMutex
      connections = &remoteConnections
      mutex.Lock()
    case ProxyComm_CLOSE_CONN_C, ProxyComm_DATA_DOWN:
      mutex = &app.LocalConnectionMutex
      connections = &app.LocalConnections
      mutex.Lock()
      thisConnection, ok := app.LocalConnections[message.Connection]
      if ! ok {
        mutex.Unlock()
        log.Tracef("No such connection %d, seq %d", message.Connection, message.Seq)
        return
      }
      log.Tracef("Seq DOWN %d %d", message.Seq, thisConnection.NextSeqOut)
      if message.Seq != thisConnection.NextSeqOut {
        log.Tracef("Queueing message UP conn %d seq %d", message.Connection, message.Seq)
        thisConnection.MessageQueue[message.Seq] = *message
        mutex.Unlock()
        return
      }
    case ProxyComm_CLOSE_CONN_S, ProxyComm_DATA_UP:
      mutex = &remoteConnectionMutex
      connections = &remoteConnections
      mutex.Lock()
      thisConnection, ok := remoteConnections[message.Connection]
      if ! ok {
        log.Tracef("No such connection %d, seq %d, adding", message.Connection, message.Seq)
        remoteConnections[message.Connection] = Connection { Connection: nil, LastSeqIn: 0, NextSeqOut: 0, MessageQueue: make(map[uint64]ProxyComm) }
        thisConnection, _ = remoteConnections[message.Connection]
      }
      log.Tracef("Seq UP %d %d", message.Seq, thisConnection.NextSeqOut)
      if message.Seq != thisConnection.NextSeqOut {
        log.Tracef("Queueing message UP conn %d seq %d", message.Connection, message.Seq)
        thisConnection.MessageQueue[message.Seq] = *message
        mutex.Unlock()
        return
      }
  }
  log.Tracef("Handling current message for connection %d", message.Connection)
  switch message.Mt {
    case ProxyComm_OPEN_CONN:
      newConnection(message)
    case ProxyComm_CLOSE_CONN_C:
      closeConnectionLocal(message)
    case ProxyComm_DATA_DOWN:
      backwardDataChunk(message)
    case ProxyComm_CLOSE_CONN_S:
      closeConnectionRemote(message)
    case ProxyComm_DATA_UP:
      forwardDataChunk(message)
  }
  log.Debugf("Processing message queue for connection %d", message.Connection)
  thisConnection := (*connections)[message.Connection]
  seq := thisConnection.NextSeqOut
  log.Tracef("Next seq: %d", seq)
//  log.Tracef("Queue %v", (*connections)[message.Connection].MessageQueue)
  for queueMessage, ok := thisConnection.MessageQueue[seq]; ok; {
    thisConnection = (*connections)[queueMessage.Connection]
    log.Debugf("Processing message queue for connection %d, seq %d", queueMessage.Connection, seq)
//    log.Tracef("Message: %v", queueMessage)
    switch queueMessage.Mt {
      case ProxyComm_CLOSE_CONN_C:
        closeConnectionLocal(&queueMessage)
      case ProxyComm_DATA_DOWN:
        backwardDataChunk(&queueMessage)
      case ProxyComm_CLOSE_CONN_S:
        closeConnectionRemote(&queueMessage)
      case ProxyComm_DATA_UP:
        forwardDataChunk(&queueMessage)
    }
    delete((*connections)[queueMessage.Connection].MessageQueue, seq)
    seq++
    queueMessage, ok = thisConnection.MessageQueue[seq]
  }
  mutex.Unlock()
}

func newConnection(message *ProxyComm) {
  log.Infof("Openning connection %4d to %s:%d", message.Connection, message.Address, message.Port)
  conn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", message.Address, message.Port))
  if err != nil {
    log.Fatal(err)
    return
  }
  thisConnection, ok := remoteConnections[message.Connection]
  if ! ok {
    log.Tracef("Connection %4d not known, creating record", message.Connection)
    remoteConnections[message.Connection] = Connection { Connection: conn, LastSeqIn: 0, NextSeqOut: 1, MessageQueue: make(map[uint64]ProxyComm) }
  } else {
    log.Tracef("Connection %4d record already exists, setting conn field to newly dialed connection", message.Connection)
    thisConnection.Connection = conn
    thisConnection.NextSeqOut = 1
    remoteConnections[message.Connection] = thisConnection
  }
  go handleRemoteSideConnection(conn, message.Connection)
}

func closeConnectionRemote(message *ProxyComm) {
  log.Debugf("Closing remote connection %4d", message.Connection)
  conn := remoteConnections[message.Connection].Connection
  delete(remoteConnections, message.Connection)
  time.AfterFunc(1 * time.Second, func() { conn.Close() })
}

func closeConnectionLocal(message *ProxyComm) {
  log.Debugf("Closing local connection %4d", message.Connection)
  conn := app.LocalConnections[message.Connection].Connection
  delete(app.LocalConnections, message.Connection)
  time.AfterFunc(1 * time.Second, func() { conn.Close() })
}

func backwardDataChunk(message *ProxyComm) {
//  log.Tracef("DATA_DOWN %v", message)
  thisConnection := app.LocalConnections[message.Connection]
  thisConnection.NextSeqOut++
  app.LocalConnections[message.Connection] = thisConnection
  n, err := thisConnection.Connection.Write(message.Data)
  if err != nil {
    log.Debugf("Error forwarding data chunk downward for connection %4d, seq %8d, length %5d, %v", message.Connection, message.Seq, len(message.Data), err)
    return
  }
  log.Debugf("Succesfully forwarded data chunk downward for connection %4d, seq %8d, length %5d, sent %5d", message.Connection, message.Seq, len(message.Data), n)
  app.LocalConnections[message.Connection] = thisConnection
}

func forwardDataChunk(message *ProxyComm) {
  thisConnection := remoteConnections[message.Connection]
  thisConnection.NextSeqOut++
  remoteConnections[message.Connection] = thisConnection
  n, err := thisConnection.Connection.Write(message.Data)
  if err != nil {
    log.Debugf("Error forwarding data chunk   upward for connection %4d, seq %8d, length %5d, %v", message.Connection, message.Seq, len(message.Data), err)
  }
  log.Debugf("Succesfully forwarded data chunk   upward for connection %4d, seq %8d, length %5d, sent %5d", message.Connection, message.Seq, len(message.Data), n)
  remoteConnections[message.Connection] = thisConnection
}

func handleRemoteSideConnection(conn net.Conn, connId uint64) {
  log.Infof("Starting remote side connection handler for connection %d", connId)
  for {
    b := make([]byte, ChunkSize)
    n, err := conn.Read(b)
    if err != nil {
      if err == io.EOF {
        log.Infof("Error reading remote connection %d: %v", connId, err)
        remoteConnectionMutex.Lock()
        connRecord, ok := remoteConnections[connId]
        if ! ok {
          log.Tracef("Connection %d was already closed and removed earlier, exiting goroutine", connId)
          remoteConnectionMutex.Unlock()
          return
        }
        seq := connRecord.LastSeqIn
        connRecord.LastSeqIn++
        remoteConnections[connId] = connRecord
        remoteConnectionMutex.Unlock()
        closeMessage := &ProxyComm {
          Mt: ProxyComm_CLOSE_CONN_C,
          Proxy: Proxyid,
          Connection: connId,
          Seq: seq,
        }
        sendProtobuf(closeMessage)
        return
      }
      log.Tracef("Error reading remote connection %d: %v, exiting goroutine", connId, err)
      return
    }
    log.Tracef("Sending data from remote connection %4d downward, length %5d", connId, n)
    remoteConnectionMutex.Lock()
    connRecord := remoteConnections[connId]
    seq := connRecord.LastSeqIn
    connRecord.LastSeqIn++
    remoteConnections[connId] = connRecord
    remoteConnectionMutex.Unlock()
    log.Tracef("%s", hex.Dump(b[:n]))
    dataMessage := &ProxyComm {
      Mt: ProxyComm_DATA_DOWN,
      Proxy: Proxyid,
      Connection: connId,
      Seq: seq,
      Data: b[:n],
    }
    sendProtobuf(dataMessage)
  }
}

func protobufServer(listenPort int) {
  l, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", listenPort))
  if err != nil {
      log.Fatalf("Error listening:", err.Error())
  }
  defer l.Close()
  log.Infof("Listening on 0.0.0.0:%d", listenPort)
  for {
      log.Trace("Listening for an incoming connection")
      conn, err := l.Accept()
      if err != nil {
          fmt.Println("Error accepting: ", err.Error())
          os.Exit(1)
      }
      go handleConnection(conn)
  }
}

func goid() int {
	var buf [64]byte
	n := runtime.Stack(buf[:], false)
	idField := strings.Fields(strings.TrimPrefix(string(buf[:n]), "goroutine "))[0]
	id, err := strconv.Atoi(idField)
	if err != nil {
		panic(fmt.Sprintf("cannot get goroutine id: %v", err))
	}
	return id
}

func setLogLevel(logLevel *int) {
  switch *logLevel {
    case 1:
      log.SetLevel(log.ErrorLevel)
    case 2:
      log.SetLevel(log.WarnLevel)
    case 3:
      log.SetLevel(log.InfoLevel)
    case 4:
      log.SetLevel(log.DebugLevel)
    case 5:
      log.SetLevel(log.TraceLevel)
    default:
      log.Fatalf("Invalid log leve %d", *logLevel)
  }
}

func main() {
  // log.SetFormatter(&log.JSONFormatter{})
  log.SetReportCaller(true)
  log.SetFormatter(&log.TextFormatter{
    CallerPrettyfier: func(f *runtime.Frame) (string, string) {
      filename := path.Base(f.File)
      return fmt.Sprintf("%s()", f.Function), fmt.Sprintf("%d %s:%4d", goid(), filename, f.Line)
    },
  })

  logLevel   := flag.Int("L", 2, "Log level. (1) Error, (2) Warn, (3) Info, (4) Debug, (5) Trace")
  proxyport  := flag.Int("l", 3128, "Our http proxy port to listen on")
  peerHost   := flag.String("h", "127.0.0.1", "Address of the peer host")
  peerPort   := flag.Int("p", 33333, "Port of the peer on peer host")
  listenPort := flag.Int("b", 33333, "Our protobuf port to listen on")
  poolInit   := flag.Int("i", 100, "Initial size of the connection pool between the ends of tunnel")
  poolCap    := flag.Int("c", 500, "Cap of the connection pool size")
  pingPool   := flag.Bool("ping", false, "To ping or not to ping on the connection pool connections")
  flag.Parse()
  setLogLevel(logLevel)
  log.Infof("Proxy port %d\n", *proxyport)
  log.Infof("Peer host:port %s:%d\n", *peerHost, *peerPort)
  log.Infof("Listening port %d\n", *listenPort)
  log.Infof("Initial pool size %d\n", *poolInit)
  log.Infof("Maximum pool size %d\n", *poolCap)
  if *pingPool {
    log.Info("Will ping connections in pool")
  } else {
    log.Info("Will not ping connections in pool")
  }

  // remote side
  remoteConnections = make(map[uint64]Connection)
  go protobufServer(*listenPort)
  time.Sleep(5000 * time.Millisecond)
  // http proxy side
  app = NewApp(Hijack, *listenPort, *peerHost, *peerPort, *poolInit, *poolCap, *pingPool)
  defer func() { if app.ConnectionPool != nil { app.ConnectionPool.Release() } } ()
  log.Warn("Ready to serve")
  log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", *proxyport), app))
}
