package main

import(
    "fmt";
    "math/rand";
    "strings";
    "bytes";
    "os";
    "os/exec";
    "net";
    "net/http";
    "log";
    "time";
    "io/ioutil";
    "bufio";
    "encoding/gob";
    "encoding/json";
    "github.com/hashicorp/memberlist";
    "github.com/fatih/color";
    "github.com/quipo/statsd";
    "bitbucket.org/bertimus9/systemstat"
)

var(
    localnode *memberlist.Node
    ml_pointer *memberlist.Memberlist
    node_meta = NodeMetadata{}
    memberlistLog = new(bytes.Buffer)
    clog = log.New(LoggerWrapper{}, "", log.Ldate | log.Ltime)
    controllerLog = new(bytes.Buffer)
    monitorapp = true
    app_bad_check_count = 0
    NetTimeout int
    pong_received = false
    cset ControllerConf
    initialized = false
    mostRecentlyJoinedNodeName string
)

func ApplicationGoneDownHandler(ip string) {
    clog.Println("Application on node with IP address", ip, "gone down.")
    cmd := exec.Command(cset.HandlersScript, ip, "down")
    err := cmd.Run()
    if err != nil {
        clog.Println("Could not execute handler on script:", err)
    }
}

func ApplicationGoneUpHandler(ip string) {
    clog.Println("Application on node with IP address", ip, "gone up.")
    cmd := exec.Command(cset.HandlersScript, ip, "up")
    err := cmd.Run()
    if err != nil {
        clog.Println("Could not execute handler on script:", err)
    }
}

type RemoteMessage struct {
    Sender memberlist.Node
    Type string
    Contents []string
}

type NodeMetadata struct {
    ControllerState string
    ApplicationState string
}
func decodeNodeMeta(n *memberlist.Node) NodeMetadata {
    buffer := new(bytes.Buffer)
    buffer.Write(n.Meta)
    var remote_meta NodeMetadata
    dec := gob.NewDecoder(buffer)
    err := dec.Decode(&remote_meta)
    if err != nil {
        clog.Println("Error when decoding remote controller state:", err)
    }
    return remote_meta
}
func nodeMetaToString(n *memberlist.Node) string {
    remote_meta := decodeNodeMeta(n)
    return fmt.Sprintf("                    Controller state: %s\n                    Application state: %s", remote_meta.ControllerState, remote_meta.ApplicationState)
}
type EventHandler struct {
}
func (e EventHandler) NotifyJoin(n *memberlist.Node) {
    green := color.New(color.FgGreen).SprintfFunc()
    selfstring := ""
    if localnode != nil && n == localnode {
        selfstring = " (self)"
    } else if initialized {
        mostRecentlyJoinedNodeName = n.Name
    }
    clog.Printf("%s\n%s", green("Node %s%s joined.", n.Name, selfstring), nodeMetaToString(n))
}
func (e EventHandler) NotifyLeave(n *memberlist.Node) {
    red := color.New(color.FgRed).SprintfFunc()
    blue := color.New(color.FgBlue).SprintfFunc()
    selfstring := ""
    if localnode != nil && n == localnode {
        selfstring = " (self)"
    }
    if decodeNodeMeta(n).ControllerState == "NiceQuitting" {
        clog.Printf("%s\n%s", blue("Node %s%s left nicely.", n.Name, selfstring), nodeMetaToString(n))
    } else {
        clog.Printf("%s\n%s", red("Node %s%s left.", n.Name, selfstring), nodeMetaToString(n))
        if decodeNodeMeta(n).ApplicationState == "ok" {
            ApplicationGoneDownHandler(n.Addr.String())
        }
    }
}
func (e EventHandler) NotifyUpdate(n *memberlist.Node) {
    blue := color.New(color.FgBlue).SprintfFunc()
    selfstring := ""
    if localnode != nil && n == localnode {
        selfstring = " (self)"
    }
    clog.Printf("%s\n%s", blue("Node %s%s updated.", n.Name, selfstring), nodeMetaToString(n))
}

type DelegateImplement struct {
}

func (d DelegateImplement) NodeMeta(limit int) []byte {
    buffer := new(bytes.Buffer)
    enc := gob.NewEncoder(buffer)
    err := enc.Encode(node_meta)
    if err != nil {
        clog.Fatal("Node metadata encode error:", err)
    }
//    buffer.Truncate(limit)
    return buffer.Bytes()
}

func (d DelegateImplement) NotifyMsg(b []byte) {
    buffer := new(bytes.Buffer)
    buffer.Write(b)
    var msg RemoteMessage
    dec := gob.NewDecoder(buffer)
    err := dec.Decode(&msg)
    if err != nil {
        clog.Println("Error when decoding remote command:", err)
        return
    }
    switch msg.Type {
        case "command":
            evaluateCommand(ml_pointer, msg.Contents, &msg.Sender)
            break
        case "ping":
            sendPong(ml_pointer, msg.Contents, &msg.Sender)
            break
        case "pong":
            pong_received = true
            break
        default:
            clog.Println("Unknown remote message type:", msg.Type)
    }
}

func (d DelegateImplement) GetBroadcasts(overhead, limit int) [][]byte {
    var brds [][]byte
    return brds
    
}

func (d DelegateImplement) LocalState(join bool) []byte {
    var state []byte
    return state
}

func (d DelegateImplement) MergeRemoteState(buf []byte, join bool) {
    
}

// end of DelegateImplement

func joinCluster(m *memberlist.Memberlist, initial bool) {
    n, err := m.Join(cset.InitialNodes)
    if err != nil {
        if(initial) {
            clog.Fatal("Could not join cluster. Please ensure this node is included in the initial nodes list.")
        } else {
            if(cset.PeriodicRejoinInterval != 0) {
                clog.Printf("Could not join cluster - network may be unreachable. Trying again later.")
            } else {
                clog.Printf("Could not join cluster - network may be unreachable.")
            }
        }
    } else {
        clog.Printf("Joined cluster (number of nodes: %d)", n)
    }
}

func sendPing(m *memberlist.Memberlist, remote *memberlist.Node, timeout int) bool {
    // returns true if ping received a reply
    buffer := new(bytes.Buffer)
    enc := gob.NewEncoder(buffer)
    msg := RemoteMessage{Type:"ping", Contents: []string{"hi there"}, Sender:*m.LocalNode()}
    err := enc.Encode(msg)
    if err != nil {
        clog.Fatal("Ping encode error:", err)
    }
    addr := net.UDPAddr{IP: remote.Addr, Port: int(remote.Port)}
    pong_received = false
    m.SendTo(&addr, buffer.Bytes())
    // timeout for ping is half of the timeout for normal net operations
    for i := timeout; i > 0; i-- {
        if pong_received {
            return true
        }
        time.Sleep(500 * time.Millisecond)
    }
    return false // timeout without reply
}

func sendPong(m *memberlist.Memberlist, msg []string, remote *memberlist.Node) {
    buffer := new(bytes.Buffer)
    enc := gob.NewEncoder(buffer)
    rmsg := RemoteMessage{Type:"pong", Contents:msg, Sender:*m.LocalNode()}
    err := enc.Encode(rmsg)
    if err != nil {
        clog.Fatal("Ping reply encode error:", err)
    }
    addr := net.UDPAddr{IP: remote.Addr, Port: int(remote.Port)}
    m.SendTo(&addr, buffer.Bytes())
}

func thisNodeReachable(m *memberlist.Memberlist) bool {
    r := m.Members()
    if len(r) < 2 {
        // we're alone in the cluster, check with a 3rd party
        resp, err := http.Get(cset.ConnectionCheckURL)
        if err != nil {
            return false
        }
        defer resp.Body.Close()
        if resp.StatusCode != 200 {
            return false
        }
        return true
    }
    i := 0
    for {
        i = rand.Intn(len(r))
        if r[i] != m.LocalNode() {
            break
        }
    }
    return sendPing(m, r[i], NetTimeout)
}

func publishControllerState(m *memberlist.Memberlist, state string) {
    if node_meta.ControllerState != state {
        defer publishControllerMetadata(m)
    }
    node_meta.ControllerState = state
}

func publishApplicationState(m *memberlist.Memberlist, state string) {
    if node_meta.ApplicationState != state {
        if state == "ok" && thisNodeReachable(m) {
            ApplicationGoneUpHandler(m.LocalNode().Addr.String())
        } else if state == "ng" {
            ApplicationGoneDownHandler(m.LocalNode().Addr.String())
        }
        defer publishControllerMetadata(m)
    }
    node_meta.ApplicationState = state
}

func publishControllerMetadata(m *memberlist.Memberlist) {
    m.UpdateNode(time.Duration(NetTimeout)*time.Second)
}

// Wrapper for the controller log so that it prints on screen and on a buffer
type LoggerWrapper struct {   
}

func (l LoggerWrapper) Write(p []byte) (n int, err error) {
    ni, erri := controllerLog.Write(p)
    fmt.Print(string(p))
    return ni, erri
}

// Controller Settings loading

type ControllerConf struct {
    NodeName     string
    BindAddr     string
    BindPort     int
    InitialNodes []string
    NetTimeout int
    AppCheckURL string
    AppExpectedReply string
    AppTolerance int
    AppCheckInterval int
    EncryptionKey string
    HandlersScript string
    StatsdAddress string
    PeriodicRejoinInterval int
    ConnectionCheckURL string
}

func loadSettings(path string) ControllerConf {
    file, err := os.Open(path)
    if err != nil {
        clog.Fatal("Could not read settings from ", path)
    }
    decoder := json.NewDecoder(file)
    settings := ControllerConf{}
    err = decoder.Decode(&settings)
    if err != nil {
        clog.Fatal("Could not decode settings from ", path)
    }
    if settings.ConnectionCheckURL == "" {
        settings.ConnectionCheckURL = "http://httpbin.org/status/200"
    }
    return settings
}

// UI functions
func printAbout() {
    fmt.Println("")
    fmt.Println("     __      (((.)))")
    fmt.Println("    |__) __   __|         PicoRed ~ version 0.4")
    fmt.Println("  _ |\\  /__\\ /  |         Distributed Server Redundancy Manager")
    fmt.Println(" /_)| \\ \\__  \\__|         (C) 2015 tny. internet media")
    fmt.Println("/                                  http://i.tny.im")
    fmt.Println("")
}

func scanToSlice() []string {
    reader := bufio.NewReader(os.Stdin)
    text, _ := reader.ReadString('\n')
    if len(text)-1 < 0 {
        return []string{""}
    }
    return strings.Split(text[:len(text)-1], " ")
}

func consoleManager(m *memberlist.Memberlist) {
    for {
        cmd := scanToSlice()
        evaluateCommand(m, cmd, nil)
    }
}

func evaluateCommand(m *memberlist.Memberlist, cmd []string, remote *memberlist.Node) {
    if remote != nil {
        clog.Printf("Evaluating command \"\"\"%s\"\"\" from node %s.\n", cmd, remote.Name)
    }
    switch cmd[0] {
        case "version":
            printAbout()
            break
        case "quit":
            fmt.Println("Unknown command. Issue \"help\" for a list of accepted commands.\nHint: use nicequit or ragequit instead.")
            break
        case "ragequit":
            clog.Println("ragequit: quitting without leaving the memberlist nicely. Other nodes will see this controller as dead and run appropriate handlers")
            os.Exit(0)
            break
        case "nicequit":
            performNiceLeave(m)
            os.Exit(0)
            break
        case "nodes":
            fmt.Printf("Cluster nodes (total: %d)\n", m.NumMembers())
            for _, member := range m.Members() {
                fmt.Printf("Node:               %s %s:%d\n%s\n", member.Name, member.Addr, member.Port, nodeMetaToString(member))
            }
            break
        case "mllog":
            fmt.Println(memberlistLog.String())
            break
        case "writelog":
            evaluateLogWritingCommand(cmd)
            break
        case "clearlog":
            evaluateLogClearingCommand(cmd)
            break
        case "appstate":
            evaluateAppStatusCommand(m, cmd)
            break
        case "remote":
            sendRemoteCommand(m, cmd)
            break
        case "reachable":
            fmt.Println(thisNodeReachable(m))
            break
        case "rejoin":
            clog.Println("Performing manual cluster rejoin...")
            joinCluster(m, false)
            break
        case "help":
            fmt.Println("Accepted commands: version, nicequit, ragequit, nodes, mllog, writelog [ml|ctrl] [filepath], clearlog [ml|ctrl], appstate [ok|ng|auto|clear], remote [peer name] [command], reachable, rejoin")
        default:
            fmt.Println("Unknown command. Issue \"help\" for a list of accepted commands.")
            break
    }
}
func evaluateLogWritingCommand(cmd []string) {
    if len(cmd) < 2 {
        fmt.Println("writelog: invalid argument.")
        return
    }
    tlog := cmd[1]
    path := ""
    if len(cmd) < 3 {
        path = "/tmp/picored-" + tlog
    } else {
        path = cmd[2]
    }
    switch tlog {
        case "ml":
            ioutil.WriteFile(path, memberlistLog.Bytes(), 0644)
            clog.Println("writelog: wrote memberlist log to", path)
            break
        case "ctrl":
            ioutil.WriteFile(path, controllerLog.Bytes(), 0644)
            clog.Println("writelog: wrote controller log to", path)
            break
        default:
            fmt.Println("writelog: invalid argument.")
            break
    }
}

func evaluateLogClearingCommand(cmd []string) {
    if len(cmd) < 2 {
        fmt.Println("clearlog: invalid argument.")
        return
    }
    tlog := cmd[1]
    switch tlog {
        case "ml":
            memberlistLog.Reset()
            clog.Println("clearlog: cleared memberlist log.")
            break
        case "ctrl":
            controllerLog.Reset()
            clog.Println("clearlog: cleared controller log.")
            break
        default:
            fmt.Println("clearlog: invalid argument.")
            break
    }
}

func evaluateAppStatusCommand(m *memberlist.Memberlist, cmd []string) {
    if len(cmd) < 2 {
        fmt.Println("appstate: invalid argument.")
        return
    }
    switch cmd[1] {
        case "ok":
            monitorapp = false
            publishControllerState(m, "ApplicationStateForced")
            publishApplicationState(m, "ok")
            clog.Println("appstate: application status forced as OK. Will not monitor application on this node.")
            break
        case "ng":
            monitorapp = false
            publishControllerState(m, "ApplicationStateForced")
            publishApplicationState(m, "ng")
            clog.Println("appstate: application status forced as not-good. Will not monitor application on this node.")
            break   
        case "auto":
            monitorapp = true
            publishControllerState(m, "Monitoring")
            clog.Println("appstate: controller set to monitor application on this node.")
            break
        case "clear":
            if node_meta.ApplicationState != "ApplicationStateForced" {
                publishApplicationState(m, "")
            }
        default:
            fmt.Println("appstate: invalid argument.")
            break
    }
}

func sendRemoteCommand(m *memberlist.Memberlist, cmd []string) {
    if len(cmd) < 3 {
        fmt.Println("remote: invalid command.")
        return
    }
    for _, member := range m.Members() {
        if member.Name == cmd[1] {
            buffer := new(bytes.Buffer)
            enc := gob.NewEncoder(buffer)
            msg := RemoteMessage{Type:"command", Contents:cmd[2:], Sender:*m.LocalNode()}
            err := enc.Encode(msg)
            if err != nil {
                clog.Fatal("Command encode error:", err)
            }
            addr := net.UDPAddr{IP: member.Addr, Port: int(member.Port)}
            m.SendTo(&addr, buffer.Bytes())
            clog.Printf("Sent command \"\"\"%s\"\"\" to node %s.\n", cmd[2:], member.Name)
            return
        }
    }
    fmt.Println("remote: could not find a node with the given name.")
}

func performNiceLeave(m *memberlist.Memberlist) {
    clog.Println("nicequit: quitting after informing other members that this controller is down for maintenance and leaving the memberlist nicely. Handlers for leaving members will not run.")
    publishControllerState(m, "NiceQuitting")
    m.Leave(time.Duration(NetTimeout)*time.Second)
    m.Shutdown()
}

func checkApplication(m *memberlist.Memberlist, cset *ControllerConf) {
    resp, err := http.Get(cset.AppCheckURL)
    if err != nil {
        app_bad_check_count++
        if(app_bad_check_count == cset.AppTolerance) {
            publishApplicationState(m, "ng")
        }
        return
    }
    defer resp.Body.Close()
    body, err := ioutil.ReadAll(resp.Body)
    if string(body) != cset.AppExpectedReply {
        app_bad_check_count++
        if(app_bad_check_count == cset.AppTolerance) {
            publishApplicationState(m, "ng")
        }
        return
    }
    // Application is ok only if this node is reachable by at least one another
    if thisNodeReachable(m) == false {
        app_bad_check_count++
        if(app_bad_check_count == cset.AppTolerance) {
            publishApplicationState(m, "ng")
        }
        return
    }
    publishApplicationState(m, "ok")
    app_bad_check_count = 0
}

func periodicRejoin(m *memberlist.Memberlist) {
    if(cset.PeriodicRejoinInterval == 0) {
        clog.Println("Periodic cluster rejoin disabled on settings.")
        return
    }
    for {
        time.Sleep(time.Duration(cset.PeriodicRejoinInterval) * time.Second)
        clog.Println("Performing periodic cluster rejoin...")
        joinCluster(m, false)
    }
}

func serverStatsdCollector() {
    prefix := "serverstats." + cset.NodeName + "."
    statsdclient := statsd.NewStatsdClient(cset.StatsdAddress, prefix)
    statsdclient.CreateSocket()
    memsamp := systemstat.GetMemSample()
    statsdclient.Gauge("mem.total", int64(memsamp.MemTotal))
    statsdclient.Gauge("swap.total", int64(memsamp.SwapTotal))
    prevcsamp := systemstat.GetCPUSample()
    for {
        lsamp := systemstat.GetLoadAvgSample()
        statsdclient.Gauge("load.1", int64(lsamp.One*100))
        statsdclient.Gauge("load.5", int64(lsamp.Five*100))
        statsdclient.Gauge("load.15", int64(lsamp.Fifteen*100))
        
        csamp := systemstat.GetCPUSample()
        cavg := systemstat.GetCPUAverage(prevcsamp, csamp);
        prevcsamp = csamp
        statsdclient.Gauge("cpu.user", int64(cavg.UserPct))
        statsdclient.Gauge("cpu.nice", int64(cavg.NicePct))
        statsdclient.Gauge("cpu.system", int64(cavg.SystemPct))
        
        memsamp = systemstat.GetMemSample()
        statsdclient.Gauge("mem.used", int64(memsamp.MemUsed))
        statsdclient.Gauge("mem.buffers", int64(memsamp.Buffers))
        statsdclient.Gauge("mem.cached", int64(memsamp.Cached))
        
        statsdclient.Gauge("swap.used", int64(memsamp.SwapUsed))
        time.Sleep(20*time.Second)
    }
}

func main() {
    printAbout()
    rand.Seed(time.Now().Unix())
    if len(os.Args) < 2 {
        clog.Fatal("Invalid arguments: please specify settings file path.")
    }
    clog.Println("Loading settings...")
    cset = loadSettings(os.Args[1])
    NetTimeout = cset.NetTimeout
    clog.Println("Initializing memberlist...")
    conf := memberlist.DefaultWANConfig()
    conf.SuspicionMult = 4
    conf.ProbeInterval = 3 * time.Second
    conf.GossipInterval = 400 * time.Millisecond
    conf.Events = EventHandler{}
    conf.Delegate = DelegateImplement{}
    conf.LogOutput = memberlistLog
    conf.BindAddr = cset.BindAddr
    conf.BindPort = cset.BindPort
    conf.Name = cset.NodeName
    conf.SecretKey = []byte(cset.EncryptionKey)
    m, err := memberlist.Create(conf)
    if err != nil {
        clog.Fatal("Failed to create memberlist: " + err.Error())
    }
    ml_pointer = m
    localnode = m.LocalNode()
    publishControllerState(m, "Initializing")

    clog.Println("Attempting initial cluster join...")
    joinCluster(m, true)
    go consoleManager(m)
    go periodicRejoin(m)
    if cset.StatsdAddress != "" {
        clog.Println("Starting sstdCollector thread (independent from memberlist/monitoring)...")
        go serverStatsdCollector()
    } else {
        clog.Println("Statsd address empty; sstdCollector not started.")
    }
    initialized = true
    publishControllerState(m, "Monitoring")
    for {
        if len(mostRecentlyJoinedNodeName) > 0 {
            sendRemoteCommand(m, []string{"remote", mostRecentlyJoinedNodeName, "appstate", "clear"})
            mostRecentlyJoinedNodeName = ""
        }
        if(monitorapp) {
            checkApplication(m, &cset)
        }
        time.Sleep(time.Duration(cset.AppCheckInterval)*time.Second)
    }
}