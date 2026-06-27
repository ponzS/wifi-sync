package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"mime"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf16"

	qrcode "github.com/skip2/go-qrcode"
)

const (
	defaultAddr        = ":8766"
	uploadDir          = "uploads"
	publicDir          = "public"
	downloadCountsFile = ".download-counts.json"
	boardStateFile     = ".board-state.json"
	maxUpload          = 8 << 30 // 8 GiB
	onlineWindow       = 45 * time.Second
	sessionIDMaxLength = 128
	boardHistoryLimit  = 512
)

var onlineSessions = newPresenceStore(onlineWindow)
var downloadCounts = newDownloadCounter(downloadCountsFile)
var textBoard = newBoardStore(boardStateFile)

type fileInfo struct {
	Name      string `json:"name"`
	Size      int64  `json:"size"`
	ModTime   string `json:"modTime"`
	Download  string `json:"download"`
	Extension string `json:"extension"`
	Downloads int    `json:"downloads"`
}

type appInfo struct {
	Port  string   `json:"port"`
	IPs   []string `json:"ips"`
	Users int      `json:"users"`
}

type heartbeatRequest struct {
	SessionID string `json:"sessionId"`
}

type heartbeatResponse struct {
	Users int `json:"users"`
}

type jsonError struct {
	Error string `json:"error"`
}

type boardStateResponse struct {
	Text      string `json:"text"`
	Version   int64  `json:"version"`
	UpdatedAt string `json:"updatedAt"`
}

type boardEvent struct {
	Type      string               `json:"type"`
	State     *boardStateResponse  `json:"state,omitempty"`
	Operation *boardOperationEvent `json:"operation,omitempty"`
}

type boardOperationRequest struct {
	SessionID   string `json:"sessionId"`
	OperationID string `json:"operationId"`
	BaseVersion int64  `json:"baseVersion"`
	Start       int    `json:"start"`
	End         int    `json:"end"`
	Text        string `json:"text"`
}

type boardOperationResponse struct {
	Text      string               `json:"text"`
	Version   int64                `json:"version"`
	UpdatedAt string               `json:"updatedAt"`
	Operation *boardOperationEvent `json:"operation,omitempty"`
}

type boardOperationEvent struct {
	SessionID   string `json:"sessionId"`
	OperationID string `json:"operationId"`
	Version     int64  `json:"version"`
	Start       int    `json:"start"`
	End         int    `json:"end"`
	Text        string `json:"text"`
	UpdatedAt   string `json:"updatedAt"`
}

type boardPersistedState struct {
	Text      string `json:"text"`
	Version   int64  `json:"version"`
	UpdatedAt string `json:"updatedAt"`
}

type presenceStore struct {
	mu      sync.Mutex
	ttl     time.Duration
	clients map[string]time.Time
}

type downloadCounter struct {
	mu     sync.Mutex
	path   string
	counts map[string]int
}

type boardStore struct {
	mu          sync.Mutex
	path        string
	text        string
	version     int64
	updatedAt   time.Time
	history     []boardOperationEvent
	subscribers map[chan boardEvent]struct{}
}

func main() {
	if err := os.MkdirAll(uploadDir, 0o755); err != nil {
		log.Fatalf("create uploads directory: %v", err)
	}
	if err := downloadCounts.load(); err != nil {
		log.Printf("load download counts: %v", err)
	}
	if err := textBoard.load(); err != nil {
		log.Printf("load board state: %v", err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/info", infoHandler)
	mux.HandleFunc("GET /api/qr", qrHandler)
	mux.HandleFunc("POST /api/heartbeat", heartbeatHandler)
	mux.HandleFunc("GET /api/board", boardSnapshotHandler)
	mux.HandleFunc("POST /api/board/ops", boardOperationHandler)
	mux.HandleFunc("GET /api/board/stream", boardStreamHandler)
	mux.HandleFunc("GET /api/files", listFilesHandler)
	mux.HandleFunc("POST /api/upload", uploadHandler)
	mux.HandleFunc("DELETE /api/files/{name}", deleteFileHandler)
	mux.HandleFunc("GET /download/{name}", downloadHandler)
	mux.Handle("/", http.FileServer(http.Dir(publicDir)))

	addr := envOrDefault("ADDR", defaultAddr)
	log.Printf("Wi-Fi file share listening on http://0.0.0.0%s", addr)
	logLocalURLs(addr)

	server := &http.Server{
		Addr:              addr,
		Handler:           logRequests(mux),
		ReadHeaderTimeout: 10 * time.Second,
	}

	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatalf("server failed: %v", err)
	}
}

func infoHandler(w http.ResponseWriter, r *http.Request) {
	port := portFromAddr(envOrDefault("ADDR", defaultAddr))
	writeJSON(w, http.StatusOK, appInfo{
		Port:  port,
		IPs:   localIPv4s(),
		Users: onlineSessions.count(time.Now()),
	})
}

func qrHandler(w http.ResponseWriter, r *http.Request) {
	target := strings.TrimSpace(r.URL.Query().Get("url"))
	if target == "" {
		target = defaultShareURL(r)
	}
	if len(target) > 2048 || !isHTTPURL(target) {
		writeError(w, http.StatusBadRequest, "二维码地址无效")
		return
	}

	png, err := qrcode.Encode(target, qrcode.Medium, 256)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "二维码生成失败")
		return
	}

	w.Header().Set("Content-Type", "image/png")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusOK)
	if _, err := w.Write(png); err != nil {
		log.Printf("write qr image: %v", err)
	}
}

func heartbeatHandler(w http.ResponseWriter, r *http.Request) {
	var payload heartbeatRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096)).Decode(&payload); err != nil {
		writeError(w, http.StatusBadRequest, "心跳数据无效")
		return
	}

	sessionID := strings.TrimSpace(payload.SessionID)
	if sessionID == "" || len(sessionID) > sessionIDMaxLength {
		writeError(w, http.StatusBadRequest, "会话标识无效")
		return
	}

	users := onlineSessions.touch(sessionID, time.Now())
	writeJSON(w, http.StatusOK, heartbeatResponse{Users: users})
}

func boardSnapshotHandler(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, textBoard.snapshot())
}

func boardOperationHandler(w http.ResponseWriter, r *http.Request) {
	var payload boardOperationRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&payload); err != nil {
		writeError(w, http.StatusBadRequest, "白板操作无效")
		return
	}

	if strings.TrimSpace(payload.SessionID) == "" || len(payload.SessionID) > sessionIDMaxLength {
		writeError(w, http.StatusBadRequest, "会话标识无效")
		return
	}
	if strings.TrimSpace(payload.OperationID) == "" || len(payload.OperationID) > sessionIDMaxLength {
		writeError(w, http.StatusBadRequest, "操作标识无效")
		return
	}

	response, err := textBoard.apply(payload, time.Now())
	if err != nil {
		switch {
		case errors.Is(err, errBoardVersionTooOld):
			writeError(w, http.StatusConflict, "白板版本过旧，请同步后重试")
		default:
			writeError(w, http.StatusBadRequest, err.Error())
		}
		return
	}

	writeJSON(w, http.StatusOK, response)
}

func boardStreamHandler(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeError(w, http.StatusInternalServerError, "当前环境不支持实时推送")
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	events, cancel := textBoard.subscribe()
	defer cancel()

	if err := writeSSE(w, boardEvent{
		Type:  "snapshot",
		State: pointer(textBoard.snapshot()),
	}); err != nil {
		return
	}
	flusher.Flush()

	ticker := time.NewTicker(20 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-r.Context().Done():
			return
		case event, ok := <-events:
			if !ok {
				return
			}
			if err := writeSSE(w, event); err != nil {
				return
			}
			flusher.Flush()
		case <-ticker.C:
			if _, err := io.WriteString(w, ": ping\n\n"); err != nil {
				return
			}
			flusher.Flush()
		}
	}
}

func listFilesHandler(w http.ResponseWriter, r *http.Request) {
	entries, err := os.ReadDir(uploadDir)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "无法读取文件列表")
		return
	}

	files := make([]fileInfo, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		info, err := entry.Info()
		if err != nil {
			continue
		}

		name := entry.Name()
		files = append(files, fileInfo{
			Name:      name,
			Size:      info.Size(),
			ModTime:   info.ModTime().Format(time.RFC3339),
			Download:  "/download/" + urlPathEscape(name),
			Extension: strings.TrimPrefix(strings.ToLower(filepath.Ext(name)), "."),
			Downloads: downloadCounts.get(name),
		})
	}

	sort.Slice(files, func(i, j int) bool {
		return files[i].ModTime > files[j].ModTime
	})

	writeJSON(w, http.StatusOK, files)
}

func uploadHandler(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, maxUpload)
	if err := r.ParseMultipartForm(32 << 20); err != nil {
		writeError(w, http.StatusBadRequest, "上传数据无效或文件过大")
		return
	}

	source, header, err := r.FormFile("file")
	if err != nil {
		writeError(w, http.StatusBadRequest, "请选择要上传的文件")
		return
	}
	defer source.Close()

	name, err := safeUploadName(header.Filename)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	path := filepath.Join(uploadDir, name)
	target, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "无法保存文件")
		return
	}
	defer target.Close()

	if _, copyErr := io.Copy(target, source); copyErr != nil {
		_ = os.Remove(path)
		writeError(w, http.StatusInternalServerError, "写入文件失败")
		return
	}

	info, err := target.Stat()
	if err != nil {
		writeError(w, http.StatusInternalServerError, "无法读取文件信息")
		return
	}

	writeJSON(w, http.StatusCreated, fileInfo{
		Name:      name,
		Size:      info.Size(),
		ModTime:   info.ModTime().Format(time.RFC3339),
		Download:  "/download/" + urlPathEscape(name),
		Extension: strings.TrimPrefix(strings.ToLower(filepath.Ext(name)), "."),
		Downloads: downloadCounts.reset(name),
	})
}

func deleteFileHandler(w http.ResponseWriter, r *http.Request) {
	name, err := safeRouteName(r.PathValue("name"))
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	path := filepath.Join(uploadDir, name)
	if err := os.Remove(path); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			writeError(w, http.StatusNotFound, "文件不存在")
			return
		}
		writeError(w, http.StatusInternalServerError, "删除文件失败")
		return
	}

	downloadCounts.delete(name)
	w.WriteHeader(http.StatusNoContent)
}

func downloadHandler(w http.ResponseWriter, r *http.Request) {
	name, err := safeRouteName(r.PathValue("name"))
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	path := filepath.Join(uploadDir, name)
	file, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			http.NotFound(w, r)
			return
		}
		writeError(w, http.StatusInternalServerError, "无法打开文件")
		return
	}
	defer file.Close()

	info, err := file.Stat()
	if err != nil {
		writeError(w, http.StatusInternalServerError, "无法读取文件信息")
		return
	}

	contentType := mime.TypeByExtension(filepath.Ext(name))
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	w.Header().Set("Content-Type", contentType)
	w.Header().Set("Content-Length", strconv.FormatInt(info.Size(), 10))
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", name))
	downloadCounts.increment(name)
	http.ServeContent(w, r, name, info.ModTime(), file)
}

func safeUploadName(name string) (string, error) {
	clean := filepath.Base(strings.TrimSpace(name))
	return validateFileName(clean)
}

func safeRouteName(name string) (string, error) {
	clean := strings.TrimSpace(name)
	return validateFileName(clean)
}

func validateFileName(name string) (string, error) {
	if name == "" || name == "." || name == ".." {
		return "", errors.New("文件名无效")
	}
	if strings.ContainsAny(name, `/\`) {
		return "", errors.New("文件名不能包含路径分隔符")
	}
	return name, nil
}

func defaultShareURL(r *http.Request) string {
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	return scheme + "://" + r.Host + "/"
}

func isHTTPURL(value string) bool {
	parsed, err := url.Parse(value)
	if err != nil {
		return false
	}
	return (parsed.Scheme == "http" || parsed.Scheme == "https") && parsed.Host != ""
}

func localIPv4s() []string {
	interfaces, err := net.Interfaces()
	if err != nil {
		return nil
	}

	seen := map[string]bool{}
	var ips []string
	for _, iface := range interfaces {
		if iface.Flags&net.FlagUp == 0 || iface.Flags&net.FlagLoopback != 0 {
			continue
		}

		addrs, err := iface.Addrs()
		if err != nil {
			continue
		}

		for _, addr := range addrs {
			var ip net.IP
			switch value := addr.(type) {
			case *net.IPNet:
				ip = value.IP
			case *net.IPAddr:
				ip = value.IP
			}

			ip = ip.To4()
			if ip == nil || ip.IsLoopback() {
				continue
			}

			text := ip.String()
			if !seen[text] {
				seen[text] = true
				ips = append(ips, text)
			}
		}
	}

	sort.Strings(ips)
	return ips
}

func newPresenceStore(ttl time.Duration) *presenceStore {
	return &presenceStore{
		ttl:     ttl,
		clients: make(map[string]time.Time),
	}
}

func (store *presenceStore) touch(sessionID string, now time.Time) int {
	store.mu.Lock()
	defer store.mu.Unlock()

	store.clients[sessionID] = now
	return store.countLocked(now)
}

func (store *presenceStore) count(now time.Time) int {
	store.mu.Lock()
	defer store.mu.Unlock()

	return store.countLocked(now)
}

func (store *presenceStore) countLocked(now time.Time) int {
	expiresBefore := now.Add(-store.ttl)
	for sessionID, seenAt := range store.clients {
		if seenAt.Before(expiresBefore) {
			delete(store.clients, sessionID)
		}
	}
	return len(store.clients)
}

func newDownloadCounter(path string) *downloadCounter {
	return &downloadCounter{
		path:   path,
		counts: make(map[string]int),
	}
}

func newBoardStore(path string) *boardStore {
	return &boardStore{
		path:        path,
		subscribers: make(map[chan boardEvent]struct{}),
	}
}

var errBoardVersionTooOld = errors.New("board version too old")

func (store *boardStore) load() error {
	store.mu.Lock()
	defer store.mu.Unlock()

	file, err := os.Open(store.path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	defer file.Close()

	var saved boardPersistedState
	if err := json.NewDecoder(file).Decode(&saved); err != nil {
		return err
	}

	store.text = saved.Text
	store.version = saved.Version
	if saved.UpdatedAt != "" {
		if updatedAt, err := time.Parse(time.RFC3339Nano, saved.UpdatedAt); err == nil {
			store.updatedAt = updatedAt
		}
	}
	if store.updatedAt.IsZero() {
		store.updatedAt = time.Now()
	}
	return nil
}

func (store *boardStore) snapshot() boardStateResponse {
	store.mu.Lock()
	defer store.mu.Unlock()

	return boardStateResponse{
		Text:      store.text,
		Version:   store.version,
		UpdatedAt: formatBoardTime(store.updatedAt),
	}
}

func (store *boardStore) apply(req boardOperationRequest, now time.Time) (boardOperationResponse, error) {
	store.mu.Lock()
	defer store.mu.Unlock()

	if req.BaseVersion < 0 || req.BaseVersion > store.version {
		return boardOperationResponse{}, errors.New("白板版本无效")
	}
	if req.BaseVersion < store.version-int64(len(store.history)) {
		return boardOperationResponse{}, errBoardVersionTooOld
	}

	op := boardOperationEvent{
		SessionID:   req.SessionID,
		OperationID: req.OperationID,
		Start:       req.Start,
		End:         req.End,
		Text:        req.Text,
	}
	for _, applied := range store.history {
		if applied.Version <= req.BaseVersion {
			continue
		}
		op = transformOperation(op, applied, true)
	}

	nextText, err := applyTextOperation(store.text, op.Start, op.End, op.Text)
	if err != nil {
		return boardOperationResponse{}, err
	}

	store.text = nextText
	store.version++
	store.updatedAt = now
	op.Version = store.version
	op.UpdatedAt = formatBoardTime(now)

	store.history = append(store.history, op)
	if len(store.history) > boardHistoryLimit {
		store.history = append([]boardOperationEvent(nil), store.history[len(store.history)-boardHistoryLimit:]...)
	}
	if err := store.saveLocked(); err != nil {
		return boardOperationResponse{}, err
	}

	store.broadcastLocked(boardEvent{
		Type:      "operation",
		Operation: pointer(op),
	})

	return boardOperationResponse{
		Text:      store.text,
		Version:   store.version,
		UpdatedAt: formatBoardTime(store.updatedAt),
		Operation: pointer(op),
	}, nil
}

func (store *boardStore) subscribe() (<-chan boardEvent, func()) {
	stream := make(chan boardEvent, 16)

	store.mu.Lock()
	store.subscribers[stream] = struct{}{}
	store.mu.Unlock()

	return stream, func() {
		store.mu.Lock()
		if _, ok := store.subscribers[stream]; ok {
			delete(store.subscribers, stream)
			close(stream)
		}
		store.mu.Unlock()
	}
}

func (store *boardStore) broadcastLocked(event boardEvent) {
	for subscriber := range store.subscribers {
		select {
		case subscriber <- event:
		default:
		}
	}
}

func (store *boardStore) saveLocked() error {
	tempPath := store.path + ".tmp"
	file, err := os.OpenFile(tempPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(boardPersistedState{
		Text:      store.text,
		Version:   store.version,
		UpdatedAt: formatBoardTime(store.updatedAt),
	}); err != nil {
		_ = file.Close()
		_ = os.Remove(tempPath)
		return err
	}
	if err := file.Close(); err != nil {
		_ = os.Remove(tempPath)
		return err
	}

	return os.Rename(tempPath, store.path)
}

func (counter *downloadCounter) load() error {
	counter.mu.Lock()
	defer counter.mu.Unlock()

	file, err := os.Open(counter.path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	defer file.Close()

	var counts map[string]int
	if err := json.NewDecoder(file).Decode(&counts); err != nil {
		return err
	}

	counter.counts = counts
	return nil
}

func (counter *downloadCounter) get(name string) int {
	counter.mu.Lock()
	defer counter.mu.Unlock()

	return counter.counts[name]
}

func (counter *downloadCounter) increment(name string) int {
	counter.mu.Lock()
	defer counter.mu.Unlock()

	counter.counts[name]++
	if err := counter.saveLocked(); err != nil {
		log.Printf("save download count: %v", err)
	}
	return counter.counts[name]
}

func (counter *downloadCounter) reset(name string) int {
	counter.mu.Lock()
	defer counter.mu.Unlock()

	counter.counts[name] = 0
	if err := counter.saveLocked(); err != nil {
		log.Printf("save download count: %v", err)
	}
	return 0
}

func (counter *downloadCounter) delete(name string) {
	counter.mu.Lock()
	defer counter.mu.Unlock()

	delete(counter.counts, name)
	if err := counter.saveLocked(); err != nil {
		log.Printf("save download count: %v", err)
	}
}

func (counter *downloadCounter) saveLocked() error {
	tempPath := counter.path + ".tmp"
	file, err := os.OpenFile(tempPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(counter.counts); err != nil {
		_ = file.Close()
		_ = os.Remove(tempPath)
		return err
	}
	if err := file.Close(); err != nil {
		_ = os.Remove(tempPath)
		return err
	}

	return os.Rename(tempPath, counter.path)
}

func logLocalURLs(addr string) {
	port := portFromAddr(addr)
	for _, ip := range localIPv4s() {
		log.Printf("LAN URL: http://%s:%s", ip, port)
	}
}

func portFromAddr(addr string) string {
	_, port, err := net.SplitHostPort(addr)
	if err == nil {
		return port
	}
	if port, ok := strings.CutPrefix(addr, ":"); ok {
		return port
	}
	return "8080"
}

func envOrDefault(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(value); err != nil {
		log.Printf("write json response: %v", err)
	}
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, jsonError{Error: message})
}

func urlPathEscape(value string) string {
	return url.PathEscape(value)
}

func writeSSE(w http.ResponseWriter, value any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(w, "data: %s\n\n", data)
	return err
}

func pointer[T any](value T) *T {
	return &value
}

func formatBoardTime(value time.Time) string {
	if value.IsZero() {
		return ""
	}
	return value.UTC().Format(time.RFC3339Nano)
}

func applyTextOperation(text string, start, end int, replacement string) (string, error) {
	if start < 0 || end < start {
		return "", errors.New("白板操作范围无效")
	}

	source := utf16.Encode([]rune(text))
	if end > len(source) {
		return "", errors.New("白板操作超出文本范围")
	}

	target := make([]uint16, 0, start+utf16Length(replacement)+len(source)-end)
	target = append(target, source[:start]...)
	target = append(target, utf16.Encode([]rune(replacement))...)
	target = append(target, source[end:]...)
	return string(utf16.Decode(target)), nil
}

func utf16Length(value string) int {
	return len(utf16.Encode([]rune(value)))
}

func transformOperation(op, applied boardOperationEvent, preferAfter bool) boardOperationEvent {
	op.Start = transformIndex(op.Start, applied, preferAfter)
	op.End = transformIndex(op.End, applied, preferAfter)
	op.End = max(op.End, op.Start)
	return op
}

func transformIndex(index int, applied boardOperationEvent, preferAfter bool) int {
	insertedLength := utf16Length(applied.Text)
	replacedLength := applied.End - applied.Start
	delta := insertedLength - replacedLength

	switch {
	case index < applied.Start:
		return index
	case index > applied.End:
		return index + delta
	case index == applied.Start:
		if preferAfter {
			return applied.Start + insertedLength
		}
		return applied.Start
	case index == applied.End:
		return applied.Start + insertedLength
	default:
		if preferAfter {
			return applied.Start + insertedLength
		}
		return applied.Start
	}
}

func logRequests(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		next.ServeHTTP(w, r)
		log.Printf("%s %s %s", r.Method, r.URL.Path, time.Since(start).Round(time.Millisecond))
	})
}
