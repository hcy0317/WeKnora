package gateway

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Tencent/WeKnora/internal/engine/lifecycle"
)

type LeaseClient interface {
	Acquire(ctx context.Context, group lifecycle.Group, request lifecycle.AcquireRequest) (lifecycle.Lease, error)
	Renew(ctx context.Context, leaseID string) error
	Release(ctx context.Context, leaseID string, reason lifecycle.ReleaseReason) error
}

type ReconcileClient interface {
	Reconcile(ctx context.Context, reconcile lifecycle.GatewayReconcile) error
}

type Route struct {
	Prefix       string
	Group        lifecycle.Group
	AllowedPaths map[string]struct{}
}

func DefaultRoutes() []Route {
	return []Route{
		{
			Prefix:       "/paddleocr",
			Group:        lifecycle.GroupPaddleOCR,
			AllowedPaths: map[string]struct{}{"/layout-parsing": {}, "/health": {}},
		},
		{
			Prefix:       "/asr",
			Group:        lifecycle.GroupASR,
			AllowedPaths: map[string]struct{}{"/v1/audio/transcriptions": {}},
		},
		{
			Prefix:       "/reranker",
			Group:        lifecycle.GroupReranker,
			AllowedPaths: map[string]struct{}{"/rerank": {}},
		},
	}
}

func DefaultAllowedBackends() map[lifecycle.Group]map[string]struct{} {
	return map[lifecycle.Group]map[string]struct{}{
		lifecycle.GroupPaddleOCR: {
			"http://paddleocr-vl:8080": {},
		},
		lifecycle.GroupASR: {
			"http://speaches:8000": {},
		},
		lifecycle.GroupReranker: {
			"http://qwen-reranker-gpu:8000": {},
			"http://qwen-reranker:8000":     {},
		},
	}
}

type Config struct {
	LeaseClient     LeaseClient
	GatewayID       string
	Routes          []Route
	AllowedBackends map[lifecycle.Group]map[string]struct{}
	HTTPClient      *http.Client
	RenewInterval   time.Duration
	StatusRetention time.Duration
	MaxRequestStats int
	HealthPaths     map[lifecycle.Group]string
	HealthTimeout   time.Duration
}

type Gateway struct {
	leaseClient     LeaseClient
	routes          []Route
	allowedBackends map[lifecycle.Group]map[string]struct{}
	httpClient      *http.Client
	renewInterval   time.Duration
	requestStatuses *requestStatusStore
	healthPaths     map[lifecycle.Group]string
	healthTimeout   time.Duration
	leaseMu         sync.RWMutex
	lastLeases      map[lifecycle.Group]lifecycle.Lease
	activeLeases    map[string]lifecycle.Lease
	shadowLeases    map[string]lifecycle.ShadowLease
	gatewayID       string
	gatewayEpoch    uint64
}

func New(config Config) (*Gateway, error) {
	if config.LeaseClient == nil {
		return nil, errors.New("gateway lease client is required")
	}
	if len(config.Routes) == 0 {
		return nil, errors.New("gateway routes are required")
	}
	for _, route := range config.Routes {
		if route.Prefix == "" || !strings.HasPrefix(route.Prefix, "/") || len(route.AllowedPaths) == 0 {
			return nil, fmt.Errorf("invalid gateway route for group %s", route.Group)
		}
		for allowedPath := range route.AllowedPaths {
			if !strings.HasPrefix(allowedPath, "/") || strings.Contains(allowedPath, "..") {
				return nil, fmt.Errorf("invalid allowed path %q for group %s", allowedPath, route.Group)
			}
		}
	}
	httpClient := config.HTTPClient
	if httpClient == nil {
		httpClient = &http.Client{Transport: http.DefaultTransport}
	}
	renewInterval := config.RenewInterval
	if renewInterval <= 0 {
		renewInterval = 10 * time.Second
	}
	statusRetention := config.StatusRetention
	if statusRetention <= 0 {
		statusRetention = 15 * time.Minute
	}
	maxRequestStats := config.MaxRequestStats
	if maxRequestStats <= 0 {
		maxRequestStats = 2000
	}
	healthPaths := config.HealthPaths
	if len(healthPaths) == 0 {
		healthPaths = map[lifecycle.Group]string{
			lifecycle.GroupPaddleOCR: "/health",
			lifecycle.GroupASR:       "/health",
			lifecycle.GroupReranker:  "/health",
		}
	}
	for group, healthPath := range healthPaths {
		if !strings.HasPrefix(healthPath, "/") || strings.Contains(healthPath, "..") {
			return nil, fmt.Errorf("invalid health path %q for group %s", healthPath, group)
		}
	}
	healthTimeout := config.HealthTimeout
	if healthTimeout <= 0 {
		healthTimeout = 2 * time.Second
	}
	gatewayID := config.GatewayID
	if gatewayID == "" {
		gatewayID = "engine-gateway"
	}
	if !validRequestID(gatewayID) {
		return nil, errors.New("gateway ID contains invalid characters")
	}
	return &Gateway{
		leaseClient:     config.LeaseClient,
		routes:          append([]Route(nil), config.Routes...),
		allowedBackends: cloneAllowedBackends(config.AllowedBackends),
		httpClient:      httpClient,
		renewInterval:   renewInterval,
		requestStatuses: newRequestStatusStore(statusRetention, maxRequestStats),
		healthPaths:     cloneHealthPaths(healthPaths),
		healthTimeout:   healthTimeout,
		lastLeases:      make(map[lifecycle.Group]lifecycle.Lease),
		activeLeases:    make(map[string]lifecycle.Lease),
		shadowLeases:    make(map[string]lifecycle.ShadowLease),
		gatewayID:       gatewayID,
		gatewayEpoch:    uint64(time.Now().UnixNano()),
	}, nil
}

func cloneHealthPaths(input map[lifecycle.Group]string) map[lifecycle.Group]string {
	cloned := make(map[lifecycle.Group]string, len(input))
	for group, healthPath := range input {
		cloned[group] = healthPath
	}
	return cloned
}

func cloneAllowedBackends(
	input map[lifecycle.Group]map[string]struct{},
) map[lifecycle.Group]map[string]struct{} {
	cloned := make(map[lifecycle.Group]map[string]struct{}, len(input))
	for group, backends := range input {
		cloned[group] = make(map[string]struct{}, len(backends))
		for backend := range backends {
			cloned[group][strings.TrimRight(backend, "/")] = struct{}{}
		}
	}
	return cloned
}

func (g *Gateway) ServeHTTP(response http.ResponseWriter, request *http.Request) {
	if request.URL.Path == "/healthz" {
		writeJSON(response, http.StatusOK, map[string]string{"status": "ok"})
		return
	}
	if strings.HasPrefix(request.URL.Path, "/v1/requests/") {
		g.serveRequestStatus(response, request)
		return
	}
	route, upstreamPath, ok := g.matchRoute(request.URL.Path)
	if !ok {
		writeJSON(response, http.StatusNotFound, map[string]string{"error": "unknown engine gateway path"})
		return
	}
	expectedMethod := http.MethodPost
	if upstreamPath == "/health" {
		expectedMethod = http.MethodGet
	}
	if request.Method != expectedMethod {
		writeJSON(response, http.StatusMethodNotAllowed, map[string]string{
			"error": "engine gateway path requires " + expectedMethod,
		})
		return
	}

	requestID := request.Header.Get("X-Request-ID")
	if !validRequestID(requestID) {
		requestID = newRequestID()
	}
	g.requestStatuses.start(requestID, route.Group)
	lease, controllerDegraded, err := g.acquireLease(request.Context(), route.Group, lifecycle.AcquireRequest{
		RequestID: requestID,
		GatewayID: g.gatewayID,
		Purpose:   "proxy_" + string(route.Group),
	})
	if err != nil {
		g.requestStatuses.fail(requestID, "lease_acquire_failed")
		writeJSON(response, http.StatusServiceUnavailable, map[string]string{"error": err.Error(), "request_id": requestID})
		return
	}
	releaseReason := lifecycle.ReleaseCompleted
	requestSucceeded := false
	defer func() {
		releaseContext, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		releaseErr := g.leaseClient.Release(releaseContext, lease.ID, releaseReason)
		g.untrackLease(lease.ID)
		if releaseErr == nil {
			_ = g.reconcile(releaseContext)
		}
		if requestSucceeded {
			g.requestStatuses.complete(requestID)
		}
	}()
	if err := g.validateLease(route.Group, lease); err != nil {
		releaseReason = lifecycle.ReleaseUpstreamError
		g.requestStatuses.fail(requestID, "invalid_controller_lease")
		writeJSON(response, http.StatusBadGateway, map[string]string{"error": err.Error(), "request_id": requestID})
		return
	}
	if !controllerDegraded {
		g.rememberValidatedLease(lease)
	}
	g.trackLease(lease, controllerDegraded)
	g.requestStatuses.proxying(requestID, lease, controllerDegraded)

	renewContext, stopRenew := context.WithCancel(request.Context())
	defer stopRenew()
	go g.renewLease(renewContext, lease.ID)

	target := strings.TrimRight(lease.Backend.URL, "/") + upstreamPath
	upstreamRequest, err := http.NewRequestWithContext(request.Context(), request.Method, target, request.Body)
	if err != nil {
		releaseReason = lifecycle.ReleaseUpstreamError
		g.requestStatuses.fail(requestID, "proxy_request_failed")
		writeJSON(response, http.StatusBadGateway, map[string]string{"error": err.Error(), "request_id": requestID})
		return
	}
	copyHeaders(upstreamRequest.Header, request.Header)
	upstreamRequest.Header.Set("X-Request-ID", requestID)
	upstreamRequest.ContentLength = request.ContentLength
	upstreamResponse, err := g.httpClient.Do(upstreamRequest)
	if err != nil {
		if request.Context().Err() != nil {
			releaseReason = lifecycle.ReleaseClientDisconnect
		} else {
			releaseReason = lifecycle.ReleaseUpstreamError
		}
		g.requestStatuses.fail(requestID, "upstream_unreachable")
		writeJSON(response, http.StatusBadGateway, map[string]string{"error": err.Error(), "request_id": requestID})
		return
	}
	defer upstreamResponse.Body.Close()
	copyHeaders(response.Header(), upstreamResponse.Header)
	response.Header().Set("X-WeKnora-Engine-Cold-Start", strconv.FormatBool(lease.ColdStart))
	response.Header().Set("X-WeKnora-Engine-Backend", lease.Backend.ID)
	response.Header().Set("X-WeKnora-Engine-Request-ID", requestID)
	if controllerDegraded {
		response.Header().Set("X-WeKnora-Engine-Controller-Degraded", "true")
	}
	response.WriteHeader(upstreamResponse.StatusCode)
	if _, err := io.Copy(response, upstreamResponse.Body); err != nil {
		releaseReason = lifecycle.ReleaseClientDisconnect
		g.requestStatuses.fail(requestID, "client_disconnected")
		return
	}
	if upstreamResponse.StatusCode >= http.StatusInternalServerError {
		releaseReason = lifecycle.ReleaseUpstreamError
		g.requestStatuses.fail(requestID, "upstream_error")
		return
	}
	requestSucceeded = true
}

func (g *Gateway) acquireLease(
	ctx context.Context,
	group lifecycle.Group,
	request lifecycle.AcquireRequest,
) (lifecycle.Lease, bool, error) {
	lease, err := g.leaseClient.Acquire(ctx, group, request)
	if err == nil {
		return lease, false, nil
	}
	if !errors.Is(err, ErrControllerUnreachable) {
		return lifecycle.Lease{}, false, err
	}

	g.leaseMu.RLock()
	lastLease, ok := g.lastLeases[group]
	g.leaseMu.RUnlock()
	if !ok {
		return lifecycle.Lease{}, false, err
	}
	if err := g.probeBackend(ctx, group, lastLease.Backend); err != nil {
		return lifecycle.Lease{}, false, fmt.Errorf("%w: last validated backend is unhealthy", ErrControllerUnreachable)
	}
	return lifecycle.Lease{
		ID:              "shadow-" + newRequestID(),
		RequestID:       request.RequestID,
		GatewayID:       request.GatewayID,
		Purpose:         request.Purpose,
		Group:           group,
		Backend:         lastLease.Backend,
		ControllerEpoch: lastLease.ControllerEpoch,
		CatalogRevision: lastLease.CatalogRevision,
	}, true, nil
}

func (g *Gateway) rememberValidatedLease(lease lifecycle.Lease) {
	g.leaseMu.Lock()
	defer g.leaseMu.Unlock()
	g.lastLeases[lease.Group] = lease
}

func (g *Gateway) trackLease(lease lifecycle.Lease, shadow bool) {
	g.leaseMu.Lock()
	defer g.leaseMu.Unlock()
	if shadow {
		g.shadowLeases[lease.ID] = lifecycle.ShadowLease{
			ID:              lease.ID,
			RequestID:       lease.RequestID,
			Group:           lease.Group,
			Purpose:         lease.Purpose,
			ControllerEpoch: lease.ControllerEpoch,
		}
		return
	}
	g.activeLeases[lease.ID] = lease
}

func (g *Gateway) untrackLease(leaseID string) {
	g.leaseMu.Lock()
	defer g.leaseMu.Unlock()
	delete(g.activeLeases, leaseID)
	delete(g.shadowLeases, leaseID)
}

func (g *Gateway) isShadowLease(leaseID string) bool {
	g.leaseMu.RLock()
	defer g.leaseMu.RUnlock()
	_, ok := g.shadowLeases[leaseID]
	return ok
}

func (g *Gateway) promoteShadowLease(leaseID string) {
	g.leaseMu.Lock()
	defer g.leaseMu.Unlock()
	shadow, ok := g.shadowLeases[leaseID]
	if !ok {
		return
	}
	delete(g.shadowLeases, leaseID)
	g.activeLeases[leaseID] = lifecycle.Lease{
		ID:              shadow.ID,
		RequestID:       shadow.RequestID,
		GatewayID:       g.gatewayID,
		Purpose:         shadow.Purpose,
		Group:           shadow.Group,
		ControllerEpoch: shadow.ControllerEpoch,
	}
}

func (g *Gateway) reconcile(ctx context.Context) error {
	client, ok := g.leaseClient.(ReconcileClient)
	if !ok {
		return nil
	}
	g.leaseMu.RLock()
	reconcile := lifecycle.GatewayReconcile{
		GatewayID:      g.gatewayID,
		GatewayEpoch:   g.gatewayEpoch,
		ActiveLeaseIDs: make([]string, 0, len(g.activeLeases)),
		ShadowLeases:   make([]lifecycle.ShadowLease, 0, len(g.shadowLeases)),
	}
	for leaseID := range g.activeLeases {
		reconcile.ActiveLeaseIDs = append(reconcile.ActiveLeaseIDs, leaseID)
	}
	for _, shadow := range g.shadowLeases {
		reconcile.ShadowLeases = append(reconcile.ShadowLeases, shadow)
	}
	g.leaseMu.RUnlock()
	return client.Reconcile(ctx, reconcile)
}

func (g *Gateway) probeBackend(ctx context.Context, group lifecycle.Group, backend lifecycle.Backend) error {
	healthPath, ok := g.healthPaths[group]
	if !ok {
		return errors.New("no health path configured for engine group")
	}
	probeContext, cancel := context.WithTimeout(ctx, g.healthTimeout)
	defer cancel()
	request, err := http.NewRequestWithContext(
		probeContext,
		http.MethodGet,
		strings.TrimRight(backend.URL, "/")+healthPath,
		nil,
	)
	if err != nil {
		return err
	}
	response, err := g.httpClient.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4<<10))
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return fmt.Errorf("backend health returned %s", response.Status)
	}
	return nil
}

func (g *Gateway) serveRequestStatus(response http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		writeJSON(response, http.StatusMethodNotAllowed, map[string]string{"error": "request status requires GET"})
		return
	}
	requestID := strings.TrimPrefix(request.URL.Path, "/v1/requests/")
	if !validRequestID(requestID) {
		writeJSON(response, http.StatusBadRequest, map[string]string{"error": "invalid request id"})
		return
	}
	status, ok := g.requestStatuses.get(requestID)
	if !ok {
		writeJSON(response, http.StatusNotFound, map[string]string{"error": "request status not found"})
		return
	}
	writeJSON(response, http.StatusOK, status)
}

func (g *Gateway) matchRoute(requestPath string) (Route, string, bool) {
	for _, route := range g.routes {
		if !strings.HasPrefix(requestPath, route.Prefix+"/") {
			continue
		}
		upstreamPath := strings.TrimPrefix(requestPath, route.Prefix)
		if _, allowed := route.AllowedPaths[upstreamPath]; allowed {
			return route, upstreamPath, true
		}
	}
	return Route{}, "", false
}

func (g *Gateway) validateLease(group lifecycle.Group, lease lifecycle.Lease) error {
	if lease.Group != group {
		return fmt.Errorf("controller returned lease for unexpected group %s", lease.Group)
	}
	backendURL := strings.TrimRight(lease.Backend.URL, "/")
	if _, allowed := g.allowedBackends[group][backendURL]; !allowed {
		return fmt.Errorf("controller returned backend outside gateway allowlist")
	}
	parsed, err := url.Parse(backendURL)
	if err != nil || parsed.Scheme != "http" || parsed.Host == "" || parsed.User != nil ||
		parsed.RawQuery != "" || parsed.Fragment != "" {
		return errors.New("controller returned invalid internal backend")
	}
	return nil
}

func (g *Gateway) renewLease(ctx context.Context, leaseID string) {
	ticker := time.NewTicker(g.renewInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if g.isShadowLease(leaseID) {
				if err := g.reconcile(ctx); err != nil {
					continue
				}
				if err := g.leaseClient.Renew(ctx, leaseID); err == nil {
					g.promoteShadowLease(leaseID)
				}
				continue
			}
			if err := g.leaseClient.Renew(ctx, leaseID); err == nil {
				_ = g.reconcile(ctx)
			}
		}
	}
}

func copyHeaders(destination, source http.Header) {
	for key, values := range source {
		if isHopByHopHeader(key) {
			continue
		}
		for _, value := range values {
			destination.Add(key, value)
		}
	}
}

func isHopByHopHeader(key string) bool {
	switch strings.ToLower(key) {
	case "connection", "proxy-connection", "keep-alive", "proxy-authenticate", "proxy-authorization",
		"te", "trailer", "transfer-encoding", "upgrade":
		return true
	default:
		return false
	}
}

func newRequestID() string {
	value := make([]byte, 16)
	if _, err := rand.Read(value); err != nil {
		return fmt.Sprintf("request-%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(value)
}

func validRequestID(value string) bool {
	if value == "" || len(value) > 128 {
		return false
	}
	for _, character := range value {
		if (character >= 'a' && character <= 'z') || (character >= 'A' && character <= 'Z') ||
			(character >= '0' && character <= '9') || strings.ContainsRune("-_.:", character) {
			continue
		}
		return false
	}
	return true
}

func writeJSON(response http.ResponseWriter, status int, payload any) {
	response.Header().Set("Content-Type", "application/json")
	response.WriteHeader(status)
	_ = json.NewEncoder(response).Encode(payload)
}
