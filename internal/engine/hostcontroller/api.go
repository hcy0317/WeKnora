package hostcontroller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/Tencent/WeKnora/internal/engine/lifecycle"
)

const (
	ClientCNGateway   = "weknora-engine-gateway"
	ClientCNBackend   = "weknora-backend"
	ClientCNBootstrap = "weknora-engine-bootstrap"
)

type API struct {
	coordinator *lifecycle.Coordinator
	configStore *lifecycle.ConfigStore
	handler     http.Handler
}

func NewAPI(coordinator *lifecycle.Coordinator, configStore *lifecycle.ConfigStore) *API {
	api := &API{coordinator: coordinator, configStore: configStore}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", api.handleHealth)
	mux.HandleFunc("POST /v1/groups/{group}/leases", api.handleAcquire)
	mux.HandleFunc("POST /v1/leases/{lease}/renew", api.handleRenew)
	mux.HandleFunc("DELETE /v1/leases/{lease}", api.handleRelease)
	mux.HandleFunc("POST /v1/gateways/{gateway}/reconcile", api.handleReconcile)
	mux.HandleFunc("GET /v1/groups/{group}", api.handleGroupStatus)
	mux.HandleFunc("GET /v1/config", api.handleConfigGet)
	mux.HandleFunc("PUT /v1/config", api.handleConfigPut)
	api.handler = api.requireKnownClient(mux)
	return api
}

func (a *API) ServeHTTP(response http.ResponseWriter, request *http.Request) {
	a.handler.ServeHTTP(response, request)
}

func (a *API) requireKnownClient(next http.Handler) http.Handler {
	return http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.URL.Path == "/healthz" {
			next.ServeHTTP(response, request)
			return
		}
		commonName := clientCommonName(request)
		if commonName != ClientCNGateway && commonName != ClientCNBackend && commonName != ClientCNBootstrap {
			writeError(response, http.StatusUnauthorized, "valid controller client certificate required")
			return
		}
		next.ServeHTTP(response, request)
	})
}

func clientCommonName(request *http.Request) string {
	if request.TLS == nil || len(request.TLS.PeerCertificates) == 0 {
		return ""
	}
	return request.TLS.PeerCertificates[0].Subject.CommonName
}

func (a *API) handleHealth(response http.ResponseWriter, _ *http.Request) {
	writeJSON(response, http.StatusOK, map[string]string{"status": "ok"})
}

type acquirePayload struct {
	RequestID string    `json:"request_id"`
	GatewayID string    `json:"gateway_id"`
	Purpose   string    `json:"purpose"`
	Deadline  time.Time `json:"deadline,omitempty"`
}

func (a *API) handleAcquire(response http.ResponseWriter, request *http.Request) {
	if clientCommonName(request) != ClientCNGateway {
		writeError(response, http.StatusForbidden, "client certificate cannot acquire engine leases")
		return
	}
	if a.coordinator == nil {
		writeError(response, http.StatusServiceUnavailable, "controller coordinator is unavailable")
		return
	}
	group := lifecycle.Group(request.PathValue("group"))
	if group != lifecycle.GroupPaddleOCR && group != lifecycle.GroupASR && group != lifecycle.GroupReranker {
		writeError(response, http.StatusNotFound, "unknown engine group")
		return
	}
	var payload acquirePayload
	if err := decodeJSON(response, request, &payload); err != nil {
		return
	}
	if payload.RequestID == "" || payload.GatewayID == "" || payload.Purpose == "" {
		writeError(response, http.StatusBadRequest, "request_id, gateway_id, and purpose are required")
		return
	}
	requestContext := request.Context()
	if !payload.Deadline.IsZero() {
		var cancel context.CancelFunc
		requestContext, cancel = context.WithDeadline(requestContext, payload.Deadline)
		defer cancel()
	}
	lease, err := a.coordinator.Acquire(requestContext, group, lifecycle.AcquireRequest{
		RequestID: payload.RequestID,
		GatewayID: payload.GatewayID,
		Purpose:   payload.Purpose,
	})
	if err != nil {
		status := http.StatusServiceUnavailable
		var failure *lifecycle.Failure
		if errors.As(err, &failure) && failure.Kind == lifecycle.FailureCooldownActive {
			status = http.StatusTooManyRequests
		}
		writeError(response, status, err.Error())
		return
	}
	writeJSON(response, http.StatusCreated, lease)
}

func (a *API) handleRenew(response http.ResponseWriter, request *http.Request) {
	if clientCommonName(request) != ClientCNGateway {
		writeError(response, http.StatusForbidden, "client certificate cannot renew engine leases")
		return
	}
	if err := a.coordinator.Renew(request.PathValue("lease")); err != nil {
		writeError(response, http.StatusNotFound, err.Error())
		return
	}
	response.WriteHeader(http.StatusNoContent)
}

func (a *API) handleRelease(response http.ResponseWriter, request *http.Request) {
	if clientCommonName(request) != ClientCNGateway {
		writeError(response, http.StatusForbidden, "client certificate cannot release engine leases")
		return
	}
	reason := lifecycle.ReleaseReason(request.URL.Query().Get("reason"))
	if reason == "" {
		reason = lifecycle.ReleaseCompleted
	}
	if reason != lifecycle.ReleaseCompleted && reason != lifecycle.ReleaseClientDisconnect &&
		reason != lifecycle.ReleaseUpstreamError {
		writeError(response, http.StatusBadRequest, "unknown release reason")
		return
	}
	_ = a.coordinator.Release(request.PathValue("lease"), reason)
	response.WriteHeader(http.StatusNoContent)
}

func (a *API) handleReconcile(response http.ResponseWriter, request *http.Request) {
	if clientCommonName(request) != ClientCNGateway {
		writeError(response, http.StatusForbidden, "client certificate cannot reconcile gateway leases")
		return
	}
	var payload lifecycle.GatewayReconcile
	if err := decodeJSON(response, request, &payload); err != nil {
		return
	}
	gatewayID := request.PathValue("gateway")
	if payload.GatewayID != "" && payload.GatewayID != gatewayID {
		writeError(response, http.StatusBadRequest, "gateway_id does not match request path")
		return
	}
	payload.GatewayID = gatewayID
	if err := a.coordinator.ReconcileGateway(payload); err != nil {
		writeError(response, http.StatusBadRequest, err.Error())
		return
	}
	response.WriteHeader(http.StatusNoContent)
}

func (a *API) handleGroupStatus(response http.ResponseWriter, request *http.Request) {
	commonName := clientCommonName(request)
	if commonName != ClientCNGateway && commonName != ClientCNBackend {
		writeError(response, http.StatusForbidden, "client certificate cannot read engine status")
		return
	}
	group := lifecycle.Group(request.PathValue("group"))
	snapshot, err := a.coordinator.Snapshot(group)
	if err != nil {
		writeError(response, http.StatusNotFound, err.Error())
		return
	}
	writeJSON(response, http.StatusOK, snapshot)
}

func (a *API) handleConfigGet(response http.ResponseWriter, request *http.Request) {
	if clientCommonName(request) != ClientCNBackend {
		writeError(response, http.StatusForbidden, "client certificate cannot read controller config")
		return
	}
	if a.configStore == nil {
		writeError(response, http.StatusServiceUnavailable, "controller config store is unavailable")
		return
	}
	config, err := a.configStore.Load()
	if err != nil {
		writeError(response, http.StatusServiceUnavailable, err.Error())
		return
	}
	setETag(response, config.Revision)
	writeJSON(response, http.StatusOK, config)
}

func (a *API) handleConfigPut(response http.ResponseWriter, request *http.Request) {
	if clientCommonName(request) != ClientCNBackend {
		writeError(response, http.StatusForbidden, "client certificate cannot update controller config")
		return
	}
	if a.configStore == nil {
		writeError(response, http.StatusServiceUnavailable, "controller config store is unavailable")
		return
	}
	expectedRevision, err := parseIfMatch(request.Header.Get("If-Match"))
	if err != nil {
		writeError(response, http.StatusPreconditionRequired, err.Error())
		return
	}
	current, err := a.configStore.Load()
	if err != nil {
		writeError(response, http.StatusServiceUnavailable, err.Error())
		return
	}
	if expectedRevision != current.Revision {
		setETag(response, current.Revision)
		writeError(response, http.StatusConflict, (&lifecycle.RevisionConflictError{
			Expected: expectedRevision,
			Actual:   current.Revision,
		}).Error())
		return
	}

	var candidate lifecycle.Config
	if err := decodeJSON(response, request, &candidate); err != nil {
		return
	}
	if candidate.SchemaVersion != current.SchemaVersion ||
		candidate.Defaults.FailureCooldownMinutes != current.Defaults.FailureCooldownMinutes ||
		!reflect.DeepEqual(candidate.Catalog, current.Catalog) ||
		!reflect.DeepEqual(candidate.Controller, current.Controller) {
		writeError(response, http.StatusBadRequest, "schema_version, failure cooldown, catalog, and controller are readonly")
		return
	}
	updated, err := a.configStore.Update(expectedRevision, candidate)
	if err != nil {
		var conflict *lifecycle.RevisionConflictError
		if errors.As(err, &conflict) {
			setETag(response, conflict.Actual)
			writeError(response, http.StatusConflict, err.Error())
			return
		}
		writeError(response, http.StatusBadRequest, err.Error())
		return
	}
	if err := a.coordinator.ApplyConfig(*updated); err != nil {
		writeError(response, http.StatusInternalServerError, fmt.Sprintf("apply controller config: %v", err))
		return
	}
	setETag(response, updated.Revision)
	writeJSON(response, http.StatusOK, updated)
}

func parseIfMatch(value string) (uint64, error) {
	if value == "" {
		return 0, errors.New("If-Match header is required")
	}
	value = strings.TrimSpace(value)
	if strings.HasPrefix(value, "W/") {
		return 0, errors.New("weak If-Match values are not supported")
	}
	value = strings.Trim(value, `"`)
	revision, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		return 0, errors.New("If-Match must contain a numeric config revision")
	}
	return revision, nil
}

func setETag(response http.ResponseWriter, revision uint64) {
	response.Header().Set("ETag", fmt.Sprintf(`"%d"`, revision))
}

func decodeJSON(response http.ResponseWriter, request *http.Request, target any) error {
	request.Body = http.MaxBytesReader(response, request.Body, 64<<10)
	decoder := json.NewDecoder(request.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		writeError(response, http.StatusBadRequest, fmt.Sprintf("invalid JSON body: %v", err))
		return err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		if err == nil {
			err = errors.New("multiple JSON values are not allowed")
		}
		writeError(response, http.StatusBadRequest, fmt.Sprintf("invalid JSON body: %v", err))
		return err
	}
	return nil
}

func writeJSON(response http.ResponseWriter, status int, payload any) {
	response.Header().Set("Content-Type", "application/json")
	response.WriteHeader(status)
	_ = json.NewEncoder(response).Encode(payload)
}

func writeError(response http.ResponseWriter, status int, message string) {
	writeJSON(response, status, map[string]string{"error": message})
}
