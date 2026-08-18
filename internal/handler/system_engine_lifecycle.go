package handler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/Tencent/WeKnora/internal/engine/adminclient"
	"github.com/Tencent/WeKnora/internal/engine/lifecycle"
	"github.com/Tencent/WeKnora/internal/logger"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/gin-gonic/gin"
)

type engineLifecycleClient interface {
	GetConfig(context.Context) (*lifecycle.Config, error)
	UpdateConfig(context.Context, uint64, lifecycle.Config) (*lifecycle.Config, error)
	GetGroupStatus(context.Context, lifecycle.Group) (lifecycle.GroupSnapshot, error)
}

type engineLifecycleDefaultsResponse struct {
	IdleMinutes            int `json:"idle_minutes"`
	StartupTimeoutSeconds  int `json:"startup_timeout_seconds"`
	FailureCooldownMinutes int `json:"failure_cooldown_minutes"`
}

type engineLifecycleGroupResponse struct {
	Mode                  lifecycle.Mode           `json:"mode"`
	IdleMinutes           *int                     `json:"idle_minutes,omitempty"`
	StartupTimeoutSeconds *int                     `json:"startup_timeout_seconds,omitempty"`
	Status                *lifecycle.GroupSnapshot `json:"status,omitempty"`
}

type engineLifecycleResponse struct {
	ControllerOnline bool                                             `json:"controller_online"`
	ObserveOnly      bool                                             `json:"observe_only"`
	Revision         uint64                                           `json:"revision"`
	Defaults         engineLifecycleDefaultsResponse                  `json:"defaults"`
	Groups           map[lifecycle.Group]engineLifecycleGroupResponse `json:"groups"`
}

type engineLifecycleDefaultsUpdate struct {
	IdleMinutes           int `json:"idle_minutes"`
	StartupTimeoutSeconds int `json:"startup_timeout_seconds"`
}

type engineLifecycleGroupUpdate struct {
	Mode                  lifecycle.Mode `json:"mode"`
	IdleMinutes           *int           `json:"idle_minutes"`
	StartupTimeoutSeconds *int           `json:"startup_timeout_seconds"`
}

type engineLifecycleUpdateRequest struct {
	Defaults *engineLifecycleDefaultsUpdate                 `json:"defaults"`
	Groups   map[lifecycle.Group]engineLifecycleGroupUpdate `json:"groups"`
}

type engineLifecyclePolicyResponse struct {
	ControllerOnline bool                                             `json:"controller_online"`
	ObserveOnly      bool                                             `json:"observe_only"`
	Revision         uint64                                           `json:"revision"`
	Defaults         engineLifecycleDefaultsResponse                  `json:"defaults"`
	Groups           map[lifecycle.Group]engineLifecycleGroupResponse `json:"groups"`
}

func (h *SystemHandler) lifecycleClient() (engineLifecycleClient, error) {
	if h.engineLifecycleClient != nil {
		return h.engineLifecycleClient, nil
	}
	return adminclient.NewFromEnvironment()
}

// GetEngineLifecycle returns the editable lifecycle policy plus live group
// state. Catalog, container identities, host paths, and mTLS material never
// cross this backend boundary.
func (h *SystemHandler) GetEngineLifecycle(c *gin.Context) {
	client, err := h.lifecycleClient()
	if err != nil {
		respondEngineControllerUnavailable(c, err)
		return
	}
	config, err := client.GetConfig(c.Request.Context())
	if err != nil {
		respondEngineControllerUnavailable(c, err)
		return
	}

	groups := make(map[lifecycle.Group]engineLifecycleGroupResponse, 3)
	for _, group := range []lifecycle.Group{
		lifecycle.GroupPaddleOCR,
		lifecycle.GroupASR,
		lifecycle.GroupReranker,
	} {
		status, statusErr := client.GetGroupStatus(c.Request.Context(), group)
		if statusErr != nil {
			respondEngineControllerUnavailable(c, statusErr)
			return
		}
		policy := config.Groups[group]
		groups[group] = engineLifecycleGroupResponse{
			Mode:                  policy.Mode,
			IdleMinutes:           policy.IdleMinutes,
			StartupTimeoutSeconds: policy.StartupTimeoutSeconds,
			Status:                &status,
		}
	}

	c.Header("ETag", fmt.Sprintf(`"%d"`, config.Revision))
	c.JSON(http.StatusOK, engineLifecycleResponse{
		ControllerOnline: true,
		ObserveOnly:      config.Controller.ObserveOnly,
		Revision:         config.Revision,
		Defaults: engineLifecycleDefaultsResponse{
			IdleMinutes:            config.Defaults.IdleMinutes,
			StartupTimeoutSeconds:  config.Defaults.StartupTimeoutSeconds,
			FailureCooldownMinutes: config.Defaults.FailureCooldownMinutes,
		},
		Groups: groups,
	})
}

// UpdateEngineLifecycle applies a whole editable-policy revision. The handler
// first overlays the submitted policy onto the current host config, preserving
// controller, catalog, and failure-cooldown fields before the controller's own
// If-Match CAS performs the authoritative write.
func (h *SystemHandler) UpdateEngineLifecycle(c *gin.Context) {
	expectedRevision, err := parseEngineLifecycleIfMatch(c.GetHeader("If-Match"))
	if err != nil {
		c.JSON(http.StatusPreconditionRequired, gin.H{"error": err.Error()})
		return
	}
	request, err := decodeEngineLifecycleUpdate(c)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	client, err := h.lifecycleClient()
	if err != nil {
		respondEngineControllerUnavailable(c, err)
		return
	}
	current, err := client.GetConfig(c.Request.Context())
	if err != nil {
		respondEngineControllerUnavailable(c, err)
		return
	}
	if current.Revision != expectedRevision {
		respondEngineLifecycleConflict(c, current.Revision, "engine lifecycle config revision changed")
		return
	}

	candidate, err := overlayEngineLifecyclePolicy(*current, request)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	updated, err := client.UpdateConfig(c.Request.Context(), expectedRevision, candidate)
	if err != nil {
		var responseError *adminclient.HTTPError
		if errors.As(err, &responseError) {
			switch responseError.StatusCode {
			case http.StatusConflict:
				respondEngineLifecycleConflict(c, responseError.Revision, responseError.Error())
			case http.StatusBadRequest, http.StatusPreconditionRequired:
				c.JSON(responseError.StatusCode, gin.H{"error": responseError.Error()})
			default:
				respondEngineControllerUnavailable(c, responseError)
			}
			return
		}
		respondEngineControllerUnavailable(c, err)
		return
	}

	h.emitEngineLifecycleConfigAudit(c.Request.Context(), *current, *updated)
	c.Header("ETag", fmt.Sprintf(`"%d"`, updated.Revision))
	c.JSON(http.StatusOK, projectEngineLifecyclePolicy(*updated))
}

func decodeEngineLifecycleUpdate(c *gin.Context) (engineLifecycleUpdateRequest, error) {
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, 64<<10)
	decoder := json.NewDecoder(c.Request.Body)
	decoder.DisallowUnknownFields()
	var request engineLifecycleUpdateRequest
	if err := decoder.Decode(&request); err != nil {
		return request, fmt.Errorf("invalid JSON body: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		if err == nil {
			err = errors.New("multiple JSON values are not allowed")
		}
		return request, fmt.Errorf("invalid JSON body: %w", err)
	}
	if request.Defaults == nil {
		return request, errors.New("defaults is required")
	}
	return request, nil
}

func overlayEngineLifecyclePolicy(
	current lifecycle.Config,
	request engineLifecycleUpdateRequest,
) (lifecycle.Config, error) {
	managedGroups := []lifecycle.Group{
		lifecycle.GroupPaddleOCR,
		lifecycle.GroupASR,
		lifecycle.GroupReranker,
	}
	if len(request.Groups) != len(managedGroups) {
		return lifecycle.Config{}, errors.New("paddleocr, asr, and reranker groups are required")
	}
	groups := make(map[lifecycle.Group]lifecycle.GroupConfig, len(managedGroups))
	for _, group := range managedGroups {
		policy, ok := request.Groups[group]
		if !ok {
			return lifecycle.Config{}, fmt.Errorf("groups.%s is required", group)
		}
		groups[group] = lifecycle.GroupConfig{
			Mode:                  policy.Mode,
			IdleMinutes:           policy.IdleMinutes,
			StartupTimeoutSeconds: policy.StartupTimeoutSeconds,
		}
	}
	current.Defaults.IdleMinutes = request.Defaults.IdleMinutes
	current.Defaults.StartupTimeoutSeconds = request.Defaults.StartupTimeoutSeconds
	current.Groups = groups
	if err := current.Validate(); err != nil {
		return lifecycle.Config{}, err
	}
	return current, nil
}

func projectEngineLifecyclePolicy(config lifecycle.Config) engineLifecyclePolicyResponse {
	groups := make(map[lifecycle.Group]engineLifecycleGroupResponse, len(config.Groups))
	for group, policy := range config.Groups {
		groups[group] = engineLifecycleGroupResponse{
			Mode:                  policy.Mode,
			IdleMinutes:           policy.IdleMinutes,
			StartupTimeoutSeconds: policy.StartupTimeoutSeconds,
		}
	}
	return engineLifecyclePolicyResponse{
		ControllerOnline: true,
		ObserveOnly:      config.Controller.ObserveOnly,
		Revision:         config.Revision,
		Defaults: engineLifecycleDefaultsResponse{
			IdleMinutes:            config.Defaults.IdleMinutes,
			StartupTimeoutSeconds:  config.Defaults.StartupTimeoutSeconds,
			FailureCooldownMinutes: config.Defaults.FailureCooldownMinutes,
		},
		Groups: groups,
	}
}

func parseEngineLifecycleIfMatch(value string) (uint64, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0, errors.New("If-Match header is required")
	}
	if strings.HasPrefix(value, "W/") {
		return 0, errors.New("weak If-Match values are not supported")
	}
	revision, err := strconv.ParseUint(strings.Trim(value, `"`), 10, 64)
	if err != nil {
		return 0, errors.New("If-Match must contain a numeric config revision")
	}
	return revision, nil
}

func respondEngineLifecycleConflict(c *gin.Context, revision uint64, message string) {
	if revision != 0 {
		c.Header("ETag", fmt.Sprintf(`"%d"`, revision))
	}
	c.JSON(http.StatusConflict, gin.H{
		"controller_online": true,
		"error":             message,
		"revision":          revision,
	})
}

func (h *SystemHandler) emitEngineLifecycleConfigAudit(
	ctx context.Context,
	oldConfig lifecycle.Config,
	newConfig lifecycle.Config,
) {
	if h.auditSvc == nil {
		return
	}
	actorID, _ := types.UserIDFromContext(ctx)
	details, _ := json.Marshal(map[string]any{
		"old_revision": oldConfig.Revision,
		"new_revision": newConfig.Revision,
		"old_policy":   projectEngineLifecyclePolicy(oldConfig),
		"new_policy":   projectEngineLifecyclePolicy(newConfig),
	})
	_ = h.auditSvc.Log(ctx, &types.AuditLog{
		TenantID:    0,
		ActorUserID: actorID,
		ActorRole:   systemAuditActorRole(ctx),
		Action:      types.AuditActionSystemSettingChanged,
		TargetType:  "engine_lifecycle_config",
		TargetID:    "host_yaml",
		Outcome:     types.AuditOutcomeSuccess,
		Details:     types.JSON(details),
	})
}

func respondEngineControllerUnavailable(c *gin.Context, err error) {
	logger.Errorf(c.Request.Context(), "engine lifecycle controller unavailable: %v", err)
	c.JSON(http.StatusServiceUnavailable, gin.H{
		"controller_online": false,
		"error":             "engine lifecycle controller is unavailable",
	})
}
