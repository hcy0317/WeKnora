package lifecycle

import (
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"path"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

const CurrentSchemaVersion = 1

type Group string

const (
	GroupPaddleOCR Group = "paddleocr"
	GroupASR       Group = "asr"
	GroupReranker  Group = "reranker"
)

var managedGroups = [...]Group{GroupPaddleOCR, GroupASR, GroupReranker}

type Mode string

const (
	ModeOnDemand Mode = "on_demand"
	ModeAlwaysOn Mode = "always_on"
)

type DefaultsConfig struct {
	IdleMinutes            int `yaml:"idle_minutes" json:"idle_minutes"`
	StartupTimeoutSeconds  int `yaml:"startup_timeout_seconds" json:"startup_timeout_seconds"`
	FailureCooldownMinutes int `yaml:"failure_cooldown_minutes" json:"failure_cooldown_minutes"`
}

type GroupConfig struct {
	Mode                  Mode `yaml:"mode" json:"mode"`
	IdleMinutes           *int `yaml:"idle_minutes,omitempty" json:"idle_minutes,omitempty"`
	StartupTimeoutSeconds *int `yaml:"startup_timeout_seconds,omitempty" json:"startup_timeout_seconds,omitempty"`
}

type CatalogBackend struct {
	ID         string   `yaml:"id" json:"id"`
	Upstream   string   `yaml:"upstream" json:"upstream"`
	GPU        bool     `yaml:"gpu,omitempty" json:"gpu,omitempty"`
	Containers []string `yaml:"containers" json:"containers"`
}

type CatalogGroup struct {
	Paths    []string         `yaml:"paths" json:"paths"`
	Backends []CatalogBackend `yaml:"backends" json:"backends"`
}

type CatalogConfig struct {
	DockerHost string                 `yaml:"docker_host" json:"docker_host"`
	Groups     map[Group]CatalogGroup `yaml:"groups" json:"groups"`
}

type TLSFilesConfig struct {
	Certificate string `yaml:"certificate" json:"certificate"`
	PrivateKey  string `yaml:"private_key" json:"private_key"`
	ClientCA    string `yaml:"client_ca" json:"client_ca"`
}

type GPUAdmissionConfig struct {
	Enabled                    bool   `yaml:"enabled" json:"enabled"`
	NvidiaSMIExecutable        string `yaml:"nvidia_smi_executable" json:"nvidia_smi_executable"`
	PollSeconds                int    `yaml:"poll_seconds" json:"poll_seconds"`
	SustainSeconds             int    `yaml:"sustain_seconds" json:"sustain_seconds"`
	CooldownSeconds            int    `yaml:"cooldown_seconds" json:"cooldown_seconds"`
	RerankerEnableBelowPercent int    `yaml:"reranker_enable_below_percent" json:"reranker_enable_below_percent"`
	RerankerDisableAtPercent   int    `yaml:"reranker_disable_at_percent" json:"reranker_disable_at_percent"`
	RerankerMinimumFreeMiB     int    `yaml:"reranker_minimum_free_mib" json:"reranker_minimum_free_mib"`
	RerankerCriticalFreeMiB    int    `yaml:"reranker_critical_free_mib" json:"reranker_critical_free_mib"`
	PaddleEnableBelowPercent   int    `yaml:"paddle_enable_below_percent" json:"paddle_enable_below_percent"`
	PaddleDisableAtPercent     int    `yaml:"paddle_disable_at_percent" json:"paddle_disable_at_percent"`
	PaddleMinimumFreeMiB       int    `yaml:"paddle_minimum_free_mib" json:"paddle_minimum_free_mib"`
	PaddleCriticalFreeMiB      int    `yaml:"paddle_critical_free_mib" json:"paddle_critical_free_mib"`
}

type ControllerConfig struct {
	ListenAddress        string             `yaml:"listen_address" json:"listen_address"`
	DockerExecutable     string             `yaml:"docker_executable" json:"docker_executable"`
	ObserveOnly          bool               `yaml:"observe_only" json:"observe_only"`
	OwnerMutex           string             `yaml:"owner_mutex" json:"owner_mutex"`
	OwnerStatePath       string             `yaml:"owner_state_path" json:"owner_state_path"`
	SweepIntervalSeconds int                `yaml:"sweep_interval_seconds" json:"sweep_interval_seconds"`
	TLS                  TLSFilesConfig     `yaml:"tls" json:"tls"`
	GPUAdmission         GPUAdmissionConfig `yaml:"gpu_admission" json:"gpu_admission"`
}

type Config struct {
	SchemaVersion int                   `yaml:"schema_version" json:"schema_version"`
	Revision      uint64                `yaml:"revision" json:"revision"`
	Defaults      DefaultsConfig        `yaml:"defaults" json:"defaults"`
	Groups        map[Group]GroupConfig `yaml:"groups" json:"groups"`
	Catalog       CatalogConfig         `yaml:"catalog,omitempty" json:"catalog,omitempty"`
	Controller    ControllerConfig      `yaml:"controller,omitempty" json:"controller,omitempty"`
}

type Policy struct {
	Mode            Mode
	IdleTimeout     time.Duration
	StartupTimeout  time.Duration
	FailureCooldown time.Duration
}

func DecodeConfig(reader io.Reader) (*Config, error) {
	decoder := yaml.NewDecoder(reader)
	decoder.KnownFields(true)

	var config Config
	if err := decoder.Decode(&config); err != nil {
		return nil, fmt.Errorf("decode engine lifecycle config: %w", err)
	}
	config.applyCompatibilityDefaults()
	if err := config.Validate(); err != nil {
		return nil, err
	}

	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return nil, errors.New("engine lifecycle config must contain exactly one YAML document")
		}
		return nil, fmt.Errorf("decode trailing engine lifecycle config: %w", err)
	}

	return &config, nil
}

func (c *Config) applyCompatibilityDefaults() {
	controllerConfigured := c.Controller.ListenAddress != "" || c.Controller.DockerExecutable != "" ||
		c.Controller.OwnerMutex != "" || c.Controller.OwnerStatePath != "" ||
		c.Controller.SweepIntervalSeconds != 0 || c.Controller.TLS != (TLSFilesConfig{}) ||
		c.Controller.GPUAdmission != (GPUAdmissionConfig{})
	if !controllerConfigured {
		return
	}
	if c.Controller.OwnerStatePath == "" {
		c.Controller.OwnerStatePath = `C:\ProgramData\WeKnora\engine-ownership\owner.txt`
	}
	if c.Controller.GPUAdmission == (GPUAdmissionConfig{}) {
		c.Controller.GPUAdmission = defaultGPUAdmissionConfig()
	}
}

func defaultGPUAdmissionConfig() GPUAdmissionConfig {
	return GPUAdmissionConfig{
		Enabled:                    true,
		NvidiaSMIExecutable:        `C:\Windows\System32\nvidia-smi.exe`,
		PollSeconds:                20,
		SustainSeconds:             20,
		CooldownSeconds:            300,
		RerankerEnableBelowPercent: 70,
		RerankerDisableAtPercent:   85,
		RerankerMinimumFreeMiB:     3500,
		RerankerCriticalFreeMiB:    2048,
		PaddleEnableBelowPercent:   60,
		PaddleDisableAtPercent:     92,
		PaddleMinimumFreeMiB:       7000,
		PaddleCriticalFreeMiB:      1024,
	}
}

func (c Config) Validate() error {
	if c.SchemaVersion != CurrentSchemaVersion {
		return fmt.Errorf("unsupported engine lifecycle schema_version %d", c.SchemaVersion)
	}
	if c.Defaults.IdleMinutes < 1 {
		return errors.New("defaults.idle_minutes must be at least 1")
	}
	if c.Defaults.StartupTimeoutSeconds < 1 {
		return errors.New("defaults.startup_timeout_seconds must be at least 1")
	}
	if c.Defaults.FailureCooldownMinutes < 1 {
		return errors.New("defaults.failure_cooldown_minutes must be at least 1")
	}
	for group := range c.Groups {
		if !isManagedGroup(group) {
			return fmt.Errorf("unknown engine group %q", group)
		}
	}

	for _, group := range managedGroups {
		groupConfig, ok := c.Groups[group]
		if !ok {
			return fmt.Errorf("groups.%s is required", group)
		}
		if groupConfig.Mode != ModeOnDemand && groupConfig.Mode != ModeAlwaysOn {
			return fmt.Errorf("groups.%s.mode must be on_demand or always_on", group)
		}
		if groupConfig.IdleMinutes != nil && *groupConfig.IdleMinutes < 1 {
			return fmt.Errorf("groups.%s.idle_minutes must be at least 1", group)
		}
		if groupConfig.StartupTimeoutSeconds != nil && *groupConfig.StartupTimeoutSeconds < 1 {
			return fmt.Errorf("groups.%s.startup_timeout_seconds must be at least 1", group)
		}
	}
	if err := c.Catalog.Validate(); err != nil {
		return err
	}
	if err := c.Controller.Validate(); err != nil {
		return err
	}

	return nil
}

func (c ControllerConfig) Validate() error {
	if c.ListenAddress == "" && c.DockerExecutable == "" && c.OwnerMutex == "" && c.OwnerStatePath == "" &&
		c.SweepIntervalSeconds == 0 && c.TLS == (TLSFilesConfig{}) && c.GPUAdmission == (GPUAdmissionConfig{}) {
		return nil
	}
	if _, _, err := net.SplitHostPort(c.ListenAddress); err != nil {
		return fmt.Errorf("controller.listen_address is invalid: %w", err)
	}
	normalizedExecutable := strings.ReplaceAll(c.DockerExecutable, `\`, "/")
	if (!filepath.IsAbs(c.DockerExecutable) && !isWindowsAbsolutePath(normalizedExecutable)) ||
		!strings.EqualFold(path.Base(normalizedExecutable), "docker.exe") {
		return errors.New("controller.docker_executable must be an absolute path to docker.exe")
	}
	if !strings.HasPrefix(c.OwnerMutex, `Global\`) {
		return errors.New(`controller.owner_mutex must use the Global\ namespace`)
	}
	normalizedOwnerStatePath := strings.ReplaceAll(c.OwnerStatePath, `\`, "/")
	if (!filepath.IsAbs(c.OwnerStatePath) && !isWindowsAbsolutePath(normalizedOwnerStatePath)) ||
		!strings.EqualFold(path.Base(normalizedOwnerStatePath), "owner.txt") {
		return errors.New("controller.owner_state_path must be an absolute path ending in owner.txt")
	}
	if c.SweepIntervalSeconds < 1 {
		return errors.New("controller.sweep_interval_seconds must be at least 1")
	}
	if err := c.GPUAdmission.Validate(); err != nil {
		return err
	}
	for field, value := range map[string]string{
		"certificate": c.TLS.Certificate,
		"private_key": c.TLS.PrivateKey,
		"client_ca":   c.TLS.ClientCA,
	} {
		normalized := strings.ReplaceAll(value, `\`, "/")
		if !filepath.IsAbs(value) && !isWindowsAbsolutePath(normalized) {
			return fmt.Errorf("controller.tls.%s must be an absolute path", field)
		}
	}
	return nil
}

func (c GPUAdmissionConfig) Validate() error {
	if !c.Enabled && c == (GPUAdmissionConfig{}) {
		return nil
	}
	if !c.Enabled {
		return errors.New("controller.gpu_admission fields require enabled: true")
	}
	normalizedExecutable := strings.ReplaceAll(c.NvidiaSMIExecutable, `\`, "/")
	if (!filepath.IsAbs(c.NvidiaSMIExecutable) && !isWindowsAbsolutePath(normalizedExecutable)) ||
		!strings.EqualFold(path.Base(normalizedExecutable), "nvidia-smi.exe") {
		return errors.New("controller.gpu_admission.nvidia_smi_executable must be an absolute path to nvidia-smi.exe")
	}
	if c.PollSeconds < 1 || c.SustainSeconds < 1 || c.CooldownSeconds < 1 {
		return errors.New("controller.gpu_admission poll, sustain, and cooldown seconds must be at least 1")
	}
	for name, value := range map[string]int{
		"reranker_enable_below_percent": c.RerankerEnableBelowPercent,
		"reranker_disable_at_percent":   c.RerankerDisableAtPercent,
		"paddle_enable_below_percent":   c.PaddleEnableBelowPercent,
		"paddle_disable_at_percent":     c.PaddleDisableAtPercent,
	} {
		if value < 0 || value > 100 {
			return fmt.Errorf("controller.gpu_admission.%s must be between 0 and 100", name)
		}
	}
	if c.RerankerEnableBelowPercent >= c.RerankerDisableAtPercent ||
		c.PaddleEnableBelowPercent >= c.PaddleDisableAtPercent {
		return errors.New("controller.gpu_admission enable thresholds must be below disable thresholds")
	}
	if c.RerankerMinimumFreeMiB < 1 || c.RerankerCriticalFreeMiB < 1 ||
		c.RerankerCriticalFreeMiB >= c.RerankerMinimumFreeMiB || c.PaddleMinimumFreeMiB < 1 ||
		c.PaddleCriticalFreeMiB < 1 || c.PaddleCriticalFreeMiB >= c.PaddleMinimumFreeMiB {
		return errors.New("controller.gpu_admission free-memory thresholds are invalid")
	}
	return nil
}

func isWindowsAbsolutePath(value string) bool {
	return len(value) >= 3 && ((value[0] >= 'A' && value[0] <= 'Z') || (value[0] >= 'a' && value[0] <= 'z')) &&
		value[1] == ':' && value[2] == '/'
}

var containerNamePattern = regexp.MustCompile(`^WeKnora-[A-Za-z0-9][A-Za-z0-9_.-]*$`)

func (c CatalogConfig) Validate() error {
	if c.DockerHost == "" && len(c.Groups) == 0 {
		return nil
	}
	if c.DockerHost != "npipe:////./pipe/dockerDesktopLinuxEngine" {
		return errors.New("catalog.docker_host must use the Docker Desktop Linux engine named pipe")
	}
	for group := range c.Groups {
		if !isManagedGroup(group) {
			return fmt.Errorf("unknown catalog engine group %q", group)
		}
	}
	ownedContainers := make(map[string]Group)
	for _, group := range managedGroups {
		catalogGroup, ok := c.Groups[group]
		if !ok {
			return fmt.Errorf("catalog.groups.%s is required", group)
		}
		if len(catalogGroup.Paths) == 0 {
			return fmt.Errorf("catalog.groups.%s.paths must not be empty", group)
		}
		for _, allowedPath := range catalogGroup.Paths {
			if !strings.HasPrefix(allowedPath, "/") || path.Clean(allowedPath) != allowedPath || strings.Contains(allowedPath, "..") {
				return fmt.Errorf("catalog.groups.%s contains invalid path %q", group, allowedPath)
			}
		}
		if len(catalogGroup.Backends) == 0 {
			return fmt.Errorf("catalog.groups.%s.backends must not be empty", group)
		}
		backendIDs := make(map[string]struct{}, len(catalogGroup.Backends))
		for _, backend := range catalogGroup.Backends {
			if backend.ID == "" {
				return fmt.Errorf("catalog.groups.%s backend id is required", group)
			}
			if _, duplicate := backendIDs[backend.ID]; duplicate {
				return fmt.Errorf("catalog.groups.%s backend id %q is duplicated", group, backend.ID)
			}
			backendIDs[backend.ID] = struct{}{}
			if backend.GPU && group != GroupReranker && group != GroupPaddleOCR {
				return fmt.Errorf("catalog.groups.%s backend %q cannot request GPU admission", group, backend.ID)
			}
			upstream, err := url.Parse(backend.Upstream)
			if err != nil || upstream.Scheme != "http" || upstream.Host == "" || upstream.User != nil || upstream.RawQuery != "" || upstream.Fragment != "" {
				return fmt.Errorf("catalog.groups.%s backend %q has invalid internal upstream", group, backend.ID)
			}
			if len(backend.Containers) == 0 {
				return fmt.Errorf("catalog.groups.%s backend %q has no containers", group, backend.ID)
			}
			for _, container := range backend.Containers {
				if !containerNamePattern.MatchString(container) {
					return fmt.Errorf("catalog.groups.%s backend %q has invalid container %q", group, backend.ID, container)
				}
				if owner, duplicate := ownedContainers[container]; duplicate && owner != group {
					return fmt.Errorf("container %q is owned by both %s and %s", container, owner, group)
				}
				ownedContainers[container] = group
			}
		}
	}
	return nil
}

func isManagedGroup(group Group) bool {
	for _, managed := range managedGroups {
		if group == managed {
			return true
		}
	}
	return false
}

func (c Config) PolicyFor(group Group) (Policy, error) {
	groupConfig, ok := c.Groups[group]
	if !ok {
		return Policy{}, fmt.Errorf("unknown engine group %q", group)
	}

	idleMinutes := c.Defaults.IdleMinutes
	if groupConfig.IdleMinutes != nil {
		idleMinutes = *groupConfig.IdleMinutes
	}
	startupTimeoutSeconds := c.Defaults.StartupTimeoutSeconds
	if groupConfig.StartupTimeoutSeconds != nil {
		startupTimeoutSeconds = *groupConfig.StartupTimeoutSeconds
	}

	return Policy{
		Mode:            groupConfig.Mode,
		IdleTimeout:     time.Duration(idleMinutes) * time.Minute,
		StartupTimeout:  time.Duration(startupTimeoutSeconds) * time.Second,
		FailureCooldown: time.Duration(c.Defaults.FailureCooldownMinutes) * time.Minute,
	}, nil
}
