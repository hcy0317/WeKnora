package hostcontroller

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"path"
	"path/filepath"
	"regexp"
	"strings"
)

const dockerDesktopLinuxHost = "npipe:////./pipe/dockerDesktopLinuxEngine"

var managedContainerPattern = regexp.MustCompile(`^WeKnora-[A-Za-z0-9][A-Za-z0-9_.-]*$`)

type processExecutor interface {
	Execute(ctx context.Context, executable string, arguments ...string) (string, error)
}

type osProcessExecutor struct{}

func (osProcessExecutor) Execute(ctx context.Context, executable string, arguments ...string) (string, error) {
	command := exec.CommandContext(ctx, executable, arguments...)
	output, err := command.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("docker command failed: %w: %s", err, strings.TrimSpace(string(output)))
	}
	return strings.TrimSpace(string(output)), nil
}

type DockerCLIOption func(*DockerCLI)

func withProcessExecutor(executor processExecutor) DockerCLIOption {
	return func(cli *DockerCLI) {
		if executor != nil {
			cli.executor = executor
		}
	}
}

type DockerCLI struct {
	executable string
	dockerHost string
	executor   processExecutor
}

func NewDockerCLI(executable, dockerHost string, options ...DockerCLIOption) (*DockerCLI, error) {
	normalized := strings.ReplaceAll(executable, `\`, "/")
	if !filepath.IsAbs(executable) && !isWindowsAbsolutePath(normalized) {
		return nil, errors.New("docker executable must be an absolute path")
	}
	if !strings.EqualFold(path.Base(normalized), "docker.exe") {
		return nil, errors.New("docker executable must name docker.exe")
	}
	if dockerHost != dockerDesktopLinuxHost {
		return nil, errors.New("docker host must be the Docker Desktop Linux engine named pipe")
	}
	cli := &DockerCLI{
		executable: executable,
		dockerHost: dockerHost,
		executor:   osProcessExecutor{},
	}
	for _, option := range options {
		option(cli)
	}
	return cli, nil
}

func isWindowsAbsolutePath(value string) bool {
	return len(value) >= 3 && ((value[0] >= 'A' && value[0] <= 'Z') || (value[0] >= 'a' && value[0] <= 'z')) &&
		value[1] == ':' && value[2] == '/'
}

func (c *DockerCLI) Run(ctx context.Context, arguments ...string) (string, error) {
	if !allowedDockerArguments(arguments) {
		return "", fmt.Errorf("docker arguments are not allowed: %q", arguments)
	}
	commandArguments := make([]string, 0, len(arguments)+2)
	commandArguments = append(commandArguments, "-H", c.dockerHost)
	commandArguments = append(commandArguments, arguments...)
	return c.executor.Execute(ctx, c.executable, commandArguments...)
}

func allowedDockerArguments(arguments []string) bool {
	if len(arguments) == 2 && arguments[0] == "start" {
		return managedContainerPattern.MatchString(arguments[1])
	}
	if len(arguments) == 4 && arguments[0] == "stop" && arguments[1] == "--time" && arguments[2] == "20" {
		return managedContainerPattern.MatchString(arguments[3])
	}
	if len(arguments) == 4 && arguments[0] == "inspect" && arguments[1] == "--format" &&
		arguments[2] == "{{json .State}}" {
		return managedContainerPattern.MatchString(arguments[3])
	}
	return false
}
