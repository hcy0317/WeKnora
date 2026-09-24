package skills

import (
	"context"
	"fmt"
	"path"
	"strings"

	"github.com/Tencent/WeKnora/internal/sandbox"
)

// PrepareShellEnvironment attaches an allowed, installed skill's runtime to
// the same shell primitive used for ordinary commands. Credentials remain the
// caller's responsibility and are resolved per tool call, never persisted here.
func (m *Manager) PrepareShellEnvironment(ctx context.Context, sessionID, skillName, command string, env map[string]string) (string, map[string]string, error) {
	if err := ctx.Err(); err != nil {
		return "", nil, err
	}
	if m == nil || !m.enabled || !m.isSkillAllowed(skillName) {
		return "", nil, fmt.Errorf("skill %q is not available to this agent", skillName)
	}
	dir, ok := m.SandboxSkillDir(skillName)
	if !ok {
		var err error
		dir, err = m.stageShellSkill(ctx, sessionID, skillName)
		if err != nil {
			return "", nil, err
		}
	} else if _, valid := sandbox.ValidatedSkillDirUnder(m.installedSkillsRoot(), dir); !valid {
		return "", nil, fmt.Errorf("invalid installed directory for skill %q", skillName)
	}
	layout := sandbox.RemoteWorkspaceLayout()
	if provider, supported := m.sandboxMgr.(sandbox.SessionWorkspaceLayoutProvider); supported && provider != nil {
		var err error
		layout, err = provider.SessionWorkspaceLayout(ctx, sessionID)
		if err != nil {
			return "", nil, fmt.Errorf("resolve skill workspace layout: %w", err)
		}
		layout = layout.Normalized()
	}
	if layout.IsHost() && !layout.HasRoot() {
		return "", nil, fmt.Errorf("host skill workspace is unavailable")
	}
	runtimeEnv := make(map[string]string, len(env)+6)
	for k, v := range env {
		runtimeEnv[k] = v
	}
	applySkillNodePath(runtimeEnv, dir)
	packageDir := sandbox.SessionSkillPackageDir(skillName)
	if layout.IsHost() {
		packageDir = path.Join(layout.Root, ".skill-packages", skillName)
	}
	applySessionPackagePathAt(runtimeEnv, packageDir)
	runtimeEnv[skillPackageEnvVar] = packageDir
	runtimeEnv[skillDirEnvVar] = dir
	outputDir := strings.TrimSpace(layout.OutputDir)
	inputDir := strings.TrimSpace(layout.InputDir)
	if layout.IsHost() {
		if root, ok := m.hostWorkspaceRoot(ctx, sessionID); ok && outputDir == "" {
			outputDir = root
		}
	}
	if outputDir == "" && !layout.IsHost() {
		outputDir = ArtifactOutputDir()
	}
	if outputDir != "" {
		runtimeEnv[artifactOutputEnvVar] = outputDir
		runtimeEnv[artifactHistoryEnvVar] = outputDir
	} else {
		delete(runtimeEnv, artifactOutputEnvVar)
		delete(runtimeEnv, artifactHistoryEnvVar)
	}
	if inputDir == "" && !layout.IsHost() {
		inputDir = sandbox.SessionInputRoot
	}
	if inputDir != "" {
		runtimeEnv[sessionInputEnvVar] = inputDir
	} else {
		delete(runtimeEnv, sessionInputEnvVar)
	}
	// Set PATH after the provider's login shell has loaded its profiles. Use a
	// child non-login shell so leading assignments and arbitrary shell grammar
	// keep their original meaning and cannot consume the setup prefix.
	prefix := sandbox.SkillCommandPath(dir)
	wrapped := "export PATH=" + sandbox.ShellQuote(prefix) + ":\"$PATH\"; exec /bin/bash --noprofile --norc -c " + sandbox.ShellQuote(command)
	return wrapped, runtimeEnv, nil
}

func (m *Manager) hostWorkspaceRoot(ctx context.Context, sessionID string) (string, bool) {
	provider, ok := m.sandboxMgr.(sandbox.SessionWorkspaceLayoutProvider)
	if !ok || provider == nil {
		return "", false
	}
	layout, err := provider.SessionWorkspaceLayout(ctx, sessionID)
	if err != nil || !layout.IsHost() || !layout.HasRoot() {
		return "", false
	}
	return layout.Root, true
}
