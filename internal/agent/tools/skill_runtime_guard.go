package tools

import (
	"strconv"
	"strings"

	"github.com/Tencent/WeKnora/internal/agent/skills"
	"github.com/Tencent/WeKnora/internal/sandbox"
)

// A skill's dependencies are supposed to be complete when the install
// finishes. When they are not, what the model gets back at chat time carries
// no direction: "No module named pip" from a uv-created venv, or a bare
// permission error against a `.venv` path, says nothing about which of the
// several plausible recoveries is the right one. The hints below attach that
// direction after the fact.
//
// The recovery is to install into the skill's own environment, in place. That
// works because the sandbox belongs to this session alone and runs as root, so
// the write lands in the session's own container and dies with it — it never
// reaches the image other sessions start from. The overlay this guidance used
// to recommend (`pip install --target /workspace/.skill-packages/<skill>`)
// bought nothing over that and split a skill's packages across two locations.
//
// An up-front command blacklist was tried and removed: matching `pip install`
// next to the skills root also rejected the recovery command this guidance
// recommends, while any indirection through a shell variable walked straight
// past it.

// These commands run with skill_name so the shell environment supplies the
// actual image or staged directory. Keep work_dir at its /workspace default.
const (
	skillPythonPackageInstallCommand = `uv pip install --python ` +
		`"${WEKNORA_SKILL_DIR:?}/.venv/bin/python" <package>`
	skillPythonPackageFallbackCommand = `"${WEKNORA_SKILL_DIR:?}/.venv/bin/python" -m ensurepip --upgrade && ` +
		`"${WEKNORA_SKILL_DIR:?}/.venv/bin/python" -m pip install <package>`
	skillPythonVenvCreateCommand   = `python3 -m venv --without-pip "${WEKNORA_SKILL_DIR:?}/.venv"`
	skillNodePackageInstallCommand = `npm --prefix "${WEKNORA_SKILL_DIR:?}" install <package>`
)

func missingSkillPackageGuidance(skillName string) string {
	skillArg := "skill_name=<skill>"
	if skillName != "" {
		skillArg = "skill_name=" + strconv.Quote(skillName)
	}
	return "For a one-session Python extra, use shell_exec(" + skillArg +
		", command=python3 -m pip install --target \"$WEKNORA_SKILL_PACKAGE_DIR\" <package>). " +
		"For Node, use npm --prefix \"$WEKNORA_SKILL_PACKAGE_DIR\" install <package>. " +
		"The named call adds the package directory to PYTHONPATH and its node_modules to NODE_PATH. " +
		"This leaves the installed skill image frozen; reinstall the skill to make dependencies permanent."
}

func isSkillVenvInstallFailure(stderr string) bool {
	if stderr == "" {
		return false
	}
	lower := strings.ToLower(stderr)
	if strings.Contains(stderr, "No module named pip") ||
		strings.Contains(stderr, "No module named 'pip'") {
		return true
	}
	return strings.Contains(lower, ".venv") &&
		(isReadOnlyFilesystemFailure(lower) || isPermissionFailure(lower))
}

func isReadOnlyFilesystemFailure(stderr string) bool {
	lower := strings.ToLower(stderr)
	return strings.Contains(lower, "read-only file system") || strings.Contains(lower, "erofs") ||
		strings.Contains(lower, "read-only filesystem")
}

func isPermissionFailure(stderr string) bool {
	lower := strings.ToLower(stderr)
	return strings.Contains(lower, "permission denied") || strings.Contains(lower, "operation not permitted") ||
		strings.Contains(lower, "eperm")
}

func skillVenvFailureGuidance(skillName, stderr string) string {
	if isReadOnlyFilesystemFailure(stderr) {
		return "The installed skill environment is read-only. Do not retry the install there; " +
			missingSkillPackageGuidance(skillName)
	}
	if isPermissionFailure(stderr) {
		return "The installed skill environment denied writes. Do not change its permissions; " +
			missingSkillPackageGuidance(skillName)
	}
	return missingSkillPackageGuidance(skillName)
}

// frozenSkillTreeGuidance preserves the installed-image recovery path for the
// old execute_skill_script caller while steering package writes into the
// session-local overlay.
func frozenSkillTreeGuidance(skillName string) string {
	pkgDir := sandbox.SessionSkillPackageDir(skillName)
	skillArg := "skill_name=<skill>"
	if skillName != "" {
		skillArg = "skill_name=" + strconv.Quote(skillName)
	}
	return "the skill tree under " + sandbox.SkillsImageRoot +
		" and its frozen venv are read-only after install. Do not chown, chmod, ensurepip, or install into them. " +
		"One-off extras go in the session overlay: python3 -m pip install --target " + pkgDir + " <package>. " +
		"Run the skill again through shell_exec(" + skillArg + ", command=...)."
}

func isFrozenSkillVenvFailure(stderr string) bool {
	return isSkillVenvInstallFailure(stderr)
}

func skillOnDemandInstallHint(skillName, scriptPath, stdout, stderr string) string {
	installer := skills.IsOnDemandInstallerPath(scriptPath) ||
		strings.Contains(strings.ToLower(scriptPath), "install_deps.py")
	failureText := strings.ToLower(stdout + stderr)
	failedInstaller := installer && (isFrozenSkillVenvFailure(stderr) ||
		strings.Contains(failureText, "安装失败") || strings.Contains(failureText, "install failed"))
	if !failedInstaller && !isFrozenSkillVenvFailure(stderr) {
		return ""
	}
	message := "Hint: " + frozenSkillTreeGuidance(skillName)
	if installer {
		message += " Skip this installer and run the skill's real script."
	}
	return message
}

func skillMissingPackageHint(skillName, stderr string) string {
	if !isMissingInterpreterModule(stderr) {
		return ""
	}
	return "Hint: this skill's frozen venv does not have that package. " +
		frozenSkillTreeGuidance(skillName)
}
