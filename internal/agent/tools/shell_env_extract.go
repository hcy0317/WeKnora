package tools

import (
	"regexp"
	"strings"
)

// assignmentPattern finds NAME=value in a model-built shell command.
// It is not used on user chat text. The name must be UPPER_SNAKE_CASE so
// flags like --model and URLs are not treated as environment variables.
var assignmentPattern = regexp.MustCompile(
	`(?:^|[;|&\s])(?:export\s+)?([A-Z_][A-Z0-9_]{0,127})=(?:"([^"]*)"|'([^']*)'|([^\s;|&]+))`,
)

var bareLiteralPattern = regexp.MustCompile(`^[0-9A-Za-z_./:@%+,=-]+$`)

func literalAssignmentValue(command string, match []int) (string, bool) {
	if len(match) < 10 {
		return "", false
	}
	if end := match[1]; end < len(command) && !strings.ContainsRune(";|& \t\r\n", rune(command[end])) {
		return "", false
	}
	group := func(i int) string {
		start, end := match[i*2], match[i*2+1]
		if start < 0 || end < 0 {
			return ""
		}
		return command[start:end]
	}
	// Single-quoted shell text is literal. Double-quoted text is accepted only
	// when it cannot expand a variable, command, or escape sequence. Bare text
	// is deliberately narrower still: metacharacters, globs and substitutions
	// make the value runtime-derived rather than a supplied credential.
	if single := group(3); single != "" {
		return single, true
	}
	if double := group(2); double != "" {
		if strings.ContainsAny(double, "$`\\") {
			return "", false
		}
		return double, true
	}
	if bare := group(4); bare != "" && bareLiteralPattern.MatchString(bare) {
		return bare, true
	}
	return "", false
}

func extractExportedEnv(command string) map[string]string {
	out := map[string]string{}
	chainEnd := -1
	chainControl := -1
	for _, match := range assignmentPattern.FindAllStringSubmatchIndex(command, -1) {
		nameStart := match[2]
		control := lastShellControl(command, nameStart)
		prefix := strings.TrimSpace(command[control:nameStart])
		positionAllowed := prefix == "" || prefix == "export"
		if !positionAllowed && chainEnd >= control && chainControl == control &&
			strings.TrimSpace(command[chainEnd:nameStart]) == "" {
			positionAllowed = true
		}
		if !positionAllowed {
			chainEnd = -1
			chainControl = control
			continue
		}
		name := command[match[2]:match[3]]
		value, literal := literalAssignmentValue(command, match)
		if name == "" || value == "" || !literal {
			chainEnd = -1
			chainControl = control
			continue
		}
		out[name] = value
		chainEnd = match[1]
		chainControl = control
	}
	return out
}

func lastShellControl(command string, end int) int {
	start := 0
	single, double, escaped := false, false, false
	for i := 0; i < end; i++ {
		ch := command[i]
		if escaped {
			escaped = false
			continue
		}
		if ch == '\\' && !single {
			escaped = true
			continue
		}
		if ch == '\'' && !double {
			single = !single
			continue
		}
		if ch == '"' && !single {
			double = !double
			continue
		}
		if !single && !double && (ch == ';' || ch == '|' || ch == '&' || ch == '\n') {
			start = i + 1
		}
	}
	return start
}

func collectUsedSkillEnv(command string, toolEnv map[string]string) map[string]string {
	out := extractExportedEnv(command)
	for name, value := range toolEnv {
		if strings.TrimSpace(value) == "" {
			continue
		}
		out[name] = value
	}
	return out
}

// maskCommandAssignments replaces the value of every NAME=value assignment with
// a placeholder. A command is logged at Info, and passing a credential inline is
// a documented way to hand a skill its key, so the raw string must never reach
// the log.
func maskCommandAssignments(command string) string {
	return assignmentPattern.ReplaceAllStringFunc(command, func(match string) string {
		eq := strings.Index(match, "=")
		if eq < 0 {
			return match
		}
		return match[:eq+1] + "***"
	})
}
