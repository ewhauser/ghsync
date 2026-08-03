// Package codeowners parses and resolves CODEOWNERS path rules without
// applying any reviewer-ranking policy.
package codeowners

import (
	"fmt"
	"regexp"
	"strings"
	"unicode"
)

// OwnerType is the syntactic identity kind carried by an owner token.
type OwnerType string

const (
	OwnerUser      OwnerType = "user"
	OwnerTeam      OwnerType = "team"
	OwnerEmail     OwnerType = "email"
	OwnerMalformed OwnerType = "malformed"
)

// Owner preserves one source token exactly while exposing its syntactic kind
// and normalized lookup name.
type Owner struct {
	Token string
	Type  OwnerType
	Name  string
}

// Rule is one valid CODEOWNERS rule. Pattern is the source spelling, including
// escapes; Line is one-based.
type Rule struct {
	Pattern string
	Line    int
	Owners  []Owner
	matcher *regexp.Regexp
}

// Match is the last rule that matched one repository-relative file path.
type Match struct {
	Pattern string
	Line    int
	Owners  []Owner
}

// Parse applies the CODEOWNERS subset of gitignore syntax. Invalid pattern
// lines are skipped, while malformed owner tokens on a valid rule are kept as
// explicit OwnerMalformed facts.
func Parse(source string) []Rule {
	lines := strings.Split(strings.ReplaceAll(source, "\r\n", "\n"), "\n")
	rules := make([]Rule, 0, len(lines))
	for index, line := range lines {
		pattern, tokens, ok := splitRule(line)
		if !ok {
			continue
		}
		matcher, err := compilePattern(pattern)
		if err != nil {
			continue
		}
		owners := make([]Owner, 0, len(tokens))
		for _, token := range tokens {
			owners = append(owners, classifyOwner(token))
		}
		rules = append(rules, Rule{
			Pattern: pattern,
			Line:    index + 1,
			Owners:  owners,
			matcher: matcher,
		})
	}
	return rules
}

// Resolve returns the owners from the last matching rule. Paths are matched
// case-sensitively and are interpreted relative to the repository root.
func Resolve(rules []Rule, path string) (Match, bool) {
	path = strings.TrimPrefix(path, "/")
	var result Match
	matched := false
	for index := range rules {
		rule := &rules[index]
		if rule.matcher.MatchString(path) {
			result = Match{
				Pattern: rule.Pattern,
				Line:    rule.Line,
				Owners:  append([]Owner(nil), rule.Owners...),
			}
			matched = true
		}
	}
	return result, matched
}

func splitRule(line string) (string, []string, bool) {
	line = strings.TrimRight(line, "\r")
	index := 0
	for index < len(line) && isSpace(line[index]) {
		index++
	}
	if index == len(line) || line[index] == '#' {
		return "", nil, false
	}
	// GitHub explicitly does not support escaping a leading comment marker.
	if strings.HasPrefix(line[index:], `\#`) {
		return "", nil, false
	}
	start := index
	escaped := false
	for index < len(line) {
		character := line[index]
		if escaped {
			escaped = false
			index++
			continue
		}
		if character == '\\' {
			escaped = true
			index++
			continue
		}
		if isSpace(character) {
			break
		}
		index++
	}
	if escaped {
		return "", nil, false
	}
	pattern := line[start:index]
	var tokens []string
	for index < len(line) {
		for index < len(line) && isSpace(line[index]) {
			index++
		}
		if index == len(line) || line[index] == '#' {
			break
		}
		start = index
		for index < len(line) && !isSpace(line[index]) {
			index++
		}
		tokens = append(tokens, line[start:index])
	}
	return pattern, tokens, pattern != ""
}

func compilePattern(pattern string) (*regexp.Regexp, error) {
	if strings.HasPrefix(pattern, "!") {
		return nil, fmt.Errorf("CODEOWNERS negation is unsupported")
	}
	if hasUnescaped(pattern, '[') || hasUnescaped(pattern, ']') {
		return nil, fmt.Errorf("CODEOWNERS character ranges are unsupported")
	}
	anchored := strings.HasPrefix(pattern, "/")
	if anchored {
		pattern = strings.TrimPrefix(pattern, "/")
	}
	directory := strings.HasSuffix(pattern, "/")
	if directory {
		pattern = strings.TrimSuffix(pattern, "/")
	}
	if pattern == "" {
		return nil, fmt.Errorf("empty CODEOWNERS pattern")
	}
	hasSlash := strings.Contains(pattern, "/")
	var expression strings.Builder
	if anchored || hasSlash {
		expression.WriteByte('^')
	} else {
		expression.WriteString(`(?:^|.*/)`)
	}
	for index := 0; index < len(pattern); {
		character := pattern[index]
		switch character {
		case '\\':
			if index+1 >= len(pattern) {
				return nil, fmt.Errorf("dangling CODEOWNERS escape")
			}
			expression.WriteString(regexp.QuoteMeta(pattern[index+1 : index+2]))
			index += 2
		case '*':
			runEnd := index
			for runEnd < len(pattern) && pattern[runEnd] == '*' {
				runEnd++
			}
			segmentStart := index == 0 || pattern[index-1] == '/'
			if runEnd-index == 2 && segmentStart {
				switch {
				case runEnd == len(pattern):
					expression.WriteString(`.*`)
					index = runEnd
					continue
				case pattern[runEnd] == '/':
					expression.WriteString(`(?:[^/]+/)*`)
					index = runEnd + 1
					continue
				}
			}
			expression.WriteString(`[^/]*`)
			index = runEnd
		case '?':
			expression.WriteString(`[^/]`)
			index++
		default:
			expression.WriteString(regexp.QuoteMeta(pattern[index : index+1]))
			index++
		}
	}
	// GitHub's documented literal-directory examples omit a trailing slash
	// (for example, /apps/github and **/logs) but still apply to files below
	// that directory. Wildcard leaf patterns such as docs/* remain limited to
	// the path depth they explicitly match.
	if directory || !hasUnescapedWildcard(lastPatternComponent(pattern)) {
		expression.WriteString(`(?:/.*)?`)
	}
	expression.WriteByte('$')
	matcher, err := regexp.Compile(expression.String())
	if err != nil {
		return nil, fmt.Errorf("compile CODEOWNERS pattern: %w", err)
	}
	return matcher, nil
}

func lastPatternComponent(pattern string) string {
	if index := strings.LastIndex(pattern, "/"); index >= 0 {
		return pattern[index+1:]
	}
	return pattern
}

func hasUnescapedWildcard(value string) bool {
	return hasUnescaped(value, '*') || hasUnescaped(value, '?')
}

func hasUnescaped(value string, target byte) bool {
	escaped := false
	for index := 0; index < len(value); index++ {
		if escaped {
			escaped = false
			continue
		}
		if value[index] == '\\' {
			escaped = true
			continue
		}
		if value[index] == target {
			return true
		}
	}
	return false
}

func classifyOwner(token string) Owner {
	owner := Owner{Token: token, Type: OwnerMalformed}
	if name, ok := strings.CutPrefix(token, "@"); ok {
		parts := strings.Split(name, "/")
		switch {
		case len(parts) == 1 && validOwnerPart(parts[0]):
			owner.Type = OwnerUser
			owner.Name = parts[0]
		case len(parts) == 2 && validOwnerPart(parts[0]) &&
			validOwnerPart(parts[1]):
			owner.Type = OwnerTeam
			owner.Name = name
		}
		return owner
	}
	if strings.Count(token, "@") == 1 {
		parts := strings.SplitN(token, "@", 2)
		if parts[0] != "" && parts[1] != "" &&
			!strings.ContainsAny(token, " /\t") {
			owner.Type = OwnerEmail
			owner.Name = token
		}
	}
	return owner
}

func validOwnerPart(value string) bool {
	if value == "" {
		return false
	}
	for _, character := range value {
		if unicode.IsLetter(character) || unicode.IsDigit(character) ||
			character == '-' || character == '_' {
			continue
		}
		return false
	}
	return true
}

func isSpace(value byte) bool {
	return value == ' ' || value == '\t' || value == '\v' || value == '\f'
}
