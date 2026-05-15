package search

import (
	"html"
	"regexp"
	"strings"
	"unicode"

	"golang.org/x/text/unicode/norm"

	"github.com/slack-go/slack"
)

var (
	userMentionRe    = regexp.MustCompile(`<@([A-Z0-9]+)(?:\|([^>]+))?>`)
	channelMentionRe = regexp.MustCompile(`<#([A-Z0-9]+)(?:\|([^>]+))?>`)
	slackTokenRe     = regexp.MustCompile(`<([^>|]+)(?:\|([^>]*))?>`)
)

type Mention struct {
	Type        string
	TargetID    string
	DisplayText string
}

func NormalizeMessage(msg slack.Message) string {
	text := normalizeMessageText(msg.Text)

	parts := []string{strings.TrimSpace(text)}
	for _, file := range msg.Files {
		if file.Title != "" {
			parts = append(parts, sanitizeText(file.Title))
		}
		if file.Name != "" && file.Name != file.Title {
			parts = append(parts, sanitizeText(file.Name))
		}
		if file.PlainText != "" {
			parts = append(parts, sanitizeText(file.PlainText))
		}
		if file.PreviewPlainText != "" && file.PreviewPlainText != file.PlainText {
			parts = append(parts, sanitizeText(file.PreviewPlainText))
		}
	}
	if msg.Edited != nil {
		parts = append(parts, "[edited]")
	}
	if msg.SubType == "message_deleted" || msg.DeletedTimestamp != "" {
		parts = append(parts, "[deleted]")
	}
	if msg.ThreadTimestamp != "" && msg.ThreadTimestamp != msg.Timestamp {
		parts = append(parts, "[thread-reply]")
	}
	return strings.TrimSpace(strings.Join(filterEmpty(parts), " "))
}

func normalizeMessageText(raw string) string {
	text := sanitizeText(raw)
	if text == "" {
		return ""
	}
	matches := slackTokenRe.FindAllStringSubmatchIndex(text, -1)
	if len(matches) == 0 {
		return sanitizeText(html.UnescapeString(text))
	}
	var b strings.Builder
	last := 0
	for _, match := range matches {
		b.WriteString(html.UnescapeString(text[last:match[0]]))
		target := text[match[2]:match[3]]
		label := ""
		if match[4] >= 0 {
			label = text[match[4]:match[5]]
		}
		b.WriteString(renderSlackToken(target, label))
		last = match[1]
	}
	b.WriteString(html.UnescapeString(text[last:]))
	return sanitizeText(b.String())
}

func renderSlackToken(target string, label string) string {
	switch {
	case strings.HasPrefix(target, "@"):
		if label != "" {
			return "@" + html.UnescapeString(label)
		}
		return "@" + html.UnescapeString(strings.TrimPrefix(target, "@"))
	case strings.HasPrefix(target, "#"):
		if label != "" {
			return "#" + html.UnescapeString(label)
		}
		return "#" + html.UnescapeString(strings.TrimPrefix(target, "#"))
	case label != "":
		return html.UnescapeString(label) + " " + html.UnescapeString(target)
	default:
		return html.UnescapeString(target)
	}
}

func ExtractMentions(text string) []Mention {
	text = sanitizeDisplayText(text)
	var mentions []Mention
	for _, match := range userMentionRe.FindAllStringSubmatch(text, -1) {
		mentions = append(mentions, Mention{
			Type:        "user",
			TargetID:    match[1],
			DisplayText: display(match[2], match[1]),
		})
	}
	for _, match := range channelMentionRe.FindAllStringSubmatch(text, -1) {
		mentions = append(mentions, Mention{
			Type:        "channel",
			TargetID:    match[1],
			DisplayText: display(match[2], match[1]),
		})
	}
	return mentions
}

func display(label string, fallback string) string {
	if label != "" {
		return label
	}
	return fallback
}

func filterEmpty(parts []string) []string {
	filtered := make([]string, 0, len(parts))
	for _, part := range parts {
		if strings.TrimSpace(part) != "" {
			filtered = append(filtered, strings.TrimSpace(part))
		}
	}
	return filtered
}

func sanitizeText(raw string) string {
	if raw == "" {
		return ""
	}
	raw = strings.ToValidUTF8(raw, "\uFFFD")
	raw = norm.NFKC.String(raw)
	var b strings.Builder
	b.Grow(len(raw))
	lastSpace := false
	for _, r := range raw {
		switch {
		case isIgnoredRune(r):
			continue
		case unicode.IsSpace(r):
			if lastSpace {
				continue
			}
			b.WriteByte(' ')
			lastSpace = true
		default:
			b.WriteRune(r)
			lastSpace = false
		}
	}
	return strings.TrimSpace(b.String())
}

func sanitizeDisplayText(raw string) string {
	return sanitizeText(html.UnescapeString(raw))
}

func isIgnoredRune(r rune) bool {
	switch r {
	case '\u200b', '\u200c', '\u200d', '\ufeff':
		return true
	}
	if unicode.IsControl(r) && r != '\n' && r != '\r' && r != '\t' {
		return true
	}
	return false
}
