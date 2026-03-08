package cli

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/vincentkoc/slacrawl/internal/config"
	"github.com/vincentkoc/slacrawl/internal/slackapi"
	"github.com/vincentkoc/slacrawl/internal/slackdesktop"
	"github.com/vincentkoc/slacrawl/internal/store"
	"github.com/vincentkoc/slacrawl/internal/syncer"
)

type App struct {
	Stdout io.Writer
	Stderr io.Writer
}

type OutputFormat string

const (
	FormatText OutputFormat = "text"
	FormatJSON OutputFormat = "json"
	FormatLog  OutputFormat = "log"
)

func New() *App {
	return &App{
		Stdout: os.Stdout,
		Stderr: os.Stderr,
	}
}

func (a *App) Run(ctx context.Context, args []string) error {
	global := flag.NewFlagSet("slacrawl", flag.ContinueOnError)
	global.SetOutput(a.Stderr)
	global.Usage = func() {}
	configPath := global.String("config", "", "config path")
	format := global.String("format", string(FormatText), "output format: text|json|log")
	jsonOut := global.Bool("json", false, "json output")
	noColor := global.Bool("no-color", false, "disable ANSI color in text output")
	if err := global.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			a.setColorEnabled(FormatText, *noColor)
			a.printHelp()
			return nil
		}
		return err
	}

	rest := global.Args()
	if len(rest) == 0 {
		a.setColorEnabled(FormatText, *noColor)
		a.printHelp()
		return nil
	}

	if *configPath == "" {
		path, err := config.DefaultConfigPath()
		if err != nil {
			return err
		}
		*configPath = path
	}

	outputFormat, err := resolveOutputFormat(*format, *jsonOut)
	if err != nil {
		return err
	}
	a.setColorEnabled(outputFormat, *noColor)

	switch rest[0] {
	case "init":
		return a.runInit(*configPath, rest[1:], outputFormat)
	case "doctor":
		return a.runDoctor(ctx, *configPath, outputFormat)
	case "status":
		return a.runStatus(ctx, *configPath, outputFormat)
	case "sync":
		return a.runSync(ctx, *configPath, rest[1:], outputFormat)
	case "search":
		return a.runSearch(ctx, *configPath, rest[1:], outputFormat)
	case "messages":
		return a.runMessages(ctx, *configPath, rest[1:], outputFormat)
	case "mentions":
		return a.runMentions(ctx, *configPath, rest[1:], outputFormat)
	case "sql":
		return a.runSQL(ctx, *configPath, rest[1:], outputFormat)
	case "users":
		return a.runUsers(ctx, *configPath, rest[1:], outputFormat)
	case "channels":
		return a.runChannels(ctx, *configPath, rest[1:], outputFormat)
	case "completion":
		return a.runCompletion(rest[1:])
	case "tail":
		return a.runTail(ctx, *configPath, rest[1:])
	case "watch":
		return a.runWatch(ctx, *configPath, rest[1:], outputFormat)
	default:
		return fmt.Errorf("unknown command: %s", rest[0])
	}
}

func (a *App) setColorEnabled(format OutputFormat, noColor bool) {
	ansiEnabled = format == FormatText && !noColor && colorAllowedByEnv() && writerIsTTY(a.Stdout)
}

func colorAllowedByEnv() bool {
	if os.Getenv("NO_COLOR") != "" {
		return false
	}
	if strings.EqualFold(os.Getenv("TERM"), "dumb") {
		return false
	}
	return true
}

func writerIsTTY(w io.Writer) bool {
	file, ok := w.(*os.File)
	if !ok {
		return false
	}
	info, err := file.Stat()
	if err != nil {
		return false
	}
	return (info.Mode() & os.ModeCharDevice) != 0
}

func resolveOutputFormat(value string, jsonOut bool) (OutputFormat, error) {
	if jsonOut {
		return FormatJSON, nil
	}
	switch OutputFormat(strings.ToLower(strings.TrimSpace(value))) {
	case "", FormatText:
		return FormatText, nil
	case FormatJSON:
		return FormatJSON, nil
	case FormatLog:
		return FormatLog, nil
	default:
		return "", fmt.Errorf("unsupported format %q: use text, json, or log", value)
	}
}

func (a *App) runInit(configPath string, args []string, format OutputFormat) error {
	fs := flag.NewFlagSet("init", flag.ContinueOnError)
	fs.SetOutput(a.Stderr)
	workspaceID := fs.String("workspace", "", "workspace id")
	dbPath := fs.String("db", "", "database path")
	if err := fs.Parse(args); err != nil {
		return err
	}

	cfg := config.Default()
	if *workspaceID != "" {
		cfg.WorkspaceID = *workspaceID
	}
	if *dbPath != "" {
		cfg.DBPath = *dbPath
	}
	if err := cfg.Normalize(); err != nil {
		return err
	}
	if err := cfg.Save(configPath); err != nil {
		return err
	}
	result := map[string]any{
		"config_path": configPath,
		"db_path":     cfg.DBPath,
	}
	return a.writeOutput("Init", result, format, true)
}

func (a *App) runDoctor(ctx context.Context, configPath string, format OutputFormat) error {
	cfg, err := loadConfig(configPath)
	if err != nil {
		return err
	}
	st, err := store.Open(cfg.DBPath)
	if err != nil {
		return err
	}
	defer st.Close()

	diag, err := slackapi.New(cfg.ResolveTokens()).Doctor(ctx)
	if err != nil && !errors.Is(err, context.Canceled) {
		return err
	}
	desktop := slackdesktop.Source{Path: cfg.Slack.Desktop.Path, Available: false}
	if cfg.Slack.Desktop.Enabled {
		desktop, err = slackdesktop.Inspect(cfg.Slack.Desktop.Path)
		if err != nil {
			return err
		}
	}
	threadCoverage := diag.ThreadCoverage
	if threadCoverage == "" {
		threadCoverage = "partial"
	}
	if err := st.SetSyncState(ctx, "doctor", "threads", "coverage", threadCoverage); err != nil {
		return err
	}
	status, err := st.Status(ctx)
	if err != nil {
		return err
	}
	channelSkips, err := st.ListSyncState(ctx, "api-bot", "channel_skip", 20)
	if err != nil {
		return err
	}
	tailState, err := st.ListSyncState(ctx, "tail", "", 20)
	if err != nil {
		return err
	}

	report := map[string]any{
		"config_path":   configPath,
		"database_path": cfg.DBPath,
		"tokens": map[string]any{
			"bot_env":      cfg.Slack.Bot.TokenEnv,
			"app_env":      cfg.Slack.App.TokenEnv,
			"user_env":     cfg.Slack.User.TokenEnv,
			"bot_enabled":  cfg.Slack.Bot.Enabled,
			"app_enabled":  cfg.Slack.App.Enabled,
			"user_enabled": cfg.Slack.User.Enabled,
			"bot_set":      cfg.ResolveTokens().Bot != "",
			"app_set":      cfg.ResolveTokens().App != "",
			"user_set":     cfg.ResolveTokens().User != "",
		},
		"slack_api":         diag,
		"desktop_source":    desktop,
		"api_channel_skips": channelSkips,
		"tail_state":        tailState,
		"status":            status,
		"fts_available":     true,
	}
	return a.writeOutput("Doctor", report, format, true)
}

func (a *App) runStatus(ctx context.Context, configPath string, format OutputFormat) error {
	cfg, err := loadConfig(configPath)
	if err != nil {
		return err
	}
	st, err := store.Open(cfg.DBPath)
	if err != nil {
		return err
	}
	defer st.Close()
	status, err := st.Status(ctx)
	if err != nil {
		return err
	}
	return a.writeOutput("Status", status, format, true)
}

func (a *App) runSync(ctx context.Context, configPath string, args []string, format OutputFormat) error {
	cfg, err := loadConfig(configPath)
	if err != nil {
		return err
	}

	fs := flag.NewFlagSet("sync", flag.ContinueOnError)
	fs.SetOutput(a.Stderr)
	source := fs.String("source", "api", "api|desktop|all")
	workspaceID := fs.String("workspace", "", "workspace id")
	channels := fs.String("channels", "", "comma separated channel ids")
	since := fs.String("since", "", "oldest slack ts or RFC3339 timestamp")
	full := fs.Bool("full", false, "full sync")
	concurrency := fs.Int("concurrency", cfg.Sync.Concurrency, "worker count")
	if err := fs.Parse(args); err != nil {
		return err
	}

	st, err := store.Open(cfg.DBPath)
	if err != nil {
		return err
	}
	defer st.Close()

	summary, err := syncer.Run(ctx, cfg, st, syncer.Options{
		Source:      syncer.Source(*source),
		WorkspaceID: coalesce(*workspaceID, cfg.WorkspaceID),
		Channels:    csv(*channels),
		Since:       *since,
		Full:        *full,
		Concurrency: *concurrency,
	})
	if err != nil {
		return err
	}
	status, err := st.Status(ctx)
	if err != nil {
		return err
	}
	result := map[string]any{
		"status":  status,
		"summary": summary,
	}
	return a.writeOutput("Sync", result, format, true)
}

func (a *App) runSearch(ctx context.Context, configPath string, args []string, format OutputFormat) error {
	cfg, err := loadConfig(configPath)
	if err != nil {
		return err
	}
	if len(args) == 0 {
		return errors.New("search query required")
	}
	st, err := store.Open(cfg.DBPath)
	if err != nil {
		return err
	}
	defer st.Close()
	results, err := st.Search(ctx, strings.Join(args, " "), 50)
	if err != nil {
		return err
	}
	return a.writeOutput("Search", results, format, false)
}

func (a *App) runMessages(ctx context.Context, configPath string, args []string, format OutputFormat) error {
	cfg, err := loadConfig(configPath)
	if err != nil {
		return err
	}
	fs := flag.NewFlagSet("messages", flag.ContinueOnError)
	fs.SetOutput(a.Stderr)
	channelID := fs.String("channel", "", "channel id")
	userID := fs.String("author", "", "user id")
	limit := fs.Int("limit", 50, "row limit")
	if err := fs.Parse(args); err != nil {
		return err
	}
	st, err := store.Open(cfg.DBPath)
	if err != nil {
		return err
	}
	defer st.Close()
	results, err := st.Messages(ctx, *channelID, *userID, store.RequireLimit(*limit))
	if err != nil {
		return err
	}
	return a.writeOutput("Messages", results, format, false)
}

func (a *App) runMentions(ctx context.Context, configPath string, args []string, format OutputFormat) error {
	cfg, err := loadConfig(configPath)
	if err != nil {
		return err
	}
	fs := flag.NewFlagSet("mentions", flag.ContinueOnError)
	fs.SetOutput(a.Stderr)
	target := fs.String("target", "", "target id or label")
	limit := fs.Int("limit", 50, "row limit")
	if err := fs.Parse(args); err != nil {
		return err
	}
	st, err := store.Open(cfg.DBPath)
	if err != nil {
		return err
	}
	defer st.Close()
	results, err := st.Mentions(ctx, *target, store.RequireLimit(*limit))
	if err != nil {
		return err
	}
	return a.writeOutput("Mentions", results, format, false)
}

func (a *App) runSQL(ctx context.Context, configPath string, args []string, format OutputFormat) error {
	cfg, err := loadConfig(configPath)
	if err != nil {
		return err
	}
	query := strings.TrimSpace(strings.Join(args, " "))
	if query == "" {
		data, err := io.ReadAll(os.Stdin)
		if err != nil {
			return err
		}
		query = strings.TrimSpace(string(data))
	}
	if query == "" {
		return errors.New("sql query required")
	}
	st, err := store.Open(cfg.DBPath)
	if err != nil {
		return err
	}
	defer st.Close()
	results, err := st.QueryReadOnly(ctx, query)
	if err != nil {
		return err
	}
	return a.writeOutput("SQL", results, format, false)
}

func (a *App) runUsers(ctx context.Context, configPath string, args []string, format OutputFormat) error {
	cfg, err := loadConfig(configPath)
	if err != nil {
		return err
	}
	query := ""
	if len(args) > 0 {
		query = args[0]
	}
	st, err := store.Open(cfg.DBPath)
	if err != nil {
		return err
	}
	defer st.Close()
	results, err := st.Users(ctx, query, 100)
	if err != nil {
		return err
	}
	return a.writeOutput("Users", results, format, false)
}

func (a *App) runChannels(ctx context.Context, configPath string, args []string, format OutputFormat) error {
	cfg, err := loadConfig(configPath)
	if err != nil {
		return err
	}
	query := ""
	if len(args) > 0 {
		query = args[0]
	}
	st, err := store.Open(cfg.DBPath)
	if err != nil {
		return err
	}
	defer st.Close()
	results, err := st.Channels(ctx, query, 100)
	if err != nil {
		return err
	}
	return a.writeOutput("Channels", results, format, false)
}

func (a *App) runTail(ctx context.Context, configPath string, args []string) error {
	cfg, err := loadConfig(configPath)
	if err != nil {
		return err
	}
	fs := flag.NewFlagSet("tail", flag.ContinueOnError)
	fs.SetOutput(a.Stderr)
	workspaceID := fs.String("workspace", "", "workspace id")
	repairEvery := fs.String("repair-every", cfg.Sync.RepairEvery, "repair interval")
	if err := fs.Parse(args); err != nil {
		return err
	}
	st, err := store.Open(cfg.DBPath)
	if err != nil {
		return err
	}
	defer st.Close()
	repairDuration, err := time.ParseDuration(*repairEvery)
	if err != nil {
		return err
	}
	return slackapi.New(cfg.ResolveTokens()).Tail(ctx, st, coalesce(*workspaceID, cfg.WorkspaceID), repairDuration)
}

func (a *App) runWatch(ctx context.Context, configPath string, args []string, format OutputFormat) error {
	cfg, err := loadConfig(configPath)
	if err != nil {
		return err
	}
	fs := flag.NewFlagSet("watch", flag.ContinueOnError)
	fs.SetOutput(a.Stderr)
	desktopEvery := fs.String("desktop-every", cfg.Sync.DesktopRefreshEvery, "desktop refresh interval")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if !cfg.Slack.Desktop.Enabled {
		return errors.New("desktop sync is disabled in config")
	}
	interval, err := time.ParseDuration(*desktopEvery)
	if err != nil {
		return err
	}
	if interval <= 0 {
		return errors.New("desktop refresh interval must be greater than zero")
	}

	st, err := store.Open(cfg.DBPath)
	if err != nil {
		return err
	}
	defer st.Close()

	syncOnce := func() error {
		summary, err := syncer.Run(ctx, cfg, st, syncer.Options{Source: syncer.SourceDesktop})
		if err != nil {
			return err
		}
		status, err := st.Status(ctx)
		if err != nil {
			return err
		}
		return a.writeOutput("Watch", map[string]any{
			"status":  status,
			"summary": summary,
		}, format, true)
	}
	if err := syncOnce(); err != nil {
		return err
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := syncOnce(); err != nil {
				return err
			}
		}
	}
}

func (a *App) writeJSON(value any) error {
	enc := json.NewEncoder(a.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(value)
}

func loadConfig(path string) (config.Config, error) {
	cfg, err := config.Load(path)
	if err != nil {
		return config.Config{}, err
	}
	return cfg, nil
}

func csv(value string) []string {
	if value == "" {
		return nil
	}
	parts := strings.Split(value, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}

func coalesce(primary string, fallback string) string {
	if primary != "" {
		return primary
	}
	return fallback
}

func WithTimeout(parent context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(parent, 30*time.Second)
}
