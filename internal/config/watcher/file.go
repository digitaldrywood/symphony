package watcher

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"time"

	"github.com/fsnotify/fsnotify"
)

type FileLoader[T any] func(string) (T, error)

type FileUpdate[T any] struct {
	Path       string
	Value      T
	Err        error
	WatcherErr bool
	At         time.Time
}

type FileOption func(*fileOptions)

type FileWatcher[T any] struct {
	path          string
	files         []watchedFile
	debounce      time.Duration
	retryInterval time.Duration
	pollInterval  time.Duration
	loader        FileLoader[T]
	logger        *slog.Logger
	newWatcher    fileWatcherFactory
	newTimer      fileTimerFactory
	newTicker     fileTickerFactory
}

type fileOptions struct {
	debounce      time.Duration
	retryInterval time.Duration
	pollInterval  time.Duration
	logger        *slog.Logger
	watchPaths    []string
	newWatcher    fileWatcherFactory
	newTimer      fileTimerFactory
	newTicker     fileTickerFactory
}

type watchedFile struct {
	path   string
	target string
}

type fileEventWatcher interface {
	Add(string) error
	Close() error
	Events() <-chan fsnotify.Event
	Errors() <-chan error
}

type fsnotifyEventWatcher struct {
	watcher *fsnotify.Watcher
}

type fileWatcherFactory func() (fileEventWatcher, error)

type fileTimer interface {
	C() <-chan time.Time
	Reset(time.Duration) bool
	Stop() bool
}

type standardFileTimer struct {
	timer *time.Timer
}

type fileTimerFactory func(time.Duration) fileTimer

type fileTicker interface {
	C() <-chan time.Time
	Stop()
}

type standardFileTicker struct {
	ticker *time.Ticker
}

type fileTickerKind uint8

const (
	filePollTicker fileTickerKind = iota
	fileRetryTicker
)

type fileTickerFactory func(fileTickerKind, time.Duration) fileTicker

type fileStamp struct {
	target  string
	modTime time.Time
	size    int64
	mode    os.FileMode
	digest  [sha256.Size]byte
	err     string
}

type fileWatchState struct {
	files       []watchedFile
	watchedDirs map[string]struct{}
	stamps      []fileStamp
}

func NewFile[T any](path string, loader FileLoader[T], opts ...FileOption) (*FileWatcher[T], error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, ErrMissingPath
	}
	if loader == nil {
		return nil, errors.New("config watch loader is required")
	}

	absolute, err := filepath.Abs(path)
	if err != nil {
		return nil, fmt.Errorf("resolve config watch path: %w", err)
	}

	cfg := fileOptions{
		debounce:      defaultDebounce,
		retryInterval: defaultFileWatchRetryInterval,
		pollInterval:  defaultFilePollInterval,
		logger:        slog.Default(),
		newWatcher:    newFileEventWatcher,
		newTimer: func(duration time.Duration) fileTimer {
			return &standardFileTimer{timer: time.NewTimer(duration)}
		},
		newTicker: newFileTicker,
	}
	for _, opt := range opts {
		opt(&cfg)
	}
	if cfg.debounce <= 0 {
		cfg.debounce = defaultDebounce
	}
	if cfg.retryInterval <= 0 {
		cfg.retryInterval = defaultFileWatchRetryInterval
	}
	if cfg.pollInterval <= 0 {
		cfg.pollInterval = defaultFilePollInterval
	}
	if cfg.logger == nil {
		cfg.logger = slog.Default()
	}
	if cfg.newWatcher == nil {
		cfg.newWatcher = newFileEventWatcher
	}
	if cfg.newTimer == nil {
		cfg.newTimer = func(duration time.Duration) fileTimer {
			return &standardFileTimer{timer: time.NewTimer(duration)}
		}
	}
	if cfg.newTicker == nil {
		cfg.newTicker = newFileTicker
	}

	path = filepath.Clean(absolute)
	files := []watchedFile{{path: path, target: resolveWatchPath(path)}}
	seen := map[string]struct{}{path: {}}
	for _, additionalPath := range cfg.watchPaths {
		additionalPath = strings.TrimSpace(additionalPath)
		if additionalPath == "" {
			continue
		}
		additionalAbsolute, err := filepath.Abs(additionalPath)
		if err != nil {
			return nil, fmt.Errorf("resolve additional config watch path: %w", err)
		}
		additionalPath = filepath.Clean(additionalAbsolute)
		if _, ok := seen[additionalPath]; ok {
			continue
		}
		seen[additionalPath] = struct{}{}
		files = append(files, watchedFile{path: additionalPath, target: resolveWatchPath(additionalPath)})
	}
	return &FileWatcher[T]{
		path:          path,
		files:         files,
		debounce:      cfg.debounce,
		retryInterval: cfg.retryInterval,
		pollInterval:  cfg.pollInterval,
		loader:        loader,
		logger:        cfg.logger,
		newWatcher:    cfg.newWatcher,
		newTimer:      cfg.newTimer,
		newTicker:     cfg.newTicker,
	}, nil
}

func newFileEventWatcher() (fileEventWatcher, error) {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, err
	}
	return &fsnotifyEventWatcher{watcher: watcher}, nil
}

func (w *fsnotifyEventWatcher) Add(path string) error {
	return w.watcher.Add(path)
}

func (w *fsnotifyEventWatcher) Close() error {
	return w.watcher.Close()
}

func (w *fsnotifyEventWatcher) Events() <-chan fsnotify.Event {
	return w.watcher.Events
}

func (w *fsnotifyEventWatcher) Errors() <-chan error {
	return w.watcher.Errors
}

func (t *standardFileTimer) C() <-chan time.Time {
	return t.timer.C
}

func (t *standardFileTimer) Reset(duration time.Duration) bool {
	return t.timer.Reset(duration)
}

func (t *standardFileTimer) Stop() bool {
	return t.timer.Stop()
}

func newFileTicker(_ fileTickerKind, duration time.Duration) fileTicker {
	return &standardFileTicker{ticker: time.NewTicker(duration)}
}

func (t *standardFileTicker) C() <-chan time.Time {
	return t.ticker.C
}

func (t *standardFileTicker) Stop() {
	t.ticker.Stop()
}

func withFileRuntime(newWatcher fileWatcherFactory, newTimer fileTimerFactory, newTicker fileTickerFactory) FileOption {
	return func(opts *fileOptions) {
		opts.newWatcher = newWatcher
		opts.newTimer = newTimer
		opts.newTicker = newTicker
	}
}

func withFileIntervals(retryInterval, pollInterval time.Duration) FileOption {
	return func(opts *fileOptions) {
		opts.retryInterval = retryInterval
		opts.pollInterval = pollInterval
	}
}

func watchDirs(paths ...string) []string {
	seen := map[string]struct{}{}
	dirs := make([]string, 0, len(paths))
	for _, path := range paths {
		dir := filepath.Dir(path)
		if _, ok := seen[dir]; ok {
			continue
		}
		seen[dir] = struct{}{}
		dirs = append(dirs, dir)
	}
	return dirs
}

func resolveWatchPath(path string) string {
	if resolved, err := filepath.EvalSymlinks(path); err == nil {
		return filepath.Clean(resolved)
	}
	return path
}

func WithFileDebounce(debounce time.Duration) FileOption {
	return func(opts *fileOptions) {
		opts.debounce = debounce
	}
}

func WithFileLogger(logger *slog.Logger) FileOption {
	return func(opts *fileOptions) {
		opts.logger = logger
	}
}

func WithFileWatchPaths(paths ...string) FileOption {
	return func(opts *fileOptions) {
		opts.watchPaths = append(opts.watchPaths, paths...)
	}
}

func (w *FileWatcher[T]) Watch(ctx context.Context) (<-chan FileUpdate[T], error) {
	if ctx == nil {
		ctx = context.Background()
	}

	fsWatcher, err := w.newWatcher()
	if err != nil {
		return nil, fmt.Errorf("create config watcher: %w", err)
	}

	state := newFileWatchState(w.files)
	pending, err := state.syncWatchPaths(fsWatcher)
	if err != nil {
		closeErr := fsWatcher.Close()
		return nil, errors.Join(err, closeErr)
	}
	state.stamps = state.captureStamps()

	updates := make(chan FileUpdate[T], 1)
	go w.run(ctx, fsWatcher, state, pending, updates)
	return updates, nil
}

func (w *FileWatcher[T]) run(
	ctx context.Context,
	fsWatcher fileEventWatcher,
	state *fileWatchState,
	pending bool,
	updates chan<- FileUpdate[T],
) {
	defer close(updates)
	defer func() {
		if err := fsWatcher.Close(); err != nil {
			w.logger.Warn("close config watcher failed", "path", w.path, "error", err)
		}
	}()

	timer := w.newTimer(w.debounce)
	stopFileTimer(timer)
	defer stopFileTimer(timer)
	pollTicker := w.newTicker(filePollTicker, w.pollInterval)
	defer pollTicker.Stop()

	var timerC <-chan time.Time
	var retryTicker fileTicker
	var retryC <-chan time.Time
	var lastUpdate *FileUpdate[T]
	setRetry := func(needed bool) {
		if needed && retryTicker == nil {
			retryTicker = w.newTicker(fileRetryTicker, w.retryInterval)
			retryC = retryTicker.C()
			return
		}
		if !needed && retryTicker != nil {
			retryTicker.Stop()
			retryTicker = nil
			retryC = nil
		}
	}
	defer func() {
		if retryTicker != nil {
			retryTicker.Stop()
		}
	}()
	setRetry(pending)

	observe := func() {
		needsRetry, err := state.syncWatchPaths(fsWatcher)
		if err != nil {
			w.logger.Warn("watch config directory failed; retrying", "path", w.path, "error", err)
		}
		setRetry(needsRetry)
		stamps := state.captureStamps()
		if slices.Equal(stamps, state.stamps) {
			return
		}
		state.stamps = stamps
		resetTimer(timer, w.debounce)
		timerC = timer.C()
	}

	for {
		select {
		case <-ctx.Done():
			return
		case event, ok := <-fsWatcher.Events():
			if !ok {
				return
			}
			if state.matches(event) {
				observe()
			}
		case err, ok := <-fsWatcher.Errors():
			if !ok {
				return
			}
			w.send(ctx, updates, FileUpdate[T]{Path: w.path, Err: err, WatcherErr: true, At: time.Now()})
		case <-pollTicker.C():
			observe()
		case <-retryC:
			observe()
		case <-timerC:
			timerC = nil
			update := w.reload(ctx)
			if sameFileUpdate(update, lastUpdate) {
				continue
			}
			w.send(ctx, updates, update)
			last := update
			lastUpdate = &last
		}
	}
}

func newFileWatchState(files []watchedFile) *fileWatchState {
	return &fileWatchState{
		files:       slices.Clone(files),
		watchedDirs: make(map[string]struct{}),
	}
}

func (s *fileWatchState) matches(event fsnotify.Event) bool {
	if event.Op&(fsnotify.Create|fsnotify.Write|fsnotify.Rename|fsnotify.Remove|fsnotify.Chmod) == 0 {
		return false
	}
	name := filepath.Clean(event.Name)
	for _, file := range s.files {
		if name == file.path || name == file.target {
			return true
		}
	}
	return false
}

func (s *fileWatchState) syncWatchPaths(fsWatcher fileEventWatcher) (bool, error) {
	for index := range s.files {
		s.files[index].target = resolveWatchPath(s.files[index].path)
	}

	pending := false
	var watchErr error
	for _, dir := range watchDirsForFiles(s.files) {
		if _, ok := s.watchedDirs[dir]; ok {
			continue
		}
		if err := fsWatcher.Add(dir); err != nil {
			pending = true
			if !errors.Is(err, os.ErrNotExist) {
				watchErr = errors.Join(watchErr, fmt.Errorf("watch config directory %s: %w", dir, err))
			}
			continue
		}
		s.watchedDirs[dir] = struct{}{}
	}
	return pending, watchErr
}

func watchDirsForFiles(files []watchedFile) []string {
	paths := make([]string, 0, len(files)*2)
	for _, file := range files {
		paths = append(paths, file.path, file.target)
	}
	return watchDirs(paths...)
}

func (s *fileWatchState) captureStamps() []fileStamp {
	stamps := make([]fileStamp, 0, len(s.files))
	for _, file := range s.files {
		stamp := fileStamp{target: file.target}
		info, err := os.Stat(file.path)
		if err != nil {
			stamp.err = err.Error()
		} else {
			stamp.modTime = info.ModTime()
			stamp.size = info.Size()
			stamp.mode = info.Mode()
			if info.Mode().IsRegular() {
				content, readErr := os.ReadFile(file.path)
				if readErr != nil {
					stamp.err = readErr.Error()
				} else {
					stamp.digest = sha256.Sum256(content)
				}
			}
		}
		stamps = append(stamps, stamp)
	}
	return stamps
}

func (w *FileWatcher[T]) reload(ctx context.Context) FileUpdate[T] {
	update := FileUpdate[T]{
		Path: w.path,
		At:   time.Now(),
	}

	value, err := w.load(ctx)
	if err != nil {
		update.Err = err
		return update
	}
	update.Value = value

	return update
}

func (w *FileWatcher[T]) load(ctx context.Context) (T, error) {
	if err := ctx.Err(); err != nil {
		var zero T
		return zero, err
	}
	value, err := w.loader(w.path)
	if err == nil {
		return value, nil
	}
	deadline := time.NewTimer(w.debounce)
	defer deadline.Stop()
	retry := time.NewTicker(retryInterval(w.debounce))
	defer retry.Stop()

	for {
		finalAttempt := false
		select {
		case <-ctx.Done():
			var zero T
			return zero, ctx.Err()
		case <-deadline.C:
			finalAttempt = true
		case <-retry.C:
		}
		if err := ctx.Err(); err != nil {
			var zero T
			return zero, err
		}
		value, err := w.loader(w.path)
		if err == nil {
			return value, nil
		}
		if finalAttempt {
			var zero T
			return zero, err
		}
	}
}

func (w *FileWatcher[T]) send(ctx context.Context, updates chan<- FileUpdate[T], update FileUpdate[T]) {
	select {
	case updates <- update:
	case <-ctx.Done():
	}
}

func sameFileUpdate[T any](update FileUpdate[T], last *FileUpdate[T]) bool {
	if last == nil || update.Path != last.Path {
		return false
	}
	if (update.Err == nil) != (last.Err == nil) {
		return false
	}
	if update.Err != nil {
		return update.Err.Error() == last.Err.Error()
	}
	return reflect.DeepEqual(update.Value, last.Value)
}
