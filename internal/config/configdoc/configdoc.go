package configdoc

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"

	"github.com/digitaldrywood/detent/internal/config"
	"github.com/digitaldrywood/detent/internal/policy"
)

const (
	beginMarker = "<!-- BEGIN GENERATED CONFIG REFERENCE -->"
	endMarker   = "<!-- END GENERATED CONFIG REFERENCE -->"
)

var sequenceIndexPattern = regexp.MustCompile(`\[\d+\]`)

type Field struct {
	Path       string
	Type       string
	Default    string
	Required   string
	Validation []string
}

type access struct {
	field int
	enter bool
}

type schemaNode struct {
	key         string
	path        string
	typ         reflect.Type
	steps       []access
	children    []*schemaNode
	synthetic   bool
	conditional bool
}

type fieldDetails struct {
	Field
	literal string
	node    *schemaNode
}

func Generate(root string, check bool) error {
	fields, nodes, err := build()
	if err != nil {
		return err
	}

	docsPath := filepath.Join(root, "docs", "config.md")
	referencePath := filepath.Join(root, "config.reference.yaml")
	currentDocs, err := os.ReadFile(docsPath)
	if err != nil {
		return fmt.Errorf("read %s: %w", docsPath, err)
	}
	normalizedDocs := normalizeLineEndings(currentDocs)
	renderedDocs, err := renderDocs(normalizedDocs, renderMarkdown(fields))
	if err != nil {
		return fmt.Errorf("render %s: %w", docsPath, err)
	}
	renderedReference := renderReferenceYAML(fields, nodes)

	if check {
		var stale []string
		if !bytes.Equal(normalizedDocs, renderedDocs) {
			stale = append(stale, filepath.ToSlash(filepath.Join("docs", "config.md")))
		}
		currentReference, readErr := os.ReadFile(referencePath)
		if readErr != nil && !errors.Is(readErr, os.ErrNotExist) {
			return fmt.Errorf("read %s: %w", referencePath, readErr)
		}
		if !bytes.Equal(normalizeLineEndings(currentReference), renderedReference) {
			stale = append(stale, filepath.Base(referencePath))
		}
		if len(stale) > 0 {
			return fmt.Errorf("generated config documentation is stale: %s; run go generate ./... to refresh it", strings.Join(stale, ", "))
		}
		return nil
	}

	if err := os.WriteFile(docsPath, renderedDocs, 0o644); err != nil {
		return fmt.Errorf("write %s: %w", docsPath, err)
	}
	if err := os.WriteFile(referencePath, renderedReference, 0o644); err != nil {
		return fmt.Errorf("write %s: %w", referencePath, err)
	}
	return nil
}

func Fields() ([]Field, error) {
	fields, _, err := build()
	if err != nil {
		return nil, err
	}
	out := make([]Field, len(fields))
	for index := range fields {
		out[index] = fields[index].Field
	}
	return out, nil
}

func build() ([]fieldDetails, []*schemaNode, error) {
	nodes := structNodes(reflect.TypeFor[config.Config](), "", nil, false, map[reflect.Type]int{})
	flat := flatten(nodes)
	defaultConfig, err := normalized(config.Default())
	if err != nil {
		return nil, nil, fmt.Errorf("normalize default config: %w", err)
	}

	fields := make([]fieldDetails, 0, len(flat))
	for _, node := range flat {
		defaultValue, literal := fieldDefault(defaultConfig, node)
		fields = append(fields, fieldDetails{
			Field: Field{
				Path:    node.path,
				Type:    yamlType(node.typ, len(node.children) > 0),
				Default: defaultValue,
			},
			literal: literal,
			node:    node,
		})
	}
	sort.Slice(fields, func(left int, right int) bool {
		return fields[left].Path < fields[right].Path
	})

	rules, err := validationRules(fields)
	if err != nil {
		return nil, nil, err
	}
	for index := range fields {
		field := &fields[index]
		field.Validation = rules[field.Path]
		field.Required = requiredness(*field)
	}
	return fields, nodes, nil
}

func structNodes(
	typ reflect.Type,
	prefix string,
	steps []access,
	conditional bool,
	stack map[reflect.Type]int,
) []*schemaNode {
	typ = indirectType(typ)
	if typ.Kind() != reflect.Struct {
		return nil
	}
	stack[typ]++
	defer func() {
		stack[typ]--
	}()

	nodes := make([]*schemaNode, 0, typ.NumField())
	for index := range typ.NumField() {
		field := typ.Field(index)
		if !field.IsExported() {
			continue
		}
		key := yamlKey(field)
		if key == "" || key == "-" {
			continue
		}

		path := key
		if prefix != "" {
			path = prefix + "." + key
		}
		nodeSteps := appendAccess(steps, access{field: index})
		node := &schemaNode{
			key:         key,
			path:        path,
			typ:         field.Type,
			steps:       nodeSteps,
			conditional: conditional || yamlOmitEmpty(field),
		}

		valueType := indirectType(field.Type)
		switch {
		case valueType == reflect.TypeFor[map[string]policy.Requirements]():
			profilePath := path + ".<name>"
			children := structNodes(reflect.TypeFor[policy.Requirements](), profilePath, nil, true, stack)
			for _, child := range children {
				child.synthetic = true
			}
			node.children = []*schemaNode{{key: "<name>", path: profilePath, typ: reflect.TypeFor[policy.Requirements](), synthetic: true, conditional: true, children: children}}
		case valueType == reflect.TypeFor[config.BackendOptions]():
			node.children = backendOptionNodes(path)
		case valueType.Kind() == reflect.Map && valueType.Key().Kind() == reflect.String && valueType.Elem().Kind() == reflect.Struct && expandable(valueType.Elem()) && stack[valueType.Elem()] == 0:
			entryPath := path + ".<name>"
			children := structNodes(valueType.Elem(), entryPath, nil, true, stack)
			for _, child := range flatten(children) {
				child.synthetic = true
			}
			node.children = []*schemaNode{{key: "<name>", path: entryPath, typ: valueType.Elem(), synthetic: true, conditional: true, children: children}}
		case valueType.Kind() == reflect.Struct && expandable(valueType) && stack[valueType] == 0:
			node.children = structNodes(valueType, path, nodeSteps, node.conditional, stack)
		case valueType.Kind() == reflect.Slice:
			elementType := indirectType(valueType.Elem())
			if elementType.Kind() == reflect.Struct && expandable(elementType) && stack[elementType] == 0 {
				elementSteps := appendAccess(steps, access{field: index, enter: true})
				node.children = structNodes(elementType, path+"[]", elementSteps, true, stack)
			}
		}
		nodes = append(nodes, node)
	}
	return nodes
}

func backendOptionNodes(prefix string) []*schemaNode {
	types := []reflect.Type{
		reflect.TypeFor[config.CodexOptions](),
		reflect.TypeFor[config.ClaudeCodeOptions](),
	}
	byKey := map[string]*schemaNode{}
	var order []string
	for _, typ := range types {
		nodes := structNodes(typ, prefix, nil, true, map[reflect.Type]int{})
		for _, node := range nodes {
			existing, ok := byKey[node.key]
			if !ok {
				node.synthetic = true
				byKey[node.key] = node
				order = append(order, node.key)
				continue
			}
			if existing.typ != node.typ {
				existing.typ = reflect.TypeFor[any]()
			}
		}
	}
	nodes := make([]*schemaNode, 0, len(order))
	for _, key := range order {
		nodes = append(nodes, byKey[key])
	}
	return nodes
}

func expandable(typ reflect.Type) bool {
	path := typ.PkgPath()
	return path == "github.com/digitaldrywood/detent/internal/config" ||
		path == "github.com/digitaldrywood/detent/internal/activehours" ||
		path == "github.com/digitaldrywood/detent/internal/connector" ||
		path == "github.com/digitaldrywood/detent/internal/gate" ||
		path == "github.com/digitaldrywood/detent/internal/intake" ||
		path == "github.com/digitaldrywood/detent/internal/retro" ||
		path == "github.com/digitaldrywood/detent/internal/scheduleowner" ||
		path == "github.com/digitaldrywood/detent/internal/selector"
}

func flatten(nodes []*schemaNode) []*schemaNode {
	var out []*schemaNode
	var walk func([]*schemaNode)
	walk = func(children []*schemaNode) {
		for _, node := range children {
			out = append(out, node)
			walk(node.children)
		}
	}
	walk(nodes)
	return out
}

func appendAccess(in []access, item access) []access {
	out := make([]access, len(in), len(in)+1)
	copy(out, in)
	return append(out, item)
}

func yamlKey(field reflect.StructField) string {
	tag := field.Tag.Get("yaml")
	if tag == "" {
		return ""
	}
	key, _, _ := strings.Cut(tag, ",")
	return key
}

func yamlOmitEmpty(field reflect.StructField) bool {
	tag := field.Tag.Get("yaml")
	_, options, ok := strings.Cut(tag, ",")
	if !ok {
		return false
	}
	for option := range strings.SplitSeq(options, ",") {
		if option == "omitempty" {
			return true
		}
	}
	return false
}

func indirectType(typ reflect.Type) reflect.Type {
	for typ.Kind() == reflect.Pointer {
		typ = typ.Elem()
	}
	return typ
}

func yamlType(typ reflect.Type, expanded bool) string {
	if typ == reflect.TypeFor[config.StringOrMap]() {
		return "string or mapping"
	}
	if typ == reflect.TypeFor[config.BackendOptions]() {
		return "mapping"
	}
	if typ == reflect.TypeFor[any]() {
		return "value"
	}
	for typ.Kind() == reflect.Pointer {
		typ = typ.Elem()
	}
	switch typ.Kind() {
	case reflect.Bool:
		return "boolean"
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return "integer"
	case reflect.Float32, reflect.Float64:
		return "number"
	case reflect.String:
		return "string"
	case reflect.Map:
		return "mapping<" + yamlType(typ.Key(), false) + ", " + yamlType(typ.Elem(), false) + ">"
	case reflect.Slice, reflect.Array:
		if expanded {
			return "list<object>"
		}
		return "list<" + yamlType(typ.Elem(), false) + ">"
	case reflect.Struct:
		if expanded {
			return "object"
		}
		return "mapping"
	default:
		return "value"
	}
}

func fieldDefault(defaultConfig config.Config, node *schemaNode) (string, string) {
	if node.synthetic && strings.HasPrefix(node.path, "runners.profiles.") {
		return describeValue(reflect.Zero(node.typ))
	}
	if node.synthetic {
		return optionDefault(node.path)
	}
	if node.path == "workspace.root" {
		return "OS temporary directory + /detent_workspaces", "null"
	}
	if node.path == "tracker.endpoint" {
		return trackerEndpointDefault()
	}
	if node.path == "tracker.status_page_url" {
		return trackerStatusPageURLDefault()
	}
	if node.path == "codex.shell" || node.path == "hooks.shell" {
		return "platform default shell", "null"
	}

	baseValue, baseOK := locate(reflect.ValueOf(&defaultConfig), node.steps, false)
	baseDescription, baseLiteral := describeValue(baseValue)

	context := config.Default()
	if _, ok := locate(reflect.ValueOf(&context), node.steps, true); !ok {
		if baseOK {
			return baseDescription, baseLiteral
		}
		return "none", "null"
	}
	context, err := normalized(context)
	if err != nil {
		if baseOK {
			return baseDescription, baseLiteral
		}
		return "none", "null"
	}
	contextValue, ok := locate(reflect.ValueOf(&context), node.steps, false)
	if !ok {
		if baseOK {
			return baseDescription, baseLiteral
		}
		return "none", "null"
	}
	description, literal := describeValue(contextValue)
	if baseOK && (node.key == "enabled" || description == baseDescription) {
		return baseDescription, baseLiteral
	}
	if description != "none" && description != "[]" && description != "{}" && description != "see child fields" {
		description += " when configured"
	}
	return description, literal
}

func trackerStatusPageURLDefault() (string, string) {
	statusURLFor := func(kind string) string {
		cfg := config.Default()
		cfg.Tracker.Kind = kind
		cfg, err := normalized(cfg)
		if err != nil {
			return ""
		}
		return cfg.Tracker.StatusPageURL
	}
	return fmt.Sprintf(
		"%s for linear; %s for github or github_local; unused otherwise",
		strconv.Quote(statusURLFor(config.TrackerLinear)),
		strconv.Quote(statusURLFor(config.TrackerGitHub)),
	), "null"
}

func trackerEndpointDefault() (string, string) {
	endpointFor := func(kind string) string {
		cfg := config.Default()
		cfg.Tracker.Kind = kind
		cfg, err := normalized(cfg)
		if err != nil {
			return ""
		}
		return cfg.Tracker.Endpoint
	}
	linearEndpoint := endpointFor(config.TrackerLinear)
	githubEndpoint := endpointFor(config.TrackerGitHub)
	return fmt.Sprintf(
		"%s for linear; %s for github or github_local; unused otherwise",
		strconv.Quote(linearEndpoint),
		strconv.Quote(githubEndpoint),
	), "null"
}

func optionDefault(path string) (string, string) {
	key := path[strings.LastIndex(path, ".")+1:]
	codexBackend := config.CodexAgentBackend(config.Default().Codex)
	codexValue, codexOK := yamlFieldValue(reflect.ValueOf(codexBackend.CodexOptions()), key)

	claudeConfig := config.Default()
	claudeConfig.Agents.Backends = []config.AgentBackend{{
		ID:      "claude",
		Kind:    config.AgentBackendClaudeCode,
		Command: "claude",
	}}
	var claudeValue reflect.Value
	claudeOK := false
	normalizedClaude, err := normalized(claudeConfig)
	if err == nil {
		claudeBackends := normalizedClaude.AgentBackendConfigs()
		if len(claudeBackends) > 0 {
			claudeValue, claudeOK = yamlFieldValue(reflect.ValueOf(claudeBackends[0].ClaudeCodeOptions()), key)
		}
	}

	switch {
	case codexOK && claudeOK:
		codexDescription, codexLiteral := describeValue(codexValue)
		claudeDescription, _ := describeValue(claudeValue)
		if key == "shell" {
			codexDescription = "platform default shell"
			codexLiteral = "null"
		}
		if codexDescription == claudeDescription {
			return codexDescription, codexLiteral
		}
		return "Codex: " + codexDescription + "; Claude Code: " + claudeDescription, codexLiteral
	case codexOK:
		description, literal := describeValue(codexValue)
		if key == "shell" {
			description = "platform default shell"
			literal = "null"
		}
		return description + " for Codex", literal
	case claudeOK:
		description, literal := describeValue(claudeValue)
		return description + " for Claude Code", literal
	default:
		return "backend-dependent", "null"
	}
}

func yamlFieldValue(value reflect.Value, key string) (reflect.Value, bool) {
	value = indirectValue(value, false)
	if !value.IsValid() || value.Kind() != reflect.Struct {
		return reflect.Value{}, false
	}
	typ := value.Type()
	for index := range typ.NumField() {
		if yamlKey(typ.Field(index)) == key {
			return value.Field(index), true
		}
	}
	return reflect.Value{}, false
}

func describeValue(value reflect.Value) (string, string) {
	value = indirectValue(value, false)
	if !value.IsValid() {
		return "none", "null"
	}
	if value.Type() == reflect.TypeFor[config.StringOrMap]() {
		item, ok := value.Interface().(config.StringOrMap)
		if !ok {
			return "none", "null"
		}
		switch {
		case item.IsString:
			return strconv.Quote(item.String), strconv.Quote(item.String)
		case item.IsMap:
			encoded, err := json.Marshal(item.Map)
			if err == nil {
				return string(encoded), string(encoded)
			}
			return "mapping", "{}"
		default:
			return "none", "null"
		}
	}
	switch value.Kind() {
	case reflect.String:
		if value.String() == "" {
			return "none", "null"
		}
		quoted := strconv.Quote(value.String())
		return quoted, quoted
	case reflect.Bool:
		return strconv.FormatBool(value.Bool()), strconv.FormatBool(value.Bool())
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		number := strconv.FormatInt(value.Int(), 10)
		return number, number
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		number := strconv.FormatUint(value.Uint(), 10)
		return number, number
	case reflect.Float32, reflect.Float64:
		number := strconv.FormatFloat(value.Float(), 'g', -1, 64)
		return number, number
	case reflect.Slice, reflect.Array:
		if value.Len() == 0 {
			return "[]", "[]"
		}
		encoded, err := json.Marshal(value.Interface())
		if err == nil {
			return string(encoded), string(encoded)
		}
		return "list", "[]"
	case reflect.Map:
		if value.Len() == 0 {
			return "{}", "{}"
		}
		encoded, err := json.Marshal(value.Interface())
		if err == nil {
			return string(encoded), string(encoded)
		}
		return "mapping", "{}"
	case reflect.Struct:
		return "see child fields", "{}"
	default:
		return "none", "null"
	}
}

func validationRules(fields []fieldDetails) (map[string][]string, error) {
	problems := make([]string, 0)
	for _, field := range fields {
		if field.node.synthetic || strings.HasPrefix(field.Path, "tracker.issues") {
			continue
		}
		context := probeBase()
		if _, ok := locate(reflect.ValueOf(&context), field.node.steps, true); ok {
			collected, err := validateNormalized(context)
			if err != nil {
				return nil, err
			}
			problems = append(problems, collected...)
		}
		for _, candidate := range candidates(field.node.typ) {
			cfg := probeBase()
			target, ok := locate(reflect.ValueOf(&cfg), field.node.steps, true)
			if !ok || !target.CanSet() || !candidate.Type().AssignableTo(target.Type()) {
				continue
			}
			target.Set(candidate)
			collected, err := validateNormalized(cfg)
			if err != nil {
				return nil, err
			}
			problems = append(problems, collected...)
		}
	}
	optionProblems, err := backendOptionProblems()
	if err != nil {
		return nil, err
	}
	problems = append(problems, optionProblems...)
	rules := make(map[string][]string, len(fields))
	ordered := append([]fieldDetails(nil), fields...)
	sort.SliceStable(ordered, func(left int, right int) bool {
		return len(ordered[left].Path) > len(ordered[right].Path)
	})
	for _, problem := range problems {
		canonical := sequenceIndexPattern.ReplaceAllString(problem, "[]")
		var matched []fieldDetails
		for _, field := range ordered {
			alias := strings.ReplaceAll(field.Path, "[]", "")
			switch {
			case containsField(canonical, field.Path), containsField(canonical, alias):
				matched = append(matched, field)
			}
		}
		for _, field := range matched {
			if hasMatchedDescendant(field, matched) {
				continue
			}
			alias := strings.ReplaceAll(field.Path, "[]", "")
			prefix := field.Path
			if !strings.HasPrefix(canonical, prefix) && strings.HasPrefix(canonical, alias) {
				prefix = alias
			}
			rule := strings.TrimSpace(strings.TrimPrefix(canonical, prefix))
			if strings.HasPrefix(rule, "[]") {
				rule = "values" + strings.TrimPrefix(rule, "[]")
			}
			if rule == "" {
				rule = canonical
			}
			rules[field.Path] = appendUnique(rules[field.Path], rule)
		}
	}
	for path := range rules {
		sort.Strings(rules[path])
	}
	return rules, nil
}

func containsField(problem string, path string) bool {
	if path == "" {
		return false
	}
	for offset := 0; offset <= len(problem)-len(path); {
		index := strings.Index(problem[offset:], path)
		if index < 0 {
			return false
		}
		index += offset
		beforeOK := index == 0 || strings.ContainsRune(" ,;([", rune(problem[index-1]))
		end := index + len(path)
		afterOK := end == len(problem) || strings.ContainsRune(" ,;)[].", rune(problem[end]))
		if beforeOK && afterOK {
			return true
		}
		offset = index + 1
	}
	return false
}

func hasMatchedDescendant(field fieldDetails, matched []fieldDetails) bool {
	prefix := strings.TrimSuffix(field.Path, "[]") + "."
	elementPrefix := strings.TrimSuffix(field.Path, "[]") + "[]."
	for _, candidate := range matched {
		if candidate.Path == field.Path {
			continue
		}
		if strings.HasPrefix(candidate.Path, prefix) || strings.HasPrefix(candidate.Path, elementPrefix) {
			return true
		}
	}
	return false
}

func appendUnique(values []string, value string) []string {
	for _, existing := range values {
		if existing == value {
			return values
		}
	}
	return append(values, value)
}

func probeBase() config.Config {
	cfg := config.Default()
	cfg.Tracker.Kind = config.TrackerMemory
	normalizedConfig, err := normalized(cfg)
	if err != nil {
		return cfg
	}
	return normalizedConfig
}

func validateNormalized(cfg config.Config) ([]string, error) {
	cfg, err := normalized(cfg)
	if err != nil {
		return nil, err
	}
	err = cfg.Validate()
	if err == nil {
		return nil, nil
	}
	var validationError config.ValidationError
	if !errors.As(err, &validationError) {
		return nil, err
	}
	return validationError.Problems, nil
}

func normalized(cfg config.Config) (config.Config, error) {
	encoded, err := yaml.Marshal(cfg)
	if err != nil {
		return config.Config{}, err
	}
	raw := make([]byte, 0, len(encoded)+32)
	raw = append(raw, "---\n"...)
	raw = append(raw, encoded...)
	raw = append(raw, "---\nConfiguration reference\n"...)
	workflow, err := config.ParseWorkflow(raw)
	if err != nil {
		return config.Config{}, err
	}
	return workflow.Config, nil
}

func locate(root reflect.Value, steps []access, initialize bool) (reflect.Value, bool) {
	current := root
	for _, step := range steps {
		current = indirectValue(current, initialize)
		if !current.IsValid() || current.Kind() != reflect.Struct {
			return reflect.Value{}, false
		}
		if initialize {
			enableStruct(current)
		}
		if step.field < 0 || step.field >= current.NumField() {
			return reflect.Value{}, false
		}
		current = current.Field(step.field)
		if !step.enter {
			continue
		}
		switch current.Kind() {
		case reflect.Pointer:
			if current.IsNil() {
				if !initialize || !current.CanSet() {
					return reflect.Value{}, false
				}
				current.Set(reflect.New(current.Type().Elem()))
			}
			current = current.Elem()
		case reflect.Slice:
			if current.Len() == 0 {
				if !initialize || !current.CanSet() {
					return reflect.Value{}, false
				}
				current.Set(reflect.MakeSlice(current.Type(), 1, 1))
			}
			current = current.Index(0)
		default:
			return reflect.Value{}, false
		}
	}
	return current, current.IsValid()
}

func indirectValue(value reflect.Value, initialize bool) reflect.Value {
	for value.IsValid() && value.Kind() == reflect.Pointer {
		if value.IsNil() {
			if !initialize || !value.CanSet() {
				return reflect.Value{}
			}
			value.Set(reflect.New(value.Type().Elem()))
		}
		value = value.Elem()
	}
	return value
}

func enableStruct(value reflect.Value) {
	typ := value.Type()
	for index := range typ.NumField() {
		field := typ.Field(index)
		if yamlKey(field) != "enabled" {
			continue
		}
		target := value.Field(index)
		if target.CanSet() && target.Kind() == reflect.Bool {
			target.SetBool(true)
		}
	}
}

func candidates(typ reflect.Type) []reflect.Value {
	if typ == reflect.TypeFor[config.StringOrMap]() {
		return []reflect.Value{
			reflect.ValueOf(config.StringValue("")),
			reflect.ValueOf(config.StringValue("__invalid__")),
			reflect.ValueOf(config.MapValue(map[string]any{"": -1})),
		}
	}
	if typ == reflect.TypeFor[config.BackendOptions]() {
		return nil
	}
	switch typ.Kind() {
	case reflect.Pointer:
		out := []reflect.Value{reflect.Zero(typ)}
		for _, candidate := range candidates(typ.Elem()) {
			pointer := reflect.New(typ.Elem())
			pointer.Elem().Set(candidate)
			out = append(out, pointer)
		}
		return out
	case reflect.String:
		values := []string{
			"", " ", "\n", "__invalid__", "github", "github_local", "linear",
			"local_sqlite", "memory", "command", "artifact", "codex", "claude_code",
			"plan", "invalid/value/extra", "0 0",
		}
		out := make([]reflect.Value, 0, len(values))
		for _, value := range values {
			candidate := reflect.New(typ).Elem()
			candidate.SetString(value)
			out = append(out, candidate)
		}
		return out
	case reflect.Bool:
		return []reflect.Value{reflect.ValueOf(false).Convert(typ), reflect.ValueOf(true).Convert(typ)}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		values := []int64{-1, 0, 1, 2, 5}
		out := make([]reflect.Value, 0, len(values))
		for _, value := range values {
			candidate := reflect.New(typ).Elem()
			candidate.SetInt(value)
			out = append(out, candidate)
		}
		return out
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		values := []uint64{0, 1, 5}
		out := make([]reflect.Value, 0, len(values))
		for _, value := range values {
			candidate := reflect.New(typ).Elem()
			candidate.SetUint(value)
			out = append(out, candidate)
		}
		return out
	case reflect.Float32, reflect.Float64:
		values := []float64{-1, 0, 1, 1.5}
		out := make([]reflect.Value, 0, len(values))
		for _, value := range values {
			candidate := reflect.New(typ).Elem()
			candidate.SetFloat(value)
			out = append(out, candidate)
		}
		return out
	case reflect.Slice:
		out := []reflect.Value{reflect.Zero(typ), reflect.MakeSlice(typ, 0, 0)}
		elementType := typ.Elem()
		switch elementType.Kind() {
		case reflect.String:
			for _, values := range [][]string{{""}, {"\n"}, {"__invalid__"}, {"Todo"}, {"Todo", "Todo"}} {
				candidate := reflect.MakeSlice(typ, len(values), len(values))
				for index, value := range values {
					candidate.Index(index).SetString(value)
				}
				out = append(out, candidate)
			}
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			for _, value := range []int64{-1, 0, 1, 4, 5} {
				candidate := reflect.MakeSlice(typ, 1, 1)
				candidate.Index(0).SetInt(value)
				out = append(out, candidate)
			}
		}
		return out
	case reflect.Map:
		out := []reflect.Value{reflect.Zero(typ), reflect.MakeMap(typ)}
		if typ.Key().Kind() == reflect.String {
			candidate := reflect.MakeMap(typ)
			key := reflect.New(typ.Key()).Elem()
			key.SetString("__invalid__")
			value := reflect.New(typ.Elem()).Elem()
			switch value.Kind() {
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				value.SetInt(-1)
			case reflect.String:
				value.SetString("")
			case reflect.Slice:
				value.Set(reflect.MakeSlice(value.Type(), 0, 0))
			}
			candidate.SetMapIndex(key, value)
			out = append(out, candidate)
		}
		return out
	default:
		return nil
	}
}

func backendOptionProblems() ([]string, error) {
	documents := []string{
		`tracker:
  kind: memory
agents:
  backends:
    - id: codex
      kind: codex
      protocol: invalid
      command: codex app-server
      provider: invalid/value
      options:
        model_provider: invalid!
        deliverable_elicitation_allowlist:
          - server: ""
            repository: invalid
        turn_timeout_ms: -1
        read_timeout_ms: -1
        stall_timeout_ms: -1
`,
		`tracker:
  kind: memory
agents:
  backends:
    - id: claude
      kind: claude_code
      protocol: invalid
      command: claude
      options:
        permission_mode: plan
        effort: invalid
        turn_timeout_ms: -1
        stall_timeout_ms: -1
`,
		`tracker:
  kind: memory
agents:
  backends:
    - id: claude
      kind: claude_code
      command: claude
      options:
        permission_mode: invalid
`,
		`tracker:
  kind: github
  github_status_source: label
  repository: example/example
  github_app_id: "1"
`,
		`tracker:
  kind: github
  github_status_source: label
  repository: example/example
  github_app_installation_id: "2"
  github_app_private_key_path: example.pem
`,
		`tracker:
  kind: github
  github_status_source: label
  repository: example/example
  github_app_id: "1"
  github_app_installation_id: "2"
`,
	}
	var problems []string
	for _, document := range documents {
		raw := []byte("---\n" + document + "---\nConfiguration reference\n")
		workflow, err := config.ParseWorkflow(raw)
		if err != nil {
			return nil, err
		}
		err = workflow.Config.Validate()
		var validationError config.ValidationError
		if err != nil && !errors.As(err, &validationError) {
			return nil, err
		}
		problems = append(problems, validationError.Problems...)
	}
	return problems, nil
}

func requiredness(field fieldDetails) string {
	hasRequiredRule := false
	conditional := false
	for _, rule := range field.Validation {
		lower := strings.ToLower(rule)
		requiredRule := strings.Contains(lower, "required") ||
			strings.Contains(lower, "must not be blank") ||
			strings.Contains(lower, "must contain at least")
		if requiredRule {
			hasRequiredRule = true
			if strings.Contains(lower, " when ") || strings.Contains(lower, " for ") ||
				strings.Contains(lower, " requires ") || strings.Contains(lower, " or ") {
				conditional = true
			}
		}
	}
	if !hasRequiredRule {
		return "No"
	}
	if conditional || field.node.conditional {
		return "Conditional"
	}
	if field.Default != "none" && field.Default != "null" {
		return "No"
	}
	return "Yes"
}

func renderMarkdown(fields []fieldDetails) string {
	var out strings.Builder
	out.WriteString("| Key | Type | Default | Required | Validation |\n")
	out.WriteString("| --- | --- | --- | --- | --- |\n")
	for _, field := range fields {
		validation := "None"
		if len(field.Validation) > 0 {
			validation = strings.Join(field.Validation, "<br>")
		}
		fmt.Fprintf(
			&out,
			"| `%s` | `%s` | `%s` | %s | %s |\n",
			markdownCell(field.Path),
			markdownCell(field.Type),
			markdownCell(field.Default),
			field.Required,
			markdownCell(validation),
		)
	}
	return strings.TrimRight(out.String(), "\n")
}

func markdownCell(value string) string {
	value = strings.ReplaceAll(value, "|", "\\|")
	return strings.ReplaceAll(value, "\n", " ")
}

func renderDocs(current []byte, table string) ([]byte, error) {
	start := bytes.Index(current, []byte(beginMarker))
	end := bytes.Index(current, []byte(endMarker))
	if start < 0 || end < 0 || end <= start {
		return nil, errors.New("generated reference markers are missing or out of order")
	}
	prefixEnd := start + len(beginMarker)
	var out bytes.Buffer
	out.Write(current[:prefixEnd])
	out.WriteString("\n\n")
	out.WriteString(table)
	out.WriteString("\n\n")
	out.Write(current[end:])
	return out.Bytes(), nil
}

func normalizeLineEndings(content []byte) []byte {
	content = bytes.ReplaceAll(content, []byte("\r\n"), []byte("\n"))
	return bytes.ReplaceAll(content, []byte("\r"), []byte("\n"))
}

func renderReferenceYAML(fields []fieldDetails, nodes []*schemaNode) []byte {
	byPath := make(map[string]fieldDetails, len(fields))
	for _, field := range fields {
		byPath[field.Path] = field
	}
	var out strings.Builder
	out.WriteString("# Code generated by go generate ./...; DO NOT EDIT.\n")
	out.WriteString("#\n")
	out.WriteString("# Every supported project key is commented out. Copy only the settings you\n")
	out.WriteString("# intend to override into detent.yaml or detent.local.yaml.\n")
	renderYAMLNodes(&out, nodes, byPath, 0)
	return []byte(out.String())
}

func renderYAMLNodes(out *strings.Builder, nodes []*schemaNode, fields map[string]fieldDetails, depth int) {
	for _, node := range nodes {
		field := fields[node.path]
		validation := "None"
		if len(field.Validation) > 0 {
			validation = strings.Join(field.Validation, "; ")
		}
		indent := strings.Repeat("  ", depth)
		fmt.Fprintf(
			out,
			"# %s# %s | type: %s | default: %s | required: %s | validation: %s\n",
			indent,
			node.path,
			field.Type,
			field.Default,
			field.Required,
			validation,
		)
		if len(node.children) == 0 {
			fmt.Fprintf(out, "# %s%s: %s\n", indent, node.key, field.literal)
			continue
		}
		fmt.Fprintf(out, "# %s%s:\n", indent, node.key)
		elementType := indirectType(node.typ)
		if elementType.Kind() == reflect.Slice {
			fmt.Fprintf(out, "# %s  -\n", indent)
			renderYAMLNodes(out, node.children, fields, depth+2)
			continue
		}
		renderYAMLNodes(out, node.children, fields, depth+1)
	}
}
