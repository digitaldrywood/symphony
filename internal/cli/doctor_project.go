package cli

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/digitaldrywood/detent/internal/activehours"
	"github.com/digitaldrywood/detent/internal/agentoverride"
	workflowconfig "github.com/digitaldrywood/detent/internal/config"
	globalconfig "github.com/digitaldrywood/detent/internal/config/global"
	"github.com/digitaldrywood/detent/internal/connector"
	"github.com/digitaldrywood/detent/internal/pause"
	projectpkg "github.com/digitaldrywood/detent/internal/project"
	runnerpkg "github.com/digitaldrywood/detent/internal/runner"
	"github.com/digitaldrywood/detent/internal/selector"
	"github.com/digitaldrywood/detent/internal/skills"
)

func checkDoctorProjects(ctx context.Context, cfg globalconfig.Config, deps doctorDeps, githubToken RuntimeSecret, allowWriteProbes bool) []doctorCheck {
	if len(cfg.Projects) == 0 {
		return []doctorCheck{
			{
				Name:   "Project workflows",
				Status: doctorWarn,
				Detail: "no projects configured",
				Hint:   "Run detent add-project to add a project.",
			},
		}
	}

	deps.pauseProjects = append([]globalconfig.Project(nil), cfg.Projects...)
	deps.pauseGitHubToken = runtimeGlobalGitHubToken(githubToken)
	checks := make([]doctorCheck, 0, len(cfg.Projects)*2)
	if cfg.Client.ProviderCapacityFile != "" {
		checks = append(checks, checkDoctorProviderCapacity(ctx, cfg))
	}
	for _, project := range cfg.Projects {
		project.GlobalActiveHours = cfg.Global.ActiveHours
		project.GlobalAgents = cfg.Global.Agents
		project.GlobalBudget = cfg.Global.Budget
		project.Identity = cfg.Global.Identity
		checks = append(checks, checkDoctorProjectWithStore(ctx, project, doctorRuntimeStorePath(cfg.Path), deps, githubToken, allowWriteProbes)...)
		if cfg.Client.Configured() {
			checks = append(checks, checkDoctorHubPolicy(ctx, cfg, project, deps))
		}
	}

	return checks
}

func checkDoctorWorkflowDrift(ctx context.Context, cfg globalconfig.Config, boot BootConfig, deps doctorDeps) []doctorCheck {
	probe, err := probeDoctorHealth(ctx, boot, deps)
	if err != nil {
		return []doctorCheck{{
			Name:   "Workflow runtime drift",
			Status: doctorOK,
			Detail: "runtime comparison skipped because no healthy live Detent instance was reachable",
		}}
	}

	runtimeByProject := make(map[string]doctorHealthWorkflow, len(probe.Health.Workflows))
	for _, workflow := range probe.Health.Workflows {
		runtimeByProject[strings.TrimSpace(workflow.ProjectID)] = workflow
	}

	checks := make([]doctorCheck, 0, len(cfg.Projects))
	for _, configuredProject := range cfg.Projects {
		id := doctorProjectID(configuredProject)
		name := "Project " + id + " workflow runtime"
		if configuredProject.Paused {
			checks = append(checks, doctorCheck{
				Name:   name,
				Status: doctorOK,
				Detail: "project " + id + " is paused; workflow drift comparison deferred until unpause",
			})
			continue
		}
		runtimeWorkflow, ok := runtimeByProject[id]
		if !ok {
			checks = append(checks, doctorCheck{
				Name:   name,
				Status: doctorWarn,
				Detail: "project " + id + " is missing workflow source status from the live instance",
				Hint:   "Restart Detent with a version that reports workflow source status, then rerun detent doctor.",
			})
			continue
		}
		if runtimeWorkflow.LastReloadError != "" {
			checks = append(checks, doctorCheck{
				Name:   name,
				Status: doctorFail,
				Detail: fmt.Sprintf(
					"project %s is running its last-good workflow loaded at %s because reload failed at %s: %s",
					id,
					doctorWorkflowTimestamp(runtimeWorkflow.LoadedAt),
					doctorWorkflowTimestamp(runtimeWorkflow.ReloadFailedAt),
					runtimeWorkflow.LastReloadError,
				),
				Hint: "Fix the configured project-definition source; Detent will clear the degraded status after a successful reload.",
			})
			continue
		}

		diskWorkflow, loadErr := loadDoctorProjectWorkflow(ctx, configuredProject, deps)
		if loadErr != nil {
			continue
		}
		if runtimeWorkflow.SourceHash == "" {
			checks = append(checks, doctorCheck{
				Name:   name,
				Status: doctorWarn,
				Detail: "project " + id + " live workflow source hash is unavailable",
				Hint:   "Restart Detent with a version that records loaded workflow source hashes.",
			})
			continue
		}
		diagnostic := doctorProjectDefinition(id, diskWorkflow.Definition)
		diagnostic.RuntimeLayout = runtimeWorkflow.Layout
		diagnostic.RuntimeRevision = runtimeWorkflow.Revision
		stale := diskWorkflow.SourceHash != runtimeWorkflow.SourceHash
		if runtimeWorkflow.Layout != "" && runtimeWorkflow.Layout != string(diskWorkflow.Definition.Layout) {
			stale = true
		}
		diagnostic.Stale = stale
		if stale {
			modifiedAt := doctorWorkflowModifiedAt(configuredProject)
			checks = append(checks, doctorCheck{
				Name:              name,
				Status:            doctorFail,
				ProjectDefinition: diagnostic,
				Detail: fmt.Sprintf(
					"project %s is running stale configuration (configured layout=%s revision=%s; runtime layout=%s revision=%s; file changed at %s, loaded at %s)",
					id,
					diskWorkflow.Definition.Layout,
					diskWorkflow.Definition.Revision,
					runtimeWorkflow.Layout,
					runtimeWorkflow.Revision,
					doctorWorkflowTimestamp(modifiedAt),
					doctorWorkflowTimestamp(runtimeWorkflow.LoadedAt),
				),
				Hint: "Check workflow watcher warnings; Detent will reconcile the file automatically, or restart the process if drift persists.",
			})
			continue
		}
		checks = append(checks, doctorCheck{
			Name:              name,
			Status:            doctorOK,
			Detail:            fmt.Sprintf("project %s loaded %s revision %s matches the configured source", id, diskWorkflow.Definition.Layout, diskWorkflow.Definition.Revision),
			ProjectDefinition: diagnostic,
		})
	}
	return checks
}

func doctorWorkflowModifiedAt(project globalconfig.Project) time.Time {
	if strings.TrimSpace(project.WorkflowRef) != "" {
		return time.Time{}
	}
	path := strings.TrimSpace(project.Workflow)
	if path == "~" || strings.HasPrefix(path, "~/") {
		home, err := os.UserHomeDir()
		if err != nil {
			return time.Time{}
		}
		path = filepath.Join(home, strings.TrimPrefix(path, "~/"))
	}
	var modified time.Time
	for _, candidate := range []string{
		path,
		workflowconfig.DefinitionPath(path),
		workflowconfig.LocalWorkflowPath(path),
		workflowconfig.LocalDefinitionPath(path),
	} {
		info, err := os.Stat(candidate)
		if err == nil && info.ModTime().After(modified) {
			modified = info.ModTime()
		}
	}
	return modified.UTC()
}

func doctorWorkflowTimestamp(value time.Time) string {
	if value.IsZero() {
		return "unknown"
	}
	return value.UTC().Format(time.RFC3339)
}

func doctorProjectCheckJobs(cfg globalconfig.Config, deps doctorDeps, githubToken RuntimeSecret, allowWriteProbes bool, workflowTokenThreshold int) []doctorCheckJob {
	if len(cfg.Projects) == 0 {
		return []doctorCheckJob{{
			Name: "Project workflows",
			Run: func(context.Context) []doctorCheck {
				return []doctorCheck{
					{
						Name:   "Project workflows",
						Status: doctorWarn,
						Detail: "no projects configured",
						Hint:   "Run detent add-project to add a project.",
					},
				}
			},
		}}
	}

	deps.pauseProjects = append([]globalconfig.Project(nil), cfg.Projects...)
	deps.pauseGitHubToken = runtimeGlobalGitHubToken(githubToken)
	jobs := make([]doctorCheckJob, 0, len(cfg.Projects))
	for _, project := range cfg.Projects {
		project.GlobalActiveHours = cfg.Global.ActiveHours
		project.GlobalAgents = cfg.Global.Agents
		project.GlobalBudget = cfg.Global.Budget
		project.Identity = cfg.Global.Identity
		id := doctorProjectID(project)
		progress := newDoctorCheckProgress()
		jobs = append(jobs, doctorCheckJob{
			Name:     "Project " + id + " checks",
			Freeze:   progress.Freeze,
			Progress: progress.Updates(),
			Run: func(jobCtx context.Context) []doctorCheck {
				jobCtx = connector.WithProgressReporter(jobCtx, progress.Pulse)
				checks := checkDoctorProjectWithProgress(jobCtx, project, doctorRuntimeStorePath(cfg.Path), deps, githubToken, allowWriteProbes, workflowTokenThreshold, progress.Set)
				if cfg.Client.Configured() {
					checks = append(checks, checkDoctorHubPolicy(jobCtx, cfg, project, deps))
				}
				return checks
			},
		})
	}
	return jobs
}

func checkDoctorProject(ctx context.Context, project globalconfig.Project, deps doctorDeps, githubToken RuntimeSecret, allowWriteProbes bool) []doctorCheck {
	return checkDoctorProjectWithStore(ctx, project, "", deps, githubToken, allowWriteProbes)
}

func checkDoctorProjectWithStore(ctx context.Context, project globalconfig.Project, storePath string, deps doctorDeps, githubToken RuntimeSecret, allowWriteProbes bool) []doctorCheck {
	return checkDoctorProjectWithProgress(ctx, project, storePath, deps, githubToken, allowWriteProbes, doctorWorkflowDefaultTokenThreshold, nil)
}

func checkDoctorProjectWithProgress(
	ctx context.Context,
	project globalconfig.Project,
	storePath string,
	deps doctorDeps,
	githubToken RuntimeSecret,
	allowWriteProbes bool,
	workflowTokenThreshold int,
	setProgress func(string, []doctorCheck),
) []doctorCheck {
	id := doctorProjectID(project)
	var checks []doctorCheck
	setDoctorCurrentCheck := func(name string) {
		if setProgress != nil {
			setProgress(name, checks)
		}
	}
	workflowCheckName := "Project " + id + " workflow"
	setDoctorCurrentCheck(workflowCheckName)
	workflow, err := loadDoctorProjectWorkflow(ctx, project, deps)
	if err != nil {
		check := doctorProjectDefinitionFailureCheck(id, project, err)
		return []doctorCheck{
			check,
			{
				Name:   "Project " + id + " source repo",
				Status: doctorWarn,
				Detail: "skipped because WORKFLOW.md could not be loaded as a project definition",
				Hint:   "Fix the project-definition files, then rerun detent doctor.",
			},
			checkDoctorIssueEffortGuidanceUnavailable(id, "WORKFLOW.md could not be loaded as a project definition"),
			checkDoctorFollowupGuidanceUnavailable(id, "WORKFLOW.md could not be loaded as a project definition"),
		}
	}
	workflow.Config = doctorWorkflowConfigWithRuntimeGitHubToken(workflow.Config, runtimeGlobalGitHubToken(githubToken))
	if project.Identity.Configured() {
		identity := project.Identity
		identity.Normalize()
		workflow.Config.Identity = identity
	}
	workflow.Config.ActiveHours = projectpkg.EffectiveActiveHours(project, workflow.Config.ActiveHours)
	validationErr := workflow.Config.Validate()
	workflowRefDriftCheck, workflowRefDrift := checkDoctorWorkflowRefDrift(
		ctx,
		id,
		project,
		projectSourceRoot(project, workflow.Config),
		validationErr,
		githubToken,
		deps,
	)
	if validationErr != nil {
		failedChecks := []doctorCheck{
			{
				Name:              workflowCheckName,
				Status:            doctorFail,
				Detail:            fmt.Sprintf("%s: %v", project.Workflow, validationErr),
				Hint:              "Fix invalid Detent configuration in detent.yaml or legacy WORKFLOW.md frontmatter.",
				ProjectDefinition: doctorProjectDefinition(id, workflow.Definition),
			},
		}
		if workflowRefDrift {
			failedChecks = append(failedChecks, workflowRefDriftCheck)
		}
		return append(failedChecks,
			doctorCheck{
				Name:   "Project " + id + " source repo",
				Status: doctorWarn,
				Detail: "skipped because WORKFLOW.md is invalid",
				Hint:   "Fix the workflow file, then rerun detent doctor.",
			},
			checkDoctorIssueEffortGuidanceUnavailable(id, "WORKFLOW.md is invalid"),
			checkDoctorFollowupGuidanceUnavailable(id, "WORKFLOW.md is invalid"),
		)
	}
	if pauseCheck, ok := checkDoctorProjectPause(ctx, id, project, workflow.Config, deps); ok {
		checks = append(checks, pauseCheck)
	}
	if activeHoursCheck, ok := checkDoctorProjectActiveHours(id, project, workflow.Config, deps); ok {
		checks = append(checks, activeHoursCheck)
	}

	workflowCheck := doctorCheck{
		Name:              workflowCheckName,
		Status:            doctorOK,
		Detail:            doctorWorkflowDetail(project.Workflow, project, workflow.Config),
		ProjectDefinition: doctorProjectDefinition(id, workflow.Definition),
	}
	if workflow.Definition.Layout == "" {
		if workflowLocationFinding, ok := checkDoctorWorkflowLocation(project); ok {
			workflowCheck.Status = workflowLocationFinding.Status
			workflowCheck.Detail = workflowLocationFinding.Detail + "; " + workflowCheck.Detail
			workflowCheck.Hint = workflowLocationFinding.Hint
		}
	}
	if workflow.Definition.Layout != "" {
		workflowCheck.Detail = doctorProjectDefinitionDetail(workflow.Definition) + "; " + workflowCheck.Detail
	}
	if workflow.Definition.Layout == workflowconfig.ProjectDefinitionLegacy {
		workflowCheck.Status = doctorWarn
		command := doctorProjectDefinitionFixCommand(doctorProjectDefinitionMigrationPath(project, workflow.Definition))
		workflowCheck.ProjectDefinition.FixCommand = command
		workflowCheck.Hint = "Migrate without changing runtime behavior: " + command
	}
	if workflowPath, err := doctorWorkflowOptimizationWorkflowPath(project); err == nil {
		findings := doctorReviewFlowWorkflowFindings(id, workflowPath, workflow.Config, workflow.Prompt)
		if len(findings) > 0 {
			workflowCheck.Status = doctorWarn
			workflowCheck.Detail += "; review-flow prose mismatch: " + doctorWorkflowFindingDetails(findings)
			if workflowCheck.Hint != "" {
				workflowCheck.Hint += " "
			}
			workflowCheck.Hint += "Align WORKFLOW.md handoff prose with the configured review-flow choice, or adjust the frontmatter if the configured choice is not intended."
			workflowCheck.WorkflowOptimization = doctorWorkflowOptimizationReport{
				Findings:  findings,
				Proposals: doctorWorkflowProposalsForFindings(id, findings, 1),
			}
		}
	}
	checks = append(checks, workflowCheck)
	if workflow.Config.SchedulersEnabled() {
		setDoctorCurrentCheck("Project " + id + " schedule ownership")
		checks = append(checks, checkDoctorScheduleOwnership(ctx, id, workflow.Config, deps))
	}
	setDoctorCurrentCheck("Project " + id + " capabilities")
	checks = append(checks, checkDoctorCapabilities(project, workflow))
	if len(doctorIssueFilingDestinations(workflow.Config)) > 0 {
		setDoctorCurrentCheck("Project " + id + " public issue exposure")
		checks = append(checks, checkDoctorPublicIssueExposure(ctx, id, workflow.Config, deps))
	}
	setDoctorCurrentCheck("Project " + id + " workflow source policy")
	if sourcePolicyCheck, ok := checkDoctorWorkflowSourcePolicy(ctx, id, project, workflow.Config, projectSourceRoot(project, workflow.Config), deps); ok {
		checks = append(checks, sourcePolicyCheck)
	}
	if workflowRefDrift {
		checks = append(checks, workflowRefDriftCheck)
	}
	setDoctorCurrentCheck("Project " + id + " local workflow overlay")
	if overlayCheck, ok := checkDoctorLocalWorkflowOverlay(ctx, id, workflow, deps); ok {
		checks = append(checks, overlayCheck)
	}
	setDoctorCurrentCheck("Project " + id + " billing mode")
	if billingCheck, ok := checkDoctorBillingMode(id, workflow.Config, false); ok {
		checks = append(checks, billingCheck)
	}
	setDoctorCurrentCheck("Project " + id + " worker GitHub credential")
	if workerGitHubCheck, ok := checkDoctorWorkerGitHubCredential(id, workflow.Config); ok {
		checks = append(checks, workerGitHubCheck)
	}
	setDoctorCurrentCheck("Project " + id + " progress brake")
	checks = append(checks, checkDoctorProgressBrake(id, workflow.Config))
	checks = append(checks, checkDoctorTerminalAttemptRecovery(id, workflow.Config))
	if strings.TrimSpace(storePath) != "" {
		setDoctorCurrentCheck("Project " + id + " lifetime limits")
		checks = append(checks, checkDoctorLifetimeLimits(ctx, id, storePath, workflow.Config, deps))
	}
	if strings.TrimSpace(storePath) != "" {
		setDoctorCurrentCheck("Project " + id + " historical throughput")
		checks = append(checks, checkDoctorHistoricalThroughput(ctx, id, storePath, workflow.Config.Agent.MaxConcurrentAgents, deps)...)
	}
	if strings.TrimSpace(storePath) != "" {
		setDoctorCurrentCheck("Project " + id + " park review")
		checks = append(checks, checkDoctorParkReview(ctx, id, storePath, workflow.Config.Observability.ParkReviewThreshold, deps))
	}
	setDoctorCurrentCheck("Project " + id + " workflow lint")
	checks = append(checks, checkDoctorWorkflowLint(ctx, id, project, workflow.Config, workflow.Prompt, workflowTokenThreshold, storePath, deps)...)
	if len(workflow.Config.Routines) > 0 {
		setDoctorCurrentCheck("Project " + id + " scheduled routines")
		checks = append(checks, checkDoctorRoutines(ctx, id, workflow.Config.Routines, storePath, deps))
	}
	if workflow.Config.BacklogAdmission.Enabled {
		setDoctorCurrentCheck("Project " + id + " backlog admission")
		checks = append(checks, checkDoctorAdmission(ctx, id, workflow, storePath, deps))
	}
	setDoctorCurrentCheck("Project " + id + " out-of-scope follow-up guidance")
	checks = append(checks, checkDoctorFollowupGuidance(id, workflow.Config.Agent.Followups, workflow.Prompt))
	setDoctorCurrentCheck("Project " + id + " pinned route models")
	checks = append(checks, checkDoctorRouteModels(ctx, id, project, workflow.Config, deps))
	if workflow.Config.Agents.ModelSelection.Configured() || len(workflow.Config.Agents.Sources) > 0 {
		checks = append(checks, checkDoctorModelSelection(id, workflow.Config))
	}
	if doctorTrackerUsesGitHubReads(workflow.Config.Tracker.Kind) {
		setDoctorCurrentCheck("Project " + id + " issue agent models")
		checks = append(checks, checkDoctorIssueAgentModels(ctx, id, project, workflow.Config, deps))
		setDoctorCurrentCheck("Project " + id + " operational completion")
		checks = append(checks, checkDoctorOperationalCompletion(ctx, id, workflow.Config, workflow.Prompt, deps))
		setDoctorCurrentCheck("Project " + id + " authorization eligibility")
		checks = append(checks, checkDoctorAuthorizationEligibility(ctx, id, project, workflow.Config, deps))
		setDoctorCurrentCheck("Project " + id + " ownership eligibility")
		checks = append(checks, checkDoctorOwnershipEligibility(ctx, id, project, workflow.Config, deps))
		setDoctorCurrentCheck("Project " + id + " configured labels")
		checks = append(checks, checkDoctorConfiguredLabels(ctx, id, project, workflow.Config, deps))
	}
	if workflow.Config.Agent.AutoPromote.Enabled {
		setDoctorCurrentCheck("Project " + id + " auto-promote")
		checks = append(checks, checkDoctorAutoPromote(ctx, id, workflow.Config, deps, time.Now()))
		if workflow.Config.Gate.Validator.Enabled {
			setDoctorCurrentCheck("Project " + id + " validator health")
			checks = append(checks, checkDoctorValidatorHealth(ctx, id, storePath, deps, time.Now()))
		}
	}
	if doctorTrackerUsesGitHubReads(workflow.Config.Tracker.Kind) && workflow.Config.Deliverable.Kind == workflowconfig.DeliverablePullRequest {
		setDoctorCurrentCheck("Project " + id + " repository merge policy")
		checks = append(checks, checkDoctorRepositoryMergePolicy(ctx, id, project, workflow.Config, deps))
	}
	if doctorTrackerUsesGitHubReads(workflow.Config.Tracker.Kind) {
		if workflow.Config.Tracker.GitHubStatusSource == workflowconfig.GitHubStatusSourceLabel {
			setDoctorCurrentCheck("Project " + id + " label status drift")
			checks = append(checks, checkDoctorLabelStatusDrift(ctx, id, workflow.Config, deps))
		}
		setDoctorCurrentCheck("Project " + id + " dependency auto-unblock")
		checks = append(checks, checkDoctorDependencyAutoUnblock(ctx, id, workflow.Config, deps))
		setDoctorCurrentCheck("Project " + id + " blocked recovery")
		checks = append(checks, checkDoctorBlockedRecovery(ctx, id, workflow.Config, storePath, deps))
	}
	if workflow.Config.Tracker.Kind == workflowconfig.TrackerLocalSQLite || workflow.Config.Tracker.Kind == workflowconfig.TrackerGitHubLocal {
		setDoctorCurrentCheck("Project " + id + " local SQLite tracker")
		checks = append(checks, checkDoctorLocalSQLiteTracker(ctx, id, project, workflow.Config, deps))
	}
	if workflow.Config.Workspace.Kind == workflowconfig.WorkspaceFilesystem {
		setDoctorCurrentCheck("Project " + id + " filesystem workspace")
		checks = append(checks, checkDoctorFilesystemWorkspace(id, workflow.Config))
		setDoctorCurrentCheck("Project " + id + " issue effort guidance")
		checks = append(checks, checkDoctorIssueEffortGuidanceForSource(id, project, workflow.Config))
		setDoctorCurrentCheck("Project " + id + " skills")
		checks = append(checks, checkDoctorFilesystemProjectSkills(id, project, workflow.Config))
		return checks
	}
	setDoctorCurrentCheck("Project " + id + " workspace growth")
	if growthCheck, ok := checkDoctorWorkspaceGrowth(ctx, id, workflow.Config.Workspace.Root, projectSourceRoot(project, workflow.Config), deps); ok {
		checks = append(checks, growthCheck)
	}

	sourceRepoCheckName := "Project " + id + " source repo"
	setDoctorCurrentCheck(sourceRepoCheckName)
	sourceRoot := projectSourceRoot(project, workflow.Config)
	if sourceRoot == "" {
		checks = append(checks, doctorCheck{
			Name:   sourceRepoCheckName,
			Status: doctorFail,
			Detail: "source root is not configured",
			Hint:   "Set workspace.source_root, project workdir, or workspace.root to an existing git checkout.",
		})
		checks = append(checks, checkDoctorIssueEffortGuidanceUnavailable(id, "source root is not configured"))
		return append(checks, checkDoctorProjectSkillsUnavailable(id, workflow.Config.Agent.Skills, "source root is not configured"))
	}
	expandedSourceRoot, err := expandDoctorWorkspacePath(sourceRoot)
	if err != nil {
		checks = append(checks, doctorCheck{
			Name:   sourceRepoCheckName,
			Status: doctorFail,
			Detail: fmt.Sprintf("%s: %v", sourceRoot, err),
			Hint:   "Set workspace.source_root or project workdir to an existing git checkout.",
		})
		checks = append(checks, checkDoctorIssueEffortGuidanceUnavailable(id, "source root could not be resolved"))
		return append(checks, checkDoctorProjectSkillsUnavailable(id, workflow.Config.Agent.Skills, "source root could not be resolved"))
	}
	if err := deps.gitWorkTree(ctx, expandedSourceRoot); err != nil {
		checks = append(checks, doctorCheck{
			Name:   sourceRepoCheckName,
			Status: doctorFail,
			Detail: fmt.Sprintf("%s: %v", expandedSourceRoot, err),
			Hint:   "Set workspace.source_root or project workdir to an existing git checkout.",
		})
		checks = append(checks, checkDoctorIssueEffortGuidanceUnavailable(id, "source repository is unavailable locally"))
		return append(checks, checkDoctorProjectSkillsUnavailable(id, workflow.Config.Agent.Skills, "source repository is unavailable locally"))
	}
	checks = append(checks, doctorCheck{
		Name:   sourceRepoCheckName,
		Status: doctorOK,
		Detail: expandedSourceRoot + " is a git worktree",
	})
	if deps.gitWorktrees != nil {
		setDoctorCurrentCheck("Project " + id + " external branch worktrees")
		checks = append(checks, checkDoctorExternalBranchWorktrees(ctx, id, expandedSourceRoot, workflow.Config.Workspace.Root, deps))
	}
	setDoctorCurrentCheck("Project " + id + " issue effort guidance")
	checks = append(checks, checkDoctorIssueEffortGuidance(id, expandedSourceRoot))
	setDoctorCurrentCheck("Project " + id + " skills")
	checks = append(checks, checkDoctorProjectSkills(id, expandedSourceRoot, workflow.Config.Agent.Skills))
	if doctorTrackerUsesGitHubReads(workflow.Config.Tracker.Kind) {
		setDoctorCurrentCheck("Project " + id + " GitHub readiness")
		checks = append(checks, checkDoctorGitHubReadiness(ctx, id, project, workflow.Config, deps, githubToken, expandedSourceRoot, allowWriteProbes)...)
	}
	return checks
}

func checkDoctorProjectActiveHours(
	id string,
	project globalconfig.Project,
	workflow workflowconfig.Config,
	deps doctorDeps,
) (doctorCheck, bool) {
	config := projectpkg.EffectiveActiveHours(project, workflow.ActiveHours)
	if !config.Configured() {
		return doctorCheck{}, false
	}
	now := time.Now().UTC()
	if deps.now != nil {
		now = deps.now().UTC()
	}
	overrideUntil := activehours.ParsePersistedOverride(project.ActiveHoursOverrideUntil)
	status, err := activehours.Evaluate(config, now, overrideUntil)
	if err != nil {
		return doctorCheck{
			Name:   "Project " + id + " active hours",
			Status: doctorFail,
			Detail: err.Error(),
			Hint:   "Fix active_hours in global.yaml or detent.yaml, then rerun detent doctor.",
		}, true
	}
	location, err := time.LoadLocation(config.Timezone)
	if err != nil {
		return doctorCheck{}, false
	}
	detail := fmt.Sprintf(
		"off hours in %s; next open %s; next close %s",
		config.Timezone,
		doctorActiveHoursTime(status.NextOpen, location),
		doctorActiveHoursTime(status.NextClose, location),
	)
	if status.WindowOpen {
		detail = fmt.Sprintf(
			"window open in %s; current close %s; following open %s",
			config.Timezone,
			doctorActiveHoursTime(status.NextClose, location),
			doctorActiveHoursTime(status.NextOpen, location),
		)
	}
	if status.OverrideActive {
		detail = "override active until " + doctorActiveHoursTime(status.OverrideUntil, location) + "; " + detail
	}
	if project.Paused {
		detail += "; manual pause still blocks dispatch"
	}
	return doctorCheck{Name: "Project " + id + " active hours", Status: doctorOK, Detail: detail}, true
}

func doctorActiveHoursTime(value time.Time, location *time.Location) string {
	if value.IsZero() {
		return "unavailable"
	}
	return value.In(location).Format("2006-01-02 15:04 MST") + " (" + value.UTC().Format("2006-01-02 15:04 UTC") + ")"
}

func checkDoctorProjectPause(
	ctx context.Context,
	id string,
	project globalconfig.Project,
	workflow workflowconfig.Config,
	deps doctorDeps,
) (doctorCheck, bool) {
	if !project.Paused {
		return doctorCheck{}, false
	}

	name := "Project " + id + " pause"
	now := time.Now().UTC()
	if deps.now != nil {
		now = deps.now().UTC()
	}
	if pause.HeldLongerThan(project, now, 7*24*time.Hour) {
		return doctorCheck{
			Name:   name,
			Status: doctorWarn,
			Detail: "project " + id + " has been paused without an exit condition since " + project.PausedAt,
			Hint:   "Set paused_until_issue or paused_until, or unpause the project.",
		}, true
	}

	var resolver connector.IssueReferenceResolver
	var projectConnector doctorAutoPromoteConnector
	trackerRepository := workflow.Tracker.Repository
	if strings.TrimSpace(project.PausedUntilIssue) != "" {
		resolverWorkflow, resolution, err := resolveDoctorPauseReference(ctx, project, workflow, deps)
		if err != nil {
			return doctorCheck{
				Name:   name,
				Status: doctorWarn,
				Detail: fmt.Sprintf("project %s pause exit issue could not be checked: %v", id, err),
				Hint:   "Fix the pause exit condition or tracker configuration, then rerun detent doctor.",
			}, true
		}
		trackerRepository = resolution.Repository
		if deps.autoPromoteConnector == nil {
			deps.autoPromoteConnector = defaultDoctorAutoPromoteConnector
		}
		projectConnector, err = deps.autoPromoteConnector(resolverWorkflow)
		if err != nil {
			return doctorCheck{
				Name:   name,
				Status: doctorWarn,
				Detail: fmt.Sprintf("project %s pause exit issue could not be checked: %v", id, err),
				Hint:   "Fix tracker credentials and rerun detent doctor.",
			}, true
		}
		if resolved, ok := projectConnector.(connector.IssueReferenceResolver); ok {
			resolver = resolved
		}
	}

	result, err := pause.Evaluate(ctx, project, now, trackerRepository, resolver)
	closeErr := closeDoctorAutoPromoteConnector(projectConnector)
	if err != nil {
		return doctorCheck{
			Name:   name,
			Status: doctorWarn,
			Detail: fmt.Sprintf("project %s pause exit condition could not be checked: %v", id, err),
			Hint:   "Fix the pause exit condition or tracker connectivity, then rerun detent doctor.",
		}, true
	}
	if closeErr != nil {
		return doctorCheck{
			Name:   name,
			Status: doctorWarn,
			Detail: fmt.Sprintf("project %s pause diagnostic connector could not be closed: %v", id, closeErr),
			Hint:   "Rerun detent doctor and check local network resources.",
		}, true
	}
	if result.Met {
		return doctorCheck{
			Name:   name,
			Status: doctorWarn,
			Detail: "project " + id + " is paused even though " + result.Detail,
			Hint:   "Keep Detent running for automatic unpause, or run detent unpause " + id + ".",
		}, true
	}

	detail := "project " + id + " is paused"
	if reason := strings.TrimSpace(project.PausedReason); reason != "" {
		detail += ": " + reason
	}
	return doctorCheck{Name: name, Status: doctorOK, Detail: detail}, true
}

func resolveDoctorPauseReference(
	ctx context.Context,
	project globalconfig.Project,
	workflow workflowconfig.Config,
	deps doctorDeps,
) (workflowconfig.Config, pause.ReferenceResolution, error) {
	projects := append([]globalconfig.Project(nil), deps.pauseProjects...)
	if projectIndex(projects, project.ID) < 0 {
		projects = append(projects, project)
	}
	trackers := make([]pause.Tracker, 0, len(projects))
	workflows := make(map[string]workflowconfig.Config, len(projects))
	for _, candidate := range projects {
		candidateID := strings.TrimSpace(candidate.ID)
		candidateWorkflow := workflow
		if !strings.EqualFold(candidateID, strings.TrimSpace(project.ID)) {
			loaded, err := loadDoctorProjectWorkflow(ctx, candidate, deps)
			if err != nil {
				continue
			}
			candidateWorkflow = loaded.Config
		}
		candidateWorkflow = doctorWorkflowConfigWithRuntimeGitHubToken(candidateWorkflow, deps.pauseGitHubToken)
		workflows[strings.ToLower(candidateID)] = candidateWorkflow
		trackers = append(trackers, pause.Tracker{
			ProjectID:  candidateID,
			Kind:       candidateWorkflow.Tracker.Kind,
			Repository: candidateWorkflow.Tracker.Repository,
		})
	}
	resolution, err := pause.ResolveReference(project.ID, project.PausedUntilIssue, trackers)
	if err != nil {
		return workflowconfig.Config{}, pause.ReferenceResolution{}, err
	}
	resolverWorkflow, ok := workflows[strings.ToLower(strings.TrimSpace(resolution.ProjectID))]
	if !ok {
		return workflowconfig.Config{}, pause.ReferenceResolution{}, fmt.Errorf("resolver project %s workflow is unavailable", resolution.ProjectID)
	}
	return resolverWorkflow, resolution, nil
}

func doctorProjectDefinitionFailureCheck(id string, project globalconfig.Project, err error) doctorCheck {
	check := doctorCheck{
		Name:   "Project " + id + " workflow",
		Status: doctorFail,
		Detail: fmt.Sprintf("%s: %v", project.Workflow, err),
		Hint:   "Fix the configured WORKFLOW.md and detent.yaml project-definition files.",
	}
	var definitionErr *workflowconfig.ProjectDefinitionError
	if !errors.As(err, &definitionErr) {
		return check
	}
	check.ProjectDefinition = doctorProjectDefinition(id, definitionErr.Definition)
	check.Detail = doctorProjectDefinitionDetail(definitionErr.Definition) + "; " + err.Error()
	if definitionErr.Definition.Migratable {
		command := doctorProjectDefinitionFixCommand(doctorProjectDefinitionMigrationPath(project, definitionErr.Definition))
		check.ProjectDefinition.FixCommand = command
		check.Hint = "Repair the layout without changing runtime behavior: " + command
	}
	return check
}

func doctorProjectDefinition(id string, definition workflowconfig.ProjectDefinition) *doctorProjectDefinitionDiagnostic {
	diagnostic := &doctorProjectDefinitionDiagnostic{
		ProjectID:         id,
		Layout:            string(definition.Layout),
		Revision:          definition.Revision,
		WorkflowPath:      definition.WorkflowPath,
		ConfigPath:        definition.ConfigPath,
		LocalWorkflowPath: definition.LocalWorkflowPath,
		LocalConfigPath:   definition.LocalConfigPath,
		LegacyKeys:        append([]string(nil), definition.LegacyKeys...),
		LocalLegacyKeys:   append([]string(nil), definition.LocalLegacyKeys...),
	}
	if definition.Migratable || definition.Layout == workflowconfig.ProjectDefinitionLegacy {
		diagnostic.FixCommand = doctorProjectDefinitionFixCommand(definition.WorkflowPath)
	}
	return diagnostic
}

func doctorProjectDefinitionDetail(definition workflowconfig.ProjectDefinition) string {
	parts := []string{
		"layout=" + string(definition.Layout),
		"revision=" + definition.Revision,
		"workflow=" + definition.WorkflowPath,
	}
	if definition.ConfigPath != "" {
		parts = append(parts, "config="+definition.ConfigPath)
	}
	if len(definition.LegacyKeys) > 0 {
		parts = append(parts, "legacy keys="+strings.Join(definition.LegacyKeys, ", "))
	}
	if len(definition.LocalLegacyKeys) > 0 {
		parts = append(parts, "local legacy keys="+strings.Join(definition.LocalLegacyKeys, ", "))
	}
	return strings.Join(parts, "; ")
}

func doctorProjectDefinitionFixCommand(workflowPath string) string {
	return "detent fix workflow-layout --workflow " + doctorShellQuote(workflowPath)
}

func doctorProjectDefinitionMigrationPath(project globalconfig.Project, definition workflowconfig.ProjectDefinition) string {
	if strings.TrimSpace(project.WorkflowRef) == "" {
		return definition.WorkflowPath
	}
	workflowPath := strings.TrimSpace(project.Workflow)
	if filepath.IsAbs(workflowPath) || strings.TrimSpace(project.Workdir) == "" {
		return workflowPath
	}
	return filepath.Join(project.Workdir, filepath.FromSlash(workflowPath))
}

func doctorShellQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "'\"'\"'") + "'"
}

func checkDoctorWorkflowLocation(project globalconfig.Project) (doctorCheck, bool) {
	if strings.TrimSpace(project.WorkflowRef) != "" || strings.TrimSpace(project.Workdir) == "" {
		return doctorCheck{}, false
	}

	repoRoot, err := expandDoctorWorkspacePath(project.Workdir)
	if err != nil {
		return doctorCheck{}, false
	}
	repoInfo, err := os.Stat(repoRoot)
	if err != nil || !repoInfo.IsDir() {
		return doctorCheck{}, false
	}
	workflowPath, err := expandDoctorWorkspacePath(project.Workflow)
	if err != nil {
		return doctorCheck{}, false
	}
	workflowName := filepath.Base(workflowPath)
	expectedPath := filepath.Join(repoRoot, workflowName)
	if doctorWorkflowFileExists(expectedPath) {
		return doctorCheck{}, false
	}

	for _, candidate := range doctorNonstandardWorkflowCandidates(repoRoot, workflowPath, workflowName) {
		if doctorWorkflowFileExists(candidate) {
			status := doctorFail
			if filepath.Clean(candidate) == filepath.Clean(workflowPath) {
				status = doctorWarn
			}
			return doctorCheck{
				Status: status,
				Detail: fmt.Sprintf("workflow found at %s; expected %s", candidate, expectedPath),
				Hint:   doctorWorkflowRootConvention(expectedPath),
			}, true
		}
	}

	return doctorCheck{
		Status: doctorFail,
		Detail: "workflow not found; expected " + expectedPath,
		Hint:   doctorWorkflowRootConvention(expectedPath),
	}, true
}

func doctorNonstandardWorkflowCandidates(repoRoot string, configuredPath string, workflowName string) []string {
	names := []string{workflowName, "workflow.md", "WORKFLOW.md"}
	directories := []string{
		filepath.Join(repoRoot, ".detent"),
		filepath.Join(filepath.Dir(repoRoot), ".detent"),
	}
	candidates := []string{configuredPath}
	seen := map[string]struct{}{filepath.Clean(filepath.Join(repoRoot, workflowName)): {}}
	for _, directory := range directories {
		for _, name := range names {
			candidate := filepath.Clean(filepath.Join(directory, name))
			if _, ok := seen[candidate]; ok {
				continue
			}
			seen[candidate] = struct{}{}
			candidates = append(candidates, candidate)
		}
	}
	return candidates
}

func doctorWorkflowFileExists(path string) bool {
	info, err := os.Stat(path)
	if err != nil || info == nil {
		return false
	}
	return info.Mode().IsRegular()
}

func doctorWorkflowRootConvention(expectedPath string) string {
	return fmt.Sprintf(
		"Move the workflow to %s. The convention is %s at the repository root, checked in; machine-local differences belong in %s.",
		expectedPath,
		filepath.Base(expectedPath),
		filepath.Base(workflowconfig.LocalWorkflowPath(expectedPath)),
	)
}

func checkDoctorLocalWorkflowOverlay(
	ctx context.Context,
	id string,
	workflow workflowconfig.Workflow,
	deps doctorDeps,
) (doctorCheck, bool) {
	paths := doctorLocalDefinitionPaths(workflow)
	if len(paths) == 0 {
		return doctorCheck{}, false
	}

	detail := strings.Join(paths, ", ") + " active"
	if len(paths) == 1 {
		detail = paths[0] + " is active"
	}
	if len(workflow.Overlay.OverriddenKeys) == 0 {
		detail += "; overrides no structured config keys (prose only)"
	} else {
		detail += "; overrides structured config keys: " + strings.Join(workflow.Overlay.OverriddenKeys, ", ")
	}
	check := doctorCheck{
		Name:   "Project " + id + " local workflow overlay",
		Status: doctorOK,
		Detail: detail,
	}
	if deps.gitTracked == nil {
		deps.gitTracked = defaultGitTracked
	}
	var trackedPaths []string
	for _, path := range paths {
		tracked, err := deps.gitTracked(ctx, path)
		if err != nil {
			check.Status = doctorWarn
			check.Detail += "; Git tracking status for " + path + " could not be determined: " + err.Error()
			check.Hint = "Verify local project-definition overlays are listed in .gitignore and are not tracked by Git."
			continue
		}
		if tracked {
			trackedPaths = append(trackedPaths, path)
		}
	}
	if len(trackedPaths) == 0 {
		if len(paths) == 1 {
			check.Detail += "; file is not tracked by Git"
		} else {
			check.Detail += "; files are not tracked by Git"
		}
		return check, true
	}
	check.Status = doctorWarn
	if len(trackedPaths) == 1 && len(paths) == 1 {
		check.Detail += "; file is tracked by Git"
		check.Hint = "Add " + filepath.Base(trackedPaths[0]) + " to .gitignore and remove it from Git tracking with git rm --cached -- " + filepath.Base(trackedPaths[0]) + "."
	} else {
		check.Detail += "; tracked by Git: " + strings.Join(trackedPaths, ", ")
		check.Hint = "Add WORKFLOW.local.md and detent.local.yaml to .gitignore and remove tracked overlays with git rm --cached."
	}
	return check, true
}

func doctorLocalDefinitionPaths(workflow workflowconfig.Workflow) []string {
	if workflow.Definition.Layout == "" && workflow.Overlay.Path != "" {
		return []string{workflow.Overlay.Path}
	}
	var candidates []string
	if workflow.Definition.LocalWorkflowPath != "" {
		candidates = append(candidates, workflow.Definition.LocalWorkflowPath)
	}
	if workflow.Definition.LocalConfigPath != "" {
		candidates = append(candidates, workflow.Definition.LocalConfigPath)
	}
	if len(candidates) == 0 && workflow.Overlay.Path != "" {
		candidates = append(candidates, workflow.Overlay.Path)
	}
	paths := make([]string, 0, len(candidates))
	for _, path := range candidates {
		if info, err := os.Stat(path); err == nil && info.Mode().IsRegular() {
			paths = append(paths, path)
		}
	}
	return paths
}

func checkDoctorBillingMode(id string, cfg workflowconfig.Config, subscriptionAuth bool) (doctorCheck, bool) {
	mode := cfg.Budget.EffectiveBillingMode()
	modeDetail := "billing_mode=" + mode
	if !cfg.Budget.BillingModeConfigured() {
		mode = workflowconfig.BillingModeSubscription
		modeDetail = "budget.billing_mode is undeclared; subscription billing is the default"
	}
	if mode == workflowconfig.BillingModeMetered {
		if subscriptionAuth {
			brakes := cfg.USDBrakes()
			armed := make([]string, 0, 2)
			if brakes.BudgetCaps {
				armed = append(armed, "budget.per_day_max_usd / budget.per_issue_max_usd")
			}
			if brakes.NoProgress {
				armed = append(armed, "agent.no_progress_spend_limit_usd")
			}
			if len(armed) == 0 {
				return doctorCheck{}, false
			}
			return doctorCheck{
				Name:   "Project " + id + " billing mode",
				Status: doctorWarn,
				Detail: "budget.billing_mode=metered arms notional USD brakes against subscription-auth usage: " + strings.Join(armed, ", ") + "; budget.enabled=false does not disarm agent.no_progress_spend_limit_usd",
				Hint:   "Set budget.billing_mode: subscription to make every notional USD brake inert. Do not rely on budget.enabled: false; it only disables budget.per_day_max_usd and budget.per_issue_max_usd.",
			}, true
		}
		warnings := validationWarningsWithPrefix(cfg.ValidationWarnings(), "budget.")
		if len(warnings) == 0 {
			return doctorCheck{}, false
		}
		return doctorCheck{
			Name:   "Project " + id + " billing mode",
			Status: doctorWarn,
			Detail: "configuration footgun: " + strings.Join(warnings, "; "),
			Hint:   "Confirm marginal API billing before keeping this notional USD brake active.",
		}, true
	}
	if mode != workflowconfig.BillingModeSubscription {
		return doctorCheck{}, false
	}

	controls := make([]string, 0, 2)
	if cfg.Budget.Enabled {
		controls = append(controls, "budget.enabled=true")
	}
	if cfg.Agent.NoProgressSpendLimitUSD > 0 {
		controls = append(controls, fmt.Sprintf("agent.no_progress_spend_limit_usd=%g", cfg.Agent.NoProgressSpendLimitUSD))
	}
	if len(controls) == 0 {
		return doctorCheck{}, false
	}
	return doctorCheck{
		Name:   "Project " + id + " billing mode",
		Status: doctorWarn,
		Detail: modeDetail + " and USD enforcement is inert; Detent will not refuse or park work from: " + strings.Join(controls, ", "),
		Hint:   "Use provider rate-window pacing and token or outcome-based brakes for subscription auth; set billing_mode=metered only for marginal API billing.",
	}, true
}

func checkDoctorWorkerGitHubCredential(id string, cfg workflowconfig.Config) (doctorCheck, bool) {
	warnings := validationWarningsWithPrefix(cfg.ValidationWarnings(), "worker.github_token:")
	if len(warnings) == 0 {
		return doctorCheck{}, false
	}
	return doctorCheck{
		Name:   "Project " + id + " worker GitHub credential",
		Status: doctorWarn,
		Detail: "configuration warning: " + strings.Join(warnings, "; "),
		Hint:   "Keep the worker REST reserve above the orchestrator dispatch floor; use a different GitHub user or App installation only when true rate-limit isolation is required.",
	}, true
}

func validationWarningsWithPrefix(warnings []string, prefix string) []string {
	filtered := make([]string, 0, len(warnings))
	for _, warning := range warnings {
		if strings.HasPrefix(warning, prefix) {
			filtered = append(filtered, warning)
		}
	}
	return filtered
}

func checkDoctorProgressBrake(id string, cfg workflowconfig.Config) doctorCheck {
	mode := cfg.Budget.EffectiveBillingMode()
	controls := make([]string, 0, 2)
	if cfg.Agent.NoProgressTokenLimit > 0 {
		controls = append(controls, fmt.Sprintf("tokens=%d", cfg.Agent.NoProgressTokenLimit))
	}
	if mode == workflowconfig.BillingModeMetered && cfg.Agent.NoProgressSpendLimitUSD > 0 {
		controls = append(controls, fmt.Sprintf("USD=%g", cfg.Agent.NoProgressSpendLimitUSD))
	}
	if len(controls) == 0 {
		return doctorCheck{
			Name:   "Project " + id + " progress brake",
			Status: doctorWarn,
			Detail: "no effective cross-session progress brake is configured",
			Hint:   "Set agent.no_progress_token_limit to a positive token budget, or use agent.no_progress_spend_limit_usd with budget.billing_mode: metered.",
		}
	}
	detail := "effective cross-session progress brake: " + strings.Join(controls, ", ")
	if mode == workflowconfig.BillingModeSubscription && cfg.Agent.NoProgressSpendLimitUSD > 0 {
		detail += fmt.Sprintf("; USD=%g is inert in subscription mode", cfg.Agent.NoProgressSpendLimitUSD)
	}
	return doctorCheck{
		Name:   "Project " + id + " progress brake",
		Status: doctorOK,
		Detail: detail,
	}
}

func checkDoctorTerminalAttemptRecovery(id string, cfg workflowconfig.Config) doctorCheck {
	limit := cfg.Recovery.EffectiveTerminalAttemptRetryLimit()
	detail := fmt.Sprintf("recovery.terminal_attempt_retry_limit=%d; park on qualifying failure %d; workspace-preparation failure limit=3", limit, cfg.Recovery.TerminalAttemptFailureLimit())
	if limit == 0 {
		detail += "; external review required before another worker session; no automatic cooldown recovery"
	} else {
		detail += "; existing breaker cooldown recovery applies"
	}
	return doctorCheck{Name: "Project " + id + " terminal attempt recovery", Status: doctorOK, Detail: detail}
}

func checkDoctorDisabledBudgetCaps(id string, cfg workflowconfig.Budget) (doctorCheck, bool) {
	if cfg.Enabled {
		return doctorCheck{}, false
	}

	caps := make([]string, 0, 2)
	if cfg.PerDayMaxUSDConfigured() && cfg.PerDayMaxUSD > 0 {
		caps = append(caps, fmt.Sprintf("budget.per_day_max_usd=%g", cfg.PerDayMaxUSD))
	}
	if cfg.PerIssueMaxUSDConfigured() && cfg.PerIssueMaxUSD > 0 {
		caps = append(caps, fmt.Sprintf("budget.per_issue_max_usd=%g", cfg.PerIssueMaxUSD))
	}
	if len(caps) == 0 {
		return doctorCheck{}, false
	}

	return doctorCheck{
		Name:   "Project " + id + " budget",
		Status: doctorWarn,
		Detail: "budget.enabled=false disables configured caps: " + strings.Join(caps, ", "),
		Hint:   "Add this exact line under budget: in WORKFLOW.md:\n  enabled: true",
	}, true
}

func checkDoctorIssueEffortGuidanceForSource(id string, project globalconfig.Project, cfg workflowconfig.Config) doctorCheck {
	sourceRoot := projectSourceRoot(project, cfg)
	if sourceRoot == "" {
		return checkDoctorIssueEffortGuidanceUnavailable(id, "source root is not configured")
	}
	expandedSourceRoot, err := expandDoctorWorkspacePath(sourceRoot)
	if err != nil {
		return checkDoctorIssueEffortGuidanceUnavailable(id, "source root could not be resolved")
	}
	info, err := os.Stat(expandedSourceRoot)
	if err != nil || !info.IsDir() {
		return checkDoctorIssueEffortGuidanceUnavailable(id, "source repository is unavailable locally")
	}
	return checkDoctorIssueEffortGuidance(id, expandedSourceRoot)
}

func checkDoctorIssueEffortGuidance(id string, sourceRoot string) doctorCheck {
	name := "Project " + id + " issue effort guidance"
	path, err := findDoctorIssueEffortGuidance(sourceRoot)
	if err != nil {
		return doctorCheck{
			Name:   name,
			Status: doctorOK,
			Detail: "skipped because " + err.Error(),
		}
	}
	if path != "" {
		return doctorCheck{
			Name:   name,
			Status: doctorOK,
			Detail: path + " mentions detent-agent effort guidance",
		}
	}
	return doctorCheck{
		Name:   name,
		Status: doctorWarn,
		Detail: "AGENTS.md and CLAUDE.md contain no detent-agent guidance",
		Hint:   "Add a project-specific effort-selection rubric; see docs/ONBOARDING.md#per-issue-agent-overrides.",
	}
}

func checkDoctorIssueEffortGuidanceUnavailable(id string, reason string) doctorCheck {
	return doctorCheck{
		Name:   "Project " + id + " issue effort guidance",
		Status: doctorOK,
		Detail: "skipped because " + reason,
	}
}

func checkDoctorFollowupGuidance(id string, cfg workflowconfig.Followups, workflowBody string) doctorCheck {
	name := "Project " + id + " out-of-scope follow-up guidance"
	if cfg.Enabled {
		return doctorCheck{
			Name:   name,
			Status: doctorOK,
			Detail: "agent.followups.enabled=true provides prompt guidance",
		}
	}
	if hasFollowupFilingGuidance(workflowBody) {
		return doctorCheck{
			Name:   name,
			Status: doctorOK,
			Detail: "WORKFLOW.md body provides out-of-scope follow-up filing guidance",
		}
	}
	return doctorCheck{
		Name:   name,
		Status: doctorWarn,
		Detail: "agent.followups.enabled=false and WORKFLOW.md body contains no out-of-scope follow-up filing guidance",
		Hint:   "Enable agent.followups.enabled or instruct agents in the WORKFLOW.md body to file separate Backlog issues for meaningful out-of-scope discoveries.",
	}
}

func hasFollowupFilingGuidance(workflowBody string) bool {
	body := strings.ToLower(workflowBody)
	describesFollowup := strings.Contains(body, "out-of-scope") ||
		strings.Contains(body, "out of scope") ||
		strings.Contains(body, "follow-up") ||
		strings.Contains(body, "follow up")
	describesTrackerItem := strings.Contains(body, "issue") || strings.Contains(body, "work item")
	describesFiling := strings.Contains(body, "file") || strings.Contains(body, "create")
	return describesFollowup && describesTrackerItem && describesFiling
}

func checkDoctorFollowupGuidanceUnavailable(id string, reason string) doctorCheck {
	return doctorCheck{
		Name:   "Project " + id + " out-of-scope follow-up guidance",
		Status: doctorOK,
		Detail: "skipped because " + reason,
	}
}

func checkDoctorProjectSkills(id string, sourceRoot string, cfg workflowconfig.Skills) doctorCheck {
	name := "Project " + id + " skills"
	detail := doctorSkillsConfigDetail(cfg)
	if !cfg.Enabled {
		return doctorCheck{Name: name, Status: doctorOK, Detail: detail + "; loaded=0; dropped=0"}
	}

	result, err := skills.Load(sourceRoot, skills.Options{
		Path:              cfg.Path,
		MaxSkillsInPrompt: cfg.MaxSkillsInPrompt,
	})
	if err != nil {
		return doctorCheck{
			Name:   name,
			Status: doctorWarn,
			Detail: detail + "; inspection failed: " + err.Error(),
			Hint:   "Fix agent.skills.path or permissions, then rerun detent doctor.",
		}
	}

	detail += fmt.Sprintf("; loaded=%d; dropped=%d", len(result.Skills), len(result.Dropped))
	if len(result.Dropped) == 0 {
		return doctorCheck{Name: name, Status: doctorOK, Detail: detail}
	}

	drops := make([]string, 0, len(result.Dropped))
	for _, drop := range result.Dropped {
		path := drop.Path
		if relative, err := filepath.Rel(sourceRoot, drop.Path); err == nil {
			path = relative
		}
		drops = append(drops, fmt.Sprintf("%s (%s: %s)", path, drop.Reason, drop.Message))
	}
	return doctorCheck{
		Name:   name,
		Status: doctorWarn,
		Detail: detail + "; drops: " + strings.Join(drops, "; "),
		Hint:   "Fix invalid or duplicate skill files, or raise agent.skills.max_skills_in_prompt.",
	}
}

func checkDoctorProjectSkillsUnavailable(id string, cfg workflowconfig.Skills, reason string) doctorCheck {
	if !cfg.Enabled {
		return checkDoctorProjectSkills(id, "", cfg)
	}
	return doctorCheck{
		Name:   "Project " + id + " skills",
		Status: doctorWarn,
		Detail: doctorSkillsConfigDetail(cfg) + "; skipped because " + reason,
		Hint:   "Make the source repository available locally, then rerun detent doctor.",
	}
}

func checkDoctorFilesystemProjectSkills(id string, project globalconfig.Project, cfg workflowconfig.Config) doctorCheck {
	sourceRoot := projectSourceRoot(project, cfg)
	if sourceRoot == "" {
		return checkDoctorProjectSkillsUnavailable(id, cfg.Agent.Skills, "source root is not configured")
	}
	expandedSourceRoot, err := expandDoctorWorkspacePath(sourceRoot)
	if err != nil {
		return checkDoctorProjectSkillsUnavailable(id, cfg.Agent.Skills, "source root could not be resolved")
	}
	info, err := os.Stat(expandedSourceRoot)
	if err != nil || !info.IsDir() {
		return checkDoctorProjectSkillsUnavailable(id, cfg.Agent.Skills, "source repository is unavailable locally")
	}
	return checkDoctorProjectSkills(id, expandedSourceRoot, cfg.Agent.Skills)
}

func doctorSkillsConfigDetail(cfg workflowconfig.Skills) string {
	return fmt.Sprintf(
		"enabled=%t; creation_enabled=%t; max_drafts_per_run=%d; path=%s; max_skills_in_prompt=%d",
		cfg.Enabled,
		cfg.Creation.Enabled,
		cfg.Creation.MaxDraftsPerRun,
		cfg.Path,
		cfg.MaxSkillsInPrompt,
	)
}

type doctorRouteModelProbeRequest struct {
	ProjectID    string
	Workspace    string
	WorkflowPath string
	RouteIndex   int
	RouteName    string
	RouteRole    string
	Model        string
	Effort       string
	Backend      workflowconfig.AgentBackend
}

func checkDoctorRouteModels(ctx context.Context, id string, project globalconfig.Project, cfg workflowconfig.Config, deps doctorDeps) doctorCheck {
	if deps.modelProbe == nil {
		deps.modelProbe = defaultDoctorRouteModelProbe
	}
	name := "Project " + id + " pinned route models"
	backends := doctorWorkflowBackendConfigsByID(cfg)
	workflowPath, workflowPathErr := doctorWorkflowOptimizationWorkflowPath(project)
	workspacePath := projectSourceRoot(project, cfg)
	if expanded, err := expandDoctorWorkspacePath(workspacePath); err == nil {
		workspacePath = expanded
	}

	var probed int
	var skipped int
	var failures []string
	var findings []doctorWorkflowOptimizationFinding
	probeModel := func(index int, route workflowconfig.AgentRoute, backend workflowconfig.AgentBackend, model string, source string) {
		probed++
		routeName := doctorRouteModelName(route, index)
		err := deps.modelProbe(ctx, doctorRouteModelProbeRequest{
			ProjectID:    id,
			Workspace:    workspacePath,
			WorkflowPath: workflowPath,
			RouteIndex:   index,
			RouteName:    routeName,
			RouteRole:    strings.TrimSpace(route.Role),
			Model:        model,
			Backend:      backend,
		})
		if err == nil {
			return
		}
		detail := fmt.Sprintf("project %s route %s model %s via %s rejected by backend: %v", id, routeName, model, source, err)
		failures = append(failures, detail)
		if workflowPathErr != nil {
			return
		}
		findings = append(findings, doctorWorkflowOptimizationFinding{
			RuleID:       doctorWorkflowRulePinnedRouteModelRejected,
			ProjectID:    id,
			WorkflowPath: workflowPath,
			Severity:     "error",
			Title:        "Pinned worker model rejected",
			Detail:       detail,
			Evidence: map[string]any{
				"backend":                 backend.ID,
				"configured_model_source": source,
				"error":                   err.Error(),
				"model":                   model,
				"route":                   routeName,
			},
		})
	}
	for index, route := range cfg.AgentRouteConfigs() {
		model := strings.TrimSpace(route.Model)
		if model == "" {
			continue
		}
		backend, ok := backends[strings.TrimSpace(route.Backend)]
		if !ok || strings.TrimSpace(backend.Kind) != workflowconfig.AgentBackendCodex {
			skipped++
			continue
		}
		probeModel(index, route, backend, model, "agents.routes.model")
	}
	probedCommandBackends := map[string]struct{}{}
	for index, route := range cfg.AgentRouteConfigs() {
		backendID := strings.TrimSpace(route.Backend)
		if _, ok := probedCommandBackends[backendID]; ok {
			continue
		}
		backend, ok := backends[backendID]
		if !ok || strings.TrimSpace(backend.Kind) != workflowconfig.AgentBackendCodex {
			continue
		}
		model := doctorWorkflowBackendCommandModel(backend)
		if model == "" {
			continue
		}
		probedCommandBackends[backendID] = struct{}{}
		probeModel(index, route, backend, model, "agents.backends.command")
	}

	if len(failures) == 0 {
		detail := fmt.Sprintf("validated %d pinned Codex route model(s)", probed)
		if skipped > 0 {
			detail += fmt.Sprintf("; skipped %d non-Codex pinned route model(s)", skipped)
		}
		return doctorCheck{Name: name, Status: doctorOK, Detail: detail}
	}
	report := doctorWorkflowOptimizationReport{
		Findings:  findings,
		Proposals: doctorWorkflowProposalsForFindings(id, findings, 1),
	}
	return doctorCheck{
		Name:                 name,
		Status:               doctorFail,
		Detail:               strings.Join(failures, "; "),
		Hint:                 "Confirm the project's intended model policy, then update the pin to a backend-supported model or remove it to inherit the provider default.",
		WorkflowOptimization: report,
	}
}

func doctorRouteModelName(route workflowconfig.AgentRoute, index int) string {
	if name := strings.TrimSpace(route.Name); name != "" {
		return name
	}
	return fmt.Sprintf("routes[%d]", index)
}

func defaultDoctorRouteModelProbe(ctx context.Context, req doctorRouteModelProbeRequest) error {
	backend, err := buildAgentBackend(req.Backend)
	if err != nil {
		return err
	}
	provider, ok := backend.(runnerpkg.AgentModelCatalogProvider)
	if !ok {
		return errors.New("backend does not advertise a model catalog")
	}
	models, err := provider.ListModels(ctx)
	if err != nil {
		return err
	}
	model := strings.TrimSpace(req.Model)
	if model == "" && strings.TrimSpace(req.Effort) != "" {
		defaultProvider, ok := backend.(runnerpkg.AgentDefaultModelProvider)
		if !ok {
			return errors.New("backend does not advertise its effective default model")
		}
		model, err = defaultProvider.DefaultModel(ctx, req.Workspace)
		if err != nil {
			return fmt.Errorf("effective default model unavailable: %w", err)
		}
	}
	if err := validateDoctorModelCatalog(models, model); err != nil {
		return err
	}
	if strings.TrimSpace(req.Effort) == "" {
		return nil
	}
	return validateDoctorEffortCatalog(models, model, req.Effort)
}

func validateDoctorModelCatalog(models []runnerpkg.AgentModel, requested string) error {
	_, err := doctorCatalogModel(models, requested)
	return err
}

func validateDoctorEffortCatalog(models []runnerpkg.AgentModel, requestedModel string, requestedEffort string) error {
	model, err := doctorCatalogModel(models, requestedModel)
	if err != nil {
		return err
	}
	want := strings.TrimSpace(requestedEffort)
	for _, effort := range model.SupportedReasoningEfforts {
		if strings.EqualFold(strings.TrimSpace(effort), want) {
			return nil
		}
	}
	supported := make([]string, 0, len(model.SupportedReasoningEfforts))
	for _, effort := range model.SupportedReasoningEfforts {
		if effort = strings.TrimSpace(effort); effort != "" {
			supported = append(supported, effort)
		}
	}
	if len(supported) == 0 {
		supported = append(supported, "none")
	}
	return fmt.Errorf("effort %q is not supported by model %q; supported efforts: %s", want, doctorCatalogModelName(model, requestedModel), strings.Join(supported, ", "))
}

func doctorCatalogModel(models []runnerpkg.AgentModel, requested string) (runnerpkg.AgentModel, error) {
	want := strings.TrimSpace(requested)
	for _, model := range models {
		if strings.TrimSpace(model.ID) != want && strings.TrimSpace(model.Model) != want {
			continue
		}
		if upgrade := strings.TrimSpace(model.Upgrade); upgrade != "" {
			return runnerpkg.AgentModel{}, fmt.Errorf("model %q is retired; use %q", want, upgrade)
		}
		return model, nil
	}
	return runnerpkg.AgentModel{}, fmt.Errorf("model %q is not available from the backend", want)
}

func doctorCatalogModelName(model runnerpkg.AgentModel, fallback string) string {
	if value := strings.TrimSpace(model.Model); value != "" {
		return value
	}
	if value := strings.TrimSpace(model.ID); value != "" {
		return value
	}
	return strings.TrimSpace(fallback)
}

func checkDoctorIssueAgentModels(ctx context.Context, id string, project globalconfig.Project, cfg workflowconfig.Config, deps doctorDeps) doctorCheck {
	name := "Project " + id + " issue agent models"
	backends := doctorWorkflowBackendConfigsByID(cfg)
	hasCodexBackend := false
	for _, backend := range backends {
		if strings.TrimSpace(backend.Kind) == workflowconfig.AgentBackendCodex {
			hasCodexBackend = true
			break
		}
	}
	if !hasCodexBackend {
		return doctorCheck{Name: name, Status: doctorOK, Detail: "detent-agent override validation skipped because no Codex backend is configured"}
	}
	if deps.autoPromoteConnector == nil {
		deps.autoPromoteConnector = defaultDoctorAutoPromoteConnector
	}
	projectConnector, err := deps.autoPromoteConnector(cfg)
	if err != nil {
		return doctorCheck{Name: name, Status: doctorFail, Detail: fmt.Sprintf("create issue override diagnostic connector: %v", err), Hint: "Fix tracker credentials and rerun detent doctor."}
	}
	if projectConnector == nil {
		return doctorCheck{Name: name, Status: doctorFail, Detail: "create issue override diagnostic connector: connector is nil", Hint: "Fix tracker configuration and rerun detent doctor."}
	}
	selectorContext := selector.Context{Persona: cfg.Tracker.Assignee}
	if identifier, ok := projectConnector.(connector.InstanceIdentifier); ok {
		selectorContext.InstanceLogin = identifier.InstanceLogin()
	}

	states := append(append([]string(nil), cfg.Tracker.ActiveStates...), cfg.Tracker.ObservedStates...)
	issues, fetchErr := projectConnector.FetchIssuesByStates(ctx, states)
	closeErr := closeDoctorAutoPromoteConnector(projectConnector)
	if fetchErr != nil {
		return doctorCheck{Name: name, Status: doctorFail, Detail: fmt.Sprintf("fetch issue agent overrides: %v", fetchErr), Hint: "Fix tracker connectivity and rerun detent doctor."}
	}
	router, err := doctorIssueAgentRouter(cfg)
	if err != nil {
		return doctorCheck{Name: name, Status: doctorFail, Detail: fmt.Sprintf("create issue agent override router: %v", err), Hint: "Fix agent route configuration and rerun detent doctor."}
	}

	if deps.modelProbe == nil {
		deps.modelProbe = defaultDoctorRouteModelProbe
	}
	workspacePath := projectSourceRoot(project, cfg)
	if expanded, err := expandDoctorWorkspacePath(workspacePath); err == nil {
		workspacePath = expanded
	}
	workflowPath, workflowPathErr := doctorWorkflowOptimizationWorkflowPath(project)
	if workflowPathErr != nil {
		workflowPath = strings.TrimSpace(project.Workflow)
	}
	probedModels := 0
	probedEfforts := 0
	failures := []string{}
	for _, issue := range issues {
		override, found, err := agentoverride.FromIssueBody(issue.Description)
		if !found {
			continue
		}
		identifier := strings.TrimSpace(issue.Identifier)
		if identifier == "" {
			identifier = strings.TrimSpace(issue.ID)
		}
		if err != nil {
			failures = append(failures, fmt.Sprintf("issue %s has invalid detent-agent block: %v", identifier, err))
			continue
		}
		roleEfforts := override.RoleEfforts()
		hasRoleEffort := false
		for _, roleEffort := range roleEfforts {
			if roleEffort.Effort != "" {
				hasRoleEffort = true
				break
			}
		}
		if override.Model == "" && override.Effort == "" && !hasRoleEffort {
			continue
		}
		probe := func(role string, field string, effort string, includeModel bool) {
			selection, routeErr := router.RouteForRole(issue, selectorContext, role)
			if routeErr != nil {
				failures = append(failures, fmt.Sprintf("issue %s detent-agent %s route selection failed: %v", identifier, field, routeErr))
				return
			}
			backend, ok := backends[strings.TrimSpace(selection.BackendID)]
			if !ok {
				failures = append(failures, fmt.Sprintf("issue %s detent-agent route %s references unavailable backend %s", identifier, selection.RouteName, selection.BackendID))
				return
			}
			model := override.Model
			if model == "" {
				model = selection.Model
			}
			probeErr := deps.modelProbe(ctx, doctorRouteModelProbeRequest{
				ProjectID:    id,
				Workspace:    workspacePath,
				WorkflowPath: workflowPath,
				RouteName:    identifier + ":" + role,
				Model:        model,
				Effort:       effort,
				Backend:      backend,
			})
			if probeErr == nil {
				return
			}
			fields := []string{}
			if includeModel && override.Model != "" {
				fields = append(fields, "model "+override.Model)
			}
			if effort != "" {
				fields = append(fields, field+" "+effort)
			}
			failures = append(failures, fmt.Sprintf("issue %s detent-agent %s rejected by backend: %v", identifier, strings.Join(fields, " "), probeErr))
		}

		if override.Model != "" || override.Effort != "" {
			probe(runnerpkg.RoleCode, "effort", override.Effort, true)
			if override.Model != "" {
				probedModels++
			}
			if override.Effort != "" {
				probedEfforts++
			}
		}
		for _, roleEffort := range roleEfforts {
			if roleEffort.Effort == "" {
				continue
			}
			probe(roleEffort.Role, roleEffort.Field, roleEffort.Effort, false)
			if !roleEffort.Inherited {
				probedEfforts++
			}
		}
	}

	if len(failures) > 0 {
		return doctorCheck{
			Name:   name,
			Status: doctorFail,
			Detail: strings.Join(failures, "; "),
			Hint:   "Fix rejected detent-agent values in the original issue bodies; remove the model key to inherit the project default model, or remove the effort key to inherit the project default effort.",
		}
	}
	detail := fmt.Sprintf("validated %d detent-agent model override(s) and %d effort override(s)", probedModels, probedEfforts)
	check := doctorCheck{Name: name, Status: doctorOK, Detail: detail}
	if closeErr != nil {
		check.Status = doctorWarn
		check.Detail += "; connector close failed: " + closeErr.Error()
		check.Hint = "Rerun detent doctor and check local network resources."
	}
	return check
}

func doctorIssueAgentRouter(cfg workflowconfig.Config) (*runnerpkg.Router, error) {
	routes := cfg.AgentRouteConfigs()
	doctorRoutes := make([]runnerpkg.Route, 0, len(routes))
	for _, route := range routes {
		doctorRoutes = append(doctorRoutes, runnerpkg.Route{
			Name:       route.Name,
			Role:       route.Role,
			BackendID:  route.Backend,
			Model:      route.Model,
			ModelField: route.ModelField,
			Default:    route.Default,
			Selector:   route.Selector,
		})
	}
	return runnerpkg.NewRouter(doctorRoutes)
}

func doctorProjectID(project globalconfig.Project) string {
	id := strings.TrimSpace(project.ID)
	if id == "" {
		return "project"
	}
	return id
}

func doctorWorkflowDetail(path string, project globalconfig.Project, cfg workflowconfig.Config) string {
	details := []string{path + " is valid"}
	if cfg.Identity.Configured() {
		details = append(details, "identity "+doctorIdentityDetail(cfg.Identity))
	}
	details = append(details, doctorReviewFlowConfigDetail(cfg))
	details = append(details, doctorWorkflowModelChoiceDetail(cfg))
	details = append(details, doctorWorkflowSessionGuardDetail(cfg))
	details = append(details, doctorWorkflowBillingModeDetail(cfg.Budget))
	details = append(details, doctorWorkflowSpendBreakerDetail(cfg))
	details = append(details, fmt.Sprintf("orphan-recovery=resume_orphaned_sessions=%t, experimental_thread_resume=%t", cfg.Agent.ResumeOrphanedSessions, cfg.Agent.ExperimentalThreadResume))
	details = append(details, fmt.Sprintf("prioritize-unblockers=%t", cfg.Agent.PrioritizeUnblockers))
	details = append(details, doctorAuthorizationDetail(project, cfg))
	return strings.Join(details, "; ")
}

func doctorWorkflowBillingModeDetail(cfg workflowconfig.Budget) string {
	detail := "billing-mode=" + cfg.EffectiveBillingMode()
	if !cfg.BillingModeConfigured() {
		detail += " (assumed)"
	}
	return detail
}

func doctorWorkflowModelChoiceDetail(cfg workflowconfig.Config) string {
	choice := doctorWorkflowWorkerModelChoice(cfg)
	if choice.Mode == "pinned" {
		return fmt.Sprintf("worker-model=pinned %s via %s", choice.Model, choice.Source)
	}
	detail := "worker-model=provider-default"
	if choice.Source == "agents.routes.model_field" {
		detail += " with issue-field overrides"
	}
	return detail
}

func doctorWorkflowSessionGuardDetail(cfg workflowconfig.Config) string {
	maxTurns := "disabled"
	if cfg.Agent.MaxTurns > 0 {
		maxTurns = strconv.Itoa(cfg.Agent.MaxTurns)
	}
	turnDuration := "disabled"
	if cfg.Agent.MaxTurnDurationMS > 0 {
		turnDuration = strconv.Itoa(cfg.Agent.MaxTurnDurationMS)
	}
	sessionDuration := "disabled"
	if cfg.Agent.MaxSessionDurationMS > 0 {
		sessionDuration = strconv.Itoa(cfg.Agent.MaxSessionDurationMS)
	}
	noProgressTimeout := "disabled"
	if cfg.Agent.NoProgressTimeoutMS > 0 {
		noProgressTimeout = strconv.Itoa(cfg.Agent.NoProgressTimeoutMS)
	}
	mergeDuration := "disabled"
	if cfg.Agent.MergeWorkerMaxDurationMS > 0 {
		mergeDuration = strconv.Itoa(cfg.Agent.MergeWorkerMaxDurationMS)
	}
	tokens := "disabled"
	if cfg.Agent.MaxSessionTokens > 0 {
		tokens = strconv.FormatInt(cfg.Agent.MaxSessionTokens, 10)
	}
	multiplier := "disabled"
	if cfg.Agent.MaxSessionContextMultiplier > 0 {
		multiplier = strconv.FormatFloat(cfg.Agent.MaxSessionContextMultiplier, 'g', -1, 64)
	}
	return fmt.Sprintf(
		"session-guard=max_turns=%s, max_turn_duration_ms=%s, max_session_duration_ms=%s, no_progress_timeout_ms=%s, merge_worker_max_duration_ms=%s, max_session_tokens=%s, max_session_context_multiplier=%s",
		maxTurns,
		turnDuration,
		sessionDuration,
		noProgressTimeout,
		mergeDuration,
		tokens,
		multiplier,
	)
}

func doctorWorkflowSpendBreakerDetail(cfg workflowconfig.Config) string {
	tokenLimit := "disabled"
	if cfg.Agent.NoProgressTokenLimit > 0 {
		tokenLimit = strconv.FormatInt(cfg.Agent.NoProgressTokenLimit, 10)
	}
	usdLimit := "disabled"
	if cfg.Agent.NoProgressSpendLimitUSD > 0 {
		usdLimit = strconv.FormatFloat(cfg.Agent.NoProgressSpendLimitUSD, 'f', 2, 64)
		if cfg.Budget.EffectiveBillingMode() == workflowconfig.BillingModeSubscription {
			usdLimit += " (inert)"
		}
	}
	return "progress-breaker=no_progress_token_limit=" + tokenLimit + ", no_progress_spend_limit_usd=" + usdLimit
}

func doctorWorkflowFindingDetails(findings []doctorWorkflowOptimizationFinding) string {
	parts := make([]string, 0, len(findings))
	for _, finding := range findings {
		if strings.TrimSpace(finding.Detail) != "" {
			parts = append(parts, finding.Detail)
		}
	}
	return strings.Join(parts, "; ")
}

func doctorIdentityDetail(identity workflowconfig.Identity) string {
	identity.Normalize()
	if !identity.Configured() {
		return "not configured; ownership defaults to assignee"
	}

	details := []string{identity.Name}
	if identity.GitHubLogin != "" {
		details = append(details, "github_login "+identity.GitHubLogin)
	}
	switch identity.OwnershipMode {
	case workflowconfig.IdentityOwnershipField:
		details = append(details, "owner field "+identity.OwnerField)
	default:
		details = append(details, "owner "+identity.OwnershipMode)
	}
	return strings.Join(details, ", ")
}

func doctorAuthorizationDetail(project globalconfig.Project, cfg workflowconfig.Config) string {
	projectAuthorization := project.Authorization.Configured()
	workflowAuthorization := cfg.Tracker.Authorization.Configured()
	switch {
	case projectAuthorization && workflowAuthorization:
		return "authorization selectors from global.yaml and WORKFLOW.md"
	case projectAuthorization:
		return "authorization selector from global.yaml"
	case workflowAuthorization:
		return "authorization selector from WORKFLOW.md"
	default:
		return "authorization allows all issues"
	}
}

func projectSourceRoot(project globalconfig.Project, cfg workflowconfig.Config) string {
	if sourceRoot := strings.TrimSpace(cfg.Workspace.SourceRoot); sourceRoot != "" {
		return sourceRoot
	}
	if workdir := strings.TrimSpace(project.Workdir); workdir != "" {
		return workdir
	}
	if root := strings.TrimSpace(cfg.Workspace.Root); root != "" {
		return root
	}
	return ""
}

func loadDoctorProjectWorkflow(ctx context.Context, project globalconfig.Project, deps doctorDeps) (workflowconfig.Workflow, error) {
	if deps.workflowCache != nil {
		return deps.workflowCache.load(ctx, project, deps)
	}
	return loadDoctorProjectWorkflowUncached(ctx, project, deps)
}

func loadDoctorProjectWorkflowUncached(ctx context.Context, project globalconfig.Project, deps doctorDeps) (workflowconfig.Workflow, error) {
	if strings.TrimSpace(project.WorkflowRef) == "" {
		workflow, err := deps.loadWorkflow(project.Workflow)
		if err == nil {
			workflow.Config = workflow.Config.WithAgentDefaults(project.GlobalAgents, project.GlobalBudget)
		}
		return workflow, err
	}
	return projectpkg.LoadWorkflowContext(ctx, project)
}
