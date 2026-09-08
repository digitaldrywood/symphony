import argparse
import datetime
import hashlib
import json
import os
import pathlib
import platform
import subprocess
import time


SUITES = {
    "hubserver": [
        "TestPilotIdleRunnerReconciliation",
        "TestPilotHostedWorkloads",
        "TestPilotHostedHistoryAfterRunnerLossAndRestart",
        "TestHostedBrowserFirstOrganization",
        "TestHostedLoginInvitationIntentAndReplay",
        "TestHostedOnboardingFirstRun",
        "TestNativeRecoveryAfterReassignmentAndRestart",
        "TestNativeCheckpointValidation",
        "TestProjectPolicyIsolationAndRestart",
        "TestRunnerRoutingClaims",
        "TestRunnerSharedHostConcurrentClaimsAndRestart",
        "TestRunnerRoutingRevocationAndDrain",
        "TestGitHubImportCheckpointCutoverAndNativeIsolation",
        "TestChangeApprovalPreservesProtectedMerge",
        "TestChangeInvalidVersionAndReviewPolicy",
        "TestChangeCIRejectsForgedAndReplayedResults",
        "TestHostedSecurityRoleProjectMatrix",
        "TestHostedSecurityReplayAndCursorRevocation",
        "TestHostedSecurityRoleRevocationAppliesImmediately",
        "TestHostedArtifactReadPermissions",
        "TestHostedArtifactGrantRevocation",
        "TestHostedConcurrentClaimsDowngradeRelease",
        "TestHostedMutationQuotaRollbackAndRetry",
        "TestHostedPlanResolutionBoundaries",
        "TestHostedProjectRetryAfterDowngrade",
        "TestHostedPlanConfigurationAndTelemetry",
        "TestHostedPlansDisabledWithoutNetwork",
        "TestHostedMetadataExcludesCustomerContent",
        "TestHostedSecurityLogsExcludeCustomerContent",
        "TestRecoveryPreservesCollaborationAndFencesAuthority",
        "TestRecoveryRejectsUnsafeSourcesAndDestinations",
        "TestSelfHostedRunnerVersionCompatibility",
    ],
    "hubgithub": [
        "TestPilotGitHubRequestBudgets",
        "TestPilotGitHubImportBudget",
        "TestPilotGitHubBackoffAndOperationBound",
        "TestSharedTransportBackoffAndRequestCounts",
    ],
    "hubclient": [
        "TestNativeSchedulerAndConnectorWithoutGitHub",
        "TestNativeExecutionContextKeepsItsOriginalFence",
        "TestNativeDelayedResponsesPreserveSuccessor",
        "TestArtifactJournalReplay",
    ],
    "artifact": [
        "TestPilotArtifactTraffic",
        "TestHTTPArtifactAccessWithoutRunners",
        "TestHTTPUploadStorageOutage",
        "TestRemoteHubAuthorizationFailsClosed",
        "TestUploadRecoveryAndImmutableManifests",
        "TestArtifactFailureStates",
        "TestRestoredCatalogCannotResurrectDeletedArtifact",
        "TestHostedTrafficAndAdmittedCompletion",
        "TestHostedRelayQuotaAndRemoteLimits",
        "TestHostedConcurrentReservationsAndDowngrade",
    ],
}


def collect(root, race):
    expected = {(package, test) for package, tests in SUITES.items() for test in tests}
    pattern = "^(" + "|".join(sorted({test for _, test in expected})) + ")$"
    command = ["go", "test", "-json", "-count=1", "-timeout=5m"]
    if race:
        command.append("-race")
    command.extend(["-run", pattern])
    command.extend("./internal/" + package for package in SUITES)
    environment = dict(os.environ)
    environment.pop("DETENT_API_TOKEN", None)
    environment["GOTOOLCHAIN"] = next(
        line.split()[1] for line in (root / "go.mod").read_text().splitlines()
        if line.startswith("toolchain ")
    )
    started = time.monotonic()
    result = subprocess.run(command, cwd=root, env=environment, capture_output=True, text=True, timeout=600)
    passed = set()
    failures = []
    samples = []
    measurements = []
    for line in result.stdout.splitlines():
        event = json.loads(line)
        package = event.get("Package", "").rsplit("/", 1)[-1]
        test = event.get("Test", "")
        if event["Action"] == "pass" and test:
            passed.add((package, test))
        if event["Action"] == "fail":
            failures.append(package + "/" + test)
        output = event.get("Output", "")
        if "PILOT " in output:
            measurement = output.split("PILOT ", 1)[1].strip()
            measurements.append(measurement)
            if measurement.startswith("workload "):
                samples.append(json.loads(measurement.removeprefix("workload ")))
    missing = sorted(package + "/" + test for package, test in expected - passed)
    if result.returncode or missing or failures:
        print(result.stderr)
        print(result.stdout)
        raise RuntimeError(f"Pilot evidence failed: missing={missing}, failed={failures}")
    files = [path for package in SUITES for path in (root / "internal" / package).glob("pilot*_test.go")]
    return {
        "schema": 1,
        "kind": "synthetic_local_protocol_evidence",
        "release_authorized": False,
        "captured_at_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "source_head": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root, text=True).strip(),
        "source_dirty": bool(subprocess.check_output(["git", "status", "--porcelain"], cwd=root, text=True).strip()),
        "test_sha256": {str(path.relative_to(root)): hashlib.sha256(path.read_bytes()).hexdigest() for path in sorted(files)},
        "platform": platform.system() + "/" + platform.machine(),
        "go": subprocess.check_output(["go", "version"], cwd=root, env=environment, text=True).strip(),
        "race": race,
        "elapsed_seconds": round(time.monotonic() - started, 3),
        "required_tests": len(expected),
        "passed_tests_and_subtests": len(passed),
        "suites": SUITES,
        "measurements": measurements,
        "workloads": samples,
        "hosted_costs": {
            "compute": None, "database": None, "event_retention": None,
            "backups": None, "network": None, "relay": None,
            "shared_baseline": None, "support": None,
            "reason": "No hosted deployment, provider rates, utilization or support sample measured. Null is unmeasured, never zero.",
        },
    }


def main():
    parser = argparse.ArgumentParser(description="Collect synthetic pilot evidence without live provider or customer data")
    parser.add_argument("--output", type=pathlib.Path, required=True)
    parser.add_argument("--race", action="store_true")
    args = parser.parse_args()
    root = pathlib.Path(__file__).resolve().parents[1]
    if args.output.exists():
        parser.error("output already exists; choose a new evidence path")
    evidence = collect(root, args.race)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("x") as output:
        json.dump(evidence, output, indent=2, sort_keys=True)
        output.write("\n")
    print(f"Passed {evidence['required_tests']} required tests; evidence: {args.output}")


if __name__ == "__main__":
    main()
