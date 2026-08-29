package harness

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadConfig(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.toml")
	data := "[agent]\ncommand = \"fake\"\nargs = [\"--prompt\"]\n[test]\ncommands = [\"true\"]\n[loop]\nmax_iterations = 2\n"
	if err := os.WriteFile(path, []byte(data), 0600); err != nil {
		t.Fatal(err)
	}
	config, err := LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if config.Agent.Command != "fake" || config.Loop.MaxIterations != 2 {
		t.Fatalf("unexpected config: %#v", config)
	}
}

func TestPromptAndFeedback(t *testing.T) {
	files := Files{AgentTemplate: "agent", ProjectContext: "context", Rules: "rules", Task: "task", FixTemplate: "fix"}
	prompt := BuildPrompt(files, RepositoryState{Status: "M file", Diff: "diff", Recent: "commit"}, "failure", 2)
	for _, value := range []string{"PROJECT CONTEXT", "REPOSITORY RULES", "CURRENT TASK", "REPOSITORY STATE", "PREVIOUS VERIFICATION RESULT", "fix"} {
		if !strings.Contains(prompt, value) {
			t.Fatalf("missing %q", value)
		}
	}
	feedback := BuildFeedback([]CommandResult{{Command: "go test", ExitCode: 1, Output: "failed"}})
	if !strings.Contains(feedback, "Exit code:\n1") || !strings.Contains(feedback, "failed") {
		t.Fatalf("unexpected feedback: %s", feedback)
	}
}

func TestCommandVerifier(t *testing.T) {
	results := (CommandVerifier{root: t.TempDir(), commands: []string{"printf ok", "exit 7"}}).Verify(context.Background())
	if !results[0].Success || results[1].Success || results[1].ExitCode != 7 {
		t.Fatalf("unexpected results: %#v", results)
	}
}

func TestLoopRetriesAndStops(t *testing.T) {
	agent := &fakeAgent{}
	loop := newTestLoop(agent, &fakeVerifier{results: [][]CommandResult{{{Command: "test", ExitCode: 1}}, {{Command: "test", Success: true}}}})
	result := loop.Run(context.Background())
	if !result.Success || len(result.Iterations) != 2 || !strings.Contains(agent.prompts[1], "VERIFICATION FAILED") {
		t.Fatalf("unexpected result: %#v", result)
	}
}

func TestLoopFailuresAndCancellation(t *testing.T) {
	failed := newTestLoop(&fakeAgent{err: errors.New("unavailable")}, &fakeVerifier{}).Run(context.Background())
	if failed.Reason != "agent failed" {
		t.Fatalf("reason = %q", failed.Reason)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	interrupted := newTestLoop(&fakeAgent{}, &fakeVerifier{}).Run(ctx)
	if interrupted.Reason != "interrupted" {
		t.Fatalf("reason = %q", interrupted.Reason)
	}
}

func TestLoopIterationLimit(t *testing.T) {
	result := newTestLoop(&fakeAgent{}, &fakeVerifier{results: [][]CommandResult{{{Command: "test", ExitCode: 1}}, {{Command: "test", ExitCode: 1}}}}).Run(context.Background())
	if result.Reason != "maximum iterations reached" || len(result.Iterations) != 2 {
		t.Fatalf("unexpected result: %#v", result)
	}
}

type fakeAgent struct {
	prompts []string
	err     error
}

func (a *fakeAgent) Run(_ context.Context, prompt string) error {
	a.prompts = append(a.prompts, prompt)
	return a.err
}

type fakeVerifier struct {
	results [][]CommandResult
	calls   int
}

func (v *fakeVerifier) Verify(context.Context) []CommandResult {
	result := v.results[v.calls]
	v.calls++
	return result
}

type fakeRepository struct{}

func (fakeRepository) Inspect(context.Context) (RepositoryState, error) {
	return RepositoryState{Status: " M file"}, nil
}
func newTestLoop(agent Agent, verifier Verifier) *Loop {
	return &Loop{files: Files{AgentTemplate: "agent", Task: "task", FixTemplate: "fix"}, max: 2, agent: agent, verifier: verifier, repository: fakeRepository{}, output: io.Discard}
}
