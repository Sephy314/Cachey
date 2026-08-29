package harness

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
)

type Config struct {
	Agent AgentConfig
	Test  TestConfig
	Git   GitConfig
	Loop  LoopConfig
}
type AgentConfig struct {
	Command string
	Args    []string
}
type TestConfig struct{ Commands []string }
type GitConfig struct{ Enabled bool }
type LoopConfig struct{ MaxIterations int }
type Files struct{ ProjectContext, Rules, Task, AgentTemplate, FixTemplate string }
type RepositoryState struct{ Status, Diff, Recent string }
type CommandResult struct {
	Command, Output string
	ExitCode        int
	Success         bool
}
type Iteration struct {
	Number       int
	Prompt       string
	AgentError   error
	Verification []CommandResult
	GitState     RepositoryState
}
type LoopResult struct {
	Success    bool
	Iterations []Iteration
	Reason     string
	LastError  error
	FinalState RepositoryState
}

func LoadConfig(path string) (Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return Config{}, fmt.Errorf("harness configuration not found: %s", path)
		}
		return Config{}, fmt.Errorf("read harness configuration: %w", err)
	}
	config := Config{Git: GitConfig{Enabled: true}}
	section := ""
	lines := strings.Split(string(data), "\n")
	for index := 0; index < len(lines); index++ {
		line := strings.TrimSpace(strings.SplitN(lines[index], "#", 2)[0])
		if line == "" {
			continue
		}
		if strings.HasPrefix(line, "[") && strings.HasSuffix(line, "]") {
			section = strings.Trim(line, "[]")
			continue
		}
		parts := strings.SplitN(line, "=", 2)
		if len(parts) != 2 {
			return Config{}, fmt.Errorf("invalid configuration at line %d", index+1)
		}
		key, value := strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1])
		for strings.HasPrefix(value, "[") && !strings.Contains(value, "]") && index+1 < len(lines) {
			index++
			value += " " + strings.TrimSpace(strings.SplitN(lines[index], "#", 2)[0])
		}
		if err := setConfig(&config, section, key, value); err != nil {
			return Config{}, fmt.Errorf("invalid configuration at line %d: %w", index+1, err)
		}
	}
	if config.Agent.Command == "" {
		return Config{}, fmt.Errorf("invalid configuration: [agent].command is required")
	}
	if len(config.Test.Commands) == 0 {
		return Config{}, fmt.Errorf("invalid configuration: [test].commands must include at least one command")
	}
	if config.Loop.MaxIterations < 1 {
		return Config{}, fmt.Errorf("invalid configuration: [loop].max_iterations must be greater than zero")
	}
	return config, nil
}
func setConfig(c *Config, section, key, value string) error {
	switch section + "." + key {
	case "agent.command":
		var err error
		c.Agent.Command, err = strconv.Unquote(value)
		return err
	case "agent.args":
		return json.Unmarshal([]byte(value), &c.Agent.Args)
	case "test.commands":
		return json.Unmarshal([]byte(value), &c.Test.Commands)
	case "git.enabled":
		var err error
		c.Git.Enabled, err = strconv.ParseBool(value)
		return err
	case "loop.max_iterations":
		var err error
		c.Loop.MaxIterations, err = strconv.Atoi(value)
		return err
	default:
		return fmt.Errorf("unknown setting %q in section %q", key, section)
	}
}
func LoadFiles(root string) (Files, error) {
	base := filepath.Join(root, ".harness")
	read := func(name string, required bool) (string, error) {
		data, err := os.ReadFile(filepath.Join(base, name))
		if os.IsNotExist(err) && !required {
			return "", nil
		}
		if err != nil {
			return "", fmt.Errorf("read harness file %s: %w", name, err)
		}
		return strings.TrimSpace(string(data)), nil
	}
	var f Files
	var err error
	if f.ProjectContext, err = read("context.md", false); err != nil {
		return f, err
	}
	if f.Rules, err = read("rules.md", false); err != nil {
		return f, err
	}
	if f.Task, err = read("tasks/current.md", true); err != nil {
		return f, err
	}
	if f.AgentTemplate, err = read("prompts/agent.md", true); err != nil {
		return f, err
	}
	if f.FixTemplate, err = read("prompts/fix.md", true); err != nil {
		return f, err
	}
	if f.Task == "" {
		return f, fmt.Errorf("current harness task is empty")
	}
	return f, nil
}

type Agent interface {
	Run(context.Context, string) error
}
type CommandAgent struct{ config AgentConfig }

func (a CommandAgent) Run(ctx context.Context, prompt string) error {
	command := exec.CommandContext(ctx, a.config.Command, append(a.config.Args, prompt)...)
	output, err := command.CombinedOutput()
	if err == nil {
		return nil
	}
	if _, ok := err.(*exec.Error); ok {
		return fmt.Errorf("failed to execute configured agent %q: executable not found in PATH", a.config.Command)
	}
	return fmt.Errorf("configured agent %q failed: %w\n%s", a.config.Command, err, output)
}

type Verifier interface {
	Verify(context.Context) []CommandResult
}
type CommandVerifier struct {
	root     string
	commands []string
}

func (v CommandVerifier) Verify(ctx context.Context) []CommandResult {
	results := make([]CommandResult, 0, len(v.commands))
	for _, value := range v.commands {
		command := exec.CommandContext(ctx, "/bin/sh", "-c", value)
		command.Dir = v.root
		output, err := command.CombinedOutput()
		result := CommandResult{Command: value, Output: limit(string(output)), Success: err == nil}
		if exit, ok := err.(*exec.ExitError); ok {
			result.ExitCode = exit.ExitCode()
		}
		if err != nil && result.ExitCode == 0 {
			result.ExitCode = 1
		}
		results = append(results, result)
	}
	return results
}

type Repository interface {
	Inspect(context.Context) (RepositoryState, error)
}
type GitRepository struct {
	root    string
	enabled bool
}

func (r GitRepository) Inspect(ctx context.Context) (RepositoryState, error) {
	if !r.enabled {
		return RepositoryState{Status: "Git inspection disabled by configuration."}, nil
	}
	run := func(args ...string) (string, error) {
		command := exec.CommandContext(ctx, "git", args...)
		command.Dir = r.root
		output, err := command.CombinedOutput()
		return limit(string(output)), err
	}
	status, err := run("status", "--short")
	if err != nil {
		return RepositoryState{}, fmt.Errorf("git status: %w", err)
	}
	diff, err := run("diff", "--")
	if err != nil {
		return RepositoryState{}, err
	}
	recent, err := run("log", "-n", "5", "--oneline")
	return RepositoryState{status, diff, recent}, err
}
func limit(value string) string {
	if len(value) <= 12000 {
		return value
	}
	return value[:12000] + "\n[output truncated]"
}
func BuildPrompt(f Files, s RepositoryState, feedback string, iteration int) string {
	prompt := fmt.Sprintf("%s\n\nPROJECT CONTEXT\n%s\n\nREPOSITORY RULES\n%s\n\nCURRENT TASK\n%s\n\nREPOSITORY STATE\nGit status:\n%s\n\nGit diff:\n%s\n\nRecent commits:\n%s", f.AgentTemplate, f.ProjectContext, f.Rules, f.Task, s.Status, s.Diff, s.Recent)
	if feedback != "" {
		prompt += "\n\nPREVIOUS VERIFICATION RESULT\n" + feedback + "\n\n" + f.FixTemplate
	}
	return fmt.Sprintf("ITERATION %d\n\n%s", iteration, prompt)
}
func BuildFeedback(results []CommandResult) string {
	feedback := "VERIFICATION FAILED\n"
	for _, r := range results {
		if !r.Success {
			feedback += fmt.Sprintf("\nCommand:\n%s\n\nExit code:\n%d\n\nOutput:\n%s\n", r.Command, r.ExitCode, r.Output)
		}
	}
	return feedback + "\nFix these failures while preserving the original task requirements."
}

type Loop struct {
	files      Files
	max        int
	agent      Agent
	verifier   Verifier
	repository Repository
	output     io.Writer
}

func NewLoop(root string, c Config, f Files, output io.Writer, _ bool) *Loop {
	return &Loop{f, c.Loop.MaxIterations, CommandAgent{c.Agent}, CommandVerifier{root, c.Test.Commands}, GitRepository{root, c.Git.Enabled}, output}
}
func (l *Loop) Run(ctx context.Context) LoopResult {
	result := LoopResult{}
	feedback := ""
	for number := 1; number <= l.max; number++ {
		if err := ctx.Err(); err != nil {
			result.Reason, result.LastError = "interrupted", err
			return result
		}
		fmt.Fprintf(l.output, "\n=== Iteration %d/%d ===\n\n[context] Building context\n", number, l.max)
		state, err := l.repository.Inspect(ctx)
		if err != nil {
			result.Reason, result.LastError = "repository inspection failed", err
			return result
		}
		iteration := Iteration{Number: number, Prompt: BuildPrompt(l.files, state, feedback, number)}
		fmt.Fprintln(l.output, "[agent] Running agent")
		if err = l.agent.Run(ctx, iteration.Prompt); err != nil {
			iteration.AgentError = err
			result.Iterations = append(result.Iterations, iteration)
			result.Reason, result.LastError = "agent failed", err
			return result
		}
		fmt.Fprintln(l.output, "[git] Inspecting repository changes\n[verify] Running configured commands")
		state, err = l.repository.Inspect(ctx)
		if err != nil {
			result.Reason, result.LastError = "repository inspection failed", err
			return result
		}
		iteration.GitState, iteration.Verification, result.FinalState = state, l.verifier.Verify(ctx), state
		result.Iterations = append(result.Iterations, iteration)
		passed := true
		for _, r := range iteration.Verification {
			passed = passed && r.Success
		}
		if passed {
			result.Success, result.Reason = true, "verification passed"
			return result
		}
		feedback = BuildFeedback(iteration.Verification)
		fmt.Fprintln(l.output, "[feedback] Verification failed; feedback will be sent to the next iteration")
	}
	result.Reason = "maximum iterations reached"
	return result
}
func PrintSummary(output io.Writer, result LoopResult) {
	if result.Success {
		fmt.Fprintln(output, "\nHarness completed successfully.")
	} else {
		fmt.Fprintln(output, "\nHarness stopped.")
	}
	fmt.Fprintf(output, "\nIterations: %d\n", len(result.Iterations))
	if !result.Success {
		fmt.Fprintf(output, "\nReason:\n%s\n", result.Reason)
		if result.LastError != nil {
			fmt.Fprintln(output, result.LastError)
		}
	}
	if len(result.Iterations) > 0 {
		fmt.Fprintln(output, "\nVerification:")
		for _, r := range result.Iterations[len(result.Iterations)-1].Verification {
			mark := "x"
			if r.Success {
				mark = "OK"
			}
			fmt.Fprintf(output, "%s %s\n", mark, r.Command)
		}
	}
	if status := strings.TrimSpace(result.FinalState.Status); status != "" {
		fmt.Fprintf(output, "\nGit status:\n%s\n", status)
	}
}
