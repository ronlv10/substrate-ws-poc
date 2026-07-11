package proxy

import (
	"log/slog"
	"os"
	"os/exec"
	"os/signal"
	"syscall"
)

// RunAgent spawns the agent (the proxy is PID 1) and returns its exit code.
// The caller must already be listening on :443 and :80 — the agent dials
// "slack.com" immediately, so ordering is the supervisor's responsibility.
// The agent's stdout is redirected to stderr: Node block-buffers stdout under
// gVisor and the logs would be lost across checkpoints.
func RunAgent(log *slog.Logger, argv []string) int {
	cmd := exec.Command(argv[0], argv[1:]...)
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr
	cmd.Env = os.Environ()
	if err := cmd.Start(); err != nil {
		log.Error("local-proxy: starting agent failed", slog.Any("error", err))
		return 1
	}
	log.Info("local-proxy: agent started", slog.Int("pid", cmd.Process.Pid))

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		sig := <-sigs
		_ = cmd.Process.Signal(sig)
	}()

	err := cmd.Wait()
	if err == nil {
		return 0
	}
	if ee, ok := err.(*exec.ExitError); ok {
		log.Error("local-proxy: agent exited", slog.Int("code", ee.ExitCode()), slog.Any("state", ee.String()))
		return ee.ExitCode()
	}
	log.Error("local-proxy: agent wait failed", slog.Any("error", err))
	return 1
}
