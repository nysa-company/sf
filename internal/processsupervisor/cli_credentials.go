package processsupervisor

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"
)

var errCLICredentials = errors.New("CLI browser credentials are unavailable")

// Fixed text only: expiry diagnostics must never include credential material.
var errClaudeAuthRenewal = errors.New("Claude authentication cannot cover the required launch window; run claude auth login, then retry qualification")

// cliSecretLookup is supervisor-owned, never adapter/config supplied in
// production. Its only implementation reads fixed provider service names from
// macOS Keychain with bounded output. No secrets enter argv, errors or Store.
type cliSecretLookup func(context.Context, string, string) ([]byte, error)

func lookupCLISecret(ctx context.Context, service, account string) ([]byte, error) {
	if runtime.GOOS != "darwin" {
		return nil, errCLICredentials
	}
	if !(service == "Claude Code-credentials" && account == "" || (service == "cursor-access-token" || service == "cursor-refresh-token") && account == "cursor-user") {
		return nil, errCLICredentials
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	args := []string{"find-generic-password", "-s", service}
	if account != "" {
		args = append(args, "-a", account)
	}
	args = append(args, "-w")
	cmd := exec.CommandContext(ctx, "/usr/bin/security", args...)
	// Keychain lookup uses the host account, not the private provider HOME.
	// Do not inherit endpoint, proxy, shell or provider configuration variables.
	owner, err := user.Current()
	if err != nil || owner.Uid != strconv.Itoa(os.Geteuid()) || !filepath.IsAbs(owner.HomeDir) || filepath.Clean(owner.HomeDir) != owner.HomeDir || owner.HomeDir == "/" {
		return nil, errCLICredentials
	}
	cmd.Env = []string{"PATH=/usr/bin:/bin", "LANG=C", "HOME=" + owner.HomeDir}
	var output limitedBuffer
	output.limit = 16 << 10
	cmd.Stdout = &output
	if cmd.Run() != nil || output.truncated {
		return nil, errCLICredentials
	}
	return bytes.TrimSpace(output.Bytes()), nil
}

// prepareCLICredentials prepares only authentication in a freshly created,
// private HOME owned by the supervisor. No user settings, hooks, MCP, history,
// plugins, API keys or cloud-provider credentials are copied. The caller owns
// cleanup and must keep this home alive until process completion/drain.
//
// Digest binds the selected credential bytes, not account billing entitlement.
// A successful status/usage qualification is still required. Returned env is
// sensitive and may only be passed directly to the qualified process.
func prepareCLICredentials(ctx context.Context, provider, home string, lookup cliSecretLookup) (env []string, digest string, err error) {
	if ctx.Err() != nil {
		return nil, "", ctx.Err()
	}
	if lookup == nil || privateExistingDirectory(home) != nil {
		return nil, "", errCLICredentials
	}
	info, statErr := os.Lstat(home)
	entries, readErr := os.ReadDir(home)
	if statErr != nil || readErr != nil || info.Mode().Perm()&0077 != 0 || len(entries) != 0 {
		return nil, "", errCLICredentials
	}
	h := sha256.New()
	h.Write([]byte("sf-cli-browser-auth/v1\x00" + provider + "\x00"))
	switch provider {
	case "claude":
		raw, err := lookup(ctx, "Claude Code-credentials", "")
		if err != nil || len(raw) > 16<<10 {
			return nil, "", errCLICredentials
		}
		var value struct {
			OAuth struct {
				AccessToken string `json:"accessToken"`
				ExpiresAt   int64  `json:"expiresAt"`
			} `json:"claudeAiOauth"`
		}
		if json.Unmarshal(raw, &value) != nil || !validCLIToken(value.OAuth.AccessToken) {
			return nil, "", errCLICredentials
		}
		if value.OAuth.ExpiresAt <= time.Now().Add(46*time.Minute).UnixMilli() {
			return nil, "", errClaudeAuthRenewal
		}
		h.Write([]byte(value.OAuth.AccessToken))
		env = []string{"CLAUDE_CODE_OAUTH_TOKEN=" + value.OAuth.AccessToken, "DISABLE_AUTOUPDATER=1", "CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC=1"}
	case "cursor":
		access, err := lookup(ctx, "cursor-access-token", "cursor-user")
		if err != nil || !validCLIToken(string(access)) {
			return nil, "", errCLICredentials
		}
		refresh, err := lookup(ctx, "cursor-refresh-token", "cursor-user")
		if err != nil || !validCLIToken(string(refresh)) {
			return nil, "", errCLICredentials
		}
		h.Write(access)
		h.Write([]byte{0})
		h.Write(refresh)
		payload, err := json.Marshal(struct {
			Access  string `json:"accessToken"`
			Refresh string `json:"refreshToken"`
		}{string(access), string(refresh)})
		if err != nil {
			return nil, "", errCLICredentials
		}
		dir := filepath.Join(home, ".cursor")
		if os.Mkdir(dir, 0700) != nil {
			return nil, "", errCLICredentials
		}
		f, err := os.OpenFile(filepath.Join(dir, "auth.json"), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
		if err != nil {
			return nil, "", errCLICredentials
		}
		_, writeErr := f.Write(payload)
		closeErr := f.Close()
		if writeErr != nil || closeErr != nil {
			return nil, "", errCLICredentials
		}
		env = []string{"AGENT_CLI_CREDENTIAL_STORE=file", "DIRENV_DISABLE=1"}
	default:
		return nil, "", errCLICredentials
	}
	return env, hex.EncodeToString(h.Sum(nil)), nil
}

func validCLIToken(token string) bool {
	return len(token) > 0 && len(token) <= 8<<10 && !strings.ContainsAny(token, "\x00\r\n\t ")
}

// vettedCLIEnvironment reuses the supervisor's fresh-home/temp policy, then
// authenticates credential bytes against the exact qualified binding before
// any launch. Failures destroy only the newly-created private environment.
func vettedCLIEnvironment(ctx context.Context, provider, expectedDigest string, lookup cliSecretLookup) ([]string, string, func(), error) {
	if len(expectedDigest) != 64 {
		return nil, "", func() {}, errCLICredentials
	}
	env, tmp, cleanup, err := vettedEnvironment("")
	if err != nil {
		return nil, "", func() {}, err
	}
	var home string
	for index, value := range env {
		if strings.HasPrefix(value, "HOME=") {
			home, err = filepath.EvalSymlinks(strings.TrimPrefix(value, "HOME="))
			if err != nil {
				cleanup()
				return nil, "", func() {}, errCLICredentials
			}
			env[index] = "HOME=" + home
		}
	}
	extra, digest, err := prepareCLICredentials(ctx, provider, home, lookup)
	if err != nil || digest != expectedDigest {
		cleanup()
		return nil, "", func() {}, errCLICredentials
	}
	return append(env, extra...), tmp, cleanup, nil
}
