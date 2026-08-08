package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/Rhizome-Project/rhizome-runtime/internal/auth"
)

func runAuth(args []string) error {
	if len(args) < 1 {
		printAuthUsage(os.Stderr)
		return errors.New("missing auth subcommand")
	}

	switch args[0] {
	case "login":
		return runAuthLogin(args[1:])
	default:
		printAuthUsage(os.Stderr)
		return fmt.Errorf("unknown auth subcommand: %s", args[0])
	}
}

func runAuthLogin(args []string) error {
	fs := flag.NewFlagSet("auth login", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	noSave := fs.Bool("no-save", false, "Do not save credentials to disk")
	printAPIKey := fs.Bool("print-api-key", false, "Print the acquired API key to stdout (requires --no-save; sensitive)")
	listenAddr := fs.String("listen-addr", ":1455", "Override callback server listen address")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return errors.New("auth login does not accept positional arguments")
	}
	if err := validateAuthLoginOutputMode(*noSave, *printAPIKey); err != nil {
		return err
	}
	credentialPath := ""
	if !*noSave {
		credentialPath = auth.DefaultAuthFilePath()
		if credentialPath == "" {
			return errors.New("could not determine the credential path; rerun with --no-save --print-api-key only if you intend to handle the API key manually")
		}
	}

	cfg := auth.DefaultOAuthConfig()
	cfg.ListenAddr = *listenAddr

	// Update redirect URI if listen address changed from default
	if *listenAddr != ":1455" {
		// Extract port from listen address
		addr := *listenAddr
		if addr[0] == ':' {
			addr = "localhost" + addr
		}
		cfg.RedirectURI = fmt.Sprintf("http://%s/auth/callback", addr)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	result, err := auth.RunOAuthFlow(ctx, cfg)
	if err != nil {
		return fmt.Errorf("OAuth flow failed: %w", err)
	}
	return completeAuthLogin(result, *noSave, *printAPIKey, credentialPath)
}

func completeAuthLogin(result *auth.OAuthResult, noSave, printAPIKey bool, credentialPath string) error {
	if result == nil || result.APIKey == "" {
		return errors.New("OAuth flow returned an empty API key")
	}
	if err := validateAuthLoginOutputMode(noSave, printAPIKey); err != nil {
		return err
	}

	if noSave {
		fmt.Fprintln(os.Stdout, result.APIKey)
		fmt.Fprintln(os.Stderr, "warning: API key written to stdout; keep terminal output and redirected files private")
	} else {
		if credentialPath == "" {
			return errors.New("credential path is required for a saved login")
		}
		creds := auth.Credentials{
			APIKey:     result.APIKey,
			Provider:   "openai",
			Email:      result.Email,
			ObtainedAt: time.Now().UTC().Format(time.RFC3339),
		}
		if err := auth.SaveCredentials(credentialPath, creds); err != nil {
			return fmt.Errorf("saving credentials: %w", err)
		}
		fmt.Fprintf(os.Stderr, "Credentials saved to %s\n", credentialPath)
	}

	fmt.Fprintf(os.Stderr, "Logged in via openai")
	if result.Email != "" {
		fmt.Fprintf(os.Stderr, " as %s", result.Email)
	}
	fmt.Fprintln(os.Stderr)

	return nil
}

func validateAuthLoginOutputMode(noSave, printAPIKey bool) error {
	if noSave && !printAPIKey {
		return errors.New("--no-save requires --print-api-key so the acquired credential is not silently discarded")
	}
	if printAPIKey && !noSave {
		return errors.New("--print-api-key requires --no-save; saved logins never print credentials")
	}
	return nil
}

func printAuthUsage(out *os.File) {
	fmt.Fprintln(out, "Auth commands:")
	fmt.Fprintln(out, "  rhizome auth login [--listen-addr :1455]")
	fmt.Fprintln(out, "  rhizome auth login --no-save --print-api-key [--listen-addr :1455]")
}
