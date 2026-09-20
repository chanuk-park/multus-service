package agent

import (
	"context"
	"fmt"
	"os"
	"strings"

	"google.golang.org/grpc/credentials"
)

// tokenCreds attaches a projected ServiceAccount token as a bearer credential.
//
// The token file is re-read on every use, so projected-token rotation is picked
// up without reconnecting -- the kubelet refreshes the file in place, and the
// next request carries the new token. RequireTransportSecurity is true so a
// bearer token is never sent over a plaintext channel, where a path attacker
// could lift and replay it.
type tokenCreds struct {
	path       string
	requireTLS bool
}

func (c tokenCreds) GetRequestMetadata(_ context.Context, _ ...string) (map[string]string, error) {
	b, err := os.ReadFile(c.path)
	if err != nil {
		return nil, fmt.Errorf("read health token %s: %w", c.path, err)
	}
	return map[string]string{"authorization": "Bearer " + strings.TrimSpace(string(b))}, nil
}

func (c tokenCreds) RequireTransportSecurity() bool { return c.requireTLS }

// transportCreds returns the dial security for the health stream: TLS trusting
// the controller CA when caPath is set, plaintext otherwise (tests only).
func transportCreds(caPath, serverName string) (credentials.TransportCredentials, error) {
	if caPath == "" {
		return nil, nil
	}
	return credentials.NewClientTLSFromFile(caPath, serverName)
}
