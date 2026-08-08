package main

import (
	"fmt"
	"mime"
	"net"
	"net/http"
	"net/url"
	"strings"
)

// managerWebLoopbackRequestGate is the network-edge guard for the local manager.
// The application routes are also exercised directly by unit tests, so this guard
// is deliberately installed on the real http.Server after the listener address is
// known. It rejects non-local peers and DNS-rebinding Host values before a route can
// read or mutate manager state.
func managerWebLoopbackRequestGate(next http.Handler, listenerAddr net.Addr) http.Handler {
	listenerHost, listenerPort, err := net.SplitHostPort(listenerAddr.String())
	if err != nil || !isLoopbackManagerWebHostname(listenerHost) {
		return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			setManagerWebSecurityHeaders(w)
			http.Error(w, "manager web listener is not loopback", http.StatusServiceUnavailable)
		})
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		setManagerWebSecurityHeaders(w)

		if !isLoopbackManagerWebPeer(r.RemoteAddr) {
			http.Error(w, "forbidden", http.StatusForbidden)
			return
		}

		requestHost, requestPort, err := parseManagerWebHost(r.Host)
		if err != nil || !isLoopbackManagerWebHostname(requestHost) || !managerWebPortMatches(requestPort, listenerPort) {
			http.Error(w, "forbidden", http.StatusForbidden)
			return
		}

		if isManagerWebMutation(r.Method) {
			if !managerWebRequestHasSameOrigin(r, requestHost, requestPort) {
				http.Error(w, "forbidden", http.StatusForbidden)
				return
			}
			if strings.HasPrefix(r.URL.Path, "/api/") && managerWebMethodExpectsJSON(r.Method) && !managerWebRequestHasJSONContentType(r) {
				http.Error(w, "application/json content type required", http.StatusUnsupportedMediaType)
				return
			}
		}

		next.ServeHTTP(w, r)
	})
}

func setManagerWebSecurityHeaders(w http.ResponseWriter) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Content-Security-Policy", "default-src 'none'; base-uri 'none'; form-action 'self'; frame-ancestors 'none'; object-src 'none'; script-src 'unsafe-inline'; style-src 'unsafe-inline'; img-src 'self' data:; font-src 'self'; connect-src 'self'")
	w.Header().Set("Cross-Origin-Opener-Policy", "same-origin")
	w.Header().Set("Cross-Origin-Resource-Policy", "same-origin")
	w.Header().Set("Permissions-Policy", "camera=(), microphone=(), geolocation=(), payment=(), usb=()")
	w.Header().Set("Referrer-Policy", "no-referrer")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("X-Frame-Options", "DENY")
	w.Header().Set("X-Permitted-Cross-Domain-Policies", "none")
}

func isLoopbackManagerWebPeer(remoteAddr string) bool {
	host, _, err := net.SplitHostPort(strings.TrimSpace(remoteAddr))
	if err != nil {
		return false
	}
	ip := net.ParseIP(strings.TrimSpace(host))
	return ip != nil && ip.IsLoopback()
}

func parseManagerWebHost(raw string) (string, string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" || strings.ContainsAny(raw, "@/?#") {
		return "", "", fmt.Errorf("invalid host")
	}

	parsed, err := url.Parse("//" + raw)
	if err != nil || parsed.Host == "" || parsed.User != nil {
		return "", "", fmt.Errorf("invalid host")
	}
	host := strings.TrimSuffix(strings.ToLower(strings.TrimSpace(parsed.Hostname())), ".")
	if host == "" {
		return "", "", fmt.Errorf("invalid host")
	}
	return host, parsed.Port(), nil
}

func isLoopbackManagerWebHostname(host string) bool {
	host = strings.TrimSuffix(strings.ToLower(strings.TrimSpace(host)), ".")
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func managerWebPortMatches(requestPort, listenerPort string) bool {
	if listenerPort == "80" && requestPort == "" {
		return true
	}
	return requestPort != "" && requestPort == listenerPort
}

func isManagerWebMutation(method string) bool {
	switch method {
	case http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete:
		return true
	default:
		return false
	}
}

func managerWebMethodExpectsJSON(method string) bool {
	switch method {
	case http.MethodPost, http.MethodPut, http.MethodPatch:
		return true
	default:
		return false
	}
}

func managerWebRequestHasSameOrigin(r *http.Request, requestHost, requestPort string) bool {
	// Non-browser clients do not normally send Origin or Fetch Metadata headers.
	// They remain supported, but JSON mutation requests must still use the explicit
	// application/json media type so a cross-site HTML form cannot forge one.
	origin := strings.TrimSpace(r.Header.Get("Origin"))
	if origin == "" {
		return !strings.EqualFold(strings.TrimSpace(r.Header.Get("Sec-Fetch-Site")), "cross-site")
	}

	parsed, err := url.Parse(origin)
	if err != nil || parsed.Scheme != "http" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" || (parsed.Path != "" && parsed.Path != "/") {
		return false
	}
	originHost, originPort, err := parseManagerWebHost(parsed.Host)
	if err != nil {
		return false
	}
	return originHost == requestHost && originPort == requestPort
}

func managerWebRequestHasJSONContentType(r *http.Request) bool {
	mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	return err == nil && strings.EqualFold(mediaType, "application/json")
}
