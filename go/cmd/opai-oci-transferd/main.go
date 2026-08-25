// opai-oci-transferd is the private Unix-socket transfer engine.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"regexp"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/regclient/regclient"
	"github.com/regclient/regclient/config"
	"github.com/regclient/regclient/types"
	"github.com/regclient/regclient/types/manifest"
	"github.com/regclient/regclient/types/platform"
	"github.com/regclient/regclient/types/ref"
)

const protocolVersion = 1
const maxBody = 2 << 20
const maxError = 1000

var version = "dev"
var urlPattern = regexp.MustCompile(`(?i)https?://[^\s]+`)
var authPattern = regexp.MustCompile(`(?i)(Bearer|Basic)\s+[^\s]+`)
var secretPattern = regexp.MustCompile(`(?i)(password|passwd|token|secret|api[_-]?key|access[_-]?key)\s*[=:]\s*[^\s,;]+`)

type stringsFlag []string

func (s *stringsFlag) String() string     { return strings.Join(*s, ",") }
func (s *stringsFlag) Set(v string) error { *s = append(*s, v); return nil }

type credentials struct {
	Username  string  `json:"username"`
	Password  string  `json:"password"`
	ExpiresAt *string `json:"expires_at,omitempty"`
}
type descriptor struct {
	Digest    string `json:"digest"`
	MediaType string `json:"media_type"`
	Size      *int64 `json:"size"`
}
type manifestRecord struct {
	Digest        string  `json:"digest"`
	MediaType     string  `json:"media_type"`
	Size          int64   `json:"size"`
	Kind          string  `json:"kind"`
	SubjectDigest *string `json:"subject_digest,omitempty"`
	Platform      *string `json:"platform,omitempty"`
}
type planRequest struct {
	ProtocolVersion        int          `json:"protocol_version"`
	Source                 string       `json:"source"`
	Destination            string       `json:"destination"`
	Platforms              []string     `json:"platforms"`
	CopyReferrers          bool         `json:"copy_referrers"`
	CopyDigestTags         bool         `json:"copy_digest_tags"`
	ReplacementPolicy      string       `json:"replacement_policy"`
	SourceCredentials      *credentials `json:"source_credentials"`
	DestinationCredentials *credentials `json:"destination_credentials"`
}
type planResponse struct {
	ProtocolVersion int              `json:"protocol_version"`
	ResolvedSource  string           `json:"resolved_source"`
	RootDigest      string           `json:"root_digest"`
	RootMediaType   string           `json:"root_media_type"`
	RootSize        int64            `json:"root_size"`
	Manifests       []manifestRecord `json:"manifests"`
	Blobs           []descriptor     `json:"blobs"`
}
type snapshot struct {
	Source            string           `json:"source"`
	ResolvedSource    string           `json:"resolved_source"`
	Destination       string           `json:"destination"`
	RootDigest        string           `json:"root_digest"`
	RootMediaType     string           `json:"root_media_type"`
	RootSize          int64            `json:"root_size"`
	Platforms         []string         `json:"platforms"`
	Manifests         []manifestRecord `json:"manifests"`
	Blobs             []descriptor     `json:"blobs"`
	Digest            string           `json:"digest"`
	CopyReferrers     bool             `json:"copy_referrers"`
	CopyDigestTags    bool             `json:"copy_digest_tags"`
	ReplacementPolicy string           `json:"replacement_policy"`
}
type copyRequest struct {
	ProtocolVersion        int          `json:"protocol_version"`
	Snapshot               snapshot     `json:"snapshot"`
	SourceCredentials      *credentials `json:"source_credentials"`
	DestinationCredentials *credentials `json:"destination_credentials"`
}
type event map[string]any

type server struct {
	mu         sync.Mutex
	operations map[string]context.CancelFunc
	maximum    int
	requests   int
	insecure   map[string]bool
	certs      map[string]string
}

func main() {
	var socket string
	var insecure, cas stringsFlag
	maximum := flag.Int("max-operations", 1, "maximum operations")
	requests := flag.Int("requests-per-registry", 3, "request concurrency")
	flag.StringVar(&socket, "socket", "", "Unix socket")
	flag.Var(&insecure, "insecure-registry", "HTTP registry")
	flag.Var(&cas, "registry-ca", "registry=CA file")
	flag.Parse()
	if socket == "" || *maximum < 1 || *requests < 1 {
		fmt.Fprintln(os.Stderr, "invalid configuration")
		os.Exit(2)
	}
	certs := map[string]string{}
	for _, item := range cas {
		host, path, ok := strings.Cut(item, "=")
		if !ok || host == "" || path == "" {
			fmt.Fprintln(os.Stderr, "invalid registry CA configuration")
			os.Exit(2)
		}
		pem, readErr := os.ReadFile(path)
		if readErr != nil {
			fmt.Fprintln(os.Stderr, "unable to read registry CA")
			os.Exit(2)
		}
		certs[strings.ToLower(host)] = string(pem)
	}
	_ = os.Remove(socket)
	ln, err := net.Listen("unix", socket)
	if err != nil {
		panic(err)
	}
	defer ln.Close()
	_ = os.Chmod(socket, 0600)
	s := &server{operations: map[string]context.CancelFunc{}, maximum: *maximum, requests: *requests, insecure: map[string]bool{}, certs: certs}
	for _, h := range insecure {
		s.insecure[strings.ToLower(h)] = true
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/health", s.health)
	mux.HandleFunc("POST /v1/plan", s.plan)
	mux.HandleFunc("POST /v1/copies/{id}", s.copy)
	mux.HandleFunc("DELETE /v1/copies/{id}", s.cancel)
	httpServer := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 30 * time.Second, WriteTimeout: 0, IdleTimeout: 60 * time.Second}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		c, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = httpServer.Shutdown(c)
	}()
	if err = httpServer.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
		panic(err)
	}
}

func (s *server) health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, 200, event{"ready": true, "protocol_version": protocolVersion, "service_version": version, "regclient_version": "v0.11.5"})
}
func decode(w http.ResponseWriter, r *http.Request, out any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, maxBody)
	d := json.NewDecoder(r.Body)
	d.DisallowUnknownFields()
	if d.Decode(out) != nil {
		writeJSON(w, 400, event{"code": "protocol_error", "message": "invalid request"})
		return false
	}
	return true
}
func hosts(source, destination ref.Ref, src, dst *credentials, requests int, insecure map[string]bool, certs map[string]string) []config.Host {
	result := []config.Host{}
	add := func(r ref.Ref, c *credentials) {
		h := config.Host{Name: r.Registry, ReqConcurrent: int64(requests)}
		if insecure[strings.ToLower(r.Registry)] {
			h.TLS = config.TLSDisabled
		}
		h.RegCert = certs[strings.ToLower(r.Registry)]
		if c != nil {
			h.User = c.Username
			h.Pass = c.Password
		}
		result = append(result, h)
	}
	add(source, src)
	if destination.Registry != source.Registry || dst != nil {
		add(destination, dst)
	}
	return result
}

func (s *server) plan(w http.ResponseWriter, r *http.Request) {
	var q planRequest
	if !decode(w, r, &q) {
		return
	}
	if q.ProtocolVersion != 1 {
		writeJSON(w, 400, event{"code": "protocol_error", "message": "unsupported protocol"})
		return
	}
	src, e := ref.New(q.Source)
	if e != nil || src.Registry == "" || src.Repository == "" {
		writeJSON(w, 400, event{"code": "invalid_reference", "message": "invalid source"})
		return
	}
	dst, e := ref.New(q.Destination)
	if e != nil || dst.Registry == "" {
		writeJSON(w, 400, event{"code": "invalid_reference", "message": "invalid destination"})
		return
	}
	rc := regclient.New(regclient.WithConfigHost(hosts(src, dst, q.SourceCredentials, q.DestinationCredentials, s.requests, s.insecure, s.certs)...))
	defer rc.Close(r.Context(), src)
	m, e := rc.ManifestGet(r.Context(), src)
	if e != nil {
		serviceError(w, e)
		return
	}
	d := m.GetDescriptor()
	resolved := src.SetDigest(d.Digest.String())
	manifests := []manifestRecord{{Digest: d.Digest.String(), MediaType: d.MediaType, Size: d.Size, Kind: "root"}}
	blobs := map[string]descriptor{}
	selected := map[string]bool{}
	for _, value := range q.Platforms {
		p, parseErr := platform.Parse(value)
		if parseErr != nil {
			writeJSON(w, 400, event{"code": "unsupported_operation", "message": "invalid platform"})
			return
		}
		selected[p.String()] = true
	}
	seen := map[string]bool{}
	if e = walk(r.Context(), rc, resolved, m, &manifests, blobs, seen, selected, 0); e != nil {
		serviceError(w, e)
		return
	}
	if q.CopyReferrers {
		// Discover referrers for every selected manifest. ImageCopy applies the
		// same recursive policy when publishing them.
		base := append([]manifestRecord(nil), manifests...)
		for _, record := range base {
			subject := resolved.SetDigest(record.Digest)
			rlist, listErr := rc.ReferrerList(r.Context(), subject)
			if listErr != nil {
				serviceError(w, listErr)
				return
			}
			for _, rd := range rlist.Descriptors {
				rm, getErr := rc.ManifestGet(r.Context(), resolved.SetDigest(rd.Digest.String()), regclient.WithManifestDesc(rd))
				if getErr != nil {
					serviceError(w, getErr)
					return
				}
				subjectDigest := record.Digest
				manifests = append(manifests, manifestRecord{Digest: rd.Digest.String(), MediaType: rd.MediaType, Size: rd.Size, Kind: "referrer", SubjectDigest: &subjectDigest})
				if e = walk(r.Context(), rc, resolved, rm, &manifests, blobs, seen, map[string]bool{}, 1); e != nil {
					serviceError(w, e)
					return
				}
			}
		}
	}
	list := make([]descriptor, 0, len(blobs))
	for _, b := range blobs {
		list = append(list, b)
	}
	sort.Slice(list, func(i, j int) bool { return list[i].Digest < list[j].Digest })
	writeJSON(w, 200, planResponse{protocolVersion, resolved.CommonName(), d.Digest.String(), d.MediaType, d.Size, manifests, list})
}
func walk(ctx context.Context, rc *regclient.RegClient, rr ref.Ref, m manifest.Manifest, records *[]manifestRecord, blobs map[string]descriptor, seen map[string]bool, selected map[string]bool, depth int) error {
	if depth > 32 || len(*records) > 10000 {
		return fmt.Errorf("manifest graph limit exceeded")
	}
	key := m.GetDescriptor().Digest.String()
	if seen[key] {
		return nil
	}
	seen[key] = true
	if m.IsList() {
		idx, ok := m.(manifest.Indexer)
		if !ok {
			return fmt.Errorf("invalid index")
		}
		children, e := idx.GetManifestList()
		if e != nil {
			return e
		}
		for _, d := range children {
			var platformName *string
			if d.Platform != nil {
				value := d.Platform.String()
				platformName = &value
				if len(selected) > 0 && !selected[value] {
					continue
				}
			} else if len(selected) > 0 {
				continue
			}
			cm, e := rc.ManifestGet(ctx, rr.SetDigest(d.Digest.String()), regclient.WithManifestDesc(d))
			if e != nil {
				return e
			}
			*records = append(*records, manifestRecord{Digest: d.Digest.String(), MediaType: d.MediaType, Size: d.Size, Kind: "manifest", Platform: platformName})
			if e = walk(ctx, rc, rr, cm, records, blobs, seen, selected, depth+1); e != nil {
				return e
			}
		}
		return nil
	}
	im, ok := m.(manifest.Imager)
	if !ok {
		return fmt.Errorf("unsupported manifest")
	}
	c, e := im.GetConfig()
	if e != nil {
		return e
	}
	size := c.Size
	blobs[c.Digest.String()] = descriptor{c.Digest.String(), c.MediaType, &size}
	layers, e := im.GetLayers()
	if e != nil {
		return e
	}
	for _, d := range layers {
		n := d.Size
		blobs[d.Digest.String()] = descriptor{d.Digest.String(), d.MediaType, &n}
	}
	return nil
}

func (s *server) copy(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == "" || len(id) > 128 {
		writeJSON(w, 400, event{"code": "protocol_error", "message": "invalid operation id"})
		return
	}
	var q copyRequest
	if !decode(w, r, &q) {
		return
	}
	if q.ProtocolVersion != 1 {
		writeJSON(w, 400, event{"code": "protocol_error", "message": "unsupported protocol"})
		return
	}
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()
	s.mu.Lock()
	if len(s.operations) >= s.maximum {
		s.mu.Unlock()
		writeJSON(w, 429, event{"code": "rate_limited", "message": "too many operations", "retryable": true})
		return
	}
	if _, ok := s.operations[id]; ok {
		s.mu.Unlock()
		writeJSON(w, 409, event{"code": "protocol_error", "message": "duplicate operation id"})
		return
	}
	s.operations[id] = cancel
	s.mu.Unlock()
	defer func() { s.mu.Lock(); delete(s.operations, id); s.mu.Unlock() }()
	if q.Snapshot.ReplacementPolicy != "no_clobber" && q.Snapshot.ReplacementPolicy != "overwrite" {
		writeJSON(w, 400, event{"code": "protocol_error", "message": "invalid replacement policy"})
		return
	}
	src, e := ref.New(q.Snapshot.ResolvedSource)
	if e != nil {
		serviceError(w, e)
		return
	}
	dst, e := ref.New(q.Snapshot.Destination)
	if e != nil {
		serviceError(w, e)
		return
	}
	rc := regclient.New(regclient.WithConfigHost(hosts(src, dst, q.SourceCredentials, q.DestinationCredentials, s.requests, s.insecure, s.certs)...))
	defer rc.Close(ctx, src)
	if existing, headErr := rc.ManifestHead(ctx, dst, regclient.WithManifestRequireDigest()); headErr == nil {
		existingDigest := existing.GetDescriptor().Digest.String()
		if existingDigest != q.Snapshot.RootDigest && q.Snapshot.ReplacementPolicy == "no_clobber" {
			writeJSON(w, http.StatusConflict, event{"code": "destination_conflict", "message": "destination already references different content", "retryable": false})
			return
		}
	}
	w.Header().Set("Content-Type", "application/x-ndjson")
	w.WriteHeader(200)
	enc := json.NewEncoder(w)
	flush, _ := w.(http.Flusher)
	var emitMu sync.Mutex
	emit := func(v event) {
		emitMu.Lock()
		defer emitMu.Unlock()
		v["protocol_version"] = protocolVersion
		_ = enc.Encode(v)
		if flush != nil {
			flush.Flush()
		}
	}
	emit(event{"type": "phase", "phase": "copying"})
	opts := []regclient.ImageOpts{regclient.ImageWithForceRecursive(), regclient.ImageWithCallback(func(kind types.CallbackKind, instance string, state types.CallbackState, cur, total int64) {
		if kind == types.CallbackBlob {
			emit(event{"type": "progress", "digest": instance, "offset": cur, "network_bytes": cur, "disposition": callbackDisposition(state)})
		}
	})}
	if q.Snapshot.CopyReferrers {
		opts = append(opts, regclient.ImageWithReferrers())
	}
	if q.Snapshot.CopyDigestTags {
		opts = append(opts, regclient.ImageWithDigestTags())
	}
	if len(q.Snapshot.Platforms) == 1 {
		opts = append(opts, regclient.ImageWithPlatform(q.Snapshot.Platforms[0]))
	} else if len(q.Snapshot.Platforms) > 1 {
		opts = append(opts, regclient.ImageWithPlatforms(q.Snapshot.Platforms))
	}
	if e = rc.ImageCopy(ctx, src, dst, opts...); e != nil {
		emit(classify(e))
		return
	}
	emit(event{"type": "phase", "phase": "verifying"})
	m, e := rc.ManifestHead(ctx, dst, regclient.WithManifestRequireDigest())
	if e != nil {
		emit(classify(e))
		return
	}
	if m.GetDescriptor().Digest.String() != q.Snapshot.RootDigest {
		emit(event{"type": "failed", "code": "digest_mismatch", "message": "destination digest mismatch", "retryable": false})
		return
	}
	emit(event{"type": "completed", "digest": q.Snapshot.RootDigest})
}
func callbackDisposition(state types.CallbackState) string {
	if state == types.CallbackSkipped {
		return "reused"
	}
	if state == types.CallbackFinished {
		return "completed"
	}
	return "transferring"
}
func (s *server) cancel(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	cancel, ok := s.operations[r.PathValue("id")]
	s.mu.Unlock()
	if !ok {
		writeJSON(w, 404, event{"code": "not_found", "message": "operation not found"})
		return
	}
	cancel()
	writeJSON(w, 202, event{"cancelled": true})
}
func sanitizeError(err error) string {
	message := err.Error()
	message = urlPattern.ReplaceAllString(message, "[URL REDACTED]")
	message = authPattern.ReplaceAllString(message, "[AUTH REDACTED]")
	message = secretPattern.ReplaceAllString(message, "$1=[REDACTED]")
	if len(message) > maxError {
		message = message[:maxError]
	}
	return message
}
func classify(err error) event {
	msg := fmt.Sprintf("registry operation failed (%T): %s", err, sanitizeError(err))
	code := "transfer_failed"
	retry := false
	if errors.Is(err, context.Canceled) {
		code = "cancelled"
	} else if errors.Is(err, context.DeadlineExceeded) {
		code = "timeout"
		retry = true
	} else {
		lower := strings.ToLower(err.Error())
		switch {
		case strings.Contains(lower, "401") || strings.Contains(lower, "authentication"):
			code = "authentication_failed"
		case strings.Contains(lower, "403") || strings.Contains(lower, "denied"):
			code = "authorization_denied"
		case strings.Contains(lower, "429"):
			code = "rate_limited"
			retry = true
		case strings.Contains(lower, "timeout"):
			code = "timeout"
			retry = true
		case strings.Contains(lower, "digest"):
			code = "digest_mismatch"
		case strings.Contains(lower, "connection") || strings.Contains(lower, "temporary"):
			code = "network_error"
			retry = true
		}
	}
	return event{"type": "failed", "code": code, "message": msg, "retryable": retry}
}
func serviceError(w http.ResponseWriter, e error) {
	v := classify(e)
	delete(v, "type")
	writeJSON(w, 502, v)
}
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	b, _ := json.Marshal(v)
	w.Header().Set("Content-Length", fmt.Sprint(len(b)))
	w.WriteHeader(status)
	_, _ = w.Write(b)
}
