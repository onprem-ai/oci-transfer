package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/regclient/regclient/config"
	"github.com/regclient/regclient/types"
	"github.com/regclient/regclient/types/ref"
)

func TestCallbackDisposition(t *testing.T) {
	t.Parallel()
	if callbackDisposition(types.CallbackSkipped) != "reused" || callbackDisposition(types.CallbackFinished) != "completed" || callbackDisposition(types.CallbackActive) != "transferring" {
		t.Fatal("unexpected callback mapping")
	}
}

func TestClassify(t *testing.T) {
	t.Parallel()
	cases := []struct {
		err   error
		code  string
		retry bool
	}{
		{context.Canceled, "cancelled", false},
		{context.DeadlineExceeded, "timeout", true},
		{assertError("HTTP 401"), "authentication_failed", false},
		{assertError("denied"), "authorization_denied", false},
		{assertError("HTTP 429"), "rate_limited", true},
		{assertError("connection reset"), "network_error", true},
		{assertError("digest invalid"), "digest_mismatch", false},
	}
	for _, tc := range cases {
		got := classify(tc.err)
		if got["code"] != tc.code || got["retryable"] != tc.retry {
			t.Fatalf("classify(%v)=%v", tc.err, got)
		}
	}
}

type assertError string

func (e assertError) Error() string { return string(e) }

func TestHosts(t *testing.T) {
	t.Parallel()
	src, _ := ref.New("source.example/team/app:v1")
	dst, _ := ref.New("target.example/team/app:v1")
	got := hosts(src, dst, &credentials{Username: "u", Password: "p"}, nil, 7, map[string]bool{"source.example": true}, map[string]string{"target.example": "pem"})
	if len(got) != 2 || got[0].TLS != config.TLSDisabled || got[0].ReqConcurrent != 7 || got[1].RegCert != "pem" {
		t.Fatalf("unexpected hosts: %#v", got)
	}
}

func TestHealthAndDecode(t *testing.T) {
	t.Parallel()
	s := &server{}
	r := httptest.NewRequest(http.MethodGet, "/v1/health", nil)
	w := httptest.NewRecorder()
	s.health(w, r)
	if w.Code != 200 || !strings.Contains(w.Body.String(), `"protocol_version":1`) {
		t.Fatalf("unexpected health: %d %s", w.Code, w.Body.String())
	}
	bad := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"unknown":1}`))
	out := planRequest{}
	w = httptest.NewRecorder()
	if decode(w, bad, &out) || w.Code != 400 {
		t.Fatal("invalid JSON accepted")
	}
}

func TestCancelUnknown(t *testing.T) {
	t.Parallel()
	s := &server{operations: map[string]context.CancelFunc{}}
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodDelete, "/v1/copies/nope", nil)
	r.SetPathValue("id", "nope")
	s.cancel(w, r)
	if w.Code != 404 {
		t.Fatalf("status %d", w.Code)
	}
}
