package proxy

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
)

// Normal shim success emits only proxy-owned summaries. An unusual non-200
// successful response is a passthrough and must bind any opaque state first.
func (h *ProxyHandler) writeDurableShimPassthrough(w http.ResponseWriter, r *http.Request, upstreamCtx context.Context, resp *http.Response) bool {
	if h.stateBindings == nil || h.stateBindings.durable == nil || resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return false
	}
	info, ok := explicitRouteResponseInfoFromResponse(resp)
	if !ok {
		return false
	}
	var err error
	if resp.StatusCode == http.StatusNoContent || resp.StatusCode == http.StatusResetContent {
		// These successes cannot carry content, but their headers may still
		// expose provider state. Commit that proof before writing any headers.
		if err = h.bindExplicitResponseHeaders(info, resp.Header); err == nil {
			copyPassthroughHeaders(w.Header(), resp.Header)
			w.WriteHeader(resp.StatusCode)
			return true
		}
		err = newResponseBodyWriteError(resp, err, false, true, false)
	} else {
		err = writeExplicitResponsesResponse(r.Context(), h, w, resp, info, nil, "")
	}
	if err != nil {
		if !h.handleResponseBodyWriteError(w, r, upstreamCtx, "responses_shim", err) {
			writeOpenAIError(w, http.StatusBadGateway, "failed to validate upstream response state", "server_error")
		}
	}
	return true
}

// Classify only local durable-store faults. Missing or mismatched ownership
// evidence remains a separate request rejection, never a safety verdict.
func durableStateFailureDetails(err error) (message, code string, ok bool) {
	if errors.Is(err, errDurableStateCapacity) {
		return errDurableStateCapacity.Error(), "state_binding_capacity_exceeded", true
	}
	for _, sentinel := range []error{errDurableStateIO, errDurableStateClosed, errDurableStateCorrupt} {
		if errors.Is(err, sentinel) {
			return "local provider-state storage is unavailable; unrecorded state was withheld; operator recovery is required", "state_binding_storage_unavailable", true
		}
	}
	return "", "", false
}

func writeDurableStateFailure(w http.ResponseWriter, err error) bool {
	message, code, ok := durableStateFailureDetails(err)
	if !ok {
		return false
	}
	writeOpenAIErrorWithDetails(w, http.StatusServiceUnavailable, message, "server_error", "", code)
	return true
}

// A binding reader yields only complete, durably recorded events. On failure
// the unrecorded frame is withheld, so a bounded error can terminate the stream
// without leaking its state or claiming the upstream failed or completed.
func writeDurableStateStreamFailure(w io.Writer, err error) bool {
	message, code, ok := durableStateFailureDetails(err)
	if !ok {
		return false
	}
	data, _ := json.Marshal(map[string]any{
		"type":  "error",
		"error": map[string]string{"type": "server_error", "code": code, "message": message},
	})
	_, _ = io.WriteString(w, "event: error\ndata: "+string(data)+"\n\n")
	return true
}
