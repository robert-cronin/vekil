package proxy

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
)

// Failure events can expose turn state inside root or nested error headers,
// including the headers later projected into websocket error frames. Inspect
// every raw representation, not just the winning overlay, before emitting it.
func durableResponsesErrorHeaderState(data []byte) ([]stateBindingToken, error) {
	var envelope struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(data, &envelope); err != nil {
		return nil, err
	}
	switch strings.TrimSpace(envelope.Type) {
	case "", "error", "response.failed":
	default:
		return nil, nil
	}
	type headerBlock struct {
		Headers map[string]json.RawMessage `json:"headers"`
	}
	var event struct {
		Headers  map[string]json.RawMessage `json:"headers"`
		Error    headerBlock                `json:"error"`
		Response struct {
			Error headerBlock `json:"error"`
		} `json:"response"`
	}
	if err := json.Unmarshal(data, &event); err != nil {
		return nil, err
	}
	var tokens []stateBindingToken
	for _, rawHeaders := range []map[string]json.RawMessage{event.Headers, event.Error.Headers, event.Response.Error.Headers} {
		headers := responsesStreamErrorHeaders(responsesWebSocketStreamError{Headers: rawHeaders})
		values, err := explicitResponseHeaderStateTokens(headers)
		if err != nil {
			return nil, err
		}
		tokens = append(tokens, values...)
	}
	return tokens, nil
}

// Bind only the final projection, using identity captured on its actual request.
// Native Chat/Messages bodies are not Responses state and are never parsed here.
func (h *ProxyHandler) bindDurableFinalHeaders(info explicitRouteResponseInfo, headers http.Header) error {
	if h == nil || h.stateBindings == nil || h.stateBindings.durable == nil {
		return nil
	}
	tokens, err := explicitResponseHeaderStateTokens(headers)
	if err != nil || len(tokens) == 0 {
		return err
	}
	if info.stateIdentity == [32]byte{} {
		return fmt.Errorf("final response is missing authenticated state ownership")
	}
	return h.bindExplicitStateTokens(info, tokens)
}

func (h *ProxyHandler) prepareDurableFinalResponseHeaders(resp *http.Response) error {
	info, ok := explicitRouteResponseInfoFromResponse(resp)
	if !ok {
		return nil // legacy or synthetic response, not an explicit provider result
	}
	if err := h.bindDurableFinalHeaders(info, resp.Header); err != nil {
		if resp.Body != nil {
			_ = resp.Body.Close()
		}
		return newResponseBodyWriteError(resp, err, false, true, false)
	}
	return nil
}

// Normal shim success emits only proxy-owned summaries. Every passthrough,
// including a final error, must bind any exposed opaque state first.
func (h *ProxyHandler) writeDurableShimPassthrough(w http.ResponseWriter, r *http.Request, upstreamCtx context.Context, resp *http.Response) bool {
	if h.stateBindings == nil || h.stateBindings.durable == nil {
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
