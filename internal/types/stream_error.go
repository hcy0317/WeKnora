package types

import "encoding/json"

const (
	StreamErrorProviderRequestID = "provider_request_id"
	StreamErrorLastSSEEventType  = "last_sse_event_type"
	StreamErrorCode              = "error_code"
	StreamErrorType              = "error_type"
	StreamErrorOutputStarted     = "output_started"
	StreamErrorUsageObserved     = "usage_observed"
	StreamErrorHTTPStatus        = "http_status"
)

// StreamErrorDetails is the structured contract carried by terminal stream
// error events. It contains only safe correlation and delivery-state facts.
type StreamErrorDetails struct {
	ProviderRequestID string
	LastSSEEventType  string
	Code              string
	Type              string
	OutputStarted     bool
	UsageObserved     bool
	HTTPStatus        int
}

func (d StreamErrorDetails) Data() map[string]interface{} {
	data := map[string]interface{}{
		StreamErrorOutputStarted: d.OutputStarted,
		StreamErrorUsageObserved: d.UsageObserved,
	}
	if d.ProviderRequestID != "" {
		data[StreamErrorProviderRequestID] = d.ProviderRequestID
	}
	if d.LastSSEEventType != "" {
		data[StreamErrorLastSSEEventType] = d.LastSSEEventType
	}
	if d.Code != "" {
		data[StreamErrorCode] = d.Code
	}
	if d.Type != "" {
		data[StreamErrorType] = d.Type
	}
	if d.HTTPStatus > 0 {
		data[StreamErrorHTTPStatus] = d.HTTPStatus
	}
	return data
}

func StreamErrorDetailsFromData(data map[string]interface{}) (StreamErrorDetails, bool) {
	if len(data) == 0 {
		return StreamErrorDetails{}, false
	}
	details := StreamErrorDetails{
		ProviderRequestID: streamErrorString(data[StreamErrorProviderRequestID]),
		LastSSEEventType:  streamErrorString(data[StreamErrorLastSSEEventType]),
		Code:              streamErrorString(data[StreamErrorCode]),
		Type:              streamErrorString(data[StreamErrorType]),
		OutputStarted:     streamErrorBool(data[StreamErrorOutputStarted]),
		UsageObserved:     streamErrorBool(data[StreamErrorUsageObserved]),
		HTTPStatus:        streamErrorInt(data[StreamErrorHTTPStatus]),
	}
	_, hasOutput := data[StreamErrorOutputStarted]
	_, hasUsage := data[StreamErrorUsageObserved]
	known := details.ProviderRequestID != "" || details.LastSSEEventType != "" ||
		details.Code != "" || details.Type != "" || details.HTTPStatus > 0 || hasOutput || hasUsage
	return details, known
}

func (d StreamErrorDetails) PossiblyBilled() bool {
	return d.OutputStarted || d.UsageObserved
}

func streamErrorString(value interface{}) string {
	text, _ := value.(string)
	return text
}

func streamErrorBool(value interface{}) bool {
	result, _ := value.(bool)
	return result
}

func streamErrorInt(value interface{}) int {
	switch number := value.(type) {
	case int:
		return number
	case int64:
		return int(number)
	case float64:
		return int(number)
	case json.Number:
		value, _ := number.Int64()
		return int(value)
	default:
		return 0
	}
}
