// Package api defines the versioned local CLI/daemon wire contract.
package api

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/nysa-company/sf/internal/domain"
)

const (
	Version         = "sf.local/v1"
	MaxMessageBytes = 1 << 20
)

type Request struct {
	Version       string          `json:"version"`
	RequestID     string          `json:"request_id"`
	Method        string          `json:"method"`
	Ticket        string          `json:"ticket,omitempty"`
	OperatorLabel string          `json:"operator_label,omitempty"`
	Parameters    json.RawMessage `json:"parameters,omitempty"`
}

type Mutation struct {
	Attempted bool   `json:"attempted"`
	Kind      string `json:"kind,omitempty"`
	Identity  string `json:"identity,omitempty"`
	Observed  bool   `json:"observed,omitempty"`
}

type Error struct {
	Code      string            `json:"code"`
	Message   string            `json:"message"`
	Retryable bool              `json:"retryable"`
	Details   map[string]string `json:"details,omitempty"`
}

type Response struct {
	Version    string             `json:"version"`
	RequestID  string             `json:"request_id"`
	OK         bool               `json:"ok"`
	Mutation   Mutation           `json:"mutation"`
	Data       json.RawMessage    `json:"data,omitempty"`
	Error      *Error             `json:"error,omitempty"`
	NextAction *domain.NextAction `json:"next_action,omitempty"`
}

func (request Request) Validate() error {
	if request.Version != Version {
		return fmt.Errorf("unsupported protocol version %q", request.Version)
	}
	if request.RequestID == "" {
		return fmt.Errorf("request_id is required")
	}
	if request.Method == "" {
		return fmt.Errorf("method is required")
	}
	return nil
}

func (response Response) Validate() error {
	if response.Version != Version {
		return fmt.Errorf("unsupported protocol version %q", response.Version)
	}
	if response.RequestID == "" {
		return fmt.Errorf("request_id is required")
	}
	if response.OK && response.Error != nil {
		return fmt.Errorf("successful response contains an error")
	}
	if !response.OK && response.Error == nil {
		return fmt.Errorf("failed response requires an error")
	}
	if !response.OK && response.NextAction == nil {
		return fmt.Errorf("failed response requires one executable next action")
	}
	if response.NextAction != nil && (response.NextAction.Code == "" || len(response.NextAction.Argv) == 0 || response.NextAction.Argv[0] == "") {
		return fmt.Errorf("next action requires executable argv")
	}
	return nil
}

func DecodeRequest(reader io.Reader) (Request, error) {
	var request Request
	if err := decode(reader, &request); err != nil {
		return Request{}, err
	}
	if err := request.Validate(); err != nil {
		return Request{}, err
	}
	return request, nil
}

func DecodeResponse(reader io.Reader) (Response, error) {
	var response Response
	if err := decode(reader, &response); err != nil {
		return Response{}, err
	}
	if err := response.Validate(); err != nil {
		return Response{}, err
	}
	return response, nil
}

func Encode(writer io.Writer, value any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("encode protocol message: %w", err)
	}
	if len(data) > MaxMessageBytes {
		return fmt.Errorf("protocol message exceeds %d bytes", MaxMessageBytes)
	}
	if _, err := writer.Write(append(data, '\n')); err != nil {
		return fmt.Errorf("write protocol message: %w", err)
	}
	return nil
}

func decode(reader io.Reader, destination any) error {
	data, err := io.ReadAll(io.LimitReader(reader, MaxMessageBytes+1))
	if err != nil {
		return fmt.Errorf("read protocol message: %w", err)
	}
	if len(data) > MaxMessageBytes {
		return fmt.Errorf("protocol message exceeds %d bytes", MaxMessageBytes)
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return fmt.Errorf("decode protocol message: %w", err)
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return fmt.Errorf("protocol message contains multiple values")
		}
		return fmt.Errorf("decode trailing protocol data: %w", err)
	}
	return nil
}
