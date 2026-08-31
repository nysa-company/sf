// Package statemachine loads and evaluates the normative JSON transition
// contract. It contains no workflow-runtime or persistence policy.
package statemachine

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"

	"github.com/nysa-company/sf/docs/plans"
	"github.com/nysa-company/sf/internal/domain"
)

var (
	ErrNoTransition        = errors.New("no transition guard matched")
	ErrAmbiguousTransition = errors.New("multiple transition guards matched")
)

const (
	MaxSpecBytes   = 1 << 20
	ApprovedSHA256 = "079e322c950b30de950a526ad78659a4bc9a9ab194123e80cb5e19f891273f2c"
)

type StateDefinition struct {
	Kind          string `json:"kind"`
	ResumeAllowed bool   `json:"resume_allowed"`
}

type Transition struct {
	ID               string   `json:"id"`
	From             []string `json:"from"`
	Trigger          string   `json:"trigger"`
	Guards           []string `json:"guards"`
	To               string   `json:"to"`
	ResumeState      string   `json:"resume_state,omitempty"`
	PhaseDisposition string   `json:"phase_disposition"`
	AllowedEffects   []string `json:"allowed_effects"`
	Invalidates      []string `json:"invalidates"`
	NextActionPolicy string   `json:"next_action_policy,omitempty"`
}

type Spec struct {
	Schema           string                     `json:"schema"`
	Authority        string                     `json:"authority"`
	Default          string                     `json:"default"`
	Selection        string                     `json:"selection"`
	States           map[string]StateDefinition `json:"states"`
	InvalidationSets map[string][]string        `json:"invalidation_sets"`
	Transitions      []Transition               `json:"transitions"`
}

func Load(reader io.Reader) (Spec, error) {
	data, err := io.ReadAll(io.LimitReader(reader, MaxSpecBytes+1))
	if err != nil {
		return Spec{}, fmt.Errorf("read state-machine spec: %w", err)
	}
	if len(data) > MaxSpecBytes {
		return Spec{}, fmt.Errorf("state-machine spec exceeds %d bytes", MaxSpecBytes)
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var spec Spec
	if err := decoder.Decode(&spec); err != nil {
		return Spec{}, fmt.Errorf("decode state-machine spec: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return Spec{}, errors.New("state-machine spec contains multiple JSON values")
		}
		return Spec{}, fmt.Errorf("decode trailing state-machine data: %w", err)
	}
	if err := spec.Validate(); err != nil {
		return Spec{}, err
	}
	return spec, nil
}

// LoadApproved binds runtime authority to the reviewed normative artifact.
// A structurally valid but locally edited transition table is not accepted.
func LoadApproved(reader io.Reader) (Spec, error) {
	data, err := io.ReadAll(io.LimitReader(reader, MaxSpecBytes+1))
	if err != nil {
		return Spec{}, fmt.Errorf("read approved state-machine spec: %w", err)
	}
	if len(data) > MaxSpecBytes {
		return Spec{}, fmt.Errorf("state-machine spec exceeds %d bytes", MaxSpecBytes)
	}
	digest := sha256.Sum256(data)
	if hex.EncodeToString(digest[:]) != ApprovedSHA256 {
		return Spec{}, errors.New("state-machine artifact does not match the approved release digest")
	}
	return Load(bytes.NewReader(data))
}

// LoadEmbeddedApproved verifies the exact reviewed artifact compiled into the
// binary. Foreground daemons therefore have no checkout-relative dependency.
func LoadEmbeddedApproved() (Spec, error) {
	return LoadApproved(bytes.NewReader(plans.StateMachine()))
}

func (s Spec) Validate() error {
	if s.Schema != "sf.state-machine.plan/v1" {
		return fmt.Errorf("unsupported state-machine schema %q", s.Schema)
	}
	if s.Authority != "normative" {
		return fmt.Errorf("state-machine authority must be normative")
	}
	if strings.TrimSpace(s.Default) == "" || strings.TrimSpace(s.Selection) == "" {
		return fmt.Errorf("state-machine default and selection rules are required")
	}

	expected := make(map[string]struct{}, len(domain.AllStates()))
	for _, state := range domain.AllStates() {
		expected[string(state)] = struct{}{}
	}
	if len(s.States) != len(expected) {
		return fmt.Errorf("state-machine defines %d states; domain defines %d", len(s.States), len(expected))
	}
	for state := range expected {
		if _, ok := s.States[state]; !ok {
			return fmt.Errorf("state-machine is missing domain state %q", state)
		}
	}
	for state := range s.States {
		if _, ok := expected[state]; !ok {
			return fmt.Errorf("state-machine contains unknown state %q", state)
		}
	}

	ids := make(map[string]struct{}, len(s.Transitions))
	for index, transition := range s.Transitions {
		if transition.ID == "" || transition.Trigger == "" || transition.To == "" {
			return fmt.Errorf("transition %d requires id, trigger, and target", index)
		}
		if _, exists := ids[transition.ID]; exists {
			return fmt.Errorf("duplicate transition id %q", transition.ID)
		}
		ids[transition.ID] = struct{}{}
		if len(transition.From) == 0 {
			return fmt.Errorf("transition %q has no source state", transition.ID)
		}
		for _, from := range transition.From {
			if from != "none" {
				if _, ok := s.States[from]; !ok {
					return fmt.Errorf("transition %q has unknown source %q", transition.ID, from)
				}
			}
		}
		if !strings.HasPrefix(transition.To, "$") {
			if _, ok := s.States[transition.To]; !ok {
				return fmt.Errorf("transition %q has unknown target %q", transition.ID, transition.To)
			}
		}
		if transition.ResumeState != "" && !strings.HasPrefix(transition.ResumeState, "$") {
			if _, ok := s.States[transition.ResumeState]; !ok {
				return fmt.Errorf("transition %q has unknown resume state %q", transition.ID, transition.ResumeState)
			}
		}
	}
	return nil
}

// Select evaluates a state and trigger against an already authenticated set of
// guard results. Exactly one transition must match.
func (s Spec) Select(from, trigger string, guards map[string]bool) (Transition, error) {
	var matches []Transition
	for _, transition := range s.Transitions {
		if transition.Trigger != trigger || !contains(transition.From, from) {
			continue
		}
		matched := true
		for _, guard := range transition.Guards {
			if !guards[guard] {
				matched = false
				break
			}
		}
		if matched {
			matches = append(matches, transition)
		}
	}
	if len(matches) == 0 {
		return Transition{}, fmt.Errorf("%w: state=%s trigger=%s", ErrNoTransition, from, trigger)
	}
	if len(matches) > 1 {
		ids := make([]string, 0, len(matches))
		for _, match := range matches {
			ids = append(ids, match.ID)
		}
		sort.Strings(ids)
		return Transition{}, fmt.Errorf("%w: %s", ErrAmbiguousTransition, strings.Join(ids, ","))
	}
	return matches[0], nil
}

func ResolveTarget(target, from, resumeState, storedState string) (domain.State, error) {
	value := target
	switch target {
	case "$same", "$from":
		value = from
	case "$resume_state":
		value = resumeState
	case "$stored":
		value = storedState
	}
	state := domain.State(value)
	if !state.Valid() {
		return "", fmt.Errorf("resolved target %q is not a known state", value)
	}
	return state, nil
}

func contains(values []string, value string) bool {
	for _, candidate := range values {
		if candidate == value {
			return true
		}
	}
	return false
}
