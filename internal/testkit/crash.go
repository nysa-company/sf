package testkit

import (
	"errors"
	"fmt"
	"sync"
)

// CrashPoint names every durable/effect boundary in the approved crash
// matrix. Keep these strings stable: they are used by subprocess tests and
// crash reports.
type CrashPoint string

const (
	BeforePlanRecord              CrashPoint = "before_plan_record"
	AfterPlanRecord               CrashPoint = "after_plan_record"
	AfterEffectClaim              CrashPoint = "after_effect_claim"
	BeforeExternalCall            CrashPoint = "before_external_call"
	AfterRemoteMutationBeforeResp CrashPoint = "after_remote_mutation_before_response"
	AfterResponseBeforeConfirm    CrashPoint = "after_response_before_confirmation"
	AfterNewLeaderBeforeOldResp   CrashPoint = "after_new_leader_before_old_response"
	AfterConfirmation             CrashPoint = "after_confirmation"
	BeforePhaseTransition         CrashPoint = "before_phase_transition"
	AfterPhaseTransition          CrashPoint = "after_phase_transition"
)

var ErrInjectedCrash = errors.New("testkit: injected crash")

// CrashController is a one-shot named crash injector. Arm one or more points,
// call Hit from production boundaries, and assert Hits after restart. The
// controller returns an error instead of terminating the test process; a
// subprocess can translate that error to an exit status when needed.
type CrashController struct {
	mu    sync.Mutex
	armed map[CrashPoint]bool
	hits  []CrashPoint
}

func NewCrashController() *CrashController {
	return &CrashController{armed: make(map[CrashPoint]bool)}
}

func (c *CrashController) Arm(points ...CrashPoint) {
	c.mu.Lock()
	for _, point := range points {
		c.armed[point] = true
	}
	c.mu.Unlock()
}

func (c *CrashController) Disarm(points ...CrashPoint) {
	c.mu.Lock()
	for _, point := range points {
		delete(c.armed, point)
	}
	c.mu.Unlock()
}

func (c *CrashController) Hit(point CrashPoint) error {
	c.mu.Lock()
	if !c.armed[point] {
		c.mu.Unlock()
		return nil
	}
	delete(c.armed, point)
	c.hits = append(c.hits, point)
	c.mu.Unlock()
	return fmt.Errorf("%w at %s", ErrInjectedCrash, point)
}

func (c *CrashController) Hits() []CrashPoint {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]CrashPoint(nil), c.hits...)
}

// ProcessScenario controls the intentionally hostile process behavior exposed
// by the fake-provider executable. These scenarios are opt-in and bounded by
// the caller's context.
type ProcessScenario string

const (
	ScenarioNormal     ProcessScenario = "normal"
	ScenarioIgnoreTERM ProcessScenario = "ignore-term"
	ScenarioSetsid     ProcessScenario = "setsid"
	ScenarioDoubleFork ProcessScenario = "double-fork"
)

func (s ProcessScenario) Valid() bool {
	switch s {
	case ScenarioNormal, ScenarioIgnoreTERM, ScenarioSetsid, ScenarioDoubleFork:
		return true
	default:
		return false
	}
}
