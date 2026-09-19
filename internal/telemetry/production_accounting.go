package telemetry

import (
	"context"
	"sort"
	"sync"
	"sync/atomic"
)

type operationContextKey struct{}

type operationOwner struct {
	operation string
	action    *ActionGuard
	state     *operationState
}

type operationState struct {
	mu       sync.Mutex
	action   *ActionGuard
	blocking map[blockingKey]uint64
	current  blockingKey
}

type blockingKey struct {
	phase  string
	reason string
}

type BlockingGuard struct {
	state    *operationState
	phase    string
	blocking string
	settled  atomic.Bool
}

var disabledBlockingGuard = &BlockingGuard{}

type ProductionAccounting struct {
	enabled      bool
	registry     *OperationRegistry
	actions      map[string]*ActionMetric
	dependencies map[string]map[string]*DependencyMetric
	waits        map[productionWaitKey]*WaitMetric
}

type productionWaitKey struct {
	operation string
	role      string
	throttle  string
}

var defaultProductionAccounting = NewProductionAccounting(true)

func DefaultProductionAccounting() *ProductionAccounting { return defaultProductionAccounting }

func NewProductionAccounting(enabled bool) *ProductionAccounting {
	registry := NewOperationRegistry(MaxMonitorOperations)
	accounting := &ProductionAccounting{
		enabled:      enabled,
		registry:     registry,
		actions:      make(map[string]*ActionMetric),
		dependencies: make(map[string]map[string]*DependencyMetric),
		waits:        make(map[productionWaitKey]*WaitMetric),
	}
	for operation := range monitorValues("operation") {
		roles := make(map[string]*DependencyMetric)
		for role := range monitorValues("role") {
			if validOperationRole(operation, role) {
				roles[role] = NewDependencyMetric(operation, role, enabled)
			}
		}
		if len(roles) != 0 {
			action := NewActionMetric(operation, MaxMonitorOperations, enabled)
			if enabled {
				action.registry = registry
			}
			accounting.actions[operation] = action
			accounting.dependencies[operation] = roles
		}
	}
	brokerResponse := productionWaitKey{operation: "key_management", role: "broker", throttle: "none"}
	accounting.waits[brokerResponse] = NewWaitMetric(brokerResponse.operation, brokerResponse.role, brokerResponse.throttle, MaxActiveWaits, enabled)
	humanConfirmation := productionWaitKey{operation: "key_management", role: "coordination", throttle: "none"}
	accounting.waits[humanConfirmation] = NewWaitMetric(humanConfirmation.operation, humanConfirmation.role, humanConfirmation.throttle, MaxActiveWaits, enabled)
	return accounting
}

func (accounting *ProductionAccounting) StartOperation(ctx context.Context, operation, phase, parentID string) (context.Context, *ActionGuard) {
	if accounting == nil || !accounting.enabled {
		return ctx, disabledActionGuard
	}
	metric := accounting.actions[operation]
	if metric == nil {
		return ctx, disabledActionGuard
	}
	action := metric.Start(phase, parentID)
	state := &operationState{action: action, blocking: make(map[blockingKey]uint64)}
	return context.WithValue(ctx, operationContextKey{}, operationOwner{operation: operation, action: action, state: state}), action
}

func (accounting *ProductionAccounting) StartOperationIfAbsent(ctx context.Context, operation, phase string) (context.Context, *ActionGuard, bool) {
	if ctx != nil {
		if owner, _ := ctx.Value(operationContextKey{}).(operationOwner); owner.operation != "" {
			return ctx, disabledActionGuard, false
		}
	}
	ctx, action := accounting.StartOperation(ctx, operation, phase, "")
	return ctx, action, action != disabledActionGuard
}

func (accounting *ProductionAccounting) StartDependency(ctx context.Context, role string) *DependencyGuard {
	if accounting == nil || !accounting.enabled || ctx == nil {
		return disabledDependencyGuard
	}
	owner, _ := ctx.Value(operationContextKey{}).(operationOwner)
	metric := accounting.dependencies[owner.operation][role]
	if metric == nil {
		return disabledDependencyGuard
	}
	return metric.Start()
}

func (accounting *ProductionAccounting) AddProcessed(ctx context.Context, role string, bytes uint64) {
	if accounting == nil || !accounting.enabled || ctx == nil || bytes == 0 {
		return
	}
	owner, _ := ctx.Value(operationContextKey{}).(operationOwner)
	if owner.action != nil {
		owner.action.Processed(role, bytes)
	}
}

func (accounting *ProductionAccounting) Progress(ctx context.Context, phase, blocking string) {
	if accounting == nil || !accounting.enabled || ctx == nil {
		return
	}
	owner, _ := ctx.Value(operationContextKey{}).(operationOwner)
	if owner.action != nil {
		owner.action.Progress(phase, blocking, 0, 0)
	}
}

func (accounting *ProductionAccounting) StartBlocking(ctx context.Context, phase, blocking string) *BlockingGuard {
	if accounting == nil || !accounting.enabled || ctx == nil {
		return disabledBlockingGuard
	}
	owner, _ := ctx.Value(operationContextKey{}).(operationOwner)
	if owner.state == nil || owner.action == nil {
		return disabledBlockingGuard
	}
	key := blockingKey{phase: phase, reason: blocking}
	owner.state.mu.Lock()
	owner.state.blocking[key]++
	owner.state.current = key
	owner.state.action.Progress(phase, blocking, 0, 0)
	owner.state.mu.Unlock()
	return &BlockingGuard{state: owner.state, phase: phase, blocking: blocking}
}

func (guard *BlockingGuard) Done() {
	if guard == nil || guard.state == nil || !guard.settled.CompareAndSwap(false, true) {
		return
	}
	guard.state.mu.Lock()
	key := blockingKey{phase: guard.phase, reason: guard.blocking}
	guard.state.blocking[key]--
	if guard.state.current == key && guard.state.blocking[key] == 0 {
		guard.state.current = blockingKey{}
		for candidate, count := range guard.state.blocking {
			if count != 0 && (guard.state.current == (blockingKey{}) || candidate.reason < guard.state.current.reason || candidate.reason == guard.state.current.reason && candidate.phase < guard.state.current.phase) {
				guard.state.current = candidate
			}
		}
		currentPhase := guard.phase
		if guard.state.current != (blockingKey{}) {
			currentPhase = guard.state.current.phase
		}
		guard.state.action.Progress(currentPhase, guard.state.current.reason, 0, 0)
	}
	guard.state.mu.Unlock()
}

func (accounting *ProductionAccounting) StartWait(ctx context.Context, role, throttle string) *WaitGuard {
	if accounting == nil || !accounting.enabled || ctx == nil {
		return disabledWaitGuard
	}
	owner, _ := ctx.Value(operationContextKey{}).(operationOwner)
	metric := accounting.waits[productionWaitKey{operation: owner.operation, role: role, throttle: throttle}]
	if metric == nil {
		return disabledWaitGuard
	}
	return metric.Start()
}

func InheritOperation(ctx, owner context.Context) context.Context {
	if ctx == nil || owner == nil {
		return ctx
	}
	operation, _ := owner.Value(operationContextKey{}).(operationOwner)
	if operation.operation == "" {
		return ctx
	}
	return context.WithValue(ctx, operationContextKey{}, operation)
}

func (accounting *ProductionAccounting) Snapshot(maxMetrics int) ([]Metric, []ActiveOperation, []OperationOverflowSnapshot, uint64) {
	if accounting == nil || !accounting.enabled {
		return nil, nil, nil, 0
	}
	operations := make([]string, 0, len(accounting.actions))
	for operation := range accounting.actions {
		operations = append(operations, operation)
	}
	sort.Strings(operations)
	var metrics []Metric
	var dropped uint64
	appendMetrics := func(candidate []Metric) {
		available := max(maxMetrics-len(metrics), 0)
		if available < len(candidate) {
			dropped += uint64(len(candidate) - available)
			candidate = candidate[:available]
		}
		metrics = append(metrics, candidate...)
	}
	for _, operation := range operations {
		if !accounting.actions[operation].touched() {
			continue
		}
		actionMetrics, _, _ := accounting.actions[operation].Snapshot()
		appendMetrics(actionMetrics)
		roles := make([]string, 0, len(accounting.dependencies[operation]))
		for role := range accounting.dependencies[operation] {
			roles = append(roles, role)
		}
		sort.Strings(roles)
		for _, role := range roles {
			dependency := accounting.dependencies[operation][role]
			if dependency.touched() {
				appendMetrics(dependency.Metrics())
			}
		}
	}
	waitKeys := make([]productionWaitKey, 0, len(accounting.waits))
	for key, wait := range accounting.waits {
		if wait.touched() {
			waitKeys = append(waitKeys, key)
		}
	}
	sort.Slice(waitKeys, func(left, right int) bool {
		if waitKeys[left].operation != waitKeys[right].operation {
			return waitKeys[left].operation < waitKeys[right].operation
		}
		if waitKeys[left].role != waitKeys[right].role {
			return waitKeys[left].role < waitKeys[right].role
		}
		return waitKeys[left].throttle < waitKeys[right].throttle
	})
	for _, key := range waitKeys {
		appendMetrics(accounting.waits[key].Metrics())
		dropped += accounting.waits[key].Dropped()
	}
	active, overflow := accounting.registry.Snapshot()
	return metrics, active, overflow, dropped
}
