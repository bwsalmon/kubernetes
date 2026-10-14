/*
Copyright 2026 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package framework

import (
	"context"
	"fmt"
	"sync"

	v1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
	fwk "k8s.io/kube-scheduler/framework"
)

// ReversibleStateData is an optional interface that CycleState plugin StateData
// can implement to support in-place undoable mutations without full cloning.
type ReversibleStateData interface {
	fwk.StateData
	// ApplyMutation applies a reversible forward delta.
	// Returns an UndoFunc that reverses this exact mutation in O(1).
	ApplyMutation(delta interface{}) (UndoFunc, error)
}

// UndoFunc is a closure that reverts a specific in-place mutation.
type UndoFunc func() error

// UndoLog records a sequence of reversible mutations across CycleState and NodeInfo.
type UndoLog struct {
	mu      sync.Mutex
	entries []UndoFunc
}

// NewUndoLog initializes an empty UndoLog.
func NewUndoLog() *UndoLog {
	return &UndoLog{
		entries: make([]UndoFunc, 0, 16),
	}
}

// Record registers an undo closure onto the transaction log.
func (u *UndoLog) Record(undo UndoFunc) {
	if u == nil || undo == nil {
		return
	}
	u.mu.Lock()
	defer u.mu.Unlock()
	u.entries = append(u.entries, undo)
}

// Len returns the number of recorded undo actions.
func (u *UndoLog) Len() int {
	if u == nil {
		return 0
	}
	u.mu.Lock()
	defer u.mu.Unlock()
	return len(u.entries)
}

// Rollback executes all recorded undo closures in LIFO (reverse) order.
// Once executed, the undo log is cleared.
func (u *UndoLog) Rollback() error {
	if u == nil {
		return nil
	}
	u.mu.Lock()
	defer u.mu.Unlock()

	var firstErr error
	for i := len(u.entries) - 1; i >= 0; i-- {
		if err := u.entries[i](); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	u.entries = u.entries[:0]
	return firstErr
}

// NodeInfoDeltaMutator wraps NodeInfo to provide reversible ApplyWithUndo operations.
type NodeInfoDeltaMutator struct {
	nodeInfo *NodeInfo
	undoLog  *UndoLog
}

// NewNodeInfoDeltaMutator creates a delta mutator bound to a target NodeInfo.
func NewNodeInfoDeltaMutator(nodeInfo *NodeInfo, undoLog *UndoLog) *NodeInfoDeltaMutator {
	return &NodeInfoDeltaMutator{
		nodeInfo: nodeInfo,
		undoLog:  undoLog,
	}
}

// RemovePodWithUndo removes a pod from the target NodeInfo and logs an undo entry
// that will re-add the pod upon rollback.
func (m *NodeInfoDeltaMutator) RemovePodWithUndo(logger klog.Logger, podInfo fwk.PodInfo) error {
	if m.nodeInfo == nil || podInfo == nil {
		return fmt.Errorf("nodeInfo and podInfo cannot be nil")
	}

	pod := podInfo.GetPod()
	if err := m.nodeInfo.RemovePod(logger, pod); err != nil {
		return err
	}

	// Record reverse operation: AddPodInfo(podInfo)
	m.undoLog.Record(func() error {
		m.nodeInfo.AddPodInfo(podInfo)
		return nil
	})
	return nil
}

// AddPodWithUndo adds a pod to the target NodeInfo and logs an undo entry
// that will remove the pod upon rollback.
func (m *NodeInfoDeltaMutator) AddPodWithUndo(logger klog.Logger, podInfo fwk.PodInfo) error {
	if m.nodeInfo == nil || podInfo == nil {
		return fmt.Errorf("nodeInfo and podInfo cannot be nil")
	}

	m.nodeInfo.AddPodInfo(podInfo)

	// Record reverse operation: RemovePod(pod)
	m.undoLog.Record(func() error {
		return m.nodeInfo.RemovePod(logger, podInfo.GetPod())
	})
	return nil
}

// TransactionalCycleState wraps fwk.CycleState with an undo log for tracked mutations.
type TransactionalCycleState struct {
	fwk.CycleState
	undoLog *UndoLog
}

// NewTransactionalCycleState wraps an existing CycleState with an undo log.
func NewTransactionalCycleState(cs fwk.CycleState) *TransactionalCycleState {
	return &TransactionalCycleState{
		CycleState: cs,
		undoLog:    NewUndoLog(),
	}
}

// UndoLog returns the associated UndoLog.
func (t *TransactionalCycleState) UndoLog() *UndoLog {
	return t.undoLog
}

// RecordUndo registers an external undo closure (e.g. from plugin PreFilterExtensions).
func (t *TransactionalCycleState) RecordUndo(undo UndoFunc) {
	t.undoLog.Record(undo)
}

// Rollback rolls back all recorded mutations on this transactional cycle state.
func (t *TransactionalCycleState) Rollback() error {
	return t.undoLog.Rollback()
}

// ApplyPreFilterRemoveWithUndo executes RunPreFilterExtensionRemovePod and records
// RunPreFilterExtensionAddPod as the undo action.
func ApplyPreFilterRemoveWithUndo(
	ctx context.Context,
	handle fwk.Handle,
	cycleState fwk.CycleState,
	preemptor *v1.Pod,
	podInfo fwk.PodInfo,
	nodeInfo fwk.NodeInfo,
	undoLog *UndoLog,
) *fwk.Status {
	status := handle.RunPreFilterExtensionRemovePod(ctx, cycleState, preemptor, podInfo, nodeInfo)
	if !status.IsSuccess() {
		return status
	}

	undoLog.Record(func() error {
		undoStatus := handle.RunPreFilterExtensionAddPod(ctx, cycleState, preemptor, podInfo, nodeInfo)
		if !undoStatus.IsSuccess() {
			return undoStatus.AsError()
		}
		return nil
	})
	return nil
}

// ApplyPreFilterAddWithUndo executes RunPreFilterExtensionAddPod and records
// RunPreFilterExtensionRemovePod as the undo action.
func ApplyPreFilterAddWithUndo(
	ctx context.Context,
	handle fwk.Handle,
	cycleState fwk.CycleState,
	preemptor *v1.Pod,
	podInfo fwk.PodInfo,
	nodeInfo fwk.NodeInfo,
	undoLog *UndoLog,
) *fwk.Status {
	status := handle.RunPreFilterExtensionAddPod(ctx, cycleState, preemptor, podInfo, nodeInfo)
	if !status.IsSuccess() {
		return status
	}

	undoLog.Record(func() error {
		undoStatus := handle.RunPreFilterExtensionRemovePod(ctx, cycleState, preemptor, podInfo, nodeInfo)
		if !undoStatus.IsSuccess() {
			return undoStatus.AsError()
		}
		return nil
	})
	return nil
}
