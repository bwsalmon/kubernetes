package scheduler

import (
	"context"

	"k8s.io/kubernetes/pkg/scheduler/framework"
)

type Batch2 struct {
	Valid     bool
	signature string
	nodes     []framework.NodeInfo
	fwk       framework.Framework
	ctx       context.Context
	state     *framework.CycleState
}

func newBatch(ctx context.Context, fwk framework.Framework, state *framework.CycleState, podInfo *framework.QueuedPodInfo, nodes []framework.NodeInfo) (*Batch, error) {
	b := &Batch2{
		Valid: true,
		nodes: nodes,
		fwk:   fwk,
		ctx:   ctx,
		state: state,
	}

	if err := b.signPod(podInfo); err != nil {
		return nil, err
	}

	b.signature = podInfo.Signature

	return b, nil
}

// If possible, set the nominated node name for the given pod from the current information. Also fill in
// the pod scheduling signature in the podInfo object.
func (b *Batch2) AssignPod(podInfo *framework.QueuedPodInfo) (string, error) {
	// Sign the pod if not already signed
	if err := b.signPod(podInfo); err != nil {
		return "", err
	}

	// If we can, grab the best node for the pod.
	if b.Valid && b.signature == podInfo.Signature {
		nodeInfo := &b.nodes[0]
		err := b.update(podInfo, nodeInfo)
		return nodeInfo.GetName(), err
	}

	return "", nil
}

func (b *Batch2) update(podInfo *framework.QueuedPodInfo, nodeInfo *framework.NodeInfo) error {
	pod := podInfo.GetPod()

	nodeInfo.AddPodInfo(podInfo)

	status := b.fwk.RunPreFilterExtensionAddPod(b.ctx, b.state, pod, podInfo, &b.nodes[0])
	if !status.IsSuccess() {
		return status.AsError()
	}

	status = b.fwk.RunFilterPlugins(b.ctx, b.state, pod, &b.nodes[0])
	if status.IsRejected() {
		b.nodes = b.nodes[1:]
		if len(b.nodes) == 0 {
			b.Valid = false
		}
	} else {
		b.Valid = false
	}

	return nil
}

func (b *Batch2) signPod(podInfo *framework.QueuedPodInfo) error {
	if podInfo.Signature == framework.Unsignable {
		sig, err := b.fwk.SignPod(b.ctx, podInfo.GetPod())
		if err != nil {
			return err
		}

		podInfo.Signature = sig
	}
	return nil
}
