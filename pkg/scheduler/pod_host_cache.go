package scheduler

/*
 * A cache to store results from pod scheduling that we didn't use for the actual pod. This allows us to use
 * this list for subsequent pods with the same scheduling parameters rather than having to evaluate a whole bunch of
 * nodes again. Note that we revalidate the host before using it, so if some out of band event made it infeasible we
 * will detect this and fall back on a search.
 *
 * We only support pods that are guaranteed to have a single pod per node, and only support pods with "simple" scheduling
 * constraints so that we can safely guarantee that the feasibility and score transfer between pods.
 *
 * As we are caching results, it is possible that things have changed on nodes underneath us (pods dying). However, we already
 * experience this race because we freeze our cache before each scheduler iteration. We use a very short expiration time to
 * ensure we do not expose ourselves to a much larger window.
 *
 * Note that this cache is not thread safe; it is reliant on the scheduler running in a single go routine.
 */

import (
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/kubernetes/pkg/scheduler/framework"
)

const (
	// Is the cache enabled?
	cacheEnabled = true

	// How long cached extries remain usable.
	expirationTime = 10 * time.Second
)

type PodHostCache struct {
	hasData bool

	controller string
	hosts      []string
	timeAdded  time.Time
}

func (c *PodHostCache) AddPod(pod *v1.Pod, sortedHosts []framework.NodePluginScores, t time.Time) {
	if !podCachingCandidate(pod) || !cacheEnabled {
		return
	}

	c.controller = c.getPodController(pod)
	c.hasData = true
	c.timeAdded = t

	c.hosts = make([]string, len(sortedHosts))
	for i, host := range sortedHosts {
		c.hosts[i] = host.Name
	}
}

func (c *PodHostCache) SuggestedHost(pod *v1.Pod, consumeHost bool) (string, bool) {
	if !c.hasData || !podCachingCandidate(pod) || !cacheEnabled {
		return "", false
	}

	controller := c.getPodController(pod)

	if c.controller != controller {
		return "", false
	}

	if time.Now().After(c.timeAdded.Add(expirationTime)) {
		c.hasData = false
		c.hosts = []string{}
		return "", false
	}

	host := c.hosts[0]
	if consumeHost {
		if len(c.hosts) > 1 {
			c.hosts = c.hosts[1:]
		} else {
			c.hasData = false
			c.hosts = []string{}
		}
	}

	return host, true
}

func (c *PodHostCache) getPodController(pod *v1.Pod) string {
	return "controller"
}

// Functions to determine if a pod is cacheable and generating a signature for those that are cacheable.

// Check if a pod is a candidate for caching.

func podCachingCandidate(p *v1.Pod) bool {
	return onePodPerNode(p) &&
		// Pods with topology spread constraints are not cacheable.
		len(p.Spec.TopologySpreadConstraints) == 0 &&

		// We only support pods with a given set of controller types.
		validPodControllerType(p) &&

		// For now ignore pods with resource claims
		// XXX do we also need to check the claims in the container resources?
		p.Spec.ResourceClaims == nil &&

		// Pods with pod affinity or anti-affinity are not cacheable.
		(p.Spec.Affinity == nil || (p.Spec.Affinity.PodAffinity == nil && p.Spec.Affinity.PodAntiAffinity == nil))
}

// Determine if a pod is a single pod per node. For now only consider pods with a fixed host port reservation.

func onePodPerNode(p *v1.Pod) bool {
	for _, container := range p.Spec.Containers {
		for _, port := range container.Ports {
			if port.HostPort > 0 {
				return true
			}
		}
	}
	return false
}

func validPodControllerType(p *v1.Pod) bool {
	// XXX fillme
	return false
}
