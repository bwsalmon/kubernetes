package helper

import (
	"encoding/json"

	"k8s.io/kubernetes/pkg/scheduler/framework"
)

func PodUnsignable() *framework.PodSignatureResult {
	return &framework.PodSignatureResult{Signable: false}
}

func EmptyPodSignature() *framework.PodSignatureResult {
	return &framework.PodSignatureResult{Signable: true}
}

func PodSignatureFromObj(object any) *framework.PodSignatureResult {
	marshalled, err := json.Marshal(object)
	return &framework.PodSignatureResult{Signable: err == nil, Signature: string(marshalled), Error: err}
}
