package helper

import (
	"encoding/json"

	"k8s.io/kubernetes/pkg/scheduler/framework"
)

// The given pod cannot be signed by the given plugin.
func PodUnsignable() *framework.PodSignatureResult {
	return &framework.PodSignatureResult{Signable: false}
}

// The given pod can be signed, and the appropriate signature
// is just an empty string.
func EmptyPodSignature() *framework.PodSignatureResult {
	return &framework.PodSignatureResult{Signable: true}
}

// Create a signature response from an arbitrary go object.
// This assumes the object is json serializable.
// Note that the golang json serializer has fixed ordering for structs and maps, so it is stable:
//
//	https://stackoverflow.com/questions/18668652/how-to-produce-json-with-sorted-keys-in-go
//
// However, there are more efficient and cleaner ways to serialize which we can
// consider in the future.
func PodSignatureFromObj(object any) *framework.PodSignatureResult {
	marshalled, err := json.Marshal(object)
	return &framework.PodSignatureResult{Signable: err == nil, Signature: string(marshalled), Error: err}
}
