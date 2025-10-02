package runtime

import (
	"encoding/json"
)

type podSignatureMakerImpl struct {
	elements map[string]string
	signable bool
}

func newPodSignatureMaker() *podSignatureMakerImpl {
	return &podSignatureMakerImpl{
		elements: map[string]string{},
		signable: true,
	}
}

// The given pod cannot be signed by the given plugin.
func (s *podSignatureMakerImpl) Unsignable() {
	s.signable = false
}

// The given pod can be signed, and the appropriate signature
// is just an empty string.
func (s *podSignatureMakerImpl) AddElement(elementName, signature string) {
	s.elements[elementName] = signature
}

// Create a signature response from an arbitrary go object.
// This assumes the object is json serializable.
// Note that the golang json serializer has fixed ordering for structs and maps, so it is stable:
//
//	https://stackoverflow.com/questions/18668652/how-to-produce-json-with-sorted-keys-in-go
//
// However, there are more efficient and cleaner ways to serialize which we can
// consider in the future.
func (s *podSignatureMakerImpl) AddElementFromObj(elementName string, object any) error {
	marshalled, err := json.Marshal(object)
	if err != nil {
		return err
	}
	s.elements[elementName] = string(marshalled)
	return nil
}

func (s *podSignatureMakerImpl) HasElement(elementName string) bool {
	_, found := s.elements[elementName]
	return found
}

func (s *podSignatureMakerImpl) Marshal() ([]byte, error) {
	return json.Marshal(s.elements)
}
