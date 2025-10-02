package runtime

import (
	"encoding/json"
	"reflect"
	"testing"
)

// --- Test Utilities ---

// Helper struct for testing AddElementFromObj with a complex type
type testObject struct {
	Name string   `json:"name"`
	ID   int      `json:"id"`
	Tags []string `json:"tags"`
}

// Helper to unmarshal the final signature for comparison
func unmarshalSignature(t *testing.T, data []byte) map[string]string {
	var result map[string]string
	if err := json.Unmarshal(data, &result); err != nil {
		t.Fatalf("Failed to unmarshal signature JSON: %v", err)
	}
	return result
}

// --- Test Cases for podSignatureMakerImpl ---

func TestNewPodSignatureMaker(t *testing.T) {
	s := newPodSignatureMaker()

	if s == nil {
		t.Fatal("newPodSignatureMaker returned nil")
	}
	if !s.signable {
		t.Errorf("Newly created maker should be signable, got signable=%v", s.signable)
	}
	if len(s.elements) != 0 {
		t.Errorf("Newly created maker's elements map should be empty, got %d elements", len(s.elements))
	}
}

func TestUnsignable(t *testing.T) {
	s := newPodSignatureMaker()

	s.Unsignable()

	if s.signable {
		t.Errorf("Unsignable() should set signable to false, got signable=%v", s.signable)
	}
}

func TestAddElement(t *testing.T) {
	s := newPodSignatureMaker()

	s.AddElement("key1", "valueA")
	s.AddElement("key2", "valueB")

	expectedElements := map[string]string{
		"key1": "valueA",
		"key2": "valueB",
	}

	if !reflect.DeepEqual(s.elements, expectedElements) {
		t.Errorf("AddElement failed. Expected: %v, Got: %v", expectedElements, s.elements)
	}

	// Test overwriting an existing element
	s.AddElement("key1", "newValue")
	expectedElements["key1"] = "newValue"
	if !reflect.DeepEqual(s.elements, expectedElements) {
		t.Errorf("AddElement overwrite failed. Expected: %v, Got: %v", expectedElements, s.elements)
	}
}

func TestAddElementFromObj_Struct(t *testing.T) {
	s := newPodSignatureMaker()

	obj := testObject{
		Name: "TestPod",
		ID:   123,
		Tags: []string{"a", "b", "c"},
	}

	err := s.AddElementFromObj("pod_spec", obj)
	if err != nil {
		t.Fatalf("AddElementFromObj failed: %v", err)
	}

	// Expected JSON string (keys are sorted by Go's encoding/json)
	expectedJSON, err := json.Marshal(obj)
	if err != nil {
		t.Fatalf("Marshal failed %v", err)
	}
	if s.elements["pod_spec"] != string(expectedJSON) {
		t.Errorf("AddElementFromObj marshalled incorrectly.\nExpected: %s\nGot: %s", expectedJSON, s.elements["pod_spec"])
	}
}

func TestAddElementFromObj_Map(t *testing.T) {
	s := newPodSignatureMaker()

	obj := map[string]string{
		"z_key": "z_value",
		"a_key": "a_value",
	}

	err := s.AddElementFromObj("config_map", obj)
	if err != nil {
		t.Fatalf("AddElementFromObj failed: %v", err)
	}

	// Expected JSON string (keys are sorted by Go's encoding/json)
	expectedJSON := `{"a_key":"a_value","z_key":"z_value"}`

	if s.elements["config_map"] != expectedJSON {
		t.Errorf("AddElementFromObj marshalled map incorrectly.\nExpected: %s\nGot: %s", expectedJSON, s.elements["config_map"])
	}
}

func TestAddElementFromObj_Failure(t *testing.T) {
	s := newPodSignatureMaker()

	// Channels are not JSON serializable, so this should cause an error
	unserializableObj := make(chan int)

	err := s.AddElementFromObj("bad_data", unserializableObj)
	if err == nil {
		t.Errorf("AddElementFromObj should have returned an error for an unserializable object, but it did not")
	}

	// Ensure the element was not added to the map on failure
	if _, found := s.elements["bad_data"]; found {
		t.Errorf("Element was added to map despite AddElementFromObj failure")
	}
}

func TestHasElement(t *testing.T) {
	s := newPodSignatureMaker()

	s.AddElement("present_key", "value")

	if !s.HasElement("present_key") {
		t.Errorf("HasElement failed: Expected 'present_key' to be present")
	}

	if s.HasElement("missing_key") {
		t.Errorf("HasElement failed: Expected 'missing_key' to be missing")
	}
}

func TestMarshal(t *testing.T) {
	s := newPodSignatureMaker()

	// Add a simple string element
	s.AddElement("signature_1", "simple_value")

	// Add a complex object element (which gets marshalled internally)
	obj := testObject{Name: "Pod", ID: 5}
	err := s.AddElementFromObj("signature_2", obj)
	if err != nil {
		t.Fatalf("AddElementFromObj failed: %v", err)
	}

	// Marshal the final signature
	signatureBytes, err := s.Marshal()
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}

	expectedMarshal, err := json.Marshal(obj)
	if err != nil {
		t.Fatalf("Marshal failed %v", err)
	}

	// Expected content (keys are sorted by Go's json.Marshal)
	expectedMap := map[string]string{
		"signature_1": "simple_value",
		// The value for signature_2 is the JSON string of the object
		"signature_2": string(expectedMarshal),
	}

	// Unmarshal the result for comparison
	actualMap := unmarshalSignature(t, signatureBytes)

	if !reflect.DeepEqual(actualMap, expectedMap) {
		t.Errorf("Marshal produced incorrect signature JSON.")
		t.Logf("Expected: %v", expectedMap)
		t.Logf("Got:      %v", actualMap)
	}
}
