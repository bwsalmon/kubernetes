package fort

/*
func TestBasicMap(t *testing.T) {
	spec := NewSpec()
	spec.Source("test")

	s := New(spec)
	src := s.Source("test")
	src.Update("key", "value")

	m := s.Get("test")
	if m != src.(KeyValueMap) {
		t.Fatalf("Got mismatched source and map")
	}

	v, _ := s.Get("test").Get("key")
	if v != "value" {
		t.Fatalf("Got unexpected value %v", v)
	}
}

type testPod struct {
	affinity string
	match    string
	node     string
}

func TestSimpleAffinity(t *testing.T) {
	spec := NewSpec()

	spec.Source("pods")

	spec.MapReduce(
		"affinity",
		func(kv *KeyValue) KeyValueSet {
			p := kv.Value.(*testPod)
			if p.affinity != "" {
				return KeyValueSet{KeyValue{Key: p.affinity, Value: kv.Key}}
			}
			return KeyValueSet{}
		},
		Identical,
		"pods",
	)

	spec.Join("podAffinity", "affinity", "pods")

	spec.MapReduce(
		"affinityNodeCount",
		func(kv *KeyValue) KeyValueSet {
			aff := kv.Value.(JoinValue).Left.Key
			pod := kv.Value.(JoinValue).Right.Value.(*testPod)
			if aff == pod.match {
				return KeyValueSet{
					KeyValue{
						Key:   aff + "/" + pod.node,
						Value: 1,
					},
				}
			}
			return KeyValueSet{}
		},
		Count,
		"podAffinity",
	)

	s := New(spec)

	pods := s.Source("pods")

	pods.Update("pa", &testPod{affinity: "aa", node: "n1"})
	pods.Update("pb", &testPod{affinity: "ab", node: "n2"})
	pods.Update("pa2", &testPod{affinity: "aa", match: "aa", node: "n3"})
	pods.Update("pc", &testPod{match: "ab", node: "n4"})
	//pods.Delete("pa")
	//pods.Delete("pa2")

	s.Print()
}
*/
