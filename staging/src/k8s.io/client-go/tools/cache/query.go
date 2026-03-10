package cache

// A shared informer defined by query that is also cloneable.
type CloneableSharedInformerQuery interface {
	SharedInformer
	// Create a clone of the InformerQuery. Use the given sources for
	// the new query. When cloning we will likely be replacing
	// "live" informers with ManualInformers and "Query" informers
	// with new cloned version of the informers.
	Clone(newSources []SharedInformer) CloneableSharedInformerQuery
}

// Generates a new SharedInformer by running the given query over
// the given set of source informers. This is a SQL query
// encoded in gocode, and so follows that standard SQL pattern.
//
// The query logically constructs an array with each tuple of
// objects from the source informers and passes this to the
// selector function.
//
// The selector function returns an object that is emitted
// by the informer. The selector is typed to make typing
// clearer in the code.
//
// If defined, the where function will be called on the tuple first,
// if it returns false the tuple will be dropped, otherwise it will
// be passed on to the GroupBy or Selector.
//
// If the groupBy function is defined, the query will first run the tuples
// from the sources through the function and then pass the aggregated
// results to the select function. See the comment on GroupByFunc
// for more details.
func QueryInformer[Out, Left, Right any](query Query[Out, Left, Right]) CloneableSharedInformerQuery {
	// FILL ME
	return nil
}

type Query[Out, Left, Right any] struct {
	Select      SelectFunc[Out, Left, Right]
	From        SharedInformer
	Join        SharedInformer
	On          JoinOnFunc[Left, Right]
	Where       FilterFunc[Left, Right]
	GroupSelect GroupSelectFunc[Out]
	GroupBy     GroupByFunc[Left, Right]
}

// Takes a tuple of inputs and returns a typed output.
// Note that a SelectFunc is a specific kind of TransformFunc.
type SelectFunc[Out, Left, Right any] func(left Left, right Right) (Out, error)

// The select function for a group query, somewhwat different from the
// "normal" select function.
type GroupSelectFunc[Out any] func(fields []GroupField) (Out, error)

// If the function returns true the tuple will be considered,
// false otherwise.
type FilterFunc[Left, Right any] func(left Left, right Right) bool

// If defined in the query, group the results before passing to the select func.
// Note that a GroupByFunc is actually a specific kind of SelectFunc.
// The GroupByFunc must return the key used to group the entries and a list of
// GroupFields that encode the aggregation functions.
//
// The result passed to the SelectFunc will be a list of final results
// from the GroupFields, with one entry per GroupKey.
type GroupByFunc[Left, Right any] func(left Left, right Right) ([]string, []GroupField)

type GroupField interface{}

// A group key defined by the given array of strings.
func GroupKey(key []string) GroupField {
	// XXX FILL ME
	return nil
}

// Converts to the count of the number of tuples in this group.
func Count() GroupField {
	// XXX FILL ME
	return nil
}

// Converts to the sum of the values for all the tuples in this group.
func Sum(val int64) GroupField {
	// XXX FILL ME
	return nil
}

// Converts to a list of distinct values for all the tuples in this group.
func Distinct(val any) GroupField {
	// XXX FILL ME
	return nil
}

// Returns one of the values passed in for all the tuples in this group.
func AnyValue(val any) GroupField {
	// XXX FILL ME
	return nil
}

// If a join is too expensive to do as a full join, the caller
// can define a JoinOnFunc. Only one of the left and right arguments
// will be non-nil, the function returns the key for that element.
type JoinOnFunc[Left, Right any] func(left Left, right Right) []string

type FlatMapFunc[Out, In any] func(obj In) []Out

func FlatMapInformer[Out, In any](
	unnestFunc FlatMapFunc[Out, In],
	source SharedInformer,
) CloneableSharedInformerQuery {
	// XXX FILL ME
	return nil
}

// After cloning we would like to replace the live informers (looking at the real system state)
// with informers we can update by hand as we try to do simulations. To do so we can use
// ManualSharedInformers. They implement the ResourcEventHandler API and so can be updated
// using OnAdd, OnUpdate and OnDelete.
type ManualSharedInformer interface {
	CloneableSharedInformerQuery
	ResourceEventHandler

	SetIsStopped()
	SetHasSynced()
}

func NewManualSharedInformer() ManualSharedInformer {
	// XXX FILL ME
	return nil
}

// Lock multiple informers together to ensure we can snapshot them
// consistently.
func LockInformerSet(informers []CloneableSharedInformerQuery) InformerLockSet {
	// XXX FILL ME
	return nil
}

type InformerLockSet interface {
	Unlock()
}

////////////////////////////////////////////////
// Example usage

type TPod struct {
	NodeName string
	Label    string
}

type TService struct {
	Name     string
	SomeData string
}

type TServiceNode struct {
	Node    string
	Service string
	Count   int64
}

func (s *TService) Matches(pod *TPod) bool {
	return pod.Label[0] == s.SomeData[0]
}

type TNode struct {
	Name    string
	Domains []string
}

type TNodeDomain struct {
	Name   string
	Domain string
}

type TServiceDomain struct {
	Service string
	Domain  string
	Count   int64
}

type DomainCount struct {
	Domain string
	Count  int64
}

type TServiceInfo struct {
	Service      string
	DomainCounts []DomainCount
}

type PodSpreadLiteInfo struct {
	ServiceNodes   CloneableSharedInformerQuery
	NodeDomains    CloneableSharedInformerQuery
	ServiceDomains CloneableSharedInformerQuery
	ServiceInfo    CloneableSharedInformerQuery

	PodUpdates     ManualSharedInformer
	ServiceUpdates ManualSharedInformer
	NodeUpdates    ManualSharedInformer
}

func NewPodSpreadLiteInfo(podInformer, serviceInformer, nodeInformer SharedInformer) *PodSpreadLiteInfo {
	d := &PodSpreadLiteInfo{}

	// Compute the number of pods on each node that match each service.
	d.ServiceNodes = QueryInformer(Query[*TServiceNode, *TPod, *TService]{
		GroupSelect: func(fields []GroupField) (*TServiceNode, error) {
			// Use the fields generated by GroupBy to create a new object
			// for each service / node group.
			return &TServiceNode{
				Service: fields[1].(string),
				Node:    fields[2].(string),
				Count:   fields[3].(int64),
			}, nil
		},

		// Full join the pod and service informers.
		From: podInformer,
		Join: serviceInformer,

		// We only care about entries where the pod matches the service.
		Where: func(pod *TPod, service *TService) bool {
			return service.Matches(pod)
		},

		// Group by service and node, and count the number of pods
		// for eaach service / node pair.
		GroupBy: func(pod *TPod, service *TService) ([]string, []GroupField) {
			return []string{service.Name, pod.NodeName},
				[]GroupField{
					AnyValue(service.Name),
					AnyValue(pod.NodeName),
					Count(),
				}
		},
	})

	// Create one entry for each node / domain pair.
	d.NodeDomains = FlatMapInformer(
		// For each node, create one entry for each domain
		// the node belongs to.
		func(node *TNode) []*TNodeDomain {
			ret := []*TNodeDomain{}
			for _, d := range node.Domains {
				ret = append(ret, &TNodeDomain{Name: node.Name, Domain: d})
			}
			return ret
		},
		nodeInformer,
	)

	// Join the service/node pod counts with the node/domain pairs.
	// Group by domain to get the total number of pods each service has in each domain.
	d.ServiceDomains = QueryInformer(Query[*TServiceDomain, *TServiceNode, *TNodeDomain]{
		GroupSelect: func(fields []GroupField) (*TServiceDomain, error) {
			return &TServiceDomain{
				Service: fields[1].(string),
				Domain:  fields[2].(string),
				Count:   fields[3].(int64),
			}, nil
		},
		From: d.ServiceNodes,
		Join: d.NodeDomains,
		On: func(service *TServiceNode, node *TNodeDomain) []string {
			if service != nil {
				return []string{service.Node}
			} else {
				return []string{node.Name}
			}
		},
		Where: func(service *TServiceNode, node *TNodeDomain) bool {
			return service.Node == node.Name
		},
		GroupBy: func(service *TServiceNode, node *TNodeDomain) ([]string, []GroupField) {
			return []string{service.Service, node.Domain},
				[]GroupField{
					AnyValue(service.Service),
					AnyValue(node.Domain),
					Sum(service.Count),
				}
		},
	})

	// Flatten the results into one entry per service, with a sub array of domains and their counts.
	d.ServiceInfo = QueryInformer(Query[*TServiceInfo, *TServiceDomain, *TServiceDomain]{
		GroupSelect: func(fields []GroupField) (*TServiceInfo, error) {
			return &TServiceInfo{
				Service:      fields[1].(string),
				DomainCounts: fields[2].([]DomainCount),
			}, nil
		},
		From: d.ServiceDomains,
		GroupBy: func(serviceDomain *TServiceDomain, _ *TServiceDomain) ([]string, []GroupField) {
			return []string{serviceDomain.Service},
				[]GroupField{
					AnyValue(serviceDomain.Service),
					Distinct(DomainCount{Domain: serviceDomain.Domain, Count: serviceDomain.Count}),
				}
		},
	})

	return d
}

func (d *PodSpreadLiteInfo) Clone() *PodSpreadLiteInfo {
	nd := &PodSpreadLiteInfo{}

	nd.PodUpdates = NewManualSharedInformer()
	nd.ServiceUpdates = NewManualSharedInformer()
	nd.NodeUpdates = NewManualSharedInformer()

	locks := LockInformerSet([]CloneableSharedInformerQuery{d.ServiceNodes, d.NodeDomains, d.ServiceDomains, d.ServiceInfo})
	defer locks.Unlock()

	nd.ServiceNodes = d.ServiceNodes.Clone([]SharedInformer{nd.PodUpdates, nd.ServiceUpdates})
	nd.NodeDomains = d.NodeDomains.Clone([]SharedInformer{nd.NodeUpdates})
	nd.ServiceDomains = d.ServiceDomains.Clone([]SharedInformer{nd.ServiceNodes, nd.NodeDomains})
	nd.ServiceInfo = d.ServiceInfo.Clone([]SharedInformer{nd.ServiceDomains})

	return nd
}
