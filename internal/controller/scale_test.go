package controller

import (
	"fmt"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/types"

	healthpb "github.com/boanlab/multus-service/api/healthpb"
	"github.com/boanlab/multus-service/internal/model"
)

// BenchmarkReportPath measures the controller's in-memory per-report work as
// the number of registered attachments grows: Registry lookup and subject/node
// checks (localFrom), then generation/sequence admission and store (AcceptLocal).
// EndpointSlice writes are excluded. Authentication is per stream, so this path
// is the same whether producer authorization is on or off.
func BenchmarkReportPath(b *testing.B) {
	for _, n := range []int{1, 10, 100, 1000, 5000} {
		b.Run(fmt.Sprintf("attachments=%d", n), func(b *testing.B) {
			reg := NewRegistry()
			atts := make([]model.Attachment, n)
			for i := range atts {
				atts[i] = model.Attachment{
					PodUID: types.UID(fmt.Sprintf("uid-%d", i)), PodName: fmt.Sprintf("p%d", i),
					PodNamespace: "ns", NodeName: "node-a", NAD: "ns/sec",
					Interface: "net1", IP: fmt.Sprintf("10.%d.%d.%d", i>>16&255, i>>8&255, i&255), PodReady: true,
				}
			}
			reg.SetService(types.NamespacedName{Namespace: "ns", Name: "svc"}, atts, model.ScopeEndpoint)
			s := &HealthServer{Registry: reg, Store: NewHealthStore(5 * time.Second)}
			s.Store.AdoptInstance("node-a", "i1")
			msgs := make([]*healthpb.LocalHealth, n)
			for i, a := range atts {
				msgs[i] = &healthpb.LocalHealth{AttachmentId: a.ID(), Nad: a.NAD, InterfaceName: a.Interface, Ip: a.IP,
					InterfaceExists: true, AddressPresent: true, LinkUsable: true}
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				m := msgs[i%n]
				r, _, ok := s.localFrom("node-a", m)
				if !ok {
					b.Fatal("report refused")
				}
				if err := s.Store.AcceptLocal(model.ReportOrigin{NodeName: "node-a", AgentInstance: "i1",
					Sequence: uint64(i + 1)}, r); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
