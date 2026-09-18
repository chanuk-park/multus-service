package multus

import (
	"errors"
	"reflect"
	"testing"
)

// Verbatim annotation from the OAI AMF Pod on the bench cluster.
const amfStatus = `[{
    "name": "cbr0",
    "interface": "eth0",
    "ips": [
        "10.42.0.22"
    ],
    "mac": "3a:24:c0:69:e7:71",
    "default": true,
    "dns": {},
    "gateway": [
        "10.42.0.1"
    ]
},{
    "name": "core/oai-core-n2",
    "interface": "n2",
    "ips": [
        "10.100.50.249"
    ],
    "mac": "aa:26:7a:d9:0f:81",
    "dns": {}
}]`

// Verbatim annotation from a Pod attaching the same NAD twice.
const doubleAttach = `[{"name":"cbr0","interface":"eth0","ips":["10.42.0.83"],"mac":"d2:1b:a3:65:49:98","default":true,"dns":{},"gateway":["10.42.0.1"]},
{"name":"slicelab/mv-lab","interface":"net1","ips":["10.100.60.11"],"mac":"b2:da:bc:6e:7f:fa","dns":{}},
{"name":"slicelab/mv-lab","interface":"net2","ips":["10.100.60.12"],"mac":"5a:5f:c5:e8:c3:74","dns":{}}]`

// Verbatim annotation from a dual-stack attachment.
const dualStack = `[{"name":"cbr0","interface":"eth0","ips":["10.42.0.82"],"default":true,"dns":{}},
{"name":"slicelab/mv-lab-ds","interface":"net1","ips":["10.100.61.10","fd00:61::10"],"mac":"de:c1:9f:db:36:f6","dns":{}}]`

func TestParseAMF(t *testing.T) {
	got, err := Parse(amfStatus)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("want 2 entries, got %d", len(got))
	}
	if !got[0].Default {
		t.Error("first entry should be the default network")
	}
	// The secondary entry carries no "default" key at all; it must decode as
	// false rather than being treated as unknown.
	if got[1].Default {
		t.Error("secondary entry must not be default")
	}
	// The interface is "n2" because the request named it. Assuming net1 here
	// would break every production Pod on this cluster.
	if got[1].Interface != "n2" {
		t.Errorf("interface = %q, want n2", got[1].Interface)
	}
}

func TestParseEmptyIsNotAnError(t *testing.T) {
	// A Pod exists for ~0.5s before multus writes the annotation. That window
	// is normal and must not produce errors.
	got, err := Parse("")
	if err != nil || got != nil {
		t.Fatalf("Parse(\"\") = %v, %v; want nil, nil", got, err)
	}
}

func TestParseGarbage(t *testing.T) {
	if _, err := Parse("{not json"); err == nil {
		t.Fatal("want error for malformed annotation")
	}
}

func TestCanonicalNAD(t *testing.T) {
	cases := []struct {
		ref, ns, want string
		wantErr       bool
	}{
		{"n2-net", "oai", "oai/n2-net", false},
		{"oai/n2-net", "other", "oai/n2-net", false},
		{"oai/n2-net@n2", "other", "oai/n2-net", false},
		{"  n2-net  ", "oai", "oai/n2-net", false},
		{"", "oai", "", true},
		{"a/b/c", "oai", "", true},
		{"/n2", "oai", "", true},
		{"n2-net", "", "", true},
	}
	for _, c := range cases {
		got, err := CanonicalNAD(c.ref, c.ns)
		if (err != nil) != c.wantErr {
			t.Errorf("CanonicalNAD(%q,%q) err = %v, wantErr %v", c.ref, c.ns, err, c.wantErr)
			continue
		}
		if got != c.want {
			t.Errorf("CanonicalNAD(%q,%q) = %q, want %q", c.ref, c.ns, got, c.want)
		}
	}
}

func TestSelectMatchesCanonicalName(t *testing.T) {
	list, _ := Parse(amfStatus)
	got, err := Select(list, "core/oai-core-n2")
	if err != nil {
		t.Fatalf("Select: %v", err)
	}
	if got.Interface != "n2" {
		t.Errorf("interface = %q, want n2", got.Interface)
	}
	if want := []string{"10.100.50.249"}; !reflect.DeepEqual(got.Addresses(), want) {
		t.Errorf("addresses = %v, want %v", got.Addresses(), want)
	}
}

func TestSelectSkipsDefaultNetwork(t *testing.T) {
	// If a NAD is promoted to the cluster default, its entry carries the
	// primary address. Publishing that is the exact failure mode this project
	// exists to prevent, so a default entry is never selectable.
	list := []NetworkStatus{{Name: "oai/n2-net", Interface: "eth0", IPs: []string{"10.42.0.9"}, Default: true}}
	if _, err := Select(list, "oai/n2-net"); !errors.Is(err, ErrNoAttachment) {
		t.Fatalf("err = %v, want ErrNoAttachment", err)
	}
}

func TestSelectMissing(t *testing.T) {
	list, _ := Parse(amfStatus)
	if _, err := Select(list, "core/does-not-exist"); !errors.Is(err, ErrNoAttachment) {
		t.Fatalf("err = %v, want ErrNoAttachment", err)
	}
}

func TestSelectAmbiguous(t *testing.T) {
	// Two entries share the same name and differ only by interface. There is no
	// principled way to pick one, so the Pod is skipped rather than guessed at.
	list, _ := Parse(doubleAttach)
	_, err := Select(list, "slicelab/mv-lab")
	if !errors.Is(err, ErrAmbiguous) {
		t.Fatalf("err = %v, want ErrAmbiguous", err)
	}
	if got := err.Error(); got == "" || !contains(got, "net1") || !contains(got, "net2") {
		t.Errorf("error should name both interfaces, got %q", got)
	}
}

func TestDualStackAddresses(t *testing.T) {
	list, _ := Parse(dualStack)
	e, err := Select(list, "slicelab/mv-lab-ds")
	if err != nil {
		t.Fatalf("Select: %v", err)
	}
	want := []string{"10.100.61.10", "fd00:61::10"}
	if !reflect.DeepEqual(e.Addresses(), want) {
		t.Errorf("addresses = %v, want %v", e.Addresses(), want)
	}
}

func TestAddressesStripsPrefixAndJunk(t *testing.T) {
	e := NetworkStatus{IPs: []string{"10.1.2.3/24", " ", "not-an-ip", "fd00::1"}}
	want := []string{"10.1.2.3", "fd00::1"}
	if got := e.Addresses(); !reflect.DeepEqual(got, want) {
		t.Errorf("addresses = %v, want %v", got, want)
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
