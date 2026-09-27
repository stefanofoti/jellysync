package peers

import "testing"

func TestResolveCaller(t *testing.T) {
	r := &Registry{peers: map[string]*Peer{
		"uuid-a": {ID: "uuid-a", Name: "alice", IP: "10.0.0.2"},
		"uuid-b": {ID: "uuid-b", Name: "bob", IP: "10.0.0.3"},
	}}

	cases := []struct {
		nodeID, ip, want string
	}{
		{"alice", "", "uuid-a"},
		{"alice", "10.0.0.3", "uuid-a"}, // name wins over IP
		{"", "10.0.0.3", "uuid-b"},
		{"carol", "10.0.0.3", "uuid-b"},
		{"uuid-b", "", "uuid-b"},
		{"carol", "10.0.0.9", ""},
		{"", "", ""},
	}
	for _, c := range cases {
		if got := r.ResolveCaller(c.nodeID, c.ip); got != c.want {
			t.Errorf("ResolveCaller(%q, %q) = %q, want %q", c.nodeID, c.ip, got, c.want)
		}
	}
}

func TestLiteralIP(t *testing.T) {
	cases := map[string]string{
		"http://10.8.0.4:8080":     "10.8.0.4",
		"http://[fd00::1]:8080":    "fd00::1",
		"http://peer.example:8080": "",
		"not a url\x7f":            "",
	}
	for in, want := range cases {
		if got := literalIP(in); got != want {
			t.Errorf("literalIP(%q) = %q, want %q", in, got, want)
		}
	}
}
