package main

import "testing"

func TestDeriveRelayWSS(t *testing.T) {
	cases := []struct{ in, want string }{
		{"https://relaymesh-f93s.onrender.com", "wss://relaymesh-f93s.onrender.com/relay-ws"},
		{"https://relaymesh-f93s.onrender.com/", "wss://relaymesh-f93s.onrender.com/relay-ws"},
		{"http://localhost:8080", "ws://localhost:8080/relay-ws"},
		{"wss://relaymesh-f93s.onrender.com", "wss://relaymesh-f93s.onrender.com/relay-ws"},
		{"relaymesh-f93s.onrender.com", "wss://relaymesh-f93s.onrender.com/relay-ws"},
	}
	for _, c := range cases {
		if got := deriveRelayWSS(c.in); got != c.want {
			t.Errorf("deriveRelayWSS(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}
