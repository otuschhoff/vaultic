package daemon

import "testing"

func TestClientInitializesSubclients(t *testing.T) {
	client := &Client{}
	client.initializeSubclients()

	if client.KV.client != client || client.Txn.client != client || client.Role.client != client || client.Generation.client != client {
		t.Fatal("daemon subclients do not share their parent client")
	}
}
