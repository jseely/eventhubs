package eventhubs

import (
	"os"
	"testing"
)

func TestSend(t *testing.T) {
	client, err := NewClient(os.Getenv("SB"), os.Getenv("HUB"), os.Getenv("KEYNAME"), os.Getenv("KEY"))
	if err != nil {
		t.Fatal(err.Error())
	}
	msg, err := client.NewMessage([]byte("My client test"))
	if err != nil {
		t.Fatal(err.Error())
	}
	err = msg.Send()
	if err != nil {
		t.Fatal(err.Error())
	}
}
