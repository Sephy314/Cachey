package protocol

import (
	"reflect"
	"testing"
)

func TestCommandSerializeAndDeserialize(t *testing.T) {
	want := Command{Type: PUT, Key: "name", Val: "Ada Lovelace"}

	data, err := want.Serialize()
	if err != nil {
		t.Fatalf("Serialize() error = %v", err)
	}

	got, err := DeSerializeCommand(data)
	if err != nil {
		t.Fatalf("DeSerializeCommand() error = %v", err)
	}
	if !reflect.DeepEqual(*got, want) {
		t.Fatalf("DeSerializeCommand() = %#v, want %#v", *got, want)
	}
}

func TestDeSerializeCommandRejectsEmptyAndMalformedData(t *testing.T) {
	for _, data := range [][]byte{nil, {}, []byte(`{"Type":`)} {
		if _, err := DeSerializeCommand(data); err != ErrorCodeInvalidCommand {
			t.Errorf("DeSerializeCommand(%q) error = %v, want %v", data, err, ErrorCodeInvalidCommand)
		}
	}
}
