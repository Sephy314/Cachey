package protocol

import "testing"

func TestCodeString(t *testing.T) {
	cases := map[Code]string{
		CodeOK:              "OK",
		CodeInvalidArgument: "InvalidArgument",
		CodeNotFound:        "NotFound",
		CodeUnimplemented:   "Unimplemented",
		CodeInternal:        "Internal",
		Code(999):           "Code(999)",
	}
	for code, want := range cases {
		if got := code.String(); got != want {
			t.Errorf("Code(%d).String() = %q, want %q", code, got, want)
		}
	}
}

func TestStatusError(t *testing.T) {
	st := Statusf(CodeNotFound, "invalid key")
	if st.Error() != "invalid key" {
		t.Errorf("Error() = %q, want %q", st.Error(), "invalid key")
	}
	if st.Code != CodeNotFound {
		t.Errorf("Code = %d, want %d", st.Code, CodeNotFound)
	}
}
