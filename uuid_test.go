package ble

import (
	"bytes"
	"testing"
)

var forward = [][]byte{
	[]byte{1, 2, 3, 4, 5, 6},
	[]byte{12, 143, 231, 123, 87, 124, 209},
	[]byte{3, 43, 223, 12, 54},
}

var reverse = [][]byte{
	[]byte{6, 5, 4, 3, 2, 1},
	[]byte{209, 124, 87, 123, 231, 143, 12},
	[]byte{54, 12, 223, 43, 3},
}

func TestReverse(t *testing.T) {

	for i := 0; i < len(forward); i++ {
		r := Reverse(forward[i])
		if !bytes.Equal(r, reverse[i]) {
			t.Errorf("Error: %v in reverse should be %v, but is: %v", forward[i], reverse[i], r)
		}
	}
}

func TestContainsIsPlainMembership(t *testing.T) {
	u := UUID16(0x1800)
	other := UUID16(0x180f)

	// Breaking change pinned here: a nil slice contains nothing. The old
	// "nil filter matches everything" behavior returned true and turned
	// membership tests into always-true footguns.
	if Contains(nil, u) {
		t.Error("Contains(nil, u) = true, want false")
	}
	if Contains([]UUID{}, u) {
		t.Error("Contains(empty, u) = true, want false")
	}
	if !Contains([]UUID{other, u}, u) {
		t.Error("Contains(s, u) = false for a present element, want true")
	}
	if Contains([]UUID{other}, u) {
		t.Error("Contains(s, u) = true for an absent element, want false")
	}
}
