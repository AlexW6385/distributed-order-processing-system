package domain

import "testing"

func TestTerminalStates(t *testing.T) {
	for status, want := range map[string]bool{
		StatusPending: false, StatusStockReserved: false, StatusPaymentPending: false,
		StatusPaid: true, StatusFailed: true, StatusExpired: true, StatusCancelled: true,
	} {
		if got := IsTerminal(status); got != want {
			t.Errorf("IsTerminal(%s) = %v, want %v", status, got, want)
		}
	}
}
