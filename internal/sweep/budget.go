package sweep

import "github.com/acme/frontier/internal/budget"

// queueClass keeps every reconciliation call behind C-B3's sweep headroom.
func queueClass() budget.Class {
	return budget.Sweep
}
