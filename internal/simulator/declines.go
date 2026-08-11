package simulator

// Decline triggers, selected by the last two digits of the amount in minor
// units. Real provider sandboxes work this way — magic card numbers or magic
// amounts — because a decline has to be reproducible on demand and cannot be
// left to chance.
//
// The trigger values are the actual ISO-8583 response codes they correspond to,
// so an amount ending in 51 declines for insufficient funds exactly as a real
// authorization carrying response code 51 would. That makes the fixtures
// self-explanatory to anyone who has worked with card messaging.
const (
	triggerInsufficientFunds = 51 // ISO-8583: not sufficient funds
	triggerDoNotHonor        = 5  // ISO-8583: do not honor
	triggerSuspectedFraud    = 59 // ISO-8583: suspected fraud
	triggerExpiredCard       = 54 // ISO-8583: expired card
)

// declineCodeFor reports whether an amount should be declined, and with what
// code. The codes returned are the normalized names the adapter maps onto its
// error classes.
func declineCodeFor(amountMinor int64) (string, bool) {
	if amountMinor <= 0 {
		return "", false
	}

	switch amountMinor % 100 {
	case triggerInsufficientFunds:
		return "insufficient_funds", true
	case triggerDoNotHonor:
		return "do_not_honor", true
	case triggerSuspectedFraud:
		return "suspected_fraud", true
	case triggerExpiredCard:
		return "expired_card", true
	default:
		return "", false
	}
}
