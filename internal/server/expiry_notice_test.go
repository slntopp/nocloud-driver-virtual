package server

import "testing"

func TestNextExpiryNoticeDoesNotBurstWhenAlreadyDue(t *testing.T) {
	// The account that finished a trial: nothing stored, the due date two
	// weeks ago. The old loop published "0 days" and then "1 day" in the
	// same pass, and "0 days" again on the next pass — three letters, all
	// of them "expires soon", at the same moment as the suspension.
	const period = 14 * 86400
	diff := int64(-14 * 86400)

	days, send, store := nextExpiryNotice(diff, period, 0, false, notificationsPeriods)
	if send || !store || days != 0 {
		t.Fatalf("first pass: days=%d send=%v store=%v", days, send, store)
	}

	days, send, store = nextExpiryNotice(diff, period, 0, true, notificationsPeriods)
	if send {
		t.Fatalf("second pass sent another letter: days=%d", days)
	}
}

func TestNextExpiryNoticeOneDayBefore(t *testing.T) {
	const period = 14 * 86400
	days, send, store := nextExpiryNotice(20*3600, period, 0, false, notificationsPeriods)
	if !send || !store || days != 1 {
		t.Fatalf("days=%d send=%v store=%v", days, send, store)
	}
	_, send, _ = nextExpiryNotice(20*3600, period, 1, true, notificationsPeriods)
	if send {
		t.Fatal("the same window was sent twice")
	}
}

func TestNextExpiryNoticeStepsDownAndRenews(t *testing.T) {
	const period = 14 * 86400
	days, send, _ := nextExpiryNotice(2*86400+100, period, 7, true, notificationsPeriods)
	if !send || days != 3 {
		t.Fatalf("step down: days=%d send=%v", days, send)
	}
	days, send, _ = nextExpiryNotice(10*86400, period, 0, true, notificationsPeriods)
	if !send || days != 15 {
		t.Fatalf("after renewal: days=%d send=%v", days, send)
	}
}

func TestNextExpiryNoticeSkipsTheBillingPeriodItself(t *testing.T) {
	_, send, store := nextExpiryNotice(6*86400, 7*86400, 0, false, notificationsPeriods)
	if send || store {
		t.Fatal("a window equal to the billing period is the start of the period, not a warning")
	}
}
