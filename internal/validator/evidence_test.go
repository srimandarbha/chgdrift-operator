package validator

import (
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestCorrelateVMIM_StitchesEventsAndLogs(t *testing.T) {
	now := time.Now()
	correlator := &EvidenceCorrelator{
		WindowStart: now.Add(-10 * time.Minute),
		WindowEnd:   now.Add(1 * time.Hour),
	}

	events := []corev1.Event{
		{
			ObjectMeta: metav1.ObjectMeta{Name: "evt-stalled"},
			InvolvedObject: corev1.ObjectReference{
				Kind: "VirtualMachineInstanceMigration",
				Name: "vmim-test",
			},
			Reason:        "VMIStalled",
			Message:       "Migration target not ready",
			Type:          corev1.EventTypeWarning,
			LastTimestamp: metav1.NewTime(now),
		},
	}

	logLines := []string{
		"info: virt-handler starting",
		"error: qemu monitor socket closed unexpectedly",
	}

	evidence := correlator.CorrelateVMIM(events, logLines)
	if len(evidence) != 2 {
		t.Fatalf("expected 2 correlated evidence entries, got %d", len(evidence))
	}

	if evidence[0].Source != "Events" || evidence[0].Severity != SeverityCritical {
		t.Errorf("unexpected event evidence: %+v", evidence[0])
	}
	if evidence[1].Source != "Logs" || evidence[1].Severity != SeverityCritical {
		t.Errorf("unexpected log evidence: %+v", evidence[1])
	}
}
