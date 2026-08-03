package validator

import (
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"example.com/drift-operator/api/v1alpha1"
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

func TestGenerateSignedReport_SigningAndVerification(t *testing.T) {
	secretKey := []byte("super-secret-cluster-signing-key-12345")
	windowID := "CHG009988"
	baselineDigest := "digest-sha256-abc123def456"

	gates := []v1alpha1.GateResult{
		{
			Name:       "PlatformHealth",
			Status:     v1alpha1.GateStatusTrue,
			Reason:     "AllOperatorsAvailable",
			Message:    "All platform operators healthy",
			ObservedAt: metav1.Now(),
		},
	}

	report, err := GenerateSignedReport(windowID, baselineDigest, gates, "Succeeded", secretKey)
	if err != nil {
		t.Fatalf("failed to generate signed report: %v", err)
	}

	if report.ReportID == "" || report.HMACSignature == "" || report.EvidenceChecksumSHA256 == "" {
		t.Fatalf("report missing expected cryptographic fields: %+v", report)
	}

	// Verify valid signature
	if !VerifyReportSignature(report, secretKey) {
		t.Fatalf("expected report signature verification to succeed")
	}

	// Verify tampered signature fails
	tamperedSecretKey := []byte("wrong-secret-key")
	if VerifyReportSignature(report, tamperedSecretKey) {
		t.Fatalf("expected tampered secret key verification to fail")
	}

	// Verify modified checksum fails
	report.EvidenceChecksumSHA256 = "corrupted-checksum-1234"
	if VerifyReportSignature(report, secretKey) {
		t.Fatalf("expected verification of report with corrupted checksum to fail")
	}
}
