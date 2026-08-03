package validator

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"example.com/drift-operator/api/v1alpha1"
)

type EvidenceSeverity string

const (
	SeverityInfo     EvidenceSeverity = "INFO"
	SeverityWarning  EvidenceSeverity = "WARNING"
	SeverityCritical EvidenceSeverity = "CRITICAL"
)

// CorrelatedEvidence captures multi-signal evidentiary evidence during a maintenance window.
type CorrelatedEvidence struct {
	Timestamp metav1.Time      `json:"timestamp"`
	Component string           `json:"component"`
	ObjectID  string           `json:"objectId"`
	Reason    string           `json:"reason"`
	Message   string           `json:"message"`
	Severity  EvidenceSeverity `json:"severity"`
	Source    string           `json:"source"` // "K8sAPI", "Events", "Logs", "Metrics"
}

// ImmutableEvidenceSnapshot captures a point-in-time frozen state of maintenance evidence.
type ImmutableEvidenceSnapshot struct {
	CapturedAt         metav1.Time          `json:"capturedAt"`
	WindowID           string               `json:"windowId"`
	CorrelatedSignals  []CorrelatedEvidence `json:"correlatedSignals"`
	ClusterVersion     string               `json:"clusterVersion"`
	MachineConfigPool  string               `json:"machineConfigPool"`
	KubeVirtPhase      string               `json:"kubeVirtPhase"`
	ActiveMigrations   int32                `json:"activeMigrations"`
	StalledMigrations  int32                `json:"stalledMigrations"`
}

// EvidenceCorrelator stitches object state transitions, warning events, and live pod logs into a unified timeline.
type EvidenceCorrelator struct {
	WindowStart time.Time
	WindowEnd   time.Time
}

// CorrelateVMIM cross-references warning events and virt-handler log lines against the maintenance window.
func (e *EvidenceCorrelator) CorrelateVMIM(events []corev1.Event, logEntries []string) []CorrelatedEvidence {
	var evidence []CorrelatedEvidence

	for _, evt := range events {
		if (e.WindowStart.IsZero() || evt.LastTimestamp.After(e.WindowStart)) && evt.Type == corev1.EventTypeWarning {
			sev := SeverityWarning
			if strings.Contains(evt.Reason, "Failed") || strings.Contains(evt.Reason, "Stalled") || strings.Contains(evt.Reason, "Error") {
				sev = SeverityCritical
			}
			evidence = append(evidence, CorrelatedEvidence{
				Timestamp: evt.LastTimestamp,
				Component: evt.InvolvedObject.Kind,
				ObjectID:  evt.InvolvedObject.Name,
				Reason:    evt.Reason,
				Message:   evt.Message,
				Severity:  sev,
				Source:    "Events",
			})
		}
	}

	for _, logLine := range logEntries {
		if strings.Contains(logLine, "qemu monitor socket closed") || strings.Contains(logLine, "attachment failed") || strings.Contains(logLine, "domain-migrated") || strings.Contains(logLine, "ERROR") {
			evidence = append(evidence, CorrelatedEvidence{
				Timestamp: metav1.Now(),
				Component: "virt-handler",
				ObjectID:  "pod/virt-handler",
				Reason:    "LogSignal",
				Message:   logLine,
				Severity:  SeverityCritical,
				Source:    "Logs",
			})
		}
	}

	return evidence
}

// CalculateSHA256 computes a SHA-256 hex checksum for raw bytes.
func CalculateSHA256(data []byte) string {
	hash := sha256.Sum256(data)
	return hex.EncodeToString(hash[:])
}

// SignHMAC256 computes an HMAC-SHA256 hex signature using a secret key.
func SignHMAC256(payload []byte, secret []byte) string {
	mac := hmac.New(sha256.New, secret)
	mac.Write(payload)
	return hex.EncodeToString(mac.Sum(nil))
}

// GenerateSignedReport builds an immutable, cryptographically signed audit report for a maintenance window.
func GenerateSignedReport(windowID string, baselineDigest string, gates []v1alpha1.GateResult, overallResult string, secretKey []byte) (*v1alpha1.SignedAuditReport, error) {
	now := metav1.Now()
	reportID := fmt.Sprintf("report-%s-%d", windowID, now.Unix())

	gateBytes, err := json.Marshal(gates)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal gate results: %w", err)
	}

	payloadToHash := fmt.Sprintf("%s:%s:%s:%s:%s", reportID, windowID, baselineDigest, overallResult, string(gateBytes))
	sha256Checksum := CalculateSHA256([]byte(payloadToHash))

	signature := SignHMAC256([]byte(sha256Checksum), secretKey)

	return &v1alpha1.SignedAuditReport{
		ReportID:               reportID,
		WindowID:               windowID,
		Timestamp:              now,
		BaselineDigest:         baselineDigest,
		EvidenceChecksumSHA256: sha256Checksum,
		HMACSignature:          signature,
		OverallResult:          overallResult,
		GateResults:            gates,
	}, nil
}

// VerifyReportSignature validates the HMAC signature of a SignedAuditReport to detect tampering.
func VerifyReportSignature(report *v1alpha1.SignedAuditReport, secretKey []byte) bool {
	if report == nil || report.HMACSignature == "" {
		return false
	}
	expectedSig := SignHMAC256([]byte(report.EvidenceChecksumSHA256), secretKey)
	return hmac.Equal([]byte(report.HMACSignature), []byte(expectedSig))
}
