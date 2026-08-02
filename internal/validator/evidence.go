package validator

import (
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
