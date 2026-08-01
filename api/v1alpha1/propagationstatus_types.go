package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ----------------------------------------------------------------------------
// ClusterAppReport: one per (cluster, app). Written ONLY by the agent running
// in that cluster, using a ServiceAccount scoped to create/update/get on this
// resource in its own namespace. Agents never touch PropagationStatus
// directly -- that keeps N agents from racing on one shared object.
// ----------------------------------------------------------------------------

// ClusterAppReportSpec is set by the agent on every reconcile of its local
// ArgoCD Application / Flux Kustomization.
type ClusterAppReportSpec struct {
	// ClusterName is the stable identifier for the reporting cluster.
	ClusterName string `json:"clusterName"`

	// AppName ties this report to a PropagationStatus.spec.appName.
	AppName string `json:"appName"`

	// ObservedRevision is the git commit SHA currently synced locally.
	ObservedRevision string `json:"observedRevision"`

	// SyncStatus mirrors the local controller's status (Synced/OutOfSync).
	// +kubebuilder:validation:Enum=Synced;OutOfSync;Unknown
	SyncStatus string `json:"syncStatus,omitempty"`

	// Health mirrors the local controller's health assessment.
	// +kubebuilder:validation:Enum=Healthy;Progressing;Degraded;Unknown
	Health string `json:"health,omitempty"`

	// MCPStatus captures MachineConfigPool node rollout state for OpenShift Virtualization.
	MCPStatus MachineConfigPoolStatus `json:"mcpStatus,omitempty"`

	// ObservedAt is when the agent captured this snapshot locally.
	ObservedAt metav1.Time `json:"observedAt"`
}

// +kubebuilder:object:root=true
// +kubebuilder:printcolumn:name="Cluster",type=string,JSONPath=`.spec.clusterName`
// +kubebuilder:printcolumn:name="App",type=string,JSONPath=`.spec.appName`
// +kubebuilder:printcolumn:name="Revision",type=string,JSONPath=`.spec.observedRevision`
// +kubebuilder:printcolumn:name="Sync",type=string,JSONPath=`.spec.syncStatus`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
type ClusterAppReport struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec ClusterAppReportSpec `json:"spec,omitempty"`
}

// +kubebuilder:object:root=true
type ClusterAppReportList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ClusterAppReport `json:"items"`
}

// ----------------------------------------------------------------------------
// PropagationStatus: one per app, owned by the platform team.
// ----------------------------------------------------------------------------

type PropagationStatusSpec struct {
	AppName                     string   `json:"appName"`
	ExpectedRevision            string   `json:"expectedRevision"`
	TargetClusters              []string `json:"targetClusters"`
	LagThresholdSeconds         int32    `json:"lagThresholdSeconds,omitempty"`
	StaleReportThresholdSeconds int32    `json:"staleReportThresholdSeconds,omitempty"`
	ParentApp                   string   `json:"parentApp,omitempty"`
	RootApp                     string   `json:"rootApp,omitempty"`
}

type MachineConfigPoolStatus struct {
	Name              string `json:"name,omitempty"` // e.g., worker, master, virt
	MachineCount      int32  `json:"machineCount,omitempty"`
	UpdatedNodeCount  int32  `json:"updatedNodeCount,omitempty"`
	UpdatingNodeCount int32  `json:"updatingNodeCount,omitempty"`
	DegradedNodeCount int32  `json:"degradedNodeCount,omitempty"`
	Phase             string `json:"phase,omitempty"` // Updated | Updating | Degraded
}

type ClusterRevisionState struct {
	ClusterName            string                  `json:"clusterName"`
	ObservedRevision       string                  `json:"observedRevision,omitempty"`
	SyncStatus             string                  `json:"syncStatus,omitempty"`
	Health                 string                  `json:"health,omitempty"`
	ObservedAt             metav1.Time             `json:"observedAt,omitempty"`
	SawReportSinceChgStart bool                    `json:"sawReportSinceChgStart,omitempty"`
	MCPStatus              MachineConfigPoolStatus `json:"mcpStatus,omitempty"`
	// State summarizes this row: InSync | Lagging | Diverged | Stale | Missing
	State string `json:"state"`
}

type PropagationStatusStatus struct {
	Phase                 string                 `json:"phase,omitempty"`
	ClusterStates         []ClusterRevisionState `json:"clusterStates,omitempty"`
	ExpectedRevisionSince metav1.Time            `json:"expectedRevisionSince,omitempty"`
	LastExpectedRevision  string                 `json:"lastExpectedRevision,omitempty"`
	DivergedClusters      []string               `json:"divergedClusters,omitempty"`
	LaggingClusters       []string               `json:"laggingClusters,omitempty"`
	StaleClusters         []string               `json:"staleClusters,omitempty"`
	MissingClusters       []string               `json:"missingClusters,omitempty"`
	ObservedGeneration    int64                  `json:"observedGeneration,omitempty"`
	Conditions            []metav1.Condition     `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Expected",type=string,JSONPath=`.spec.expectedRevision`
// +kubebuilder:printcolumn:name="Diverged",type=string,JSONPath=`.status.divergedClusters`
// +kubebuilder:printcolumn:name="Lagging",type=string,JSONPath=`.status.laggingClusters`
type PropagationStatus struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   PropagationStatusSpec   `json:"spec,omitempty"`
	Status PropagationStatusStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type PropagationStatusList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []PropagationStatus `json:"items"`
}

// ----------------------------------------------------------------------------
// ChangeWindow: CHG maintenance window tracking.
// ----------------------------------------------------------------------------

type HardRefreshConfig struct {
	MaxAttempts         int32  `json:"maxAttempts"`
	WaitBetweenSeconds  int32  `json:"waitBetweenSeconds"`
	ActionExecutionMode string `json:"actionExecutionMode,omitempty"`
}

type ChangeWindowSpec struct {
	CHGNumber                   string            `json:"chgNumber"`
	ReleaseTag                  string            `json:"releaseTag"`
	BaselineRevision            string            `json:"baselineRevision,omitempty"`
	ExpectedRevision            string            `json:"expectedRevision,omitempty"`
	RootApp                     string            `json:"rootApp"`
	ImpactedApps                []string          `json:"impactedApps,omitempty"`
	StartTime                   metav1.Time       `json:"startTime"`
	EndTime                     metav1.Time       `json:"endTime"`
	StaleReportThresholdSeconds int32             `json:"staleReportThresholdSeconds,omitempty"`
	HardRefresh                 HardRefreshConfig `json:"hardRefresh,omitempty"`
}

type SilentClusterState struct {
	App              string      `json:"app"`
	Cluster          string      `json:"cluster"`
	State            string      `json:"state"` // WentSilentDuringChg | SilentBeforeChgStart
	LastSeenAt       metav1.Time `json:"lastSeenAt"`
	SilentForSeconds int64       `json:"silentForSeconds"`
}

type ActionAttemptHistory struct {
	Attempt       int32       `json:"attempt"`
	Type          string      `json:"type"`
	TriggeredAt   metav1.Time `json:"triggeredAt"`
	TriggerResult string      `json:"triggerResult"`
	LogRef        string      `json:"logRef,omitempty"`
	LogSummary    string      `json:"logSummary,omitempty"`
	TailLogs      []string    `json:"tailLogs,omitempty"` // Capped inline logs (max 20 lines / 2KB)
}

type ActionRecord struct {
	App            string                 `json:"app"`
	Cluster        string                 `json:"cluster"`
	Attempts       int32                  `json:"attempts"`
	MaxAttempts    int32                  `json:"maxAttempts"`
	Result         string                 `json:"result"`
	LastAttemptAt  metav1.Time            `json:"lastAttemptAt,omitempty"`
	NextEligibleAt metav1.Time            `json:"nextEligibleAt,omitempty"`
	History        []ActionAttemptHistory `json:"history,omitempty"`
}

type ValidationResult struct {
	AllChangesApplied bool     `json:"allChangesApplied"`
	IssuesFound       []string `json:"issuesFound,omitempty"`
	Passed            bool     `json:"passed"`
}

type ChangeWindowStatus struct {
	Phase          string                        `json:"phase,omitempty"`
	OverallStatus  string                        `json:"overallStatus,omitempty"`
	SilentClusters []SilentClusterState          `json:"silentClusters,omitempty"`
	Actions        map[string]ActionRecord       `json:"actions,omitempty"`
	Validation     ValidationResult              `json:"validation,omitempty"`
	LastReportedAt metav1.Time                   `json:"lastReportedAt,omitempty"`
	AppStates      map[string]AppClusterStateMap `json:"appStates,omitempty"`
}

type AppClusterStateMap struct {
	Phase         string                 `json:"phase"`
	ClusterStates []ClusterRevisionState `json:"clusterStates"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="CHG",type=string,JSONPath=`.spec.chgNumber`
// +kubebuilder:printcolumn:name="Tag",type=string,JSONPath=`.spec.releaseTag`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Overall",type=string,JSONPath=`.status.overallStatus`
type ChangeWindow struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ChangeWindowSpec   `json:"spec,omitempty"`
	Status ChangeWindowStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type ChangeWindowList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ChangeWindow `json:"items"`
}

func init() {
	SchemeBuilder.Register(
		&ClusterAppReport{}, &ClusterAppReportList{},
		&PropagationStatus{}, &PropagationStatusList{},
		&ChangeWindow{}, &ChangeWindowList{},
	)
}
