package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ----------------------------------------------------------------------------
// Supporting types for spoke-collected observability data.
// ----------------------------------------------------------------------------

// EventSummary captures a single de-duplicated Kubernetes Warning event.
type EventSummary struct {
	// Reason is the machine-readable event reason (e.g. FailedScheduling, BackOff).
	Reason string `json:"reason"`
	// Message is the human-readable event message.
	Message string `json:"message"`
	// Count is the total number of times this event was observed.
	Count int32 `json:"count"`
	// LastObservedAt is when the event was most recently seen.
	LastObservedAt metav1.Time `json:"lastObservedAt"`
	// InvolvedObject identifies the resource that generated this event (e.g. "Pod/svc-payments-7f9c-x2j4k").
	InvolvedObject string `json:"involvedObject"`
}

// ObjectChangeSummary describes a single resource touched by the last ArgoCD sync.
type ObjectChangeSummary struct {
	// Kind is the resource kind (e.g. Deployment, VirtualMachine, ConfigMap).
	Kind string `json:"kind"`
	// Name is the resource name.
	Name string `json:"name"`
	// ChangeType is one of: Created | Updated | Deleted.
	ChangeType string `json:"changeType"`
	// ChangedFields is a best-effort list of mutated field paths, relayed from ArgoCD's diff.
	// +listType=atomic
	ChangedFields []string `json:"changedFields,omitempty"`
}

// DependencyRef describes a single external resource referenced by the application workloads.
type DependencyRef struct {
	// Kind is the resource kind (e.g. ConfigMap, Secret, DataVolume, NetworkAttachmentDefinition).
	Kind string `json:"kind"`
	// Name is the resource name.
	Name string `json:"name"`
	// Ready indicates whether the dependency exists and is in a usable state.
	Ready bool `json:"ready"`
	// Note carries optional human-readable context (e.g. "owned by a different Application: platform-secrets").
	Note string `json:"note,omitempty"`
}

// VMHealthStatus captures the runtime health of an OpenShift Virtualization VirtualMachine
// during a CHG window. A VM whose spec changed but whose VMI hasn't restarted is a
// common silent failure mode: ArgoCD shows Synced while the workload still runs the old config.
type VMHealthStatus struct {
	// Name is the VirtualMachine name.
	Name string `json:"name"`
	// Ready reflects VMI status.conditions[Ready].
	Ready bool `json:"ready"`
	// LiveMigratable reflects VMI status.conditions[LiveMigratable].
	LiveMigratable bool `json:"liveMigratable"`
	// RestartRequired is true when VM status.conditions[RestartRequired] is True —
	// meaning the running VMI has not yet picked up a spec change.
	RestartRequired bool `json:"restartRequired"`
	// DataVolumesBound is true when all referenced DataVolumes have phase == Succeeded.
	DataVolumesBound bool `json:"dataVolumesBound"`
	// ActiveMigration is the name of any in-flight VirtualMachineInstanceMigration, empty otherwise.
	ActiveMigration string `json:"activeMigration,omitempty"`
}

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

	// RecentEvents is a de-duplicated list of Warning events collected from the
	// application namespace. Only populated when the app is OutOfSync or Degraded.
	// +listType=atomic
	RecentEvents []EventSummary `json:"recentEvents,omitempty"`

	// TailLogs contains the last N log lines from non-Ready pods in the application.
	// Only populated when the app is OutOfSync or Degraded.
	// +listType=atomic
	TailLogs []string `json:"tailLogs,omitempty"`

	// ObjectChanges summarises resources touched by the most recent ArgoCD sync.
	// +listType=atomic
	ObjectChanges []ObjectChangeSummary `json:"objectChanges,omitempty"`

	// Dependencies lists external resources that the application references and their readiness.
	// +listType=atomic
	Dependencies []DependencyRef `json:"dependencies,omitempty"`

	// VMStatus contains OpenShift Virtualization health checks for VM-backed workloads.
	// +listType=atomic
	VMStatus []VMHealthStatus `json:"vmStatus,omitempty"`
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

// ClusterRevisionState represents the aggregated state for one cluster within a PropagationStatus.
// The hub-side aggregator copies observable fields from ClusterAppReport so that
// ChangeWindowReconciler has a single place to read all per-cluster data.
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

	// Spoke-collected observability data relayed by the propagation aggregator.

	// VMStatus contains OpenShift Virtualization health checks. A non-empty
	// RestartRequired or ActiveMigration blocks the hub validation gate.
	// +listType=atomic
	VMStatus []VMHealthStatus `json:"vmStatus,omitempty"`

	// RecentEvents is a de-duplicated list of Warning events from the app namespace.
	// +listType=atomic
	RecentEvents []EventSummary `json:"recentEvents,omitempty"`

	// ObjectChanges lists resources touched by the last ArgoCD sync.
	// +listType=atomic
	ObjectChanges []ObjectChangeSummary `json:"objectChanges,omitempty"`

	// Dependencies lists external resource readiness checks.
	// +listType=atomic
	Dependencies []DependencyRef `json:"dependencies,omitempty"`
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
	EvidenceRepoURL             string            `json:"evidenceRepoURL,omitempty"` // e.g., https://nexus.company.com:8081/repository/gitops-evidence
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

// ValidationResult captures the outcome of post-CHG validation across all gates.
// Passed is the logical AND of all boolean gate fields.
type ValidationResult struct {
	// AllChangesApplied is true when every target cluster is InSync.
	AllChangesApplied bool `json:"allChangesApplied"`
	// HealthCheckPassed is true when all clusters report Healthy.
	HealthCheckPassed bool `json:"healthCheckPassed"`
	// MCPUpdatedOnTime is true when no MachineConfigPool is still Updating or Degraded.
	MCPUpdatedOnTime bool `json:"mcpUpdatedOnTime"`
	// EventsClean is true when no unresolved Warning events exist since the window opened.
	EventsClean bool `json:"eventsClean"`
	// ObjectsConverged is true when ArgoCD's resource list reports all objects Synced/Healthy.
	ObjectsConverged bool `json:"objectsConverged"`
	// DependenciesReady is true when all referenced ConfigMaps/Secrets/DataVolumes are ready.
	DependenciesReady bool `json:"dependenciesReady"`
	// VMChecksPassed is true when no VM has RestartRequired=true or an in-flight migration.
	VMChecksPassed bool `json:"vmChecksPassed"`
	// IssuesFound is a human-readable list of the failing checks.
	// +listType=atomic
	IssuesFound []string `json:"issuesFound,omitempty"`
	// Passed is the AND of all gate fields above.
	Passed bool `json:"passed"`
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
