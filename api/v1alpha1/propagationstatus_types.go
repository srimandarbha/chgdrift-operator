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



// GateStatus represents tri-state gate evaluation: True (Passed), False (Failed), Unknown.
type GateStatus string

const (
	GateStatusTrue    GateStatus = "True"
	GateStatusFalse   GateStatus = "False"
	GateStatusUnknown GateStatus = "Unknown"
)

// EvidenceRef references immutable evidence captured during a change window.
type EvidenceRef struct {
	Source       string      `json:"source"`
	URI          string      `json:"uri,omitempty"`
	SHA256       string      `json:"sha256,omitempty"`
	CapturedAt   metav1.Time `json:"capturedAt"`
	WindowStart  metav1.Time `json:"windowStart,omitempty"`
	WindowEnd    metav1.Time `json:"windowEnd,omitempty"`
	ContentType  string      `json:"contentType,omitempty"`
	Query        string      `json:"query,omitempty"`
	CollectionID string      `json:"collectionID,omitempty"`
}

// GateResult provides tri-state assessment for an individual validation gate.
type GateResult struct {
	Name       string      `json:"name"`
	Status     GateStatus  `json:"status"` // True | False | Unknown
	Reason     string      `json:"reason,omitempty"`
	Message    string      `json:"message,omitempty"`
	ObservedAt metav1.Time `json:"observedAt"`
	// +listType=atomic
	Evidence []EvidenceRef `json:"evidence,omitempty"`
}

// VirtualizationImpactStatus captures OpenShift Virtualization platform health.
type VirtualizationImpactStatus struct {
	HyperConvergedHealth string `json:"hyperConvergedHealth,omitempty"` // Healthy | Degraded | Unknown
	VirtHandlerReady     bool   `json:"virtHandlerReady,omitempty"`
	ActiveMigrations     int32  `json:"activeMigrations,omitempty"`
	StalledMigrations    int32  `json:"stalledMigrations,omitempty"`
	UnmigratableVMIs     int32  `json:"unmigratableVMIs,omitempty"`
}

// MachineConfigPoolStatus captures OpenShift MCO pool state.
type MachineConfigPoolStatus struct {
	Name                  string             `json:"name,omitempty"` // e.g., worker, master, virt
	MachineCount          int32              `json:"machineCount,omitempty"`
	ReadyMachineCount     int32              `json:"readyMachineCount,omitempty"`
	UpdatedNodeCount      int32              `json:"updatedNodeCount,omitempty"`
	UpdatingNodeCount     int32              `json:"updatingNodeCount,omitempty"`
	UnavailableNodeCount  int32              `json:"unavailableNodeCount,omitempty"`
	DegradedNodeCount     int32              `json:"degradedNodeCount,omitempty"`
	CurrentRenderedConfig string             `json:"currentRenderedConfig,omitempty"`
	DesiredRenderedConfig string             `json:"desiredRenderedConfig,omitempty"`
	Paused                bool               `json:"paused,omitempty"`
	Phase                 string             `json:"phase,omitempty"` // Updated | Updating | Degraded | Unknown
	Conditions            []metav1.Condition `json:"conditions,omitempty"`
}

// ClusterOperatorStatus captures OpenShift platform operator health.
type ClusterOperatorStatus struct {
	Name        string `json:"name"`
	Available   bool   `json:"available"`
	Degraded    bool   `json:"degraded"`
	Progressing bool   `json:"progressing"`
	Version     string `json:"version,omitempty"`
}

// VirtualizationWorkloadSummary captures VMI counts and migration metrics.
type VirtualizationWorkloadSummary struct {
	TotalVMIs          int32 `json:"totalVMIs,omitempty"`
	RunningVMIs        int32 `json:"runningVMIs,omitempty"`
	LiveMigratableVMIs int32 `json:"liveMigratableVMIs,omitempty"`
	ActiveMigrations   int32 `json:"activeMigrations,omitempty"`
	StalledMigrations  int32 `json:"stalledMigrations,omitempty"`
	UnmigratableVMIs   int32 `json:"unmigratableVMIs,omitempty"`
}

// EvidenceSeverity describes the severity of correlated maintenance signals.
type EvidenceSeverity string

const (
	SeverityInfo     EvidenceSeverity = "INFO"
	SeverityWarning  EvidenceSeverity = "WARNING"
	SeverityCritical EvidenceSeverity = "CRITICAL"
)

// CorrelatedEvidence correlates K8s API objects, warning events, and live pod logs.
type CorrelatedEvidence struct {
	Timestamp metav1.Time      `json:"timestamp"`
	Component string           `json:"component"` // e.g. VirtualMachineInstanceMigration, virt-handler
	ObjectID  string           `json:"objectId"`
	EventType string           `json:"eventType"` // e.g. VMIStalled, StorageTargetNotReady, MultusNetworkAttachmentFailed
	Message   string           `json:"message"`
	Severity  EvidenceSeverity `json:"severity"`
	Source    string           `json:"source"`    // K8sAPI | EventLog | PodLog
}

// ResourceNodeStatus describes a single node in a Topological DAG evaluation.
type ResourceNodeStatus struct {
	ID        string   `json:"id"`
	Kind      string   `json:"kind"`
	Namespace string   `json:"namespace,omitempty"`
	Name      string   `json:"name"`
	State     string   `json:"state"` // Healthy | Degraded | Updating | Blocked
	// +listType=atomic
	ParentIDs []string `json:"parentIds,omitempty"`
	BlockedBy string   `json:"blockedBy,omitempty"`
}

// TopologicalDAGResult encapsulates topological dependency graph evaluation.
type TopologicalDAGResult struct {
	Evaluated     bool                 `json:"evaluated"`
	Healthy       bool                 `json:"healthy"`
	BlockedReason string               `json:"blockedReason,omitempty"`
	// +listType=atomic
	Nodes         []ResourceNodeStatus `json:"nodes,omitempty"`
}

// CausalNode describes a single node in the platform dependency graph.
type CausalNode struct {
	Stage       string   `json:"stage"`                 // ClusterVersion | NodeConfig | ControlPlane | VirtOperators | WorkloadExecution | LiveMigration
	Resource    string   `json:"resource"`              // e.g. MachineConfigPool/worker, KubeVirt/kubevirt-kubevirt
	Status      string   `json:"status"`                // Healthy | Degraded | Updating | Unknown
	RootCauseOf []string `json:"rootCauseOf,omitempty"` // Downstream resources impacted
	ImpactedBy  string   `json:"impactedBy,omitempty"`  // Upstream cause
}

// DependencyGraphResult encapsulates causal dependency evaluation across the platform stack.
type DependencyGraphResult struct {
	Healthy           bool         `json:"healthy"`
	RootCauseResource string       `json:"rootCauseResource,omitempty"`
	RootCauseSummary  string       `json:"rootCauseSummary,omitempty"`
	// +listType=atomic
	Nodes             []CausalNode `json:"nodes,omitempty"`
}

// PlatformObservationStatus encapsulates infrastructure observation telemetry.
type PlatformObservationStatus struct {
	// +listType=atomic
	ClusterOperators []ClusterOperatorStatus       `json:"clusterOperators,omitempty"`
	// +listType=atomic
	MachineConfigPools []MachineConfigPoolStatus    `json:"machineConfigPools,omitempty"`
	VirtHealth         VirtualizationImpactStatus  `json:"virtHealth,omitempty"`
	VirtWorkloads      VirtualizationWorkloadSummary `json:"virtWorkloads,omitempty"`
	ClusterVersion     ClusterVersionStatus        `json:"clusterVersion,omitempty"`
	KubeVirt           KubeVirtOperatorStatus      `json:"kubeVirt,omitempty"`
	CDI                CDIOperatorStatus           `json:"cdi,omitempty"`
	SSP                SSPOperatorStatus           `json:"ssp,omitempty"`
	NodeMaintenance    NodeMaintenanceStatus       `json:"nodeMaintenance,omitempty"`
	// +listType=atomic
	MigrationPolicies  []MigrationPolicyStatus     `json:"migrationPolicies,omitempty"`
	DependencyGraph    DependencyGraphResult       `json:"dependencyGraph,omitempty"`
	TopologicalDAG     TopologicalDAGResult        `json:"topologicalDAG,omitempty"`
	// +listType=atomic
	CorrelatedEvidence []CorrelatedEvidence        `json:"correlatedEvidence,omitempty"`
	ObservedAt         metav1.Time                 `json:"observedAt,omitempty"`
}

// ClusterVersionStatus captures OpenShift ClusterVersion upgrade state.
type ClusterVersionStatus struct {
	// Version is the currently applied OCP version.
	Version string `json:"version,omitempty"`
	// DesiredVersion is the target version the cluster is upgrading to.
	DesiredVersion string `json:"desiredVersion,omitempty"`
	// Progressing is true when the cluster is actively upgrading.
	Progressing bool `json:"progressing,omitempty"`
	// Available is true when the cluster reports Available=True.
	Available bool `json:"available,omitempty"`
	// Channel is the update channel (e.g. stable-4.14).
	Channel string `json:"channel,omitempty"`
}

// KubeVirtOperatorStatus captures KubeVirt operator deployment health.
type KubeVirtOperatorStatus struct {
	// Phase is Deployed | Deploying | Deleting | "".
	Phase string `json:"phase,omitempty"`
	// TargetVersion is the desired KubeVirt version.
	TargetVersion string `json:"targetVersion,omitempty"`
	// ObservedVersion is the currently running version.
	ObservedVersion string `json:"observedVersion,omitempty"`
	// Ready is true when phase is Deployed and versions match.
	Ready bool `json:"ready,omitempty"`
}

// CDIOperatorStatus captures CDI operator deployment health.
type CDIOperatorStatus struct {
	Phase           string `json:"phase,omitempty"`
	TargetVersion   string `json:"targetVersion,omitempty"`
	ObservedVersion string `json:"observedVersion,omitempty"`
	Ready           bool   `json:"ready,omitempty"`
}

// SSPOperatorStatus captures SSP operator deployment health.
type SSPOperatorStatus struct {
	Phase           string `json:"phase,omitempty"`
	TargetVersion   string `json:"targetVersion,omitempty"`
	ObservedVersion string `json:"observedVersion,omitempty"`
	Ready           bool   `json:"ready,omitempty"`
}

// NodeMaintenanceStatus captures active NodeMaintenance objects.
type NodeMaintenanceStatus struct {
	ActiveMaintenanceNodes int32 `json:"activeMaintenanceNodes,omitempty"`
	// +listType=atomic
	MaintenanceNodeNames []string `json:"maintenanceNodeNames,omitempty"`
}

// MigrationPolicyStatus captures KubeVirt MigrationPolicy configuration.
type MigrationPolicyStatus struct {
	Name                  string `json:"name,omitempty"`
	BandwidthPerMigration string `json:"bandwidthPerMigration,omitempty"`
	AllowAutoConverge     bool   `json:"allowAutoConverge,omitempty"`
}

// BaselineSnapshot captures platform state at the start of a maintenance window.
type BaselineSnapshot struct {
	CapturedAt            metav1.Time `json:"capturedAt"`
	ClusterVersion        string      `json:"clusterVersion,omitempty"`
	MachineConfigPoolHash string      `json:"machineConfigPoolHash,omitempty"`
	KubeVirtVersion       string      `json:"kubeVirtVersion,omitempty"`
	CDIVersion            string      `json:"cdiVersion,omitempty"`
	SSPVersion            string      `json:"sspVersion,omitempty"`
	ClusterOperatorDigest string      `json:"clusterOperatorDigest,omitempty"`
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

	// AppNamespace is the target workload namespace on the spoke cluster.
	AppNamespace string `json:"appNamespace,omitempty"`

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

	// VirtStatus captures OpenShift Virtualization platform impact and workload health.
	VirtStatus VirtualizationImpactStatus `json:"virtStatus,omitempty"`

	// PlatformObservation captures infrastructure observation telemetry (ClusterOperators, Virt workloads).
	PlatformObservation PlatformObservationStatus `json:"platformObservation,omitempty"`

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
	AppNamespace                string   `json:"appNamespace,omitempty"`
	ExpectedRevision            string   `json:"expectedRevision"`
	TargetClusters              []string `json:"targetClusters"`
	LagThresholdSeconds         int32    `json:"lagThresholdSeconds,omitempty"`
	StaleReportThresholdSeconds int32    `json:"staleReportThresholdSeconds,omitempty"`
	ParentApp                   string   `json:"parentApp,omitempty"`
	RootApp                     string   `json:"rootApp,omitempty"`
}

// ClusterRevisionState represents the aggregated state for one cluster within a PropagationStatus.
// The peer-side aggregator or fleet aggregator copies observable fields from ClusterAppReport so that
// ChangeWindowReconciler has a single place to read all per-cluster data.
type ClusterRevisionState struct {
	ClusterName            string                     `json:"clusterName"`
	AppNamespace           string                     `json:"appNamespace,omitempty"`
	ObservedRevision       string                     `json:"observedRevision,omitempty"`
	SyncStatus             string                     `json:"syncStatus,omitempty"`
	Health                 string                     `json:"health,omitempty"`
	ObservedAt             metav1.Time                `json:"observedAt,omitempty"`
	SawReportSinceChgStart bool                       `json:"sawReportSinceChgStart,omitempty"`
	MCPStatus              MachineConfigPoolStatus    `json:"mcpStatus,omitempty"`
	VirtStatus             VirtualizationImpactStatus `json:"virtStatus,omitempty"`
	PlatformObservation    PlatformObservationStatus `json:"platformObservation,omitempty"`
	// State summarizes this row: InSync | Lagging | Diverged | Stale | Missing
	State string `json:"state"`

	// Spoke-collected observability data relayed by the propagation aggregator.

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
	TargetNamespaces            []string          `json:"targetNamespaces,omitempty"`
	StartTime                   metav1.Time       `json:"startTime"`
	EndTime                     metav1.Time       `json:"endTime"`
	StabilizationPeriodSeconds  int32             `json:"stabilizationPeriodSeconds,omitempty"`
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
// Passed is true ONLY when every mandatory gate evaluates to True and no gate is Unknown or False.
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
	// VirtImpactPassed is true when OpenShift Virtualization component health and live migration criteria are met.
	VirtImpactPassed bool `json:"virtImpactPassed"`
	// ClusterOperatorsHealthy is true when all OpenShift platform ClusterOperators are Available and non-degraded.
	ClusterOperatorsHealthy bool `json:"clusterOperatorsHealthy"`
	// GateResults carries structured tri-state (True/False/Unknown) assessment per validation gate.
	// +listType=atomic
	GateResults []GateResult `json:"gateResults,omitempty"`
	// IssuesFound is a human-readable list of the failing checks.
	// +listType=atomic
	IssuesFound []string `json:"issuesFound,omitempty"`
	// Passed is true only when every mandatory gate is True and zero gates are Unknown/False.
	Passed bool `json:"passed"`
}

// TimelineEntry records a single state transition or evidence event during a maintenance window.
type TimelineEntry struct {
	Timestamp    metav1.Time `json:"timestamp"`
	Stage        string      `json:"stage"`        // PreChecking | InProgress | Stabilizing
	Category     string      `json:"category"`     // Platform | Workload | Event | Log
	Resource     string      `json:"resource"`     // e.g. NodeMaintenance/node-01
	Event        string      `json:"event"`        // Created | Updated | Degraded | Recovered
	Description  string      `json:"description"`  // Human & LLM readable summary
	CausalImpact string      `json:"causalImpact,omitempty"`
}

type ChangeWindowStatus struct {
	Phase                  string                        `json:"phase,omitempty"`
	OverallStatus          string                        `json:"overallStatus,omitempty"`
	SilentClusters         []SilentClusterState          `json:"silentClusters,omitempty"`
	Actions                map[string]ActionRecord       `json:"actions,omitempty"`
	Validation             ValidationResult              `json:"validation,omitempty"`
	StabilizationStartedAt *metav1.Time                  `json:"stabilizationStartedAt,omitempty"`
	LastReportedAt         metav1.Time                   `json:"lastReportedAt,omitempty"`
	AppStates              map[string]AppClusterStateMap `json:"appStates,omitempty"`
	Baseline               *BaselineSnapshot             `json:"baseline,omitempty"`
	// +listType=atomic
	Timeline               []TimelineEntry               `json:"timeline,omitempty"`
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
