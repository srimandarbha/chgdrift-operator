package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
)

var (
	ClusterStateGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "gitops_propagation_cluster_state",
			Help: "1 if the cluster is in this state for this app, 0 otherwise.",
		},
		[]string{"app", "cluster", "state"},
	)

	ClusterLagSecondsGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "gitops_propagation_lag_seconds",
			Help: "Seconds this cluster has been trailing expected_revision.",
		},
		[]string{"app", "cluster"},
	)

	ClusterReportAgeGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "gitops_propagation_report_age_seconds",
			Help: "Seconds since the agent in this cluster captured its last report.",
		},
		[]string{"app", "cluster"},
	)
)

func init() {
	metrics.Registry.MustRegister(
		ClusterStateGauge,
		ClusterLagSecondsGauge,
		ClusterReportAgeGauge,
	)
}

func RecordClusterMetrics(app, cluster, currentState string, isInSync bool, lagSeconds, reportAgeSeconds float64) {
	possibleStates := []string{"InSync", "Propagating", "Lagging", "Diverged", "Stale", "Missing"}
	for _, st := range possibleStates {
		v := 0.0
		if st == currentState {
			v = 1.0
		}
		ClusterStateGauge.WithLabelValues(app, cluster, st).Set(v)
	}

	ClusterLagSecondsGauge.WithLabelValues(app, cluster).Set(lagSeconds)
	ClusterReportAgeGauge.WithLabelValues(app, cluster).Set(reportAgeSeconds)
}

// DeleteClusterMetrics removes gauge metrics for a deleted app/cluster combination.
func DeleteClusterMetrics(app, cluster string) {
	possibleStates := []string{"InSync", "Propagating", "Lagging", "Diverged", "Stale", "Missing"}
	for _, st := range possibleStates {
		ClusterStateGauge.DeleteLabelValues(app, cluster, st)
	}
	ClusterLagSecondsGauge.DeleteLabelValues(app, cluster)
	ClusterReportAgeGauge.DeleteLabelValues(app, cluster)
}
