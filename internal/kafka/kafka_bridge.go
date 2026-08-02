package kafka

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	gitopsv1alpha1 "example.com/drift-operator/api/v1alpha1"
)

type KafkaConfig struct {
	Brokers          []string
	IngestTopic      string
	EmitTopic        string
	SecurityProtocol string
	CAFilePath       string
	ClientCertPath   string
	ClientKeyPath    string
	ServerCN         string
}

type KafkaBridge struct {
	Writer      *kafka.Writer
	Reader      *kafka.Reader
	Client      client.Client
	Namespace   string
	ClusterName string
}

func NewKafkaTLSConfig(caCertPath, clientCertPath, clientKeyPath, serverCN string) (*tls.Config, error) {
	if caCertPath == "" {
		return nil, nil
	}

	caCert, err := os.ReadFile(caCertPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read CA cert from %s: %w", caCertPath, err)
	}

	caCertPool := x509.NewCertPool()
	if !caCertPool.AppendCertsFromPEM(caCert) {
		return nil, fmt.Errorf("failed to parse PEM CA certificate")
	}

	tlsConfig := &tls.Config{
		RootCAs:            caCertPool,
		ServerName:         serverCN,
		InsecureSkipVerify: false,
	}

	if clientCertPath != "" && clientKeyPath != "" {
		cert, err := tls.LoadX509KeyPair(clientCertPath, clientKeyPath)
		if err != nil {
			return nil, fmt.Errorf("failed to load client cert keypair: %w", err)
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}

	return tlsConfig, nil
}

func NewKafkaBridge(cfg KafkaConfig, k8sClient client.Client, namespace string) (*KafkaBridge, error) {
	tlsConfig, err := NewKafkaTLSConfig(cfg.CAFilePath, cfg.ClientCertPath, cfg.ClientKeyPath, cfg.ServerCN)
	if err != nil {
		return nil, fmt.Errorf("setting up Kafka TLS: %w", err)
	}

	var dialer *kafka.Dialer
	if tlsConfig != nil {
		dialer = &kafka.Dialer{
			Timeout:   10 * time.Second,
			DualStack: true,
			TLS:       tlsConfig,
		}
	}

	writer := &kafka.Writer{
		Addr:                   kafka.TCP(cfg.Brokers...),
		Topic:                  cfg.EmitTopic,
		Balancer:               &kafka.LeastBytes{},
		RequiredAcks:           kafka.RequireAll,
		Async:                  false,
		AllowAutoTopicCreation: true,
	}
	if dialer != nil {
		writer.Transport = &kafka.Transport{
			TLS: dialer.TLS,
		}
	}

	var reader *kafka.Reader
	if cfg.IngestTopic != "" {
		readerConfig := kafka.ReaderConfig{
			Brokers:  cfg.Brokers,
			GroupID:  "drift-operator-group",
			Topic:    cfg.IngestTopic,
			MinBytes: 10,
			MaxBytes: 1024 * 1024,
			Dialer:   dialer,
		}
		reader = kafka.NewReader(readerConfig)
	}

	clusterName := os.Getenv("CLUSTER_NAME")
	return &KafkaBridge{
		Writer:      writer,
		Reader:      reader,
		Client:      k8sClient,
		Namespace:   namespace,
		ClusterName: clusterName,
	}, nil
}

func isClusterTargeted(clusterName string, targetClusters []string) bool {
	if clusterName == "" || strings.EqualFold(clusterName, "hub") || len(targetClusters) == 0 {
		return true
	}
	for _, tc := range targetClusters {
		if tc == "*" || strings.EqualFold(tc, clusterName) {
			return true
		}
	}
	return false
}

func (kb *KafkaBridge) ProduceReport(ctx context.Context, chgNumber string, payload []byte) error {
	msg := kafka.Message{
		Key:   []byte(chgNumber),
		Value: payload,
		Time:  time.Now(),
	}
	return kb.Writer.WriteMessages(ctx, msg)
}

type IngestEventPayload struct {
	EventType  string `json:"eventType"`
	CHGDetails struct {
		CHGNumber                   string      `json:"chgNumber"`
		StartTime                   metav1.Time `json:"startTime"`
		EndTime                     metav1.Time `json:"endTime"`
		StaleReportThresholdSeconds int32       `json:"staleReportThresholdSeconds"`
	} `json:"chgDetails"`
	GitDetails struct {
		ReleaseTag       string `json:"releaseTag"`
		ExpectedRevision string `json:"expectedRevision"`
		BaselineRevision string `json:"baselineRevision"`
	} `json:"gitDetails"`
	BlastRadius struct {
		RootApp          string            `json:"rootApp"`
		ImpactedApps     []string          `json:"impactedApps"`
		TargetNamespaces []string          `json:"targetNamespaces,omitempty"`
		AppNamespaces    map[string]string `json:"appNamespaces,omitempty"`
		TargetClusters   []string          `json:"targetClusters"`
	} `json:"blastRadius"`
}

// Start implements manager.Runnable. It runs only when the instance is the elected leader.
func (kb *KafkaBridge) Start(ctx context.Context) error {
	defer func() {
		if kb.Reader != nil {
			_ = kb.Reader.Close()
		}
		if kb.Writer != nil {
			_ = kb.Writer.Close()
		}
	}()

	if kb.Reader == nil {
		<-ctx.Done()
		return nil
	}

	logger := log.Log.WithName("kafka-consumer")
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			msg, err := kb.Reader.FetchMessage(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return nil
				}
				logger.Error(err, "failed to fetch Kafka message; retrying in 2s")
				select {
				case <-ctx.Done():
					return nil
				case <-time.After(2 * time.Second):
				}
				continue
			}

			var payload IngestEventPayload
			if err := json.Unmarshal(msg.Value, &payload); err != nil {
				logger.Error(err, "failed to unmarshal Kafka message; skipping")
				if cerr := kb.Reader.CommitMessages(ctx, msg); cerr != nil {
					logger.Error(cerr, "failed to commit malformed Kafka message offset")
				}
				continue
			}

			if payload.CHGDetails.CHGNumber == "" {
				logger.Info("Kafka message missing chgNumber; skipping")
				if cerr := kb.Reader.CommitMessages(ctx, msg); cerr != nil {
					logger.Error(cerr, "failed to commit empty Kafka message offset")
				}
				continue
			}

			if !isClusterTargeted(kb.ClusterName, payload.BlastRadius.TargetClusters) {
				logger.V(1).Info("Local cluster not targeted in CHG blastRadius; skipping ChangeWindow creation",
					"cluster", kb.ClusterName, "chg", payload.CHGDetails.CHGNumber)
				if cerr := kb.Reader.CommitMessages(ctx, msg); cerr != nil {
					logger.Error(cerr, "failed to commit offset for untargeted CHG event")
				}
				continue
			}

			chg := &gitopsv1alpha1.ChangeWindow{
				ObjectMeta: metav1.ObjectMeta{
					Name:      strings.ToLower(payload.CHGDetails.CHGNumber),
					Namespace: kb.Namespace,
				},
				Spec: gitopsv1alpha1.ChangeWindowSpec{
					CHGNumber:                   payload.CHGDetails.CHGNumber,
					ReleaseTag:                  payload.GitDetails.ReleaseTag,
					BaselineRevision:            payload.GitDetails.BaselineRevision,
					ExpectedRevision:            payload.GitDetails.ExpectedRevision,
					RootApp:                     payload.BlastRadius.RootApp,
					ImpactedApps:                payload.BlastRadius.ImpactedApps,
					TargetNamespaces:            payload.BlastRadius.TargetNamespaces,
					StartTime:                   payload.CHGDetails.StartTime,
					EndTime:                     payload.CHGDetails.EndTime,
					StaleReportThresholdSeconds: payload.CHGDetails.StaleReportThresholdSeconds,
				},
			}

			created := false
			for !created {
				if err := kb.Client.Create(ctx, chg); err != nil {
					if apierrors.IsAlreadyExists(err) {
						logger.V(1).Info("ChangeWindow already exists; duplicate Kafka delivery",
							"chg", payload.CHGDetails.CHGNumber)
						created = true
					} else {
						logger.Error(err, "failed to create ChangeWindow from Kafka message; retrying in 2s",
							"chg", payload.CHGDetails.CHGNumber)
						select {
						case <-ctx.Done():
							return nil
						case <-time.After(2 * time.Second):
						}
					}
				} else {
					logger.Info("ChangeWindow created from Kafka event", "chg", payload.CHGDetails.CHGNumber)
					created = true
				}
			}

			if cerr := kb.Reader.CommitMessages(ctx, msg); cerr != nil {
				logger.Error(cerr, "failed to commit Kafka offset after successful Create",
					"chg", payload.CHGDetails.CHGNumber)
			}
		}
	}
}
