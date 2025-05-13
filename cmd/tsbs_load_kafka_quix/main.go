package main

import (
	"bufio"
	"encoding/json"
	"encoding/base64"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type brokerConfig struct {
	BootstrapServers string `json:"bootstrap.servers"`
	SaslMechanism    string `json:"sasl.mechanism"`
	SaslUsername     string `json:"sasl.username"`
	SaslPassword     string `json:"sasl.password"`
	SecurityProtocol string `json:"security.protocol"`
	SSLCertBase64    string `json:"ssl.ca.cert"`

	cert []byte
}

type Quix struct {
	APIURL      string
	Workspace   string
	Topic       string
	Token       string

	producer    *kafka.Producer
	kafkaTopic  string


}

func (q *Quix) fetchBrokerConfig() (*brokerConfig, error) {
	endpoint := fmt.Sprintf("%s/workspaces/%s/broker/librdkafka", strings.TrimSuffix(q.APIURL, "/"), q.Workspace)
	req, err := http.NewRequest("GET", endpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("creating request failed: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+q.Token)
	req.Header.Set("Accept", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("executing request failed: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("reading body failed: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected response %q (%d): %s", http.StatusText(resp.StatusCode), resp.StatusCode, string(body))
	}

	var quixConfig brokerConfig
	if err := json.Unmarshal(body, &quixConfig); err != nil {
		return nil, fmt.Errorf("decoding body failed: %w", err)
	}

	cert, err := base64.StdEncoding.DecodeString(quixConfig.SSLCertBase64)
	if err != nil {
		return nil, fmt.Errorf("decoding certificate failed: %w", err)
	}
	quixConfig.cert = cert

	return &quixConfig, nil
}

func (q *Quix) Connect() error {
    quixConfig, err := q.fetchBrokerConfig()
	if err != nil {
		log.Fatalf("Failed to fetch broker kafkaConfig: %v", err)
	}

	kafkaConfig := kafka.ConfigMap{
		"bootstrap.servers":            quixConfig.BootstrapServers,
		"security.protocol":            quixConfig.SecurityProtocol,
		"sasl.username":                quixConfig.SaslUsername,
		"sasl.password":                quixConfig.SaslPassword,
		"sasl.mechanism":               quixConfig.SaslMechanism,
	}

    // Add the CA certificate sent by the server if there is any. Newer cloud
    // instances do not need this and we can go with the system certificates.
    if len(quixConfig.cert) > 0 {
        // Write to file
        certDir := filepath.Join(".", "certificates")
        os.MkdirAll(certDir, 0700)
        certPath := filepath.Join(certDir, "ca.pem")
        if err := os.WriteFile(certPath, quixConfig.cert, 0600); err != nil {
            return fmt.Errorf("failed to write CA cert: %w", err)
        }
        kafkaConfig["ssl.ca.location"] = certPath
    }

	producer, err := kafka.NewProducer(&kafkaConfig)
	if err != nil {
		log.Fatalf("Creating Kafka producer failed: %v", err)
	}
	q.producer = producer

	return nil
}

func main() {
	// Command-line args
	apiURL := flag.String("api-url", "https://portal-api.platform.quix.io", "Quix API base URL")
	workspace := flag.String("workspace", "", "Quix workspace ID")
	topic := flag.String("topic", "", "Kafka topic")
	token := flag.String("token", "", "Quix bearer token")
	flag.Parse()

	if *workspace == "" || *topic == "" || *token == "" {
		log.Fatal("--workspace, --topic, and --token are required")
	}

	quix := Quix{
		APIURL:       *apiURL,
		Workspace:    *workspace,
		Topic:        *topic,
		Token:        *token,
		kafkaTopic:   *workspace + "-" + *topic,
	}
    quix.Connect()

    producer := quix.producer
    defer producer.Close()
    kafkaTopic := quix.kafkaTopic
	scanner := bufio.NewScanner(os.Stdin)
	count := 0
	flushAt := 10000

	for scanner.Scan() {
		var jsonObj map[string]interface{}
		if err := json.Unmarshal(scanner.Bytes(), &jsonObj); err != nil {
			log.Printf("Skipping invalid JSON line: %v", err)
			continue
		}
		msgBytes, _ := json.Marshal(jsonObj)

		err := producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &kafkaTopic, Partition: kafka.PartitionAny},
			Value:          msgBytes,
		}, nil)
		if err != nil {
			log.Printf("Failed to produce message: %v", err)
		}

		count++
		if count == flushAt {
			producer.Flush(15000)
			count = 0
		}
	}

	if err := scanner.Err(); err != nil {
		log.Fatalf("Error reading stdin: %v", err)
	}
	producer.Flush(15000)
	fmt.Println("All messages flushed.")
}
