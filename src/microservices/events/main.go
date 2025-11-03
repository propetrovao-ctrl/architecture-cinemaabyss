package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
)

// Event models based on API specification
type MovieEvent struct {
	MovieID     int      `json:"movie_id"`
	Title       string   `json:"title"`
	Action      string   `json:"action"`
	UserID      *int     `json:"user_id,omitempty"`
	Rating      *float64 `json:"rating,omitempty"`
	Genres      []string `json:"genres,omitempty"`
	Description *string  `json:"description,omitempty"`
}

type UserEvent struct {
	UserID    int     `json:"user_id"`
	Action    string  `json:"action"`
	Timestamp string  `json:"timestamp"`
	Username  *string `json:"username,omitempty"`
	Email     *string `json:"email,omitempty"`
}

type PaymentEvent struct {
	PaymentID int     `json:"payment_id"`
	UserID    int     `json:"user_id"`
	Amount    float64 `json:"amount"`
	Status    string  `json:"status"`
	Timestamp string  `json:"timestamp"`
	MethodType *string `json:"method_type,omitempty"`
}

type EventResponse struct {
	Status   string      `json:"status"`
	Partition int        `json:"partition"`
	Offset   int64       `json:"offset"`
	Event    interface{} `json:"event"`
}

type EventWrapper struct {
	ID        string      `json:"id"`
	Type      string      `json:"type"`
	Timestamp string      `json:"timestamp"`
	Payload   interface{} `json:"payload"`
}

var (
	kafkaWriter *kafka.Writer
	ctx         context.Context
	cancel      context.CancelFunc
	wg          sync.WaitGroup
)

func main() {
	// Инициализация контекста
	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()

	// Получаем адрес Kafka из переменных окружения
	kafkaBroker := os.Getenv("KAFKA_BROKERS")
	if kafkaBroker == "" {
		kafkaBroker = "kafka:9092"
	}

	// Создаем Kafka writer для отправки сообщений
	kafkaWriter = &kafka.Writer{
		Addr:     kafka.TCP(kafkaBroker),
		Balancer: &kafka.LeastBytes{},
	}
	defer kafkaWriter.Close()

	// Запускаем consumers для каждого топика в отдельных горутинах
	topics := map[string]string{
		"movie-events":   "movie",
		"user-events":    "user",
		"payment-events": "payment",
	}

	for topic, eventType := range topics {
		wg.Add(1)
		go startConsumer(ctx, kafkaBroker, topic, eventType)
	}

	// Настройка HTTP маршрутов
	http.HandleFunc("/api/events/health", healthHandler)
	http.HandleFunc("/api/events/movie", handleMovieEvent)
	http.HandleFunc("/api/events/user", handleUserEvent)
	http.HandleFunc("/api/events/payment", handlePaymentEvent)

	// Запуск HTTP сервера
	port := os.Getenv("PORT")
	if port == "" {
		port = "8082"
	}

	log.Printf("Starting events service on port %s", port)
	log.Printf("Kafka broker: %s", kafkaBroker)

	// Обработка сигналов для graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		if err := http.ListenAndServe(":"+port, nil); err != nil {
			log.Fatalf("HTTP server error: %v", err)
		}
	}()

	// Ожидание сигнала завершения
	<-sigChan
	log.Println("Shutting down...")
	cancel()
	wg.Wait()
	log.Println("Shutdown complete")
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]bool{"status": true})
}

func handleMovieEvent(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var event MovieEvent
	if err := json.NewDecoder(r.Body).Decode(&event); err != nil {
		http.Error(w, fmt.Sprintf("Invalid request body: %v", err), http.StatusBadRequest)
		return
	}

	// Создаем обертку события
	eventWrapper := EventWrapper{
		ID:        fmt.Sprintf("movie-%d-%s", event.MovieID, event.Action),
		Type:      "movie",
		Timestamp: time.Now().UTC().Format(time.RFC3339),
		Payload:   event,
	}

	// Отправляем в Kafka
	partition, offset, err := sendToKafka("movie-events", eventWrapper)
	if err != nil {
		log.Printf("Error sending movie event to Kafka: %v", err)
		http.Error(w, fmt.Sprintf("Failed to send event: %v", err), http.StatusInternalServerError)
		return
	}

	// Возвращаем ответ
	response := EventResponse{
		Status:    "success",
		Partition: partition,
		Offset:    offset,
		Event:     eventWrapper,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(response)
}

func handleUserEvent(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var event UserEvent
	if err := json.NewDecoder(r.Body).Decode(&event); err != nil {
		http.Error(w, fmt.Sprintf("Invalid request body: %v", err), http.StatusBadRequest)
		return
	}

	// Создаем обертку события
	eventWrapper := EventWrapper{
		ID:        fmt.Sprintf("user-%d-%s", event.UserID, event.Action),
		Type:      "user",
		Timestamp: event.Timestamp,
		Payload:   event,
	}

	// Отправляем в Kafka
	partition, offset, err := sendToKafka("user-events", eventWrapper)
	if err != nil {
		log.Printf("Error sending user event to Kafka: %v", err)
		http.Error(w, fmt.Sprintf("Failed to send event: %v", err), http.StatusInternalServerError)
		return
	}

	// Возвращаем ответ
	response := EventResponse{
		Status:    "success",
		Partition: partition,
		Offset:    offset,
		Event:     eventWrapper,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(response)
}

func handlePaymentEvent(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var event PaymentEvent
	if err := json.NewDecoder(r.Body).Decode(&event); err != nil {
		http.Error(w, fmt.Sprintf("Invalid request body: %v", err), http.StatusBadRequest)
		return
	}

	// Создаем обертку события
	eventWrapper := EventWrapper{
		ID:        fmt.Sprintf("payment-%d-%s", event.PaymentID, event.Status),
		Type:      "payment",
		Timestamp: event.Timestamp,
		Payload:   event,
	}

	// Отправляем в Kafka
	partition, offset, err := sendToKafka("payment-events", eventWrapper)
	if err != nil {
		log.Printf("Error sending payment event to Kafka: %v", err)
		http.Error(w, fmt.Sprintf("Failed to send event: %v", err), http.StatusInternalServerError)
		return
	}

	// Возвращаем ответ
	response := EventResponse{
		Status:    "success",
		Partition: partition,
		Offset:    offset,
		Event:     eventWrapper,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(response)
}

func sendToKafka(topic string, event EventWrapper) (int, int64, error) {
	eventJSON, err := json.Marshal(event)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to marshal event: %w", err)
	}

	message := kafka.Message{
		Topic: topic,
		Key:   []byte(event.ID),
		Value: eventJSON,
	}

	err = kafkaWriter.WriteMessages(ctx, message)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to write message: %w", err)
	}

	log.Printf("Sent event to Kafka topic %s: ID=%s, Type=%s", topic, event.ID, event.Type)

	// В segmentio/kafka-go Writer не возвращает partition и offset напрямую
	// Эти значения будут доступны в consumer при чтении сообщения
	// Для MVP возвращаем 0, так как основная информация будет в логах consumer
	return 0, 0, nil
}

func startConsumer(ctx context.Context, broker, topic, eventType string) {
	defer wg.Done()

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{broker},
		Topic:    topic,
		GroupID:  "events-service-consumer",
		MinBytes: 10e3, // 10KB
		MaxBytes: 10e6, // 10MB
	})
	defer reader.Close()

	log.Printf("Started consumer for topic: %s (type: %s)", topic, eventType)

	for {
		select {
		case <-ctx.Done():
			log.Printf("Stopping consumer for topic: %s", topic)
			return
		default:
			message, err := reader.ReadMessage(ctx)
			if err != nil {
				log.Printf("Error reading message from %s: %v", topic, err)
				time.Sleep(time.Second)
				continue
			}

			// Парсим событие
			var eventWrapper EventWrapper
			if err := json.Unmarshal(message.Value, &eventWrapper); err != nil {
				log.Printf("Error unmarshaling event from %s: %v", topic, err)
				continue
			}

			// Логируем обработанное событие
			log.Printf("[%s] Processed event: ID=%s, Type=%s, Partition=%d, Offset=%d, Payload=%s",
				eventType,
				eventWrapper.ID,
				eventWrapper.Type,
				message.Partition,
				message.Offset,
				string(message.Value),
			)
		}
	}
}

