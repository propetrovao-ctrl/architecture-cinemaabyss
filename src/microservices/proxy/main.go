package main

import (
	"encoding/json"
	"io"
	"log"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

type ProxyConfig struct {
	MonolithURL           string
	MoviesServiceURL      string
	EventsServiceURL      string
	GradualMigration      bool
	MoviesMigrationPercent int
}

var config ProxyConfig
var rng *rand.Rand

func main() {
	// Инициализация конфигурации из переменных окружения
	loadConfig()

	// Инициализация генератора случайных чисел
	rng = rand.New(rand.NewSource(time.Now().UnixNano()))

	// Настройка HTTP маршрутов
	http.HandleFunc("/health", healthHandler)
	http.HandleFunc("/api/", apiHandler)

	// Запуск сервера
	port := os.Getenv("PORT")
	if port == "" {
		port = "8000"
	}
	log.Printf("Starting proxy service on port %s", port)
	log.Printf("Monolith URL: %s", config.MonolithURL)
	log.Printf("Movies Service URL: %s", config.MoviesServiceURL)
	log.Printf("Gradual Migration: %v, Percent: %d%%", config.GradualMigration, config.MoviesMigrationPercent)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}

func loadConfig() {
	config.MonolithURL = getEnv("MONOLITH_URL", "http://monolith:8080")
	config.MoviesServiceURL = getEnv("MOVIES_SERVICE_URL", "http://movies-service:8081")
	config.EventsServiceURL = getEnv("EVENTS_SERVICE_URL", "http://events-service:8082")
	
	gradualMigrationStr := getEnv("GRADUAL_MIGRATION", "true")
	config.GradualMigration = gradualMigrationStr == "true"
	
	migrationPercentStr := getEnv("MOVIES_MIGRATION_PERCENT", "50")
	var err error
	config.MoviesMigrationPercent, err = strconv.Atoi(migrationPercentStr)
	if err != nil || config.MoviesMigrationPercent < 0 || config.MoviesMigrationPercent > 100 {
		log.Printf("Invalid MOVIES_MIGRATION_PERCENT, defaulting to 50")
		config.MoviesMigrationPercent = 50
	}
}

func getEnv(key, defaultValue string) string {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	return value
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"status": "healthy",
		"service": "proxy-service",
	})
}

func apiHandler(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path
	
	log.Printf("Incoming request: %s %s", r.Method, path)

	// Определяем целевой сервис на основе пути
	var targetURL string

	if strings.HasPrefix(path, "/api/movies") {
		// Балансировка для movies между монолитом и movies-service
		if config.GradualMigration && shouldRouteToMoviesService() {
			targetURL = config.MoviesServiceURL
			log.Printf("Routing to movies-service: %s", path)
		} else {
			targetURL = config.MonolithURL
			log.Printf("Routing to monolith: %s", path)
		}
	} else if strings.HasPrefix(path, "/api/events") {
		// События направляем в events-service
		targetURL = config.EventsServiceURL
		log.Printf("Routing to events-service: %s", path)
	} else {
		// Все остальные запросы направляем в монолит
		targetURL = config.MonolithURL
		log.Printf("Routing to monolith: %s", path)
	}

	// Проксируем запрос
	proxyRequest(w, r, targetURL)
}

func shouldRouteToMoviesService() bool {
	// Генерируем случайное число от 0 до 99
	random := rng.Intn(100)
	return random < config.MoviesMigrationPercent
}

func proxyRequest(w http.ResponseWriter, r *http.Request, targetBaseURL string) {
	// Парсим базовый URL
	targetURL, err := url.Parse(targetBaseURL)
	if err != nil {
		http.Error(w, "Invalid target URL", http.StatusInternalServerError)
		return
	}

	// Копируем исходный URL и заменяем хост
	proxyURL := *r.URL
	proxyURL.Scheme = targetURL.Scheme
	proxyURL.Host = targetURL.Host

	// Создаем новый запрос
	proxyReq, err := http.NewRequest(r.Method, proxyURL.String(), r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Копируем заголовки
	for key, values := range r.Header {
		for _, value := range values {
			proxyReq.Header.Add(key, value)
		}
	}

	// Устанавливаем заголовок X-Forwarded-For для отслеживания исходного клиента
	if clientIP := r.RemoteAddr; clientIP != "" {
		proxyReq.Header.Set("X-Forwarded-For", clientIP)
	}

	// Выполняем запрос
	client := &http.Client{
		Timeout: 30 * time.Second,
	}

	resp, err := client.Do(proxyReq)
	if err != nil {
		log.Printf("Error proxying request: %v", err)
		http.Error(w, "Service unavailable", http.StatusServiceUnavailable)
		return
	}
	defer resp.Body.Close()

	// Копируем заголовки ответа
	for key, values := range resp.Header {
		for _, value := range values {
			w.Header().Add(key, value)
		}
	}

	// Устанавливаем статус код
	w.WriteHeader(resp.StatusCode)

	// Копируем тело ответа
	_, err = io.Copy(w, resp.Body)
	if err != nil {
		log.Printf("Error copying response body: %v", err)
	}
}

