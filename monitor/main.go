package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

type Percentiles struct {
	P50 float64
	P90 float64
	P99 float64
}

type ResourceStats struct {
	Min   float64
	Max   float64
	Values []float64
}

func (r *ResourceStats) Add(value float64) {
	r.Values = append(r.Values, value)
	if len(r.Values) == 1 {
		r.Min = value
		r.Max = value
	} else {
		if value < r.Min {
			r.Min = value
		}
		if value > r.Max {
			r.Max = value
		}
	}
}

func (r *ResourceStats) Median() float64 {
	if len(r.Values) == 0 {
		return 0
	}
	sorted := make([]float64, len(r.Values))
	copy(sorted, r.Values)
	for i := 0; i < len(sorted)-1; i++ {
		for j := i + 1; j < len(sorted); j++ {
			if sorted[i] > sorted[j] {
				sorted[i], sorted[j] = sorted[j], sorted[i]
			}
		}
	}
	mid := len(sorted) / 2
	if len(sorted)%2 == 0 {
		return (sorted[mid-1] + sorted[mid]) / 2
	}
	return sorted[mid]
}

func (r *ResourceStats) Reset() {
	r.Values = nil
	r.Min = 0
	r.Max = 0
}

type ContainerStats struct {
	Port        int
	Count       float64
	Lag         float64
	Percentiles Percentiles
	CPU         string
	Memory      string
	CPUStats    ResourceStats
	MemStats    ResourceStats
	LagStats    ResourceStats
	Healthy     bool
}

type RunInfo struct {
	StartTime    time.Time
	EndTime      time.Time
	TotalMsgs    int64
	IsRunning    bool
	LastCount    float64
	PeakRate     float64
	AvgRate      float64
}

var currentRun RunInfo

type ContainerResourceHistory struct {
	CPU ResourceStats
	Mem ResourceStats
	Lag ResourceStats
}

var resourceHistory [6]ContainerResourceHistory

const (
	Reset   = "\033[0m"
	Bold    = "\033[1m"
	Italic  = "\033[3m"
	Red     = "\033[31m"
	Green   = "\033[32m"
	Yellow  = "\033[33m"
	Blue    = "\033[34m"
	Magenta = "\033[35m"
	Cyan    = "\033[36m"
	White   = "\033[37m"
	Gray    = "\033[90m"
	BgBlack = "\033[40m"
)

func main() {
	basePort := getEnvInt("BASE_PORT", 8080)
	numContainers := getEnvInt("NUM_CONTAINERS", 6)
	intervalSec := getEnvInt("INTERVAL_SEC", 2)

	client := &http.Client{Timeout: 3 * time.Second}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	fmt.Print(BgBlack)
	fmt.Print("\033[2J\033[H")

	ticker := time.NewTicker(time.Duration(intervalSec) * time.Second)
	defer ticker.Stop()

	prevTotal := 0.0
	cooldownUntil := time.Time{}

	for {
		select {
		case <-sigChan:
			fmt.Print(Reset)
			fmt.Println("\nShutting down monitor...")
			return
		case <-ticker.C:
			stats := collectStats(client, basePort, numContainers)

			totalCount := 0.0
			totalLag := 0.0
			activeContainers := 0

			for _, s := range stats {
				if s.Healthy {
					totalCount += s.Count
					totalLag += s.Lag
					activeContainers++
				}
			}

			now := time.Now()

			if totalCount > prevTotal && prevTotal == 0 && !currentRun.IsRunning {
				currentRun = RunInfo{
					StartTime: now,
					IsRunning: true,
					LastCount: totalCount,
				}
			}

			if currentRun.IsRunning {
				currentRun.TotalMsgs = int64(totalCount)
				currentRun.LastCount = totalCount

				rate := totalCount - prevTotal
				if rate > currentRun.PeakRate {
					currentRun.PeakRate = rate
				}
				elapsed := now.Sub(currentRun.StartTime).Seconds()
				if elapsed > 0 {
					currentRun.AvgRate = totalCount / elapsed
				}

				if totalCount == prevTotal && totalCount > 0 {
					if cooldownUntil.IsZero() {
						cooldownUntil = now.Add(5 * time.Second)
					} else if now.After(cooldownUntil) {
						currentRun.EndTime = now
						currentRun.IsRunning = false
						cooldownUntil = time.Time{}
					}
				} else {
					cooldownUntil = time.Time{}
				}
			}

			prevTotal = totalCount

			printDashboard(stats, totalCount, totalLag, int64(activeContainers))
		}
	}
}

func collectStats(client *http.Client, basePort, numContainers int) []ContainerStats {
	stats := make([]ContainerStats, numContainers)
	dockerStats := fetchDockerStats()

	for i := 0; i < numContainers; i++ {
		port := basePort + i
		stats[i] = ContainerStats{Port: port, Healthy: false}

		count, err := fetchMetric(client, port, "spring.kafka.listener", "COUNT")
		if err != nil {
			continue
		}
		stats[i].Count = count
		stats[i].Healthy = true

		lag, _ := fetchKafkaLag(client, port)
		stats[i].Lag = lag
		resourceHistory[i].Lag.Add(lag)
		stats[i].LagStats = resourceHistory[i].Lag

		p50, _ := fetchPercentile(client, port, "0.5")
		p90, _ := fetchPercentile(client, port, "0.9")
		p99, _ := fetchPercentile(client, port, "0.99")

		stats[i].Percentiles = Percentiles{P50: p50, P90: p90, P99: p99}

		containerName := fmt.Sprintf("kafka-consumer-high-performance-app-%d", i+1)
		if ds, ok := dockerStats[containerName]; ok {
			stats[i].CPU = ds["cpu"]
			stats[i].Memory = ds["memory"]

			if cpuVal, err := parsePercentage(ds["cpu"]); err == nil {
				resourceHistory[i].CPU.Add(cpuVal)
				stats[i].CPUStats = resourceHistory[i].CPU
			}
			if memVal, err := parseMemoryMB(ds["memory"]); err == nil {
				resourceHistory[i].Mem.Add(memVal)
				stats[i].MemStats = resourceHistory[i].Mem
			}
		}
	}

	return stats
}

func parsePercentage(s string) (float64, error) {
	s = strings.TrimSuffix(s, "%")
	var val float64
	_, err := fmt.Sscanf(s, "%f", &val)
	return val, err
}

func parseMemoryMB(s string) (float64, error) {
	parts := strings.Split(s, "/")
	if len(parts) == 0 {
		return 0, fmt.Errorf("invalid memory format")
	}
	memStr := strings.TrimSpace(parts[0])
	memStr = strings.TrimSuffix(memStr, "MiB")
	memStr = strings.TrimSuffix(memStr, "GiB")
	var val float64
	_, err := fmt.Sscanf(memStr, "%f", &val)
	return val, err
}

func fetchKafkaLag(client *http.Client, port int) (float64, error) {
	url := fmt.Sprintf("http://localhost:%d/actuator/metrics/kafka.consumer.fetch.manager.records.lag", port)

	resp, err := client.Get(url)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("status %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, err
	}

	var result map[string]interface{}
	if err := json.Unmarshal(body, &result); err != nil {
		return 0, err
	}

	measurements, ok := result["measurements"].([]interface{})
	if !ok {
		return 0, fmt.Errorf("invalid measurements")
	}

	for _, m := range measurements {
		measurement, ok := m.(map[string]interface{})
		if !ok {
			continue
		}

		stat, _ := measurement["statistic"].(string)
		value, _ := measurement["value"].(float64)

		if stat == "VALUE" {
			return value, nil
		}
	}

	return 0, fmt.Errorf("lag not found")
}

func fetchMetric(client *http.Client, port int, metricName, statistic string) (float64, error) {
	url := fmt.Sprintf("http://localhost:%d/actuator/metrics/%s", port, metricName)

	resp, err := client.Get(url)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("status %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, err
	}

	var result map[string]interface{}
	if err := json.Unmarshal(body, &result); err != nil {
		return 0, err
	}

	measurements, ok := result["measurements"].([]interface{})
	if !ok {
		return 0, fmt.Errorf("invalid measurements")
	}

	for _, m := range measurements {
		measurement, ok := m.(map[string]interface{})
		if !ok {
			continue
		}

		stat, _ := measurement["statistic"].(string)
		value, _ := measurement["value"].(float64)

		if stat == statistic {
			return value, nil
		}
	}

	return 0, fmt.Errorf("statistic %s not found", statistic)
}

func fetchPercentile(client *http.Client, port int, percentile string) (float64, error) {
	url := fmt.Sprintf("http://localhost:%d/actuator/metrics/spring.kafka.listener.percentile?tag=phi:%s", port, percentile)

	resp, err := client.Get(url)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("status %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, err
	}

	var result map[string]interface{}
	if err := json.Unmarshal(body, &result); err != nil {
		return 0, err
	}

	measurements, ok := result["measurements"].([]interface{})
	if !ok {
		return 0, fmt.Errorf("invalid measurements")
	}

	for _, m := range measurements {
		measurement, ok := m.(map[string]interface{})
		if !ok {
			continue
		}

		stat, _ := measurement["statistic"].(string)
		value, _ := measurement["value"].(float64)

		if stat == "VALUE" {
			return value, nil
		}
	}

	return 0, fmt.Errorf("percentile not found")
}

var (
	dockerCache     map[string]map[string]string
	dockerCacheTime time.Time
	dockerCacheTTL  = 5 * time.Second
)

func fetchDockerStats() map[string]map[string]string {
	if dockerCache != nil && time.Since(dockerCacheTime) < dockerCacheTTL {
		return dockerCache
	}

	cmd := exec.Command("docker", "stats", "--no-stream", "--format", "{{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}\t{{.MemPerc}}")
	output, err := cmd.Output()
	if err != nil {
		return dockerCache
	}

	stats := make(map[string]map[string]string)
	lines := strings.Split(strings.TrimSpace(string(output)), "\n")

	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		parts := strings.Split(line, "\t")
		if len(parts) >= 4 {
			name := strings.TrimSpace(parts[0])
			stats[name] = map[string]string{
				"cpu":    strings.TrimSpace(parts[1]),
				"memory": strings.TrimSpace(parts[2]),
				"memPct": strings.TrimSpace(parts[3]),
			}
		}
	}

	dockerCache = stats
	dockerCacheTime = time.Now()
	return stats
}

func fetchSQSMessageCount() int {
	cmd := exec.Command("aws", "sqs", "get-queue-attributes",
		"--queue-url", "http://localhost:4566/000000000000/output-queue",
		"--attribute-names", "ApproximateNumberOfMessages",
		"--endpoint-url", "http://localhost:4566",
		"--output", "json")
	output, err := cmd.Output()
	if err != nil {
		return -1
	}

	var result map[string]interface{}
	if err := json.Unmarshal(output, &result); err != nil {
		return -1
	}

	attrs, ok := result["Attributes"].(map[string]interface{})
	if !ok {
		return -1
	}

	countStr, ok := attrs["ApproximateNumberOfMessages"].(string)
	if !ok {
		return -1
	}

	var count int
	fmt.Sscanf(countStr, "%d", &count)
	return count
}

func printDashboard(stats []ContainerStats, totalCount, totalLag float64, activeContainers int64) {
	fmt.Print("\033[H\033[2J")

	fmt.Printf("%s%s", Bold, Cyan)
	fmt.Println("╔══════════════════════════════════════════════════════════════════════════════════════════╗")
	fmt.Println("║                           KAFKA CONSUMER MONITOR                                        ║")
	fmt.Println("╚══════════════════════════════════════════════════════════════════════════════════════════╝")
	fmt.Printf("%s", Reset)

	fmt.Printf("\n  %s%sUpdated: %s%s\n\n", Gray, Bold, time.Now().Format("15:04:05"), Reset)

	fmt.Printf("  %s%s── RUN STATUS ──────────────────────────────────────────────────────%s\n", Bold, Magenta, Reset)
	if currentRun.IsRunning || (!currentRun.EndTime.IsZero()) {
		fmt.Printf("  %sStart:%s     %s\n", Bold, Reset, currentRun.StartTime.Format("15:04:05"))
		if !currentRun.EndTime.IsZero() {
			elapsed := currentRun.EndTime.Sub(currentRun.StartTime)
			fmt.Printf("  %sEnd:%s       %s%s%s\n", Bold, Reset, Green, currentRun.EndTime.Format("15:04:05"), Reset)
			fmt.Printf("  %sDuration:%s  %s%.1fs%s\n", Bold, Reset, Green, elapsed.Seconds(), Reset)
			fmt.Printf("  %sTotal:%s    %s%d msgs%s\n", Bold, Reset, Yellow, currentRun.TotalMsgs, Reset)
			fmt.Printf("  %sAvg Rate:%s %s%.0f msg/s%s\n", Bold, Reset, Cyan, currentRun.AvgRate, Reset)
			fmt.Printf("  %sPeak Rate:%s %s%.0f msg/s%s\n", Bold, Reset, Magenta, currentRun.PeakRate, Reset)

			sqsCount := fetchSQSMessageCount()
			if sqsCount >= 0 {
				fmt.Printf("  %sSQS Queue:%s %s%d msgs%s\n", Bold, Reset, Cyan, sqsCount, Reset)
				if int64(sqsCount) == currentRun.TotalMsgs {
					fmt.Printf("  %sStatus:%s   %s✓ All messages in SQS%s\n", Bold, Reset, Green, Reset)
				} else if int64(sqsCount) < currentRun.TotalMsgs {
					diff := currentRun.TotalMsgs - int64(sqsCount)
					fmt.Printf("  %sStatus:%s   %s⚠ %d msgs pending in SQS%s\n", Bold, Reset, Yellow, diff, Reset)
				}
			}
		} else {
			elapsed := time.Since(currentRun.StartTime)
			fmt.Printf("  %sDuration:%s %s%.1fs%s (running)\n", Bold, Reset, Yellow, elapsed.Seconds(), Reset)
			fmt.Printf("  %sTotal:%s    %s%d msgs%s\n", Bold, Reset, Yellow, currentRun.TotalMsgs, Reset)
		}
	} else {
		fmt.Printf("  %s%sNo load running%s\n", Gray, Italic, Reset)
		fmt.Printf("  %sStart a load: cd load_generator && go run main.go <number>%s\n", Gray, Reset)
	}
	fmt.Println()

	fmt.Printf("  %s%s── SUMMARY ─────────────────────────────────────────────────────────%s\n", Bold, Magenta, Reset)
	fmt.Printf("  %sContainers:%s  %s%d%s active\n", Bold, Reset, Green, activeContainers, Reset)
	fmt.Printf("  %sProcessed:%s  %s%.0f msgs%s\n", Bold, Reset, Cyan, totalCount, Reset)
	lagColor := Green
	if totalLag > 0 {
		lagColor = Yellow
	}
	if totalLag > 100 {
		lagColor = Red
	}
	fmt.Printf("  %sKafka Lag:%s   %s%.0f msgs%s\n", Bold, Reset, lagColor, totalLag, Reset)
	fmt.Println()

	fmt.Printf("  %s%s── PER CONTAINER ───────────────────────────────────────────────────%s\n", Bold, Magenta, Reset)
	fmt.Println()

	fmt.Printf("  %s%s%-8s   %-9s %-9s  %-12s  %-12s  %-12s  %-18s %-18s%s\n",
		Bold, White, "PORT", "MSGS", "LAG", "P50", "P90", "P99", "CPU", "MEMORY", Reset)
	fmt.Printf("  %s%s%-8s   %-9s %-9s  %-12s  %-12s  %-12s  %-18s %-18s%s\n",
		Bold, Gray, "", "", "(max)", "", "", "", "(min/med/max)", "(min/med/max)", Reset)
	fmt.Printf("  %s%s%s\n", Gray, strings.Repeat("─", 115), Reset)

	for _, s := range stats {
		portStr := fmt.Sprintf("%s%d%s", Gray, s.Port, Reset)
		countStr := "-"
		lagStr := "-"
		p50Str := "-"
		p90Str := "-"
		p99Str := "-"
		cpuStr := "-"
		memStr := "-"

		if s.Healthy {
			countStr = fmt.Sprintf("%d", int64(s.Count))

			if len(s.LagStats.Values) > 0 {
				maxLag := int64(s.LagStats.Max)
				if maxLag > 100 {
					lagStr = fmt.Sprintf("%s%d%s", Red, maxLag, Reset)
				} else if maxLag > 0 {
					lagStr = fmt.Sprintf("%s%d%s", Yellow, maxLag, Reset)
				} else {
					lagStr = fmt.Sprintf("%s%d%s", Green, maxLag, Reset)
				}
			}

			p50Str = formatLatency(s.Percentiles.P50)
			p90Str = formatLatency(s.Percentiles.P90)
			p99Str = formatLatency(s.Percentiles.P99)

			if s.CPU != "" && len(s.CPUStats.Values) > 0 {
				cpuStr = fmt.Sprintf("%.1f/%.1f/%.1f%%", s.CPUStats.Min, s.CPUStats.Median(), s.CPUStats.Max)
			}
			if s.Memory != "" && len(s.MemStats.Values) > 0 {
				memStr = fmt.Sprintf("%.0f/%.0f/%.0f MiB", s.MemStats.Min, s.MemStats.Median(), s.MemStats.Max)
			}
		}

		fmt.Printf("  %-20s %-9s %-9s  %-12s  %-12s  %-12s  %-18s %-18s\n",
			portStr, countStr, lagStr, p50Str, p90Str, p99Str, cpuStr, memStr)
	}

	fmt.Println()
	fmt.Printf("  %s%sPress Ctrl+C to stop monitoring%s\n", Gray, Italic, Reset)
}

func formatLatency(seconds float64) string {
	if seconds == 0 {
		return fmt.Sprintf("%s%-14s%s", Gray, "0.00ms", Reset)
	}

	ms := seconds * 1000
	color := Green
	if ms > 10 {
		color = Yellow
	}
	if ms > 50 {
		color = Red
	}

	return fmt.Sprintf("%s%-14s%s", color, fmt.Sprintf("%.2fms", ms), Reset)
}

func getEnvInt(key string, fallback int) int {
	if val := os.Getenv(key); val != "" {
		var n int
		fmt.Sscanf(val, "%d", &n)
		if n > 0 {
			return n
		}
	}
	return fallback
}
