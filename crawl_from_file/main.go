package main

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const (
	baseURLMonthly = "https://data.binance.vision/data/spot/monthly/klines/"
	baseURLDaily   = "https://data.binance.vision/data/spot/daily/klines/"
	baseDir        = "static"
	dataType       = "daily"
)

var (
	startDate = time.Date(2018, 8, 1, 0, 0, 0, 0, time.UTC)
	endDate   = time.Now()
	symbols   = []string{"BTCUSDT", "ETHUSDT", "BNBUSDT"}
	intervals = []string{"1m"}
)

func createDirectory(path string) error {
	return os.MkdirAll(path, os.ModePerm)
}

func getSaveDir(symbol, interval string) string {
	return filepath.Join(baseDir, symbol, interval)
}

func generateMonthlyURLs(symbol, interval string, startDate, endDate time.Time) []string {
	baseURL := fmt.Sprintf("%s%s/%s/", baseURLMonthly, symbol, interval)
	var urls []string
	currentDate := startDate
	for !currentDate.After(endDate) {
		yearMonth := currentDate.Format("2006-01")
		url := fmt.Sprintf("%s%s-%s-%s.zip", baseURL, symbol, interval, yearMonth)
		urls = append(urls, url)
		currentDate = currentDate.AddDate(0, 1, 0)
	}
	return urls
}

func generateDailyURLs(symbol, interval string, startDate, endDate time.Time) []string {
	baseURL := fmt.Sprintf("%s%s/%s/", baseURLDaily, symbol, interval)
	var urls []string
	currentDate := startDate
	for !currentDate.After(endDate) {
		dateStr := currentDate.Format("2006-01-02")
		url := fmt.Sprintf("%s%s-%s-%s.zip", baseURL, symbol, interval, dateStr)
		urls = append(urls, url)
		currentDate = currentDate.AddDate(0, 0, 1)
	}
	return urls
}

func downloadFile(url, savePath string) error {
	log.Printf("Starting download: %s\n", url)
	resp, err := http.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("failed to download %s: %s", url, resp.Status)
	}

	out, err := os.Create(savePath)
	if err != nil {
		return err
	}
	defer out.Close()

	_, err = io.Copy(out, resp.Body)
	if err == nil {
		log.Printf("Downloaded %s to %s\n", url, savePath)
	}
	return err
}

func processSymbolInterval(symbol, interval string, startDate, endDate time.Time, dataType string) error {
	saveDir := getSaveDir(symbol, interval)
	var urls []string
	if dataType == "monthly" {
		urls = generateMonthlyURLs(symbol, interval, startDate, endDate)
	} else {
		urls = generateDailyURLs(symbol, interval, startDate, endDate)
	}

	var wg sync.WaitGroup
	// var mu sync.Mutex
	// var errs []error

	for _, url := range urls {
		wg.Add(1)
		go func(url string) {
			defer wg.Done()
			fileName := filepath.Base(url)
			savePath := filepath.Join(saveDir, fileName)
			downloadFile(url, savePath)
			// if err := downloadFile(url, savePath); err != nil {
			// 	mu.Lock()
			// 	errs = append(errs, err)
			// 	mu.Unlock()
			// }
		}(url)
	}

	wg.Wait()

	// if len(errs) > 0 {
	// 	return fmt.Errorf("errors occurred during download: %v", errs)
	// }

	return nil
}

func main() {
	startTime := time.Now()

	log.Println("Starting the download process...")
	for _, symbol := range symbols {
		for _, interval := range intervals {
			dirPath := getSaveDir(symbol, interval)
			if err := createDirectory(dirPath); err != nil {
				log.Fatalf("Error creating directory: %v\n", err)
			}
		}
	}

	var wg sync.WaitGroup
	for _, symbol := range symbols {
		for _, interval := range intervals {
			wg.Add(1)
			go func(symbol, interval string) {
				defer wg.Done()
				log.Printf("Starting with symbol %s and interval %s", symbol, interval)
				if err := processSymbolInterval(symbol, interval, startDate, endDate, dataType); err != nil {
					log.Printf("Error processing symbol %s and interval %s: %v\n", symbol, interval, err)
				}
			}(symbol, interval)
		}
	}
	wg.Wait()
	log.Println("Download complete.")

	endTime := time.Now()
	duration := endTime.Sub(startTime)
	log.Printf("Total time taken: %s\n", duration)
}