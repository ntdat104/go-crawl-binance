package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"
)

type Kline struct {
	OpenTime                 int64
	Open                     string
	High                     string
	Low                      string
	Close                    string
	Volume                   string
	CloseTime                int64
	QuoteAssetVolume         string
	NumberOfTrades           int
	TakerBuyBaseAssetVolume  string
	TakerBuyQuoteAssetVolume string
	Ignore                   string
}

func getBinanceKlines(symbol, interval string, endTime int64, fetchLimit int) ([]Kline, error) {
	var allKlines []Kline
	currentTime := endTime
	
	for {
		url := fmt.Sprintf("https://api.binance.com/api/v3/klines?symbol=%s&interval=%s&endTime=%d&limit=%d", symbol, interval, currentTime, fetchLimit)
		resp, err := http.Get(url)
		if err != nil {
			return nil, fmt.Errorf("error fetching data: %w", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != 200 {
			return nil, fmt.Errorf("error fetching data: %s", resp.Status)
		}

		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return nil, fmt.Errorf("error reading response body: %w", err)
		}

		var klines [][]interface{}
		if err := json.Unmarshal(body, &klines); err != nil {
			return nil, fmt.Errorf("error unmarshalling response body: %w", err)
		}

		if len(klines) == 0 {
			break
		}
		
		for _, k := range klines {
			openTime := int64(k[0].(float64))
			open := k[1].(string)
			high := k[2].(string)
			low := k[3].(string)
			close := k[4].(string)
			volume := k[5].(string)
			closeTime := int64(k[6].(float64))
			quoteAssetVolume := k[7].(string)
			numberOfTrades := int(k[8].(float64))
			takerBuyBaseAssetVolume := k[9].(string)
			takerBuyQuoteAssetVolume := k[10].(string)
			ignore := k[11].(string)

			allKlines = append(allKlines, Kline{
				OpenTime:                 openTime,
				Open:                     open,
				High:                     high,
				Low:                      low,
				Close:                    close,
				Volume:                   volume,
				CloseTime:                closeTime,
				QuoteAssetVolume:         quoteAssetVolume,
				NumberOfTrades:           numberOfTrades,
				TakerBuyBaseAssetVolume:  takerBuyBaseAssetVolume,
				TakerBuyQuoteAssetVolume: takerBuyQuoteAssetVolume,
				Ignore:                   ignore,
			})
		}

		currentTime = allKlines[0].OpenTime - 1
		time.Sleep(100 * time.Millisecond) // Respect rate limits
	}

	return allKlines, nil
}

func saveKlinesToCSV(klines []Kline, symbol, interval string, folder string) error {
	if err := os.MkdirAll(folder, os.ModePerm); err != nil {
		return fmt.Errorf("error creating folder: %w", err)
	}

	filePath := fmt.Sprintf("%s/%s_%s.csv", folder, symbol, interval)
	file, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("error creating CSV file: %w", err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	headers := []string{"Open Time", "Open", "High", "Low", "Close", "Volume", "Close Time"}
	if err := writer.Write(headers); err != nil {
		return fmt.Errorf("error writing headers to CSV file: %w", err)
	}

	for _, k := range klines {
		row := []string{
			strconv.FormatInt(k.OpenTime, 10),
			k.Open,
			k.High,
			k.Low,
			k.Close,
			k.Volume,
			strconv.FormatInt(k.CloseTime, 10),
		}
		if err := writer.Write(row); err != nil {
			return fmt.Errorf("error writing row to CSV file: %w", err)
		}
	}

	fmt.Printf("Kline data for %s with interval %s saved to %s\n", symbol, interval, filePath)
	return nil
}

func fetchAndSaveKlines(symbol, interval string, endTime int64) {
	fmt.Printf("Fetching kline data for %s with interval %s...\n", symbol, interval)
	klines, err := getBinanceKlines(symbol, interval, endTime, 1000)
	if err != nil {
		fmt.Printf("Error fetching klines: %v\n", err)
		return
	}

	fmt.Printf("Saving kline data for %s with interval %s...\n", symbol, interval)
	if err := saveKlinesToCSV(klines, symbol, interval, "csv"); err != nil {
		fmt.Printf("Error saving klines: %v\n", err)
	}
}

func fetchAllKlinesParallel(symbols, intervals []string) {
	endTime := time.Now().Unix() * 1000 // current time in milliseconds

	var wg sync.WaitGroup
	for _, symbol := range symbols {
		for _, interval := range intervals {
			wg.Add(1)
			go func(symbol, interval string) {
				defer wg.Done()
				fetchAndSaveKlines(symbol, interval, endTime)
			}(symbol, interval)
		}
	}
	wg.Wait()
}

func main() {
	symbols := []string{"BTCUSDT", "ETHUSDT", "BNBUSDT", "ADAUSDT"}
	intervals := []string{"1d"}
	fetchAllKlinesParallel(symbols, intervals)
}