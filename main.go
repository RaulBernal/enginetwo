package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"time"

	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/joho/godotenv"
)

type Block struct {
	Height             int       `json:"height"`
	Version            string    `json:"version"`
	ChainID            string    `json:"chain_id"`
	Time               time.Time `json:"time"`
	ProposerAddressRaw string    `json:"proposer_address_raw"`
}

type Transaction struct {
	Index       int `json:"index"`
	BlockHeight int `json:"block_height"`
	Messages    []struct {
		Value struct {
			Amount      string `json:"amount"`
			FromAddress string `json:"from_address"`
			ToAddress   string `json:"to_address"`
		} `json:"value"`
	} `json:"messages"`
}

func InitEnv() {
	if err := godotenv.Load("../dev_influxdb.env"); err != nil {
		log.Fatalf("Error loading .env file: %v", err)
	}
}

func ConnectToInfluxDB() influxdb2.Client {
	InitEnv()
	url := os.Getenv("INFLUXDB_URL")
	token := os.Getenv("INFLUXDB_TOKEN")
	client := influxdb2.NewClient(url, token)

	health, err := client.Health(context.Background())
	if err != nil || health.Status != "pass" {
		log.Fatalf("Failed to connect to InfluxDB: %v, Status: %s", err, health.Status)
	}
	log.Println("Connected to InfluxDB successfully")
	return client
}

func writeBlockToInfluxDB(client influxdb2.Client, block Block) {
	org := os.Getenv("INFLUXDB_INIT_ORG")
	bucket := os.Getenv("INFLUXDB_INIT_BUCKET")

	writeAPI := client.WriteAPI(org, bucket)
	p := influxdb2.NewPoint("blocks2",
		map[string]string{"proposer_address_raw": block.ProposerAddressRaw},
		map[string]interface{}{
			"height": block.Height,
			"time":   block.Time.Format(time.RFC3339),
		}, block.Time)
	log.Printf("Writing block with height %d to InfluxDB", block.Height)
	writeAPI.WritePoint(p)
	writeAPI.Flush()
	log.Printf("Block written to InfluxDB: %v\n", block.Height)
}

func getExistingBlocks(client influxdb2.Client, startBlock, endBlock int) ([]int, error) {
	queryAPI := client.QueryAPI(os.Getenv("INFLUXDB_INIT_ORG"))
	query := fmt.Sprintf(`from(bucket:"%s") |> range(start: -1y) |> filter(fn: (r) => r._measurement == "blocks2") |> filter(fn: (r) => r.height >= %d and r.height <= %d) |> keep(columns: ["height"]) |> distinct(column: "height")`, os.Getenv("INFLUXDB_INIT_BUCKET"), startBlock, endBlock)
	log.Printf("Querying existing blocks from %d to %d", startBlock, endBlock)
	result, err := queryAPI.Query(context.Background(), query)
	if err != nil {
		return nil, err
	}
	defer result.Close()

	var blocks []int
	for result.Next() {
		blocks = append(blocks, int(result.Record().ValueByKey("height").(int64)))
	}
	if result.Err() != nil {
		return nil, result.Err()
	}
	log.Printf("Found %d existing blocks", len(blocks))
	return blocks, nil
}

func fetchLastBlockNumber() (int, error) {
	query := `query { latestBlockHeight }`
	endpoint := "http://89.117.57.206:8546/graphql/query"
	reqBody := fmt.Sprintf(`{"query": "%s"}`, query)
	req, err := http.NewRequest("POST", endpoint, bytes.NewBufferString(reqBody))
	if err != nil {
		return 0, fmt.Errorf("failed to create request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("failed to execute request: %v", err)
	}
	defer resp.Body.Close()

	var result struct {
		Data struct {
			LatestBlockHeight int `json:"latestBlockHeight"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return 0, fmt.Errorf("failed to decode response: %v", err)
	}
	log.Printf("Latest Block-height: %d", result.Data.LatestBlockHeight)
	return result.Data.LatestBlockHeight, nil
}

func fetchBlockDataFromAPI(fromHeight, toHeight int) ([]Block, error) {
	query := fmt.Sprintf(`{ blocks(filter: { from_height: %d, to_height: %d }) { time height version chain_id proposer_address_raw } }`, fromHeight, toHeight)
	reqBody := fmt.Sprintf(`{"query": "%s"}`, query)
	req, err := http.NewRequest("POST", "http://89.117.57.206:8546/graphql/query", bytes.NewBufferString(reqBody))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to execute request: %v", err)
	}
	defer resp.Body.Close()

	var result struct {
		Data struct {
			Blocks []Block `json:"blocks"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to decode response: %v", err)
	}

	return result.Data.Blocks, nil
}

func verifyAndInsertBlocks(client influxdb2.Client, startBlock int) {
	for {
		existingBlocks, err := getExistingBlocks(client, startBlock, startBlock+9)
		if err != nil {
			log.Printf("Error checking existing blocks: %v", err)
			time.Sleep(1 * time.Minute) // Espera antes de reintentar para no sobrecargar el servidor
			continue
		}

		log.Printf("Checking for existing blocks starting from block %d", startBlock)
		if len(existingBlocks) < 10 {
			log.Printf("Less than 10 blocks found, fetching more blocks")
			blocks, err := fetchBlockDataFromAPI(startBlock, startBlock+9) // adjust this to fetch exactly 10 blocks
			if err != nil {
				log.Printf("Error fetching block data: %v", err)
				continue
			}
			for _, block := range blocks {
				if !contains(existingBlocks, block.Height) {
					writeBlockToInfluxDB(client, block)
				}
			}
		}

		lastBlock, err := fetchLastBlockNumber()
		if err != nil {
			log.Printf("Error fetching last block number: %v", err)
			break
		}

		// Adjust startBlock here based on condition
		if startBlock+10 > lastBlock {
			log.Printf("Reached last known block number %d, waiting for new blocks to be generated...", lastBlock)
			time.Sleep(1 * time.Minute)
			continue // After waiting, it continues without incrementing startBlock prematurely
		}

		startBlock += 9 // Increment after ensuring we are not at the end
	}
}

func contains(slice []int, item int) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}

func fetchTransactionData(client *http.Client, fromHeight, toHeight int) ([]Transaction, error) {
	log.Printf("Fetching transactions from height %d to %d", fromHeight, toHeight)
	query := fmt.Sprintf(`query { transactions(filter: { message: { type_url: send, route: bank }, from_block_height: %d, to_block_height: %d }) { index block_height messages { value { ... on BankMsgSend { amount from_address to_address } } } } }`, fromHeight, toHeight)
	reqBody := fmt.Sprintf(`{"query": "%s"}`, query)

	req, err := http.NewRequest("POST", "http://89.117.57.206:8546/graphql/query", bytes.NewBufferString(reqBody))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")

	log.Printf("Sending GraphQL query: %s", reqBody)

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to execute request: %v", err)
	}
	defer resp.Body.Close()

	responseBody := readResponseBody(resp)
	log.Printf("Response from server: %s", responseBody)

	if resp.StatusCode != http.StatusOK {
		log.Printf("Received non-OK HTTP status: %d. Response Body: %s", resp.StatusCode, responseBody)
		return nil, fmt.Errorf("received non-OK HTTP status: %d", resp.StatusCode)
	}

	var result struct {
		Data struct {
			Transactions []Transaction `json:"transactions"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil { //borrar
		body, _ := ioutil.ReadAll(resp.Body) // Read the body for logging
		log.Printf("Failed to decode response: %v. Response body: %s", err, string(body))
		return nil, fmt.Errorf("failed to decode response: %v", err)
	}
	log.Printf("Decoded %d transactions", len(result.Data.Transactions))
	return result.Data.Transactions, nil
}

func readResponseBody(resp *http.Response) string { //borrrar
	bodyBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Printf("Error reading response body: %v", err)
		return ""
	}
	// Convertimos el cuerpo a string para poder loguearlo
	bodyString := string(bodyBytes)
	// Restablecemos el cuerpo del response para que pueda ser leído de nuevo si es necesario
	resp.Body = ioutil.NopCloser(bytes.NewBuffer(bodyBytes))
	return bodyString
}

func writeTransactionToInfluxDB(client influxdb2.Client, tx Transaction) {
	org := os.Getenv("INFLUXDB_INIT_ORG")
	bucket := os.Getenv("INFLUXDB_INIT_BUCKET")

	writeAPI := client.WriteAPI(org, bucket)
	for _, msg := range tx.Messages {
		p := influxdb2.NewPoint("tx_send",
			map[string]string{"from_address": msg.Value.FromAddress, "to_address": msg.Value.ToAddress},
			map[string]interface{}{
				"amount":       msg.Value.Amount,
				"block_height": tx.BlockHeight,
			}, time.Now())
		writeAPI.WritePoint(p)
		log.Printf("Writing transaction for block %d to InfluxDB", tx.BlockHeight)
	}
	writeAPI.Flush()
}

func verifyAndInsertTransactions(client influxdb2.Client, startBlock int) {
	httpClient := &http.Client{}
	for {
		// Obtener el último número de bloque
		lastBlock, err := fetchLastBlockNumber()
		if err != nil {
			log.Printf("Error fetching last block number for transactions: %v", err)
			time.Sleep(1 * time.Minute) // Espera antes de reintentar para no sobrecargar el servidor
			continue
		}

		// Si el bloque inicial ya está más allá del último bloque conocido, esperar por nuevos bloques
		if startBlock > lastBlock {
			log.Printf("Reached last known block number %d, waiting for new blocks to be generated...", lastBlock)
			time.Sleep(1 * time.Minute) // Espera para dar tiempo a que se generen nuevos bloques
			continue
		}

		transactions, err := fetchTransactionData(httpClient, startBlock, startBlock+99)
		if err != nil {
			log.Printf("Error fetching transaction data: %v", err)
			time.Sleep(1 * time.Minute) // Espera antes de reintentar
			continue
		}

		for _, tx := range transactions {
			writeTransactionToInfluxDB(client, tx)
		}

		// Actualizar el bloque inicial para la siguiente iteración
		startBlock += 100
	}
}

func main() {
	InitEnv()
	client := ConnectToInfluxDB()
	go verifyAndInsertBlocks(client, 101900)
	//go verifyAndInsertTransactions(client, 1)

	select {}
}
