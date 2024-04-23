package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	_ "github.com/mattn/go-sqlite3"
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

const graphql_endpoint string = "http://89.117.57.206:8546/graphql/query"
const sqlite3_file string = "./data.sqlite3"

func connectToSQLite() *sql.DB {
	db, err := sql.Open("sqlite3", sqlite3_file)
	if err != nil {
		log.Fatalf("Error opening database: %v", err)
		return nil
	}

	// Create tables if don't exist, with indexes
	sqlStmt := `
    CREATE TABLE IF NOT EXISTS transactions (
		block_height INTEGER,
		tx_index INTEGER,
		time TEXT,
		amount TEXT,
		from_address TEXT,
		to_address TEXT,
		PRIMARY KEY (block_height, tx_index)
	);

    CREATE TABLE IF NOT EXISTS blocks (
        height INTEGER PRIMARY KEY,
        time TEXT,
        version TEXT,
        chain_id TEXT,
        proposer_address_raw TEXT
    );`
	_, err = db.Exec(sqlStmt)
	if err != nil {
		log.Fatalf("%q: %s\n", err, sqlStmt)
		return nil
	}

	return db
}

func writeBlockToSQLite(db *sql.DB, block Block) {
	stmt, err := db.Prepare("INSERT INTO blocks(height, time, version, chain_id, proposer_address_raw) VALUES(?,?,?,?,?) ON CONFLICT(height) DO NOTHING")
	if err != nil {
		log.Fatal(err)
	}
	defer stmt.Close()

	res, err := stmt.Exec(block.Height, block.Time.Format(time.RFC3339), block.Version, block.ChainID, block.ProposerAddressRaw)
	if err != nil {
		log.Fatal(err)
	}
	num, err := res.RowsAffected()
	if err != nil {
		log.Fatal(err)
	}
	if num == 0 {
		log.Printf("Block with height %d already exists, skipped", block.Height)
	} else {
		log.Printf("Block with height %d inserted", block.Height)
	}

}

func getExistingBlocks(db *sql.DB, startBlock, endBlock int) ([]int, error) {
	var blocks []int
	query := `SELECT height FROM blocks WHERE height BETWEEN ? AND ?`
	rows, err := db.Query(query, startBlock, endBlock)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var height int
	for rows.Next() {
		if err := rows.Scan(&height); err != nil {
			return nil, err
		}
		blocks = append(blocks, height)
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}

	return blocks, nil
}

func fetchLastBlockHeightFromDB(db *sql.DB) (int, error) {
	var lastBlockHeight int
	query := `SELECT height FROM blocks ORDER BY height DESC LIMIT 1`
	err := db.QueryRow(query).Scan(&lastBlockHeight)
	if err != nil {
		if err == sql.ErrNoRows {
			// No rows found, significa que no hay bloques, se puede manejar según sea necesario
			return 0, nil
		}
		log.Printf("Error fetching last block height from database: %v", err)
		return 0, err
	}
	log.Printf("Latest Block-height from DB: %d", lastBlockHeight)
	return lastBlockHeight, nil
}

func getBlockTime(db *sql.DB, blockHeight int) (time.Time, error) {
	// Declarar la variable antes de usarla
	var blockTimeString string

	// Consulta para obtener el tiempo del bloque como un string
	query := `SELECT time FROM blocks WHERE height = ?`
	err := db.QueryRow(query, blockHeight).Scan(&blockTimeString)
	if err != nil {
		log.Printf("Error fetching block time for height %d: %v", blockHeight, err)
		return time.Time{}, err
	}

	// Convertir el string de tiempo a time.Time
	blockTime, err := time.Parse(time.RFC3339, blockTimeString)
	if err != nil {
		log.Printf("Error parsing block time string for height %d: %v", blockHeight, err)
		return time.Time{}, err
	}

	return blockTime, nil
}

func fetchLastBlockNumber() (int, error) { //TODO: maybe get the latest -1 to ensure stability
	query := `query { latestBlockHeight }`
	reqBody := fmt.Sprintf(`{"query": "%s"}`, query)
	req, err := http.NewRequest("POST", graphql_endpoint, bytes.NewBufferString(reqBody))
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
	req, err := http.NewRequest("POST", graphql_endpoint, bytes.NewBufferString(reqBody))
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

func verifyAndInsertBlocks(db *sql.DB, startBlock int) {
	for {
		// Log para seguir cuáles rangos se están verificando
		log.Printf("Checking existing blocks from height %d to %d", startBlock, startBlock+9)

		// Obtener bloques existentes en el rango deseado
		existingBlocks, err := getExistingBlocks(db, startBlock, startBlock+9)
		if err != nil {
			log.Printf("Error checking existing blocks: %v", err)
			time.Sleep(1 * time.Minute) // Espera antes de reintentar para no sobrecargar el servidor
			continue
		}

		var blocks []Block // Declarar blocks aquí para hacerlo disponible en el scope más amplio.

		// Si hay menos de 10 bloques existentes, buscar más datos
		if len(existingBlocks) < 10 {
			log.Printf("Fetching new block data from API for blocks %d to %d", startBlock, startBlock+9)
			blocks, err = fetchBlockDataFromAPI(startBlock, startBlock+9)
			if err != nil {
				log.Printf("Error fetching block data: %v", err)
				continue
			}
			for _, block := range blocks {
				// Solo escribe bloques que no existen ya en la base de datos
				if !contains(existingBlocks, block.Height) {
					writeBlockToSQLite(db, block)
				}
			}
		}

		// Obtener el número del último bloque disponible desde la API
		lastBlock, err := fetchLastBlockNumber()
		if err != nil {
			log.Printf("Error fetching last block number: %v", err)
			break
		}

		// Verificar si hemos alcanzado el último bloque conocido
		if startBlock+10 > lastBlock {
			log.Printf("Reached last known block number %d, waiting for new blocks to be generated...", lastBlock)
			time.Sleep(10 * time.Minute) // Espera antes de comprobar de nuevo
			continue
		}

		// Incrementar el punto de inicio para el siguiente lote de bloques
		// Asegurar que no omitimos ningún bloque, particularmente los múltiplos de 10
		if len(blocks) > 0 {
			startBlock = blocks[len(blocks)-1].Height + 1 // Mover startBlock al siguiente al último bloque insertado/verificado
		} else {
			startBlock += 10 // Si no se encontraron bloques nuevos, simplemente moverse al siguiente rango
		}
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

	req, err := http.NewRequest("POST", graphql_endpoint, bytes.NewBufferString(reqBody))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")

	// log.Printf("Sending GraphQL query: %s", reqBody)

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to execute request: %v", err)
	}
	defer resp.Body.Close()

	var result struct {
		Data struct {
			Transactions []Transaction `json:"transactions"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to decode response: %v", err)
	}
	log.Printf("Decoded %d transactions", len(result.Data.Transactions))
	return result.Data.Transactions, nil
}

func writeTransactionToSQLite(db *sql.DB, tx Transaction) {
	var blockTime time.Time
	var err error
	maxRetries := 3
	retryInterval := 1 * time.Minute // Retries starts with 1 minute, and go exponentially

	for retries := 0; retries < maxRetries; retries++ {
		blockTime, err = getBlockTime(db, tx.BlockHeight)
		if err == nil {
			break
		}
		log.Printf("Failed to get block time for block height %d, retrying... %v", tx.BlockHeight, err)
		time.Sleep(retryInterval)
		retryInterval *= 2 // Duplicates the retry interval
	}

	if err != nil {
		log.Printf("Failed to get block time for block height %d after %d retries: %v", tx.BlockHeight, maxRetries, err)
		return
	}

	stmt, err := db.Prepare("INSERT INTO transactions(block_height, tx_index, time, amount, from_address, to_address) VALUES(?,?,?,?,?,?) ON CONFLICT(block_height, tx_index) DO NOTHING")
	if err != nil {
		log.Fatal(err)
	}
	defer stmt.Close()

	for _, msg := range tx.Messages {
		_, err = stmt.Exec(tx.BlockHeight, tx.Index, blockTime.Format(time.RFC3339), msg.Value.Amount, msg.Value.FromAddress, msg.Value.ToAddress)
		if err != nil {
			log.Printf("Failed to insert transaction for block height %d, tx_index %d: %v", tx.BlockHeight, tx.Index, err)
			return
		}
	}
	log.Printf("Transaction for block %d written to SQLite", tx.BlockHeight)
}

func verifyAndInsertTransactions(db *sql.DB, startBlock int) {
	httpClient := &http.Client{}
	for {
		lastBlock, err := fetchLastBlockHeightFromDB(db)
		if err != nil {
			log.Printf("Error fetching last block height from DB: %v", err)
			time.Sleep(1 * time.Minute)
			continue
		}

		if startBlock > lastBlock {
			log.Printf("Reached last known block height %d, waiting for new blocks to be generated...", lastBlock)
			time.Sleep(1 * time.Minute)
			continue
		}

		upperBlock := startBlock + 99
		if upperBlock > lastBlock {
			upperBlock = lastBlock
		}

		transactions, err := fetchTransactionData(httpClient, startBlock, upperBlock)
		if err != nil {
			log.Printf("Error fetching transaction data: %v", err)
			time.Sleep(1 * time.Minute)
			continue
		}

		for _, tx := range transactions {
			writeTransactionToSQLite(db, tx)
		}

		if len(transactions) > 0 {
			startBlock = transactions[len(transactions)-1].BlockHeight + 1
		} else {
			startBlock = upperBlock + 1
		}

		if startBlock > lastBlock {
			log.Printf("Waiting for new blocks as startBlock %d is beyond the last known block %d...", startBlock, lastBlock)
			time.Sleep(1 * time.Minute)
		}
	}
}

func main() {
	// Connect or/and init the SQLite database
	db := connectToSQLite()
	defer db.Close()

	// Iniciar las goroutines para verificar e insertar bloques y transacciones
	go verifyAndInsertBlocks(db, 115115) // if you break the script update the
	go verifyAndInsertTransactions(db, 1)

	select {}
}
