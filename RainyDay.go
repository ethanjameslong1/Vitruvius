package main

import (
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	_ "github.com/lib/pq"
)

type rainData struct {
	NodeID   string    `json:"node_id"`
	Param    string    `json:"parameter"`
	Valuehrf string    `json:"valuehrf"`
	Time     time.Time `json:"timestamp"`
}

type Coordinate struct {
	Lat float32
	Lon float32
}

func main() {
	db, err := dbConnect()
	if err != nil {
		log.Printf("Error Connecting to DB: %v", err)
	}
	rainData, err := getRecords(db)
	if err != nil {
		log.Printf("Error Connecting to DB: %v", err)
	}
	nodeMap := makeNodeMap()
	dayRainMap := make(map[string][]float64)

	for _, rData := range rainData {

		coord := nodeMap[rData.NodeID]

		lat := coord.Lat
		lon := coord.Lon
		ti := rData.Time
		day := ti.Format("2006-01-02")
		_, ok := dayRainMap[day]
		if ok {
			continue
		}
		fmt.Println(day)

		url := fmt.Sprintf("https://archive-api.open-meteo.com/v1/archive?latitude=%.2f&longitude=%.2f&hourly=rain&start_date=%s&end_date=%s&hourly=rain",
			lat, lon, day, day)

		resp, err := http.Get(url)
		if err != nil {
			log.Fatalf("Error making request: %v", err)
		}

		defer resp.Body.Close()

		body, err := io.ReadAll(resp.Body)
		if err != nil {
			log.Fatalf("Error reading body: %v", err)
		}

		var result struct {
			Hourly struct {
				Rain []float64 `json:"rain"`
			} `json:"hourly"`
		}

		if err := json.Unmarshal(body, &result); err != nil {
			log.Fatalf("Error unmarshaling JSON: %v", err)
		}
		_, ok = dayRainMap[day]
		if !ok {
			dayRainMap[day] = result.Hourly.Rain
		}

	}
}

func dbConnect() (*sql.DB, error) {
	connStr := "postgres://crate@localhost:5432/doc?sslmode=disable"

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatal(err)
	}
	return db, err
}

func getRecords(db *sql.DB) ([]rainData, error) {
	query := `SELECT node_id, parameter, valuehrf, timestamp FROM rawsensordata limit 100;`
	var res []rainData

	var d rainData

	r, err := db.Query(query)
	if err != nil {
		log.Printf("Error querying: %v", err)
	}
	defer r.Close()

	for r.Next() {
		err := r.Scan(&d.NodeID, &d.Param, &d.Valuehrf, &d.Time)
		if err != nil {
			log.Printf("Error scanning row: %v", err)
		}
		res = append(res, d)
	}

	if err = r.Err(); err != nil {
		return res, err
	}

	return res, nil
}

func findNodeCoords(nodeID string) (float32, float32) {
	return 6.9, 6.9
}

func makeNodeMap() map[string]Coordinate {
	filepath := "data/AoT_Chicago.complete.humidity/nodes.csv"
	file, err := os.Open(filepath)
	if err != nil {
		fmt.Println("Error opening file:", err)
		return nil
	}
	defer file.Close()
	r := csv.NewReader(file)
	idToCoord := make(map[string]Coordinate, 0)

	for {
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}
		if record[0] == "node_id" {
			continue
		}
		lat, err := strconv.ParseFloat(record[4], 32)
		lon, err := strconv.ParseFloat(record[5], 32)
		idToCoord[record[0]] = Coordinate{Lat: float32(lat), Lon: float32(lon)}

	}
	return idToCoord
}
