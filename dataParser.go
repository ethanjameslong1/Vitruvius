package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"time"

	_ "github.com/lib/pq" // Use the Postgres driver
)

type SensorData struct {
	Timestamp string `json:"timestamp"`
	NodeID    string `json:"node_id"`
	Subsystem string `json:"subsystem"`
	Sensor    string `json:"sensor"`
	Parameter string `json:"parameter"`
	ValueRaw  string `json:"value_raw"`
	ValueHrf  string `json:"value_hrf"`
}

func insertRecord(db *sql.DB, d SensorData) error {
	layout := "2006/01/02 15:04:05"
	parsedTime, err := time.Parse(layout, d.Timestamp)
	if err != nil {
		return fmt.Errorf("could not parse timestamp '%s': %v", d.Timestamp, err)
	}

	query := `INSERT INTO rawsensordata 
		(timestamp, node_id, subsystem, sensor, parameter, valueRaw, valueHrf) 
		VALUES ($1, $2, $3, $4, $5, $6, $7)`

	vRaw, _ := strconv.ParseFloat(d.ValueRaw, 64)
	vHrf, _ := strconv.ParseFloat(d.ValueHrf, 64)

	_, err = db.Exec(query,
		parsedTime,
		d.NodeID,
		d.Subsystem,
		d.Sensor,
		d.Parameter,
		vRaw,
		vHrf,
	)
	fmt.Printf("Error if exists: %v", err)
	return err
}

func listen(db *sql.DB) {
	dataHandler := func(w http.ResponseWriter, req *http.Request) {
		var data []SensorData
		err := json.NewDecoder(req.Body).Decode(&data)
		if err != nil {
			log.Printf("error found, but something happened! %v", err)
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if len(data) > 0 {
			log.Printf("Received %d records.", len(data))
			for _, dat := range data {
				insertRecord(db, dat)
			}

			w.WriteHeader(http.StatusOK)
		} else {
			log.Printf("Data is less nothing somehow")
			w.WriteHeader(http.StatusOK)
		}
	}
	http.HandleFunc("/data", dataHandler)
	log.Fatal(http.ListenAndServe(":8000", nil))
}

func dbSetup() (*sql.DB, error) {
	connStr := "postgres://crate@localhost:5432/doc?sslmode=disable"

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatal(err)
	}

	// DROP TABLE - COMMENT IN PROD OR WHEN PERSISTING DATA
	_, err = db.Exec(`DROP TABLE IF EXISTS rawsensordata`)
	if err != nil {
		log.Fatalf("Could not drop table: %v", err)
	}
	// END DROP TABLE

	q := `CREATE TABLE rawsensordata (
        timestamp TIMESTAMP WITH TIME ZONE,
        node_id   TEXT,
        subsystem TEXT,
        sensor    TEXT,
        parameter TEXT,
        valueRaw  DOUBLE PRECISION,
        valueHrf  DOUBLE PRECISION
    )`

	_, err = db.Exec(q)
	if err != nil {
		log.Fatalf("Error running command: %v\nQuery: %s", err, q)
	}
	fmt.Println("Database ready: RawSensorData table recreated.")
	return db, nil
}

func main() {
	db, err := dbSetup()
	if err != nil {
		log.Fatal("Database setup failed: ", err)
	}
	defer db.Close()

	listen(db)
}
