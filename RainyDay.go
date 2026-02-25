package main

import (
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/kshedden/formula"
	"github.com/kshedden/statmodel/glm"
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
	humData, preData, err := getRecords(db)
	if err != nil {
		log.Printf("Error Connecting to DB: %v", err)
	}
	// fmt.Printf("hum Data: %v\npre Data: %v\n", humData, preData)
	nodeMap := makeNodeMap()
	dayRainMap := make(map[string][]float64)

	for _, rData := range preData {

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

	type observation struct {
		humidity float64
		pressure float64
	}

	syncMap := make(map[string]*observation)

	for _, h := range humData {
		key := h.Time.Format("2006-01-02-15:04:05")
		val, _ := strconv.ParseFloat(h.Valuehrf, 64)
		syncMap[key] = &observation{humidity: val}
	}
	for _, p := range preData {
		key := p.Time.Format("2006-01-02-15:04:05")
		if obs, ok := syncMap[key]; ok {
			val, _ := strconv.ParseFloat(p.Valuehrf, 64)
			obs.pressure = val
		}
	}

	var hum []float64
	var pre []float64
	var isRain []float64

	for key, obs := range syncMap {
		if obs.pressure != 0 && obs.humidity != 0 {
			date := key[:10]
			hour, _ := strconv.Atoi(key[11:13])
			rainVal := 0.0
			if _, ok := dayRainMap[date]; !ok {
				continue
			} else {
				if dayRainMap[date][hour] > 0 {
					rainVal = 1.0
				}
				hum = append(hum, obs.humidity)
				pre = append(pre, obs.pressure)
				isRain = append(isRain, rainVal)
			}
		}
	}
	for _, va := range isRain {
		if va == 1.0 {
			fmt.Printf("%v, ", va)
		}
	}

	standardize(hum)
	standardize(pre)

	runRainModel(hum, pre, isRain)
}

func standardize(data []float64) []float64 {
	var sum, ssq float64
	n := float64(len(data))
	for _, v := range data {
		sum += v
	}
	mean := sum / n
	for _, v := range data {
		ssq += math.Pow(v-mean, 2)
	}
	std := math.Sqrt(ssq / n)

	res := make([]float64, len(data))
	for i, v := range data {
		res[i] = (v - mean) / std
	}
	return res
}

func runRainModel(hum, pre, isRain []float64) {
	names := []string{"humidity", "pressure", "isRain"}

	datax := []interface{}{
		hum,
		pre,
		isRain,
	}

	rainSource := formula.NewSource(datax, names)

	msg := "Logistic regression: Predicting rain using humidity and pressure."

	fml := []string{"isRain", "1 + humidity + pressure"}

	f, err := formula.NewMulti(fml, rainSource, nil)
	if err != nil {
		panic(err)
	}

	da, err := f.Parse()
	if err != nil {
		panic(err)
	}
	da = da.DropNA()

	xnames := []string{"icept", "humidity", "pressure"}
	c := glm.DefaultConfig()
	c.Family = glm.NewFamily(glm.BinomialFamily)

	model, err := glm.NewGLM(da, "isRain", xnames, c)
	if err != nil {
		panic(err)
	}

	rslt := model.Fit()
	smry := rslt.Summary()

	fmt.Printf("\n%s\n", msg)
	fmt.Printf(smry.String() + "\n\n")

	smry = smry.SetScale(math.Exp, "Parameters are shown as odds ratios")
	fmt.Printf(smry.String() + "\n\n")
}

func dbConnect() (*sql.DB, error) {
	connStr := "postgres://crate@localhost:5432/doc?sslmode=disable"

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatal(err)
	}
	return db, err
}

func getRecords(db *sql.DB) ([]rainData, []rainData, error) {
	query := `SELECT node_id, parameter, valuehrf, timestamp FROM rawsensordata WHERE parameter IN ('humidity', 'pressure') ORDER BY timestamp ASC limit 50000;`
	var preRes []rainData
	var humRes []rainData

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
		if d.Param == "humidity" {
			humRes = append(humRes, d)
		} else if d.Param == "pressure" {
			preRes = append(preRes, d)
		}
	}
	if err = r.Err(); err != nil {
		return nil, nil, err
	}

	return humRes, preRes, nil
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
