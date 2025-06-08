package main

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"log/slog"
	"net/http"
	"os"
	"sync"

	_ "github.com/go-sql-driver/mysql"
	echo "github.com/labstack/echo/v4"
)

type Address struct {
	Street  string `json:"street"`
	City    string `json:"city"`
	State   string `json:"state"`
	ZipCode string `json:"zip_code"`
	Country string `json:"country"`
}

type User struct {
	ID          string    `json:"id"`
	Name        string    `json:"name"`
	Email       string    `json:"email"`
	PhoneNumber string    `json:"phone_number"`
	Addresses   []Address `json:"addresses"`
}

var usersChan chan User

func extractData(users *[]User) {
	file, err := os.Open("users_data.json")
	if err != nil {
		log.Fatalf("Failed to open file: %v", err)
	}
	defer file.Close()

	bytes, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("Failed to read file: %v", err)
	}

	if err := json.Unmarshal(bytes, &users); err != nil {
		log.Fatalf("Failed to parse JSON: %v", err)
	}

}

func insertDataIntoDB(wg *sync.WaitGroup, db *sql.DB) {
	defer wg.Done()
	u := <-usersChan
	fmt.Println(u) // just to see some logs that the app is working

	// user ID is TenantID in DB
	result, err := db.Exec(`INSERT INTO users (TenantID, name, email, phone) VALUES (?, ?, ?, ?)`, u.ID, u.Name, u.Email, u.PhoneNumber)
	if err != nil {
		log.Fatal("Insert failed:", err)
	}
	id, err := result.LastInsertId()
	if err != nil {
		log.Fatal("Failed to get last insert ID:", err)
	}
	for i := 0; i < len(u.Addresses); i++ {
		_, err = db.Exec(`INSERT INTO address (User_id, City, state, ZipCode, Country) VALUES
			 (?, ?, ?, ?, ?)`, id, u.Addresses[i].City, u.Addresses[i].State,
			u.Addresses[i].ZipCode, u.Addresses[i].Country)
		if err != nil {
			log.Fatal("Insert failed2:", err)
		}
	}

}

func getDB() *sql.DB {
	db, err := sql.Open("mysql", "mori:Eqweri89!<@tcp(127.0.0.1:3306)/newdb?parseTime=true")
	if err != nil {
		log.Fatal(err)
	}
	// defer db.Close()

	// 10 concurrency
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(10)
	if err := db.Ping(); err != nil {
		log.Fatal("DB not reachable:", err)
	}

	return db
}

func insertData(usersSize int, db *sql.DB) {
	var wg sync.WaitGroup
	for i := 0; i < usersSize; i++ {
		wg.Add(1)
		go insertDataIntoDB(&wg, db)
		if (i+1)%10 == 0 {
			wg.Wait() //handling the 10 goroutin
		}
	}

}

func insertDataIntoChann(users *[]User) {
	for _, user := range *users {
		usersChan <- user
	}
	close(usersChan)
}

func getUserInfoFromDB(db *sql.DB, id string) (*[]User, error) {
	rows, err := db.Query(`SELECT id, name, email, phone FROM users WHERE TenantID = ?`, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var users []User
	fmt.Println(123456)
	for rows.Next() {
		var u User
		err = rows.Scan(&u.ID, &u.Name, &u.Email, &u.PhoneNumber)
		if err != nil {
			return nil, err
		}
		u.Addresses = []Address{}
		users = append(users, u)
	}
	for i, u := range users {
		rows, err = db.Query(`SELECT City, State, ZipCode, Country FROM address WHERE User_id = ?`, u.ID)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var tmpA Address
			err = rows.Scan(&tmpA.City, &tmpA.State, &tmpA.ZipCode, &tmpA.Country)
			if err != nil {
				return nil, err
			}
			users[i].Addresses = append(users[i].Addresses, tmpA)
			users = append(users, u)
		}
		users[i].ID = id //if we wanna return the tenantID
	}
	return &users, nil
}
func getUserInfo(c echo.Context) error {
	// considering that simply TenantID comes as query string
	id := c.QueryParam("id")
	db := c.Get("db").(*sql.DB)
	if db == nil {
		return errors.New("could not find the db connection")
	}
	users, err := getUserInfoFromDB(db, id)
	if err != nil {
		return c.JSON(500, err)
	}
	return c.JSON(200, users)
}

func main() {
	db := getDB()

	//after i insert the data into my db , i ignored the functions of it and just API here , u can just uncommnet them to see how they work

	// usersChan = make(chan User)
	// var users []User

	// extractData(&users)
	// fmt.Println(len(users)) //just to make sure all users r loaded

	// go insertDataIntoChann(&users) //sending data into chan
	// insertData(len(users), db)

	//API using echo
	e := echo.New()
	e.Use(DBMiddleware(db))
	e.GET("/here", getUserInfo)
	if err := e.Start(":8080"); err != nil && !errors.Is(err, http.ErrServerClosed) {
		slog.Error("failed to start server", "error", err)
	}
}

func DBMiddleware(db *sql.DB) echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			c.Set("db", db) // then we can recover it in out handler
			return next(c)
		}
	}
}
