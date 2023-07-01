package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"

	"github.com/jcelliott/lumber"
)

type Logger interface {
	Fatal(string, ...interface{})
	Error(string, ...interface{})
	Warn(string, ...interface{})
	Info(string, ...interface{})
	Debug(string, ...interface{})
	Trace(string, ...interface{})
}

type Driver struct {
	mutex   sync.Mutex
	dir     string
	log     Logger
	mutexes map[string]*sync.Mutex
}

type Options struct {
	Logger
}

func New(dir string, options *Options) (*Driver, error) {
	dir = filepath.Clean(dir)

	opts := Options{}

	if options != nil {
		opts = *options
	}

	if opts.Logger == nil {
		opts.Logger = lumber.NewConsoleLogger(lumber.INFO)
	}

	driver := &Driver{
		dir:     dir,
		log:     opts.Logger,
		mutexes: make(map[string]*sync.Mutex),
	}

	if _, err := stat(dir); err == nil {
		opts.Logger.Debug("'%s' Database is already exists\n", dir)
		return driver, nil
	}

	opts.Logger.Debug("Creating the database at '%s'\n", dir)

	return driver, os.MkdirAll(dir, 0755)
}

func (d *Driver) Write(collection, resource string, v interface{}) error {
	if collection == "" {
		return fmt.Errorf("Missing collection")
	}

	if resource == "" {
		return fmt.Errorf("Missing resource")
	}

	mutex := d.getOrCreateMutex(collection)
	mutex.Lock()
	defer mutex.Unlock()

	dir := filepath.Join(d.dir, collection)
	fnlPath := filepath.Join(dir, resource+".json")
	tmpPath := fnlPath + ".tmp"

	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	b, err := json.MarshalIndent(v, "", "\t")

	if err != nil {
		return err
	}

	b = append(b, byte('\n'))

	if err := ioutil.WriteFile(tmpPath, b, 0644); err != nil {
		return err
	}

	return os.Rename(tmpPath, fnlPath)
}

func (d *Driver) Read(collection, resource string, v interface{}) error {
	if collection == "" {
		return fmt.Errorf("Missing collection")
	}

	if resource == "" {
		return fmt.Errorf("Missing resource")
	}

	record := filepath.Join(d.dir, collection, resource)

	if _, err := stat(record); err != nil {
		return err
	}

	b, err := ioutil.ReadFile(record + ".json")

	if err != nil {
		return err
	}

	return json.Unmarshal(b, &v)
}

func (d *Driver) ReadAll(collection string) ([]string, error) {
	if collection == "" {
		return nil, fmt.Errorf("Missing collection")
	}

	dir := filepath.Join(d.dir, collection)

	if _, err := stat(dir); err != nil {
		return nil, err
	}

	files, _ := ioutil.ReadDir(dir)

	var records []string

	for _, file := range files {
		b, err := ioutil.ReadFile(filepath.Join(dir, file.Name()))

		if err != nil {
			return nil, err
		}

		records = append(records, string(b))
	}

	return records, nil
}

func (d *Driver) Update(collection, resource string, v interface{}) error {
	if collection == "" {
		return fmt.Errorf("Missing collection")
	}

	if resource == "" {
		return fmt.Errorf("Missing resource")
	}

	mutex := d.getOrCreateMutex(collection)
	mutex.Lock()
	defer mutex.Unlock()

	dir := filepath.Join(d.dir, collection, resource+".json")

	if _, err := stat(dir); err != nil {
		return err
	}

	b, err := json.MarshalIndent(v, "", "\t")

	if err != nil {
		return err
	}

	b = append(b, byte('\n'))

	return ioutil.WriteFile(dir, b, 0644)
}

func (d *Driver) Delete(collection, resource string) error {
	path := filepath.Join(collection, resource)

	mutex := d.getOrCreateMutex(collection)
	mutex.Lock()
	defer mutex.Unlock()

	dir := filepath.Join(d.dir, path)

	switch fi, err := stat(dir); {
	case fi == nil, err != nil:
		return fmt.Errorf("Unable to find file or directory named %v", path)

	case fi.Mode().IsDir():
		return os.RemoveAll(dir)

	case fi.Mode().IsRegular():
		return os.RemoveAll(dir + ".json")
	}

	return nil
}

func (d *Driver) getOrCreateMutex(collection string) *sync.Mutex {
	d.mutex.Lock()
	defer d.mutex.Unlock()
	m, ok := d.mutexes[collection]

	if !ok {
		m = &sync.Mutex{}
		d.mutexes[collection] = m
	}

	return m
}

func stat(path string) (fi os.FileInfo, err error) {
	if fi, err = os.Stat(path); os.IsNotExist(err) {
		fi, err = os.Stat(path + ".json")
	}
	return
}

type Address struct {
	City    string
	State   string
	Country string
	Pincode json.Number
}

type User struct {
	Name    string
	Age     json.Number
	Contact string
	Company string
	Address Address
}

func main() {
	dir := "./"

	db, err := New(dir, nil)

	if err != nil {
		fmt.Println("Error: ", err)
	}

	employees := []User{
		{
			"Prasad",
			"22",
			"7875701298",
			"One Convergence",
			Address{
				"Ahmednagar",
				"Maharashtra",
				"India",
				"414001",
			},
		},
		{
			"Harshita",
			"22",
			"9268289224",
			"One Convergence",
			Address{
				"Karol Bagh",
				"Delhi",
				"India",
				"110005",
			},
		},
		{
			"Tushar",
			"22",
			"7020112184",
			"ByteAlly",
			Address{
				"New Sangvi",
				"Pune",
				"India",
				"411027",
			},
		},
		{
			"Kanchan",
			"22",
			"7972693862",
			"Ignite solutions",
			Address{
				"Morane",
				"Dhule",
				"India",
				"424002",
			},
		},
	}

	for _, value := range employees {
		db.Write("Users", value.Name, User{
			Name:    value.Name,
			Age:     value.Age,
			Contact: value.Contact,
			Company: value.Company,
			Address: value.Address,
		})
	}

	if records, err := db.ReadAll("Users"); err != nil {
		fmt.Println("Error: ", err)
	} else {
		fmt.Println(records)
	}

	var record User

	if err := db.Read("Users", "Prasad", &record); err != nil {
		fmt.Println("Error: ", err)
	} else {
		fmt.Println(record)
	}

	if err := db.Update("Users", "Prasad", User{
		Name:    "Prasad",
		Age:     "23",
		Contact: "7875701298",
		Company: "One Convergence",
		Address: Address{
			City:    "Hydrabad",
			State:   "Tamilnadu",
			Country: "India",
			Pincode: "500033",
		},
	}); err != nil {
		fmt.Println("Error: ", err)
	} else {
		fmt.Println("Record updated successfully")
	}

	if err := db.Read("Users", "Prasad", &record); err != nil {
		fmt.Println("Error: ", err)
	} else {
		fmt.Println(record)
	}

	if err := db.Delete("Users", "Prasad"); err != nil {
		fmt.Println("Error: ", err)
	} else {
		fmt.Println("Record deleted a successfully")
	}

	if err := db.Delete("Users", ""); err != nil {
		fmt.Println("Error: ", err)
	} else {
		fmt.Println("Database deleted a successfully")
	}
}
