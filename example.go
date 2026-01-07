package main

import (
	"fmt"
	"log"
)

// Example usage of the database
func main() {
	// Create or open a database
	db, err := NewStorage("example.db")
	if err != nil {
		log.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Insert some data
	fmt.Println("Inserting data...")
	if err := db.Put("user:1", "isabella"); err != nil {
		log.Fatalf("Put failed: %v", err)
	}
	if err := db.Put("user:2", "cam"); err != nil {
		log.Fatalf("Put failed: %v", err)
	}
	if err := db.Put("user:3", "alice"); err != nil {
		log.Fatalf("Put failed: %v", err)
	}

	// Retrieve data
	fmt.Println("\nRetrieving data...")
	value1, err := db.Get("user:1")
	if err != nil {
		log.Fatalf("Get failed: %v", err)
	}
	fmt.Printf("user:1 = %s\n", value1)

	value2, err := db.Get("user:2")
	if err != nil {
		log.Fatalf("Get failed: %v", err)
	}
	fmt.Printf("user:2 = %s\n", value2)

	// Update a value
	fmt.Println("\nUpdating user:1...")
	if err := db.Put("user:1", "leonor"); err != nil {
		log.Fatalf("Update failed: %v", err)
	}

	updated, err := db.Get("user:1")
	if err != nil {
		log.Fatalf("Get failed: %v", err)
	}
	fmt.Printf("user:1 = %s (updated)\n", updated)

	// Delete a value
	fmt.Println("\nDeleting user:2...")
	if err := db.Delete("user:2"); err != nil {
		log.Fatalf("Delete failed: %v", err)
	}

	_, err = db.Get("user:2")
	if err != nil {
		fmt.Println("user:2 successfully deleted (not found as expected)")
	}

	fmt.Println("\nDatabase operations completed successfully!")
}
