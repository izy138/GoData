package main

import (
	'encoding/binary'
	'errors'
	'hash/crc32'
	'os'
)

// Log entry types for what kind of operation is being logged
const (
	LogTypePut = 1 // insert or update a key-value pair
	LogTypeDelete = 2 // delete a key-value pair
)

// LogEntry represents a single entry in the log 
type LogEntry struct {
	LSN       uint64 // Log Sequence Number - unique ID for the entry
	EntrySize uint32 // Total size of the entry in bytes
	Type      byte   // PUT or DELETE
	KeyLen    uint16 // Length of the key string
	ValueLen  uint16 // Length of the value string (0 for DELETE)
	Key       string // The actual key string
	Value     string // The actual value string (empty for DELETE)
	Checksum  uint32 // Checksum of the entry using CRC32 hash to detect corruption
}

//WAL manages the write-ahead log file
type Wal struct {
	file *os.File // the actual log file .wal on the disk
	path string // the path to the WAL log file
	lastLSN uint64 // the last LSN assigned used for an entry in the log
}

// Serialize converts a LogEntry into a byte slice for writing to disk
func (e *LogEntry) Serialize() []byte {

	//calculate total size needed for the entry
	totalSize := 8 + 4 + 1 + 2 + 2 + len(e.Key) + len(e.Value) + 4 // 8 bytes for LSN, 4 bytes for EntrySize, 1 byte for Type, 2 bytes for KeyLen, 2 bytes for ValueLen, len(Key) bytes for Key, len(Value) bytes for Value, 4 bytes for Checksum

	// create byte array to hold everything 
	data := make([]byte, totalSize)
	
	offset := 0

	// Write entry info to the byte array
	binary.LittleEndian.PutUint64(data[offset:offset+8], e.LSN)
	offset += 8
	binary.LittleEndian.PutUint32(data[offset:offset+4], e.EntrySize)
	offset += 4
	binary.LittleEndian.PutUint8(data[offset:offset+1], e.Type)
	offset += 1
	binary.LittleEndian.PutUint16(data[offset:offset+2], e.KeyLen)
	offset += 2
	binary.LittleEndian.PutUint16(data[offset:offset+2], e.ValueLen)
	offset += 2

	copy(data[offset:offset+len(e.Key)], []byte(e.Key))
	offset += len(e.Key)
	copy(data[offset:offset+len(e.Value)], []byte(e.Value))
	offset += len(e.Value)


	// data = [
    // // LSN (8 bytes)
    // 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    // // EntrySize (4 bytes) 
    // 0x21, 0x00, 0x00, 0x00, 
    // // Type (1 byte)
    // 0x01,  // PUT
    // // KeyLen (2 bytes)
    // 0x06, 0x00,  
    // // ValueLen (2 bytes)
    // 0x04, 0x00, 
    // // Key "user:1" (6 bytes)
    // 0x75, 0x73, 0x65, 0x72, 0x3A, 0x31,  // u s e r : 1
    // // Value "john" (4 bytes)
    // 0x6A, 0x6F, 0x68, 0x6E,  // j o h n
    // // Checksum space (4 bytes) - still empty!
    // 0x00, 0x00, 0x00, 0x00
	// ]
	// offset = 27 (bytes 0-26) 

	//checksum is a fingerprint for the data. It is a single number that represents all the data.
	//it is used to detect corruption of the data. it is calculated by taking the data and running it through a hash function. returns a single number. if one byte changes, the checksum will change, alerting you that something is wrong.

	// checksumData = data[0:26] 
	//bytes 0-26 contain all the entry info and the key and value.
	checksumData := data[0:offset] //we dont use data[0:] because we dont want to include the checksum in the checksum calculation. 

	//this runs the CRC32 hash function on the checksumData and returns a 32 bit number.
	//very sensitive to small changes in the data.
	//Input:  27 bytes [0x01, 0x00, 0x00, ..., 0x6E] 
	//Output: 0x8A3F2B1C (a single 32-bit number)
	checksum := crc32.ChecksumIEEE(checksumData)

	//this converts the checksum into 4 bytes and writes it to the data array at the offset.
	binary.LittleEndian.PutUint32(data[offset:offset+4], e.Checksum)

	//Before:
	//data[27:31] = [0x00, 0x00, 0x00, 0x00]  // Empty checksum space

	//After PutUint32 with checksum = 0x8A3F2B1C:
	//data[27:31] = [0x1C, 0x2B, 0x3F, 0x8A]  // Little-endian bytes

	return data
}

// Deserialize converts a byte slice into a LogEntry object
func Deserialize(data []byte) (*LogEntry, error) {
	//need at least minimum header size initialized
	minHeaderSize := 8 + 4 + 1 + 2 + 2 + 4 // LSN, EntrySize, Type, KeyLen, ValueLen, Checksum
	if len(data) < minHeaderSize {
		return nil, errors.New("insufficient data for log entry header")
	}

	offset := 0
	entry := &LogEntry{}

	// Read LSN (8 bytes)
	entry.LSN = binary.LittleEndian.Uint64(data[offset:offset+8])
	offset += 8
	// Read EntrySize (4 bytes)
	entry.EntrySize = binary.LittleEndian.Uint32(data[offset:offset+4])
	offset += 4

	// Validate we have enough data
	if len(data) < int(entry.EntrySize) {
		return nil, errors.New("incomplete log entry")
	}

	// Read Type (1 byte)
	entry.Type = data[offset]
	offset += 1
	// Read KeyLen (2 bytes)
	entry.KeyLen = binary.LittleEndian.Uint16(data[offset:offset+2])
	offset += 2
	// Read ValueLen (2 bytes)
	entry.ValueLen = binary.LittleEndian.Uint16(data[offset:offset+2])
	offset += 2

	// Read Key
	if offset+int(entry.KeyLen) > len(data) {
		return nil, errors.New("invalid key length")
	}
	entry.Key = string(data[offset : offset+int(entry.KeyLen)])
	offset += int(entry.KeyLen)
	
	// Read Value
	if offset+int(entry.ValueLen) > len(data) {
		return nil, errors.New("invalid value length")
	}
	entry.Value = string(data[offset : offset+int(entry.ValueLen)])
	offset += int(entry.ValueLen)
	
	// Read Checksum (4 bytes)
	if offset+4 > len(data) {
		return nil, errors.New("missing checksum")
	}
	entry.Checksum = binary.LittleEndian.Uint32(data[offset : offset+4])
	
	return entry, nil
}

// checks if the checksum of the entry is valid
func (e *LogEntry) ValidateChecksum() bool {

	//re-serialize the entry
	data := e.Serialize()

	//calculate the checksum of data except the last 4 bytes
	checksumData := data[0:len(data)-4]
	//run the CRC32 hash function on the checksumData and returns a 32 bit number.
	calculatedChecksum := crc32.ChecksumIEEE(checksumData)

	//compare the checksum of the re-serialized data to the checksum in the entry
	return calculatedChecksum == e.Checksum
}

func NewWAL(path string) (*WAL, error) {
	// WAL file path is the database path + ".wal" (ex. "test.db.wal")
	walPath := dbPath + ".wal"

	//
	file, err := os.OpenFile(walPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open WAL file: %w", err)
	}

	wal := &WAL{
		file: file,
		path: walPath,
		lastLSN: 0,
	}

	stat, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("failed to stat WAL file: %w", err)
	}

	if stat.Size() > 0 {
		if err := wal.scanForLastLSN(); err != nil {
			return nil, fmt.Errorf("failed to scan WAL file: %w", err)
		}
	}

	// start the background goroutine to flush the WAL to disk
	return wal, nil

}

func (w *WAL) scanForLastLSN() error {
	// Get file size
	stat, err := w.file.Stat()
	if err != nil {
		return err
	}
	
	fileSize := stat.Size()
	offset := int64(0)
	
	// Read through all entries
	for offset < fileSize {
		// Read entry header to get size
		headerBuf := make([]byte, 12) // LSN(8) + EntrySize(4)
		_, err := w.file.ReadAt(headerBuf, offset)
		if err != nil {
			// Reached end or corrupted entry
			break
		}
		
		lsn := binary.LittleEndian.Uint64(headerBuf[0:8])
		entrySize := binary.LittleEndian.Uint32(headerBuf[8:12])
		
		// Update lastLSN if this is higher
		if lsn > w.lastLSN {
			w.lastLSN = lsn
		}
		
		// Move to next entry
		offset += int64(entrySize)
	}
	
	return nil
	// **What this does:**
// - Reads through the entire WAL file
// - Finds the highest LSN number
// - Sets `lastLSN` so new entries continue from there

// **Example:**
// ```
// WAL file contains:
// Entry 1: LSN=1
// Entry 2: LSN=2  
// Entry 3: LSN=3

// After scan: w.lastLSN = 3
// Next append will use: LSN=4
}


// Append writes a new log entry to the WAL
func (w *WAL) Append(typ byte, key, value string) (uint64, error) {
	// Increment LSN for this new entry
	w.lastLSN++
	
	// Create the log entry
	entry := &LogEntry{
		LSN:      w.lastLSN,
		Type:     typ,
		Key:      key,
		Value:    value,
		KeyLen:   uint16(len(key)),
		ValueLen: uint16(len(value)),
	}
	
	// Serialize to bytes
	data := entry.Serialize()
	
	// Write to file (goes to end because we opened with O_APPEND)
	n, err := w.file.Write(data)
	if err != nil {
		return 0, fmt.Errorf("failed to write to WAL: %w", err)
	}
	
	if n != len(data) {
		return 0, fmt.Errorf("incomplete WAL write: wrote %d of %d bytes", n, len(data))
	}
	
	return w.lastLSN, nil

	// wal.Append(LogTypePut, "user:1", "john")

// Step by step:
// 1. w.lastLSN++ → now lastLSN = 1
// 2. Create entry with LSN=1
// 3. Serialize: [31 bytes of data]
// 4. Write to file at end
// 5. Return LSN=1
}

// Sync forces the OS to write buffered data to physical disk
// This is THE most important method for durability!
func (w *WAL) Sync() error {
	return w.file.Sync()
}

// ReadAll reads all log entries from the WAL file
func (w *WAL) ReadAll() ([]*LogEntry, error) {
	// Get file size
	stat, err := w.file.Stat()
	if err != nil {
		return nil, err
	}
	
	fileSize := stat.Size()
	if fileSize == 0 {
		return []*LogEntry{}, nil // Empty WAL
	}
	
	// Read entire file into memory
	data := make([]byte, fileSize)
	_, err = w.file.ReadAt(data, 0)
	if err != nil {
		return nil, fmt.Errorf("failed to read WAL: %w", err)
	}
	
	// Parse entries
	entries := []*LogEntry{}
	offset := 0
	
	for offset < len(data) {
		// Need at least 12 bytes for header
		if offset+12 > len(data) {
			break // Not enough data for another entry
		}
		
		// Read entry size
		entrySize := binary.LittleEndian.Uint32(data[offset+8 : offset+12])
		
		// Check if we have complete entry
		if offset+int(entrySize) > len(data) {
			// Incomplete entry - stop here (probably crashed during write)
			break
		}
		
		// Deserialize entry
		entry, err := DeserializeLogEntry(data[offset : offset+int(entrySize)])
		if err != nil {
			// Corrupted entry - stop here
			break
		}
		
		// Verify checksum
		if !entry.VerifyChecksum() {
			// Checksum mismatch - stop here (corrupted!)
			break
		}
		
		// Entry is valid, add to list
		entries = append(entries, entry)
		
		// Move to next entry
		offset += int(entrySize)
	}
	
	return entries, nil
	// **What this does:**
// - Reads the entire WAL file into memory
// - Parses each entry one by one
// - **Stops at first corrupted entry** (incomplete or bad checksum)
// - Returns all valid entries

// **Example:**
// 
// WAL file (100 bytes):
// [Entry 1: 31 bytes, checksum ✓]
// [Entry 2: 35 bytes, checksum ✓]
// [Entry 3: 20 bytes, checksum ✗] ← Corrupted!
// [Entry 4: 14 bytes] ← Never checked

// ReadAll() returns: [Entry 1, Entry 2]
// Stops at corrupted Entry 3
}




// Close closes the WAL file
func (w *WAL) Close() error {
	if w.file != nil {
		return w.file.Close()
	}
	return nil
}

// Truncate removes all entries from the WAL
// Used after checkpoint when all operations are safely in pages
func (w *WAL) Truncate() error {
	// Close current file
	if err := w.file.Close(); err != nil {
		return err
	}
	
	// Delete the file
	if err := os.Remove(w.path); err != nil {
		return err
	}
	
	// Create new empty WAL
	file, err := os.OpenFile(w.path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	
	w.file = file
	w.lastLSN = 0
	
	return nil

// 	// What this does:

// Deletes the entire WAL file
// Creates a fresh empty one
// Used after checkpoint (we'll cover this later)
}
// TestWALOperations tests writing and reading WAL entries
func TestWALOperations() {
	fmt.Println("\n=== Testing WAL Operations ===")
	
	// Clean up any existing test WAL
	os.Remove("test_wal.db.wal")
	
	// 1. Create new WAL
	wal, err := NewWAL("test_wal.db")
	if err != nil {
		fmt.Printf("❌ Failed to create WAL: %v\n", err)
		return
	}
	fmt.Println("✓ Created WAL file")
	
	// 2. Write some entries
	lsn1, _ := wal.Append(LogTypePut, "user:1", "john_doe")
	fmt.Printf("✓ Appended entry LSN=%d: PUT user:1=john_doe\n", lsn1)
	
	lsn2, _ := wal.Append(LogTypePut, "user:2", "jane_smith")
	fmt.Printf("✓ Appended entry LSN=%d: PUT user:2=jane_smith\n", lsn2)
	
	lsn3, _ := wal.Append(LogTypeDelete, "user:1", "")
	fmt.Printf("✓ Appended entry LSN=%d: DELETE user:1\n", lsn3)
	
	// 3. Sync to disk
	wal.Sync()
	fmt.Println("✓ Synced to disk")
	
	// 4. Read entries back
	entries, err := wal.ReadAll()
	if err != nil {
		fmt.Printf("❌ Failed to read WAL: %v\n", err)
		return
	}
	
	fmt.Printf("\n✓ Read %d entries from WAL:\n", len(entries))
	for _, entry := range entries {
		typeName := "PUT"
		if entry.Type == LogTypeDelete {
			typeName = "DELETE"
		}
		fmt.Printf("  LSN=%d %s %s=%s (checksum: %08X)\n", 
			entry.LSN, typeName, entry.Key, entry.Value, entry.Checksum)
	}
	
	// 5. Close
	wal.Close()
	fmt.Println("\n✓ Closed WAL")
	
	// 6. Reopen and verify persistence
	fmt.Println("\n--- Testing Persistence ---")
	wal2, _ := NewWAL("test_wal.db")
	fmt.Printf("✓ Reopened WAL, lastLSN=%d\n", wal2.lastLSN)
	
	entries2, _ := wal2.ReadAll()
	fmt.Printf("✓ Still has %d entries after reopen\n", len(entries2))
	
	wal2.Close()
	
	// Cleanup
	os.Remove("test_wal.db.wal")
}