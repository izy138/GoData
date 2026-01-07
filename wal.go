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
