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