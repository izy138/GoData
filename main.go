package main

import (
	"encoding/binary" // convert numbers into bytes
	"errors"          // creating error message
	"fmt"             // for printing and formatting any strings
	"os"              // for file opterations like create,open,read,write
)

// database rules
const (
	PageSize    = 4096       // db stores data in chunks calls pages. 4KB is the common size
	HeaderSize  = 64         // the first 64 bytes of a file will contain metadata about my db
	MagicNumber = 0x4D594442 // "MYDB" in hex, acts like a signature. db checks the start of file for it make sure its a db file
	Version     = 1
)

// data container - Pages hold the data, and the db needs to know what page its looking at,
// whats inside it and whether changes have been made.
type Page struct {
	ID          uint32         // tells us which page it is (Page1,2,etc)
	Data        [PageSize]byte // the 4KD of storage for the key-value pairs
	IsDirty     bool           // check for if the page has been changed since it was loaded from the disk. if yes, db saves it.
	RecordCount uint16         // count of how many key-value pairs are stored in the page.
}

// The database storage manager - keeps track of where every page is stored
type Storage struct {
	file       *os.File          // actual database file on the disk
	pageSize   int               // how big each page is (will be 4096 bytes)
	pageIndex  map[string]uint32 // key to page ID mapping: map that gives us "key'user:1' is stored in page 1"
	pages      map[uint32]*Page  // the loaded pages cache: is the pages we've loaded into memory
	nextPageID uint32            // which ID to give the next new page
	totalPages uint32            // how many pages exist in total
}

// when opening a db file, we need to know how its organized, its a header tag that acts like a table of contents
type Header struct {
	Magic      uint32 // 'MYDB' signature to verify the file
	Version    uint32 // the version of our databases format
	PageSize   uint32 // the size of the pages (4096 bytes)
	TotalPages uint32 // how many pages are in the database
	NextPageID uint32 // What ID the next new page will be
}

// tries to open an existing file for reading/writing.
// if it fails = file doesnt exist, so we create a new file.
func NewStorage(filename string) (*Storage, error) {
	// first try to open existing file
	// if successful: file = our opened file
	// if something went wrong: err contains the error.
	file, err := os.OpenFile(filename, os.O_RDWR, 0644)

	// if there is an error in opening the file, the file doesnt exist, so create it
	if err != nil {
		file, err = os.Create(filename)
		//if we cant create a file, returns error
		if err != nil {
			return nil, fmt.Errorf("failed to created db file: %w", err)
		}
	}

	// creates the Storage struct and initialize the pageIndex and pages mappings,
	// which both start as empty. sets the file we opened/created to the storage.
	storage := &Storage{
		file:      file,
		pageSize:  PageSize,
		pageIndex: make(map[string]uint32),
		pages:     make(map[uint32]*Page),
	}

	// checks if the file is new (empty) or if it exists
	stat, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("failed to stat file: %w", err)
	}

	// if the size is 0 then that it is an empty file, so we set up a new db
	// stat.Size checks how many bytes are in the file
	if stat.Size() == 0 {
		// initializes a new file, with header
		if err := storage.initializeNewFile(); err != nil {
			return nil, err
		}
	} else {
		if err := storage.loadHeader(); err != nil {
			return nil, err
		}
		if err := storage.buildIndex(); err != nil {
			return nil, err
		}
	}

	return storage, nil
	// METHOD LOGIC:
	// 1. Try to open file "test.db"
	//    ↓
	// 2. Did that work?
	//    ├─ No → Try to create new file
	//    └─ Yes → Continue
	//    ↓
	// 3. Get file info (size, etc.)
	//    ↓
	// 4. Is the file empty (0 bytes)?
	//    ├─ Yes → This is a NEW database
	//    │        → Set up header and initial structure
	//    └─ No → This is an EXISTING database
	//            → Read the header to understand the structure
	//            → Build index by scanning existing data
}

// we a have new empty file, that we want to become a database.
func (s *Storage) initializeNewFile() error {
	// we create the header struct for it.
	// the "birth certificate" literally the header of any notebook page: name, date,"page count: 0"
	header := Header{
		Magic:      MagicNumber,        // sig that identifies the db file
		Version:    Version,            // 1
		PageSize:   uint32(s.pageSize), // 4096 bytes per page
		TotalPages: 0,                  // 0 (no data pages exist in the db yet)
		NextPageID: 0,                  // WHen we create the first page, it will start as page 0)
	}

	// updates the in-memory Storage object to match the header.
	// tracks the state of the db
	s.nextPageID = 0
	s.totalPages = 0

	// calls another function to actually write the 64 bytes to the file.
	return s.writeHeader(&header) //passes a pointer address to the header

	// NEW DATABASE INITIALIZATION:
	// 1. We have an empty file (0 bytes)
	//    ↓
	// 2. Create a Header struct with initial values:
	//    - Magic: "MYDB"
	//    - Version: 1
	//    - PageSize: 4096
	//    - TotalPages: 0 (no data yet)
	//    - NextPageID: 0 (first page will be #0)
	//    ↓
	// 3. Update our Storage object to match:
	//    - s.nextPageID = 0
	//    - s.totalPages = 0
	//    ↓
	// 4. Write this header to the first 64 bytes of file
	//    ↓
	// 5. File now looks like:
	//    [64 bytes of header][rest of file is empty]

	// 	Byte 0-63:  HEADER
	//             Magic: "MYDB"
	//             Version: 1
	//             PageSize: 4096
	//             TotalPages: 0
	//             NextPageID: 0

	// Byte 64+:   [Empty space - no pages created yet]
}

func (s *Storage) writeHeader(header *Header) error {
	headerBytes := make([]byte, HeaderSize) // makes an empty 64 byte array

	// coverts the numbers into bytes to be stored into the 64-byte array headerBytes
	// PutUInt32 puts the unisigned int 32 bit
	binary.LittleEndian.PutUint32(headerBytes[0:4], header.Magic)
	binary.LittleEndian.PutUint32(headerBytes[4:8], header.Version)
	binary.LittleEndian.PutUint32(headerBytes[8:12], header.PageSize)
	binary.LittleEndian.PutUint32(headerBytes[12:16], header.TotalPages)
	binary.LittleEndian.PutUint32(headerBytes[16:20], header.NextPageID)

	// writes data starting a speicif position : WriteAt(data, offset)
	// will write all 64 bytes to the start of the file.
	_, err := s.file.WriteAt(headerBytes, 0)
	if err != nil {
		return fmt.Errorf("failed to read header: %w", err)
	}
	// forces the OS to wrtie the data to the disk
	// without doing this, the data could sit in memory and be lost with program crash
	return s.file.Sync()
	// 	CREATING A NEW DATABASE:
	// 1. User runs: NewStorage("test.db")
	//    ↓
	// 2. File doesn't exist, so create empty file
	//    ↓
	// 3. File size = 0, so call initializeNewFile()
	//    ↓
	// 4. Create header with initial values
	//    ↓
	// 5. writeHeader() converts numbers to bytes
	//    ↓
	// 6. Write 64 bytes to start of file
	//    ↓
	// 7. Force write to disk with Sync()
	//    ↓
	// 8. File now has proper database header!
}

// we load a file that contains data
// this will read the header to understand how its organized
func (s *Storage) loadHeader() error {
	// create empty 64 byte aray for reading the header
	headerBytes := make([]byte, HeaderSize)

	// opens and reads the file header from the start
	_, err := s.file.ReadAt(headerBytes, 0)
	if err != nil {
		return fmt.Errorf("failed to read header: %w", err)
	}

	// converts the BYTES back into numbers
	// Uint32 converts 4 bytes back into a 32 bit number
	header := Header{
		Magic:      binary.LittleEndian.Uint32(headerBytes[0:4]),
		Version:    binary.LittleEndian.Uint32(headerBytes[4:8]),
		PageSize:   binary.LittleEndian.Uint32(headerBytes[8:12]),
		TotalPages: binary.LittleEndian.Uint32(headerBytes[12:16]),
		NextPageID: binary.LittleEndian.Uint32(headerBytes[16:20]),
	}

	// validates the header info
	if header.Magic != MagicNumber {
		return errors.New("invalid file format: magic number mismatch")
	}
	if header.Version != Version {
		return fmt.Errorf("incorrect version %d", header.Version)
	}
	if header.PageSize != uint32(s.pageSize) {
		return fmt.Errorf("page size mismatch: expected %d, got %d", s.pageSize, header.PageSize)
	}

	// updates the Storage object
	// sets the variables to match the file
	s.nextPageID = header.NextPageID
	s.totalPages = header.TotalPages

	return nil
	// 	LOADING EXISTING DATABASE:
	// 1. We have a file with size > 0 (contains data)
	//    ↓
	// 2. Create 64-byte array to hold header
	//    ↓
	// 3. Read first 64 bytes from file into array
	//    ↓
	// 4. Convert bytes back to numbers:
	//    - Bytes 0-3 → Magic number
	//    - Bytes 4-7 → Version
	//    - Bytes 8-11 → PageSize
	//    - Bytes 12-15 → TotalPages
	//    - Bytes 16-19 → NextPageID
	//    ↓
	// 5. VALIDATE everything:
	//    ✓ Magic = "MYDB"? (Is this our file?)
	//    ✓ Version = 1? (Can we understand it?)
	//    ✓ PageSize = 4096? (Matches our expectations?)
	//    ↓
	// 6. Update our Storage object with file's values
	//    ↓
	// 7. Ready to work with existing database!
}

// we opened an existing database, there are pages with data,
// but dont know what kets are stored and where
func (s *Storage) buildIndex() error {
	// loops through all the pages. s.totalPages = 3 it loops though pageID 0,1,2
	for pageID := uint32(0); pageID < s.totalPages; pageID++ {

		// loads each page into memory
		page, err := s.loadPage(pageID)
		if err != nil {
			return fmt.Errorf("failed to load page %d during index build: %w", pageID, err)
		}

		// Scan records in the page add to index
		// RecordCount contains the number of key value pairs in the page
		offset := 2 // skips the RecordCount header the first 2 butes of each page contains record count.
		for i := uint16(0); i < page.RecordCount; i++ {

			if offset+4 > len(page.Data) {
				break
			}

			// 	Page Data (4096 bytes):
			//	0  1
			// [2][0]     ← Record Count (2 bytes) offset=2 skips it
			//	2  3  4  5
			// [6][0][4][0] ← Record 1 header (key length, value length)
			//   6    7    8    9	10   11
			// ['u']['s']['e']['r'][':']['1'] ← Record 1 data (key + value)
			//  12   13   14   15
			// ['j']['o']['h']['n']

			// page.Data[2:4] contains key length
			// page.Data[4:6] contains value length
			keyLen := binary.LittleEndian.Uint16(page.Data[offset : offset+2])
			valueLen := binary.LittleEndian.Uint16(page.Data[offset : offset+2])
			// move the position forward by 4 bytes to get to the value indexes
			offset += 4

			// makes sure we dont read past the end of the page.
			if offset+int(keyLen)+int(valueLen) > len(page.Data) {
				break
			}

			// key is recorded using the current offset and the key length.
			// converts the bytes into a string (key)
			key := string(page.Data[offset : offset+int(keyLen)])
			// adds to key to index: "key _ is stored in page 0"
			s.pageIndex[key] = pageID

			// the offset moves up past the key and value,
			// to record the next key and value length and continue the loop until the page ends.
			offset += int(keyLen) + int(valueLen)
		}
	}
	return nil

	// 	BUILDING THE INDEX:
	// 1. Header told us: "This database has 3 pages total"
	//    ↓
	// 2. For each page (0, 1, 2):
	//    ↓
	// 3. Load page from disk into memory (4KB of data)
	//    ↓
	// 4. Read first 2 bytes: "This page has 2 records"
	//    ↓
	// 5. For each record in this page:
	//    a. Read record header: keyLen=6, valueLen=4
	//    b. Safety check: do we have 10 more bytes?
	//    c. Extract key: bytes[6:12] = "user:1"
	//    d. Add to index: pageIndex["user:1"] = currentPageID
	//    e. Move forward: offset += 6 + 4 = 10
	//    ↓
	// 6. Repeat for next record in same page
	//    ↓
	// 7. Move to next page
	//    ↓
	// 8. When done: pageIndex contains location of every key!
}

// pageOffset() - Calculate where pages live in the file
// loadPage() - Read a page from disk into memory
// writePage() - Write a page from memory to disk
// allocateNewPage() - Create a brand new page
// updateHeader() - Save current database state

// calculates the exact address where the page is stored in the file
func (s *Storage) pageOffset(pageID uint32) int64 {
	return int64(HeaderSize + pageID*uint32(s.pageSize))
}

//0-63 : the header
//64-4159 : Page 0
//4160-8255 : Page 1
//8256-12351 : Page 2

// Example:
// pageID = 0
// HeaderSize = 64 bytes
// s.pageSize = 4096 bytes
// offset = 64 + (0 * 4096) = 64

// pageID = 2
// offset = 64 + (2 * 4096) = 64 + 8192 = 8256

func (s *Storage) loadPage(pageID uint32) (*Page, error) {
	// checks if the page is in cache already
	// looks in the in-memory cache (the s.pages map)
	// **reading directly from memory is 1000x faster than reading from the disk
	if page, exists := s.pages[pageID]; exists {
		return page, nil
	}

	// reads the page from disk
	offset := s.pageOffset(pageID)       // uses the pageOffset() function to find the exact byte position
	pageData := make([]byte, s.pageSize) // creates a 4096 byte array to hold the page data to hold the data read from disk

	_, err := s.file.ReadAt(pageData, offset) // reads exactly 4096 bytes starting at the calculated offset
	// ReadAt lets you read from any position in the file
	// example: we want Page 1 which starts from 4160-8255.
	// so it will be: s.file.ReadAt(pageData, 4160)
	if err != nil {
		return nil, fmt.Errorf("failed to read page %d: %w", pageID, err)
	}

	// creates a page object
	page := &Page{
		ID:      pageID,
		IsDirty: false,
	}
	copy(page.Data[:], pageData)
	// creates a new page struct and sets the ID and marks it as clean (isDirty = false because it has not been changed ie it matches whats on the disk)

	// next we parse the page metadata, every page has a mini header
	if len(pageData) >= 2 {
		page.RecordCount = binary.LittleEndian.Uint16(pageData[0:2])
	}
	// bytes 0-1: Record count (how many key-value pairs are in this page)
	// bytes 2+: Actual records (key-value pairs)

	// so example lets say there are 3 records (3 key value pairs) in a page
	// uint16 is 16 bits, so 2 bytes.
	// we want to store the number 3 in these 16 bits.
	// Decimal = 3 , Binary: 00000011, Hex: 0x03
	// we need 16 bits so in binary: 00000000 00000011 and hex: 0x00 0x03
	// Big Endian is the most significant bit first: 0x00, 0x03
	// Little Endian is the least significant bit first: 0x03, 0x00
	// so when we get the pageData it would be: binary.LittleEndian.Uint16([0x03, 0x00]) = 3

	// Cache the loaded page
	// stores the page in memory cache for faster future access
	s.pages[pageID] = page

	return page, nil
}

// the first access in Disk would be ~5ms, the second acces in memeory would be ~0.0005ms (1000x faster)

// Makes changes permanent (crucial)
func (s *Storage) writePage(page *Page) error {
	// when you modify a page by adding or deleting a record, we need to update the page.RecordCount
	// this method ensures the first 2 bytes of the page always reflect the current record count

	// update the record count number in page data
	// example: have it update to 3 pages: sets the slice[0] = byte(value) to the low priority bit 0x03 , and slice[1]= byte(value >> 8) to high prio 0x00
	binary.LittleEndian.PutUint16(page.Data[0:2], page.RecordCount)

	// gets the exact byte position when the page would be found in the file
	offset := s.pageOffset(page.ID)

	// writes the new pages 4096 bytes to disk
	_, err := s.file.WriteAt(page.Data[:], offset)
	if err != nil {
		return fmt.Errorf("failed to write page %d: %w", page.ID, err)
	}

	page.IsDirty = false
	// the page in disk now match what is in memory
	// we dont have to waste time to write it in disk until it changes again.

	return s.file.Sync()
	//force disk write, forces the os to write to disk, without it, the data could sit in os buffers and lost when power is off
}

// Start:
// page := &Page{
//     ID: 1,
//     RecordCount: 3,  // We added a record
//     IsDirty: true,   // Needs to be written
//     Data: [0x02,0x00,...] // Still shows old count!
// }
// Step-by-step execution:

// Fix header: page.Data[0:2] becomes [0x03,0x00]
// Calculate position: offset = 4160 for page 1
// Write 4096 bytes: All of page.Data gets written to disk at position 4160
// Mark clean: page.IsDirty = false
// Force sync: OS writes from buffer to actual disk

// Final state:
// gopage := &Page{
//     ID: 1,
//     RecordCount: 3,
//     IsDirty: false,  // Clean! Matches disk
//     Data: [0x03,0x00,...] // Header fixed
// }

func (s *Storage) allocateNewPage() *Page {
	// Creates a new page object using the next availble page id,
	// the page only exists in memory and needs to be written to the disk, so isDirty is true
	// and the RecordCount is 0 beccause the new page starts as empty.
	page := &Page{
		ID:          s.nextPageID,
		IsDirty:     true,
		RecordCount: 0,
	}

	//initialize the pages header record count as 0
	binary.LittleEndian.PutUint16(page.Data[0:2], 0)
	//Byte 0: 0x00  ← Low byte of record count (0 records)
	// Byte 1: 0x00  ← High byte of record count
	// Byte 2: 0x00  ← Uninitialized data

	//adds to cache
	//stores the new page in the in-memory cache
	s.pages[page.ID] = page
	//update the metadata: nextPageID and totalPages is incremented to keep track of correct page number
	s.nextPageID++
	s.totalPages++

	return page
}

// allocateNewPage() is called when:

// Database is empty: First page creation
// All existing pages are full: Need more space for new records
// Optimal performance: Sometimes we pre-allocate pages

func (s *Storage) updateHeader() error {
	header := Header{
		Magic:      MagicNumber,
		Version:    Version,
		PageSize:   uint32(s.pageSize),
		TotalPages: s.totalPages,
		NextPageID: s.nextPageID,
		//The first three fields never change, but the last two are dynamic and reflect our current database state.
	}
	//writeHeader() function to actually save these values to the file.
	return s.writeHeader(&header)
	// In Memory (what we're working with):
	// s.totalPages = 3    // We have 3 pages
	// s.nextPageID = 3    // Next new page will be #3
	// On Disk (what the file header says):
	// TotalPages: 2       // File still thinks we have 2 pages!
	// NextPageID: 2       // File thinks next page should be #2!
	// Without updateHeader(): If our program crashes, when we restart:

	// We read the old header from disk
	// We think we only have 2 pages
	// We think nextPageID = 2
	// Data loss! Page 2 exists but we don't know about it
}

func (s *Storage) Close() error {
	// Like Save all and exit it makes sure everything in memory gets written to disk before shutting down.
	// goes through each page in the database to check if dirty (new changes)
	for _, page := range s.pages {
		if page.IsDirty {
			if err := s.writePage(page); err != nil {
				return err // Stop immediately if page write fails
			}
		}
	}

	//update header metadata
	if err := s.updateHeader(); err != nil {
		return err // Stop if header update fails
	}
	return s.file.Close()
}

func serializeRecord(key, value string) []byte {
	//converts the string to bytes
	keyBytes := []byte(key)     //key = [user:1] length:5
	valueBytes := []byte(value) //value = [isa] length:3

	//calculates the total size needed
	recordSize := 4 + len(keyBytes) + len(valueBytes) // 4 + 6 + 3 = 13 bytes
	record := make([]byte, recordSize)                //creates the byte array 13 byte array filled with 0

	//takes the length (6) of the key= [user:1] and converts it to bytes at index 0-1 [0x06, 0x00, 0,0,0,0,0,0,0,0,0,0,0]
	binary.LittleEndian.PutUint16(record[0:2], uint16(len(keyBytes)))
	//writes the length (3) of the value = [isa]  at index 2-3 [0x06, 0x00, 0x03, 0x00, 0,0,0,0,0,0,0,0]
	binary.LittleEndian.PutUint16(record[2:4], uint16(len(valueBytes)))

	//copies 'user:1' to positions 4-8 [0x06, 0x00, 0x03, 0x00, 'u, 's', 'e', 'r', ':', '1',0,0,0,0]
	copy(record[4:4+len(keyBytes)], keyBytes)
	// copies 'isa' to positions 10-12 [0x05, 0x00, 0x03, 0x00, 'u', 's', 'e', 'r', ':','1', 'i', 's', 'a']
	copy(record[4+len(keyBytes):], valueBytes)

	return record
}

// reverse of serializeRecord() - it takes bytes and extracts the original key-value pair.
func deserializeRecord(data []byte, offset int) (key, value string, bytesRead int, err error) {
	// data = [0x01,0x00,0x06,0x00,0x03,0x00,'u','s','e','r',':','1','i','s','a']
	//          0    1     2    3    4    5   6   7   8   9   10  11  12  13  14
	// offset is still 2
	// need at least 4 bytes to read the header (2 for keyLen + 2 for valueLen)
	if offset+4 > len(data) {
		return "", "", 0, errors.New("insufficient data for record header")
	}

	// Example: data[2:4] = [0x06, 0x00] → keyLen = 6
	keyLen := binary.LittleEndian.Uint16(data[offset : offset+2])
	// Example: data[4:6] = [0x03, 0x00] → valueLen = 3
	valueLen := binary.LittleEndian.Uint16(data[offset+2 : offset+4])
	// Example: totalLen = 4 (header) + 6 (key) + 3 (value) = 13 bytes
	totalLen := 4 + int(keyLen) + int(valueLen)

	//make sure I actually have 9 bytes of data available
	// prevents reading beyond the end of the data array
	if offset+totalLen > len(data) {
		return "", "", 0, errors.New("insufficient data for complete record")
	}
	// Extract key string from data
	// Example: offset=2, keyLen=6
	//   Start: offset+4 = 2+4 = 6
	//   End:   offset+4+ keyLen = 2+4+6 = 12
	//   key = string(data[6:12]) = string(['u','s','e','r',':','1']) = "user:1"
	key = string(data[offset+4 : offset+4+int(keyLen)])

	// Extract value string from data
	// Example: offset=2, keyLen=6, totalLen=13
	//   Start: offset+4+keyLen = 2+4+6 = 12
	//   End:   offset+totalLen = 2+13 = 15
	//   value = string(data[12:15]) = string(['i','s','a']) = "isa"
	value = string(data[offset+4+int(keyLen) : offset+totalLen])

	// Return extracted key-value pair and total bytes consumed
	// bytesRead tells caller where next record starts (current offset + 13) = 15
	return key, value, totalLen, nil
}

//Page level record functions (add, find, delete records)

// finds the end of existing records in a page and appends the new record there.
func (p *Page) addRecord(key, value string) error {
	// Serioalize the key and value into record = [0x05, 0x00, 0x03, 0x00, 'u, 's', 'e', 'r', '2', 'c', 'a', 'm']
	record := serializeRecord(key, value)

	// Find where records end in the page, goes through all records on the page using the recordcount
	offset := 2 // Skip record count
	for i := uint16(0); i < p.RecordCount; i++ {
		if offset+4 > len(p.Data) {
			return errors.New("corrupted page: invalid record offset")
		}

		keyLen := binary.LittleEndian.Uint16(p.Data[offset : offset+2])
		valueLen := binary.LittleEndian.Uint16(p.Data[offset+2 : offset+4])
		offset += 4 + int(keyLen) + int(valueLen)
	}
	// Current Page Layout:
	// [0-1]:   0x01, 0x00           		// RecordCount = 1
	// [2-5]:   0x06, 0x00, 0x03, 0x00  	// Record 1 header: key length= 6, value length= 3
	// [6-11]:  'u','s','e','r',':','1' 	// Record 1 key: "user:1" (6 bytes)
	// [12-14]: 'i','s','a'					// Record 1 value: "isa" (3 bytes)
	// len(record) = 13 (header(4 bytes) + key(6 bytes) + value(3 bytes) = 13)
	// [15+] is empty space
	//
	// Check if there's enough space
	if offset+len(record) > len(p.Data) {
		return errors.New("page full: not enough space for record")
	}
	// offset = 15           				// Used space
	// len(record) = 13	        			// New record size
	// total_needed = 15 + 13 = 28 bytes
	// len(p.Data) = 4096       			// Page size
	// 28 < 4096 ✓              			// Fits!

	// Add the record
	//p.Data[15:28] = [0x05, 0x00, 0x03, 0x00, 'u', 's', 'e', 'r', ':', '2', 'c', 'a', 'm']
	//					15	  16 	17    18	19	 20   21   22	23	 24	  25   26	27
	copy(p.Data[offset:offset+len(record)], record)

	p.RecordCount++
	p.IsDirty = true

	return nil
}

// scans through all record in the page for a matching key
func (p *Page) findRecord(key string) (value string, found bool) {
	//skips the record count
	offset := 2

	// goes through the recordCount and deserializes the content
	for i := uint16(0); i < p.RecordCount; i++ {
		recordKey, recordValue, bytesRead, err := deserializeRecord(p.Data[:], offset)
		// Returns: "user:1", "isa", 15, nil
		// Returns: "user:2", "cam", 28, nil
		if err != nil {
			return "", false // Corrupted page
		}

		if recordKey == key {
			return recordValue, true
		}

		offset += bytesRead
	}
	return "", false
}

// remove data from a page
// finds a removes a specific key-value pair from the page, and then shifts
// all the remaining data left to fill the gap.
func (p *Page) deleteRecord(key string) bool {
	// method is called to delete the 2nd record: deleteRecord("user:1")

	offset := 2 // skip record count - the first 2 bytes

	//loop through all the records in the page
	for i := uint16(0); i < p.RecordCount; i++ {
		recordKey, _, bytesRead, err := deserializeRecord(p.Data[:], offset)
		// ^ first pass returns return "user:1", "isa", 13, nil
		if err != nil {
			return false // Corrupted page
		}

		//recordKey = "user:1"
		//bytesRead = 13
		//offset = 2
		//Check: "user:1" == "user:1" - its a match!
		if recordKey == key {
			// Found the record to delete - shift remaining records left
			nextOffset := offset + bytesRead           // 2 + 13 = 15 is the next offset - where record 2 starts
			remainingBytes := len(p.Data) - nextOffset // 4096 - 15 = 4081 bytes remaining

			// THE SHIFT OPERATION:
			// the Destination (What byte we are copying to) = p.Data[2:] <- we are copying starting at byte 2
			// the Source (What we are copying) = p.Data[15:15+4081] <- we are copy everything in the record between byte 15 and 4096
			copy(p.Data[offset:], p.Data[nextOffset:nextOffset+remainingBytes])
			// we are OVERWRITING record 1, so everything after the record is shifted left.
			//  [2]:     0x05  ← copied from [15]
			//	[3]:     0x00  ← copied from [16]
			//	[4]:     0x03  ← copied from [17]
			//	[5]:     0x00  ← copied from [18]
			//	[6]:     'u'   ← copied from [19]
			//	[7]:     's'   ← copied from [20]
			//	[8]:     'e'   ← copied from [21]
			//	[9]:     'r'   ← copied from [22]
			//	[10]:    ':'   ← copied from [23]
			//	[11]:    '2'   ← copied from [24]
			//	[12]:    'c'   ← copied from [25]
			//	[13]:    'a'   ← copied from [26]
			//	[14]:    'm'   ← copied from [27]
			//	[15+]:   empty ← the rest shifts but stays empty
			p.RecordCount--  // update the record count
			p.IsDirty = true // we changed the data so it is dirty
			return true
		}
		// we update this offset to keep track of the offset
		// for example if we had to delete record 2 instead, and we had a 3rd record after it,
		// the offset would start at byte 15 for shifting operation.
		// nextOffset = 15 + 12 = 27
		// remainingBytes = 4096 - 27 = 4069
		// copy(p.Data[15:], p.Data[27:27+4069])
		offset += bytesRead
	}

	return false
}

// Storage.Put() - used for Inserting or Updating Data
// method called to update user:1 = db.Put("user:1", "leonor")
func (s *Storage) Put(key, value string) error {
	// Case 1: Key exists already
	// Check if key already exists
	// looks in the in-memory index - the fast lookup map
	// we check the page index first because its in RAM (fast lookup)
	// we avoid scanning through all the pages on the disk (very slow)
	//
	// s.pageIndex["user:1"] → returns pageID = 0, exists = true
	if pageID, exists := s.pageIndex[key]; exists {
		// loads page 0 from disk (or cache is already loaded)
		page, err := s.loadPage(pageID)
		if err != nil {
			return err
		}

		// delete old record and add new one
		//BEFORE deleteRecord:
		//[0-1]:   RecordCount = 2
		//[2-14]:  "user:1" = "isa"      ← DELETE THIS
		//[15-27]: "user:2" = "cam"
		//
		//AFTER deleteRecord:
		//[0-1]:   RecordCount = 1
		//[2-14]:  "user:2" = "cam"          ← Shifted left!
		//[15+]:   empty space
		page.deleteRecord(key)
		if err := page.addRecord(key, value); err != nil {
			return err
		}
		//AFTER addRecord:
		//[0-1]:   RecordCount = 2
		//[2-14]:  "user:2" = "cam"
		//[15-30]: "user:1" = "leonor"  ← NEW! (might be different size)
		//[31+]:   empty space
		return nil
	}

	// Case 2: Key doesn't exist - find a page with space or create new page
	// method called: db.Put("user:3", "alice")  exists = false
	var targetPage *Page

	// Try to find a page with space (simple linear search for now)
	for pageID := uint32(0); pageID < s.totalPages; pageID++ {
		page, err := s.loadPage(pageID)
		if err != nil {
			continue
		}

		// Estimate if record will fit
		recordSize := 4 + len(key) + len(value)
		usedSpace := 2 // Record count header
		for i := uint16(0); i < page.RecordCount; i++ {
			if usedSpace+4 > len(page.Data) {
				break
			}
			keyLen := binary.LittleEndian.Uint16(page.Data[usedSpace : usedSpace+2])
			valueLen := binary.LittleEndian.Uint16(page.Data[usedSpace+2 : usedSpace+4])
			usedSpace += 4 + int(keyLen) + int(valueLen)
		}

		if usedSpace+recordSize <= len(page.Data) {
			targetPage = page
			break
		}
	}

	// If no page has space, allocate a new one
	if targetPage == nil {
		targetPage = s.allocateNewPage()
	}

	// Add the record
	if err := targetPage.addRecord(key, value); err != nil {
		return err
	}

	// Update index
	s.pageIndex[key] = targetPage.ID

	return nil
}

func (s *Storage) Get(key string) (string, error) {
	pageID, exists := s.pageIndex[key]
	if !exists {
		return "", errors.New("key not found")
	}

	page, err := s.loadPage(pageID)
	if err != nil {
		return "", err
	}

	value, found := page.findRecord(key)
	if !found {
		return "", errors.New("key not found in expected page")
	}

	return value, nil
}

func (s *Storage) Delete(key string) error {
	pageID, exists := s.pageIndex[key]
	if !exists {
		return errors.New("key not found")
	}

	page, err := s.loadPage(pageID)
	if err != nil {
		return err
	}

	if !page.deleteRecord(key) {
		return errors.New("key not found in expected page")
	}

	// Remove from index
	delete(s.pageIndex, key)

	return nil
}
