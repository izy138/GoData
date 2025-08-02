package main


import (
	"encoding/binary" 	// convert numbers into bytes
	"errors" 			// creating error message
	"fmt" 				// for printing and formatting any strings
	"os" 				// for file opterations like create,open,read,write
)

//database rules
const (
	PageSize = 4096 // db stores data in chunks calls pages. 4KB is the common size 
	HeaderSize = 64 // the first 64 bytes of a file will contain metadata about my db
	MagicNumber = 0x4D594442 // "MYDB" in hex, acts like a signature. db checks the start of file for it make sure its a db file
	Version = 1 
)

// data container - Pages hold the data, and the db needs to know what page its looking at, 
//  whats inside it and whether changes have been made.
type Page struct {
	ID 			uint32 				// tells us which page it is (Page1,2,etc)
	Data 		[PageSize]byte		// the 4KD of storage for the key-value pairs
	isDirty 	bool			// check for if the page has been changed since it was loaded from the disk. if yes, db saves it.
	RecordCount uint16		// count of how many key-value pairs are stored in the page.
}

// The database storage manager - keeps track of where every page is stored
type Storage struct {
	file		*os.File 		// actual database file on the disk
	pageSize 	int				// how big each page is (will be 4096 bytes)
	pageIndex	map[string]uint32	// key to page ID mapping: map that gives us "key'user:1' is stored in page 1"
	pages		map[uint32]*Page	// the loaded pages cache: is the pages we've loaded into memory
	nextPageID	uint32			// which ID to give the next new page
	totalPages  uint32			// how many pages exist in total
}

//when opening a db file, we need to know how its organized, its a header tag that acts like a table of contents
type Header struct {
	Magic 		uint32 		// 'MYDB' signature to verify the file 
	Version 	uint32		// the version of our databases format
	PageSize 	uint32		// the size of the pages (4096 bytes)
	TotalPages 	uint32		// how many pages are in the database
	nextPageID	uint32		// What ID the next new page will be
}

// tries to open an existing file for reading/writing.
// if it fails = file doesnt exist, so we create a new file.
func NewStorage(filename string) (*Storage,error) {
	// first try to open existing file 
	// if successful: file = our opened file
	// if something went wrong: err contains the error.
	file, err := os.OpenFile(filename, os.O_RDWR, 0644) 
	
	// if there is an error in opening the file, the file doesnt exist, so create it
	if err != nil {
		file,err = os.Create(filename)
		//if we cant create a file, returns error
		if err != nil {
			return nil, fmt.Errorf("failed to created db file: %w", err)
		}
	}

	// creates the Storage struct and initialize the pageIndex and pages mappings,
	//  which both start as empty. sets the file we opened/created to the storage.
	storage := &Storage {
		file: 		file,
		pageSize: 	PageSize,
		pageIndex:	make(map[string]uint32),
		pages: 		make(map[uint32]*Page),
	}

	// checks if the file is new (empty) or if it exists
	stat, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("failed to stat file: %w", err)
	}

	//if the size is 0 then that it is an empty file, so we set up a new db
	// stat.Size checks how many bytes are in the file
	if stat.Size() == 0 {
		// initializes a new file, with header
		if err:= storage.initializeNewFile(); err != nil {
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
	//we create the header struct for it.
	// the "birth certificate" literally the header of any notebook page: name, date,"page count: 0"
	header := Header {
		Magic:		MagicNumber,		// sig that identifies the db file
		Version:	Version,			// 1
		PageSize: 	uint32(s.pageSize),	// 4096 bytes per page
		TotalPages: 0,					// 0 (no data pages exist in the db yet)
		nextPageID: 0,					// WHen we create the first page, it will start as page 0)
	}

	//updates the in-memory Storage object to match the header.
	// tracks the state of the db
	s.nextPageID = 0;
	s.totalPages = 0;
	
	//calls another function to actually write the 64 bytes to the file.
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
	
	//coverts the numbers into bytes to be stored into the 64-byte array headerBytes
	//PutUInt32 puts the unisigned int 32 bit 
	binary.LittleEndian.PutUint32(headerBytes[0:4], header.Magic)
	binary.LittleEndian.PutUint32(headerBytes[4:8], header.Version)
	binary.LittleEndian.PutUint32(headerBytes[8:12], header.PageSize)
	binary.LittleEndian.PutUint32(headerBytes[12:16], header.TotalPages)
	binary.LittleEndian.PutUint32(headerBytes[16:20], header.NextPageID)
	
	// writes data starting a speicif position : WriteAt(data, offset)
	// will write all 64 bytes to the start of the file.
	_, err := s.file.WriteAt(headerBytes,0)
	if err != nil {
		return fmt.Errorf("failed to read header: %w", err)
	}
	//forces the OS to wrtie the data to the disk
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

// we loaded a file that contains data
// this will read the header to understand how its organized
func (s *Storage) loadHeader() error{
	//create empty 64 byte aray for reading the header
	headerBytes := make([]byte,HeaderSize)

	//opens and reads the file header from the start
	_,err := s.file.ReadAt(headerBytes,0)
	if err != nil {
		return fmt.Errorf("failed to read header: %w", err)
	}

	//converts the BYTES back into numbers
	//Uint32 converts 4 bytes back into a 32 bit number
	header := Header {
		Magic: binary.LittleEndian.Uint31(headerBytes[0:4])
		Version: binary.LittleEndian.Uint31(headerBytes[4:8])
		PageSize: binary.LittleEndian.Uint31(headerBytes[8:12])
		TotalPages: binary.LittleEndian.Uint31(headerBytes[12:16])
		NextPageID: binary.LittleEndian.Uint31(headerBytes[16:20])
	}

	//validates the header info
	if header.Magic != MagicNumber {
		return errors.New("invalid file format: magic number mismatch")
	}
	if header.Version != Version {
		retunr fmt.Errorf("incorrect version %d", header.Version)
	}
	if header.PageSize != uinte32(s.pageSize){
		return fmt.Errorf("page size mismatch: expected %d, got %d", s.pageSize, header.PageSize)
	}

	//updates the Storage object
	//sets the variables to match the file
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

//we opened an existing database, there are pages with data,
//but dont know what kets are stored and where
func(s *Storage) buildIndex() error {
	//loops through all the pages. s.totalPages = 3 it loops though pageID 0,1,2
	for pageID := uint32(0); pageID < s.totalPages; pageID++ {

		//loads each page into memory
		page, err := s.loadPage(pageID)
		if err != nil {
			return fmt.Errorf("failed to load page %d during index build: %w", pageID,err)
		}

		// Scan records in the page add to index
		// RecordCount contains the number of key value pairs in the page
		offset := 2 // skips the RecordCount header the first 2 butes of each page contains record count.
		for i := uint16(0); i <page.RecordCount; i++ { 

			if offset+4 >len(page.Data){
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
			keyLen := binary.LittleEndian.Uint16(page.Data[offset:offset+2])
			valueLen := binary.LittleEndian.Uint16(page.Data[offset:offset+2])
			// move the position forward by 4 bytes to get to the value indexes
			offset += 4
			
			//makes sure we dont read past the end of the page. 
			if offset+int(keyLen)+int(valueLen) > len(page.Data){
				break
			}

			//key is recorded using the current offset and the key length.
			//converts the bytes into a string (key)
			key := string(page.Data[offset:offset+int(keyLen)])
			//adds to key to index: "key _ is stored in page 0"
			s.pageIndex[key] = pageID

			//the offset moves up past the key and value, 
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

