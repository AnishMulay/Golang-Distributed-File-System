package main

import (
	"encoding/gob"
)

// init registers all message types with the gob encoder
func init() {
	// Register all message types for gob encoding/decoding
	gob.Register(MessageStoreFile{})
	gob.Register(MessageGetFile{})
	gob.Register(MessageDeleteFile{})
	gob.Register(MessageOpenFile{})
	gob.Register(MessageReadFile{})
	gob.Register(MessageWriteFile{})
	gob.Register(MessageCloseFile{})
	gob.Register(MessageMkdir{})
	gob.Register(MessagePutFile{})
	gob.Register(MessageGetFileContent{})
	gob.Register(MessageGetFileResponse{})
	gob.Register(MessageDeleteFileContent{})
	gob.Register(MessageDeleteFileResponse{})
	gob.Register(MessageFileExists{})
	gob.Register(MessageFileExistsResponse{})
	gob.Register(MessageTouchFile{})
	gob.Register(MessageTouchFileResponse{})
	gob.Register(MessageCatFile{})
	gob.Register(MessageCatFileResponse{})
	gob.Register(MessageLsDirectory{})
	gob.Register(MessageLsDirectoryResponse{})
	gob.Register(MessageMkdirResponse{})
}