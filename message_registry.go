package main

import (
	"encoding/gob"
)

// RegisterMessageTypes registers all message types with gob
func RegisterMessageTypes() {
	// Register message types
	gob.Register(MessageStoreFile{})
	gob.Register(MessageGetFile{})
	gob.Register(MessageDeleteFile{})
	gob.Register(MessageOpenFile{})
	gob.Register(MessageReadFile{})
	gob.Register(MessageWriteFile{})
	gob.Register(MessageCloseFile{})
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
	gob.Register(MessageMkdir{})
	gob.Register(MessageMkdirResponse{})
	gob.Register(FileInfo{})
}