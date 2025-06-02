package main

// Message is the container for all message payloads
type Message struct {
	Payload any
}

// File operation messages
type MessageStoreFile struct {
	Key  string
	Size int64
	Path string
	Mode uint32
}

type MessageGetFile struct {
	Key string
}

type MessageDeleteFile struct {
	Path string
}

// File access messages
type MessageOpenFile struct {
	Path  string
	Flags int
	Mode  uint32
}

type MessageReadFile struct {
	Path   string
	Offset int64
	Length int
}

type MessageWriteFile struct {
	Path   string
	Offset int64
	Data   []byte
}

type MessageCloseFile struct {
	Path string
}

// Directory operation messages
type MessageMkdir struct {
	Path      string
	Recursive bool
}

type MessageMkdirResponse struct {
	Success bool
	Error   string
}

type MessageLsDirectory struct {
	Path string
}

type FileInfo struct {
	Name    string
	Size    int64
	Mode    uint32
	IsDir   bool
	ModTime int64 // Unix timestamp
}

type MessageLsDirectoryResponse struct {
	Files []FileInfo
	Error string
}

// File content messages
type MessagePutFile struct {
	Path    string
	Mode    uint32
	Content []byte
}

type MessageGetFileContent struct {
	Path string
}

type MessageGetFileResponse struct {
	Content []byte
	Mode    uint32
	Error   string
}

type MessageDeleteFileContent struct {
	Path string
}

type MessageDeleteFileResponse struct {
	Success bool
	Error   string
}

// File status messages
type MessageFileExists struct {
	Path string
}

type MessageFileExistsResponse struct {
	Exists bool
	Error  string
}

type MessageTouchFile struct {
	Path string
	Mode uint32
}

type MessageTouchFileResponse struct {
	Success bool
	Error   string
}

type MessageCatFile struct {
	Path string
}

type MessageCatFileResponse struct {
	Content []byte
	Error   string
}
