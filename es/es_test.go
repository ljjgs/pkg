package es

import "testing"

var (
	urls = []string{"127.0.0.1"}
)

func InitES(t *testing.B) {
	InitSimpleClient(urls, "test", "test")
}
