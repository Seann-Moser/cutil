package cachec

import (
	"net/http"
	"testing"
)

type ResponseData struct {
	Status  int
	Message string
	Err     error
	Data    []byte
	Cookies []*http.Cookie `json:"-"`
}

func TestType(t *testing.T) {

	if getType(ResponseData{}) != "ResponseData" {
		t.Errorf("not equal %s!== %s", getType(ResponseData{}), "ResponseData")
	}
	if getType(&ResponseData{}) != "ResponseData" {
		t.Errorf("not equal %s!== %s", getType(&ResponseData{}), "ResponseData")
	}
	if getType([]*ResponseData{}) != "[]*cachec.ResponseData" {
		t.Errorf("not equal %s!== %s", getType([]*ResponseData{}), "[]*cachec.ResponseData")
	}
	if getType(map[string]int{}) != "map[string]int" {
		t.Errorf("not equal %s!== %s", getType(map[string]int{}), "map[string]int")
	}
	if getType(1235) != "int" {
		t.Errorf("not equal %s!== %s", getType(1235), "int")
	}
	if getType(nil) != "nil" {
		t.Errorf("not equal %s!== %s", getType(nil), "nil")
	}
}
