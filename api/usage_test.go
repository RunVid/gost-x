package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/go-gost/core/auth"
	"github.com/go-gost/x/internal/pineusage"
	"github.com/google/uuid"
)

type usageAuth struct{}

func (usageAuth) Authenticate(_ context.Context, user, password string, _ ...auth.Option) (string, bool) {
	return user, user == "coord" && password == "private"
}

func TestUsageAuthenticationAndCumulativeRead(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	Register(router, &Options{Auther: usageAuth{}})
	request := func(authenticated bool) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodGet, "/usage", nil)
		if authenticated {
			req.SetBasicAuth("coord", "private")
		}
		response := httptest.NewRecorder()
		router.ServeHTTP(response, req)
		return response
	}
	if status := request(false).Code; status != http.StatusUnauthorized {
		t.Fatalf("unauthenticated status: %d", status)
	}
	before := pineusage.Read()
	pineusage.Upload.Add(3)
	pineusage.Download.Add(5)
	for range 2 {
		response := request(true)
		var result pineusage.Snapshot
		if response.Code != http.StatusOK {
			t.Fatal(response.Code)
		}
		if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil {
			t.Fatal(err)
		}
		if _, err := uuid.Parse(result.ProcessID); err != nil {
			t.Fatal(err)
		}
		if result.UploadBytes != before.UploadBytes+3 || result.DownloadBytes != before.DownloadBytes+5 {
			t.Fatalf("read reset or changed counters: %+v", result)
		}
	}
}
