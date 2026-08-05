package influx

import (
	"context"
	"log"
	"net"
	"net/http"
	"time"

	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/api"
	ihttp "github.com/influxdata/influxdb-client-go/v2/api/http"
	"github.com/influxdata/influxdb-client-go/v2/api/write"
)

const defaultFlushInterval = 5 * time.Second

type PointWriter interface {
	WritePoint(point *write.Point)
}

type BlockingWriteAPI interface {
	WritePoint(ctx context.Context, point ...*write.Point) error
}

type AccessConfig struct {
	ClientID     string
	ClientSecret string
}

func (c AccessConfig) Enabled() bool {
	return c.ClientID != "" && c.ClientSecret != ""
}

type accessHeaderDoer struct {
	next         ihttp.Doer
	clientID     string
	clientSecret string
}

func (d *accessHeaderDoer) Do(req *http.Request) (*http.Response, error) {
	req.Header.Set("CF-Access-Client-Id", d.clientID)
	req.Header.Set("CF-Access-Client-Secret", d.clientSecret)
	return d.next.Do(req)
}

func newTransport() *http.Transport {
	return &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   5 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		ForceAttemptHTTP2:     true,
		TLSHandshakeTimeout:   10 * time.Second,
		MaxIdleConns:          100,
		MaxIdleConnsPerHost:   100,
		IdleConnTimeout:       10 * time.Minute,
		ExpectContinueTimeout: 1 * time.Second,
	}
}

func NewClient(influxURL, token string, timeout time.Duration, access AccessConfig) influxdb2.Client {
	opts := influxdb2.DefaultOptions().
		SetHTTPRequestTimeout(uint(timeout / time.Millisecond)).
		SetUseGZip(true).
		SetFlushInterval(uint(defaultFlushInterval / time.Millisecond))

	httpClient := &http.Client{
		Timeout:   timeout,
		Transport: newTransport(),
	}

	var doer ihttp.Doer = httpClient
	if access.Enabled() {
		doer = &accessHeaderDoer{
			next:         httpClient,
			clientID:     access.ClientID,
			clientSecret: access.ClientSecret,
		}
	}
	opts.HTTPOptions().SetHTTPDoer(doer)

	return influxdb2.NewClientWithOptions(influxURL, token, opts)
}

type Writer struct {
	client   influxdb2.Client
	writeAPI api.WriteAPI
}

func NewWriter(influxURL, token, org, bucket string, timeout time.Duration, access AccessConfig) *Writer {
	client := NewClient(influxURL, token, timeout, access)
	writeAPI := client.WriteAPI(org, bucket)

	w := &Writer{client: client, writeAPI: writeAPI}

	go func() {
		for err := range writeAPI.Errors() {
			log.Printf("[InfluxDB] Write error: %v", err)
		}
	}()

	return w
}

func (w *Writer) WritePoint(point *write.Point) {
	w.writeAPI.WritePoint(point)
}

func (w *Writer) Flush() {
	w.writeAPI.Flush()
}

func (w *Writer) Close() {
	w.writeAPI.Flush()
	w.client.Close()
}
