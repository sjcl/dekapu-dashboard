package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"log-parser/internal/autosave"
	"log-parser/internal/cloudsave"
	"log-parser/internal/envutil"
	"log-parser/internal/handler"
	"log-parser/internal/influx"
	"log-parser/internal/offset"
	"log-parser/internal/watcher"
)

func accessConfigFromEnv() (influx.AccessConfig, error) {
	c := influx.AccessConfig{
		ClientID:     os.Getenv("CF_ACCESS_CLIENT_ID"),
		ClientSecret: os.Getenv("CF_ACCESS_CLIENT_SECRET"),
	}
	if (c.ClientID == "") != (c.ClientSecret == "") {
		return influx.AccessConfig{}, fmt.Errorf(
			"CF_ACCESS_CLIENT_ID and CF_ACCESS_CLIENT_SECRET must be set together")
	}
	return c, nil
}

func runApp(ctx context.Context, dataDir string) error {
	logDir, err := envutil.Get("VRCHAT_LOG_DIR")
	if err != nil {
		return err
	}
	influxURL, err := envutil.Get("INFLUXDB_URL")
	if err != nil {
		return err
	}
	influxToken, err := envutil.Get("INFLUXDB_TOKEN")
	if err != nil {
		return err
	}
	influxOrg, err := envutil.Get("INFLUXDB_ORG")
	if err != nil {
		return err
	}
	influxBucket, err := envutil.Get("INFLUXDB_BUCKET")
	if err != nil {
		return err
	}
	influxAccess, err := accessConfigFromEnv()
	if err != nil {
		return err
	}
	if influxAccess.Enabled() {
		log.Printf("InfluxDB: Cloudflare Access service token enabled")
	}

	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return fmt.Errorf("failed to create data dir: %w", err)
	}

	influxTimeout := envutil.Seconds("INFLUXDB_TIMEOUT_SECONDS", 30*time.Second)
	influxWriter := influx.NewWriter(influxURL, influxToken, influxOrg, influxBucket, influxTimeout, influxAccess)
	defer influxWriter.Close()

	cloudRepo := cloudsave.NewJSONRepository(filepath.Join(dataDir, "cloudsave.json"))
	var httpSender autosave.Sender
	if override := os.Getenv("AUTOSAVE_ORIGIN_OVERRIDE"); override != "" {
		originURL, err := url.Parse(override)
		if err != nil {
			return fmt.Errorf("invalid AUTOSAVE_ORIGIN_OVERRIDE: %w", err)
		}
		httpSender = autosave.NewHTTPSenderWithOriginOverride(&http.Client{Timeout: 15 * time.Second}, originURL)
		log.Printf("AutoSave origin override: %s", override)
	} else {
		httpSender = autosave.NewHTTPSender(&http.Client{Timeout: 15 * time.Second})
	}
	queue := autosave.NewSaveDispatcher(httpSender)
	autosaveInterval := 1800
	if v := os.Getenv("AUTOSAVE_INTERVAL_SECONDS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			autosaveInterval = n
		}
	}
	autoSaveMgr := autosave.NewManager(cloudRepo, time.Duration(autosaveInterval)*time.Second, queue)
	defer autoSaveMgr.Close()

	enableAutosave := envutil.Bool("ENABLE_AUTOSAVE", true)

	newHandler := func(path string) watcher.LineHandler {
		return handler.NewHandler(filepath.Base(path), influxWriter, autoSaveMgr, enableAutosave)
	}

	offsetRepo := offset.NewJSONRepository(filepath.Join(dataDir, "offsets.json"))
	w := watcher.NewLogWatcher(logDir, newHandler, offsetRepo, true)

	log.Printf("Starting watcher %s. Log dir: %s", version, logDir)
	return w.Run(ctx)
}
