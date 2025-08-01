package httpclient

import (
	"context"
	"time"

	"github.com/go-kratos/kratos/v2/log"
	"resty.dev/v3"
)

// Client wraps resty client with additional functionality.
type Client struct {
	client *resty.Client
	logger *log.Helper
}

// Config holds configuration for HTTP client.
type Config struct {
	Timeout          time.Duration
	RetryCount       int
	RetryWaitTime    time.Duration
	RetryMaxWaitTime time.Duration
	BaseURL          string
	UserAgent        string
	Headers          map[string]string
	Debug            bool
}

// DefaultConfig returns default HTTP client configuration.
func DefaultConfig() *Config {
	return &Config{
		Timeout:          30 * time.Second,
		RetryCount:       3,
		RetryWaitTime:    1 * time.Second,
		RetryMaxWaitTime: 30 * time.Second,
		UserAgent:        "ETL-System/1.0",
		Headers:          make(map[string]string),
		Debug:            false,
	}
}

// NewClient creates a new HTTP client with the given configuration.
func NewClient(config *Config, logger *log.Helper) *Client {
	if config == nil {
		config = DefaultConfig()
	}
	if logger == nil {
		logger = log.NewHelper(log.DefaultLogger)
	}

	client := resty.New()

	// Set basic configuration
	client.SetTimeout(config.Timeout)
	client.SetRetryCount(config.RetryCount)
	client.SetRetryWaitTime(config.RetryWaitTime)
	client.SetRetryMaxWaitTime(config.RetryMaxWaitTime)

	if config.BaseURL != "" {
		client.SetBaseURL(config.BaseURL)
	}

	if config.UserAgent != "" {
		client.SetHeader("User-Agent", config.UserAgent)
	}

	// Set custom headers
	for key, value := range config.Headers {
		client.SetHeader(key, value)
	}

	// Enable debug mode if configured
	if config.Debug {
		client.SetDebug(true)
	}

	// Configure debug mode
	if config.Debug {
		client.SetDebug(true)
	}

	return &Client{
		client: client,
		logger: logger,
	}
}

// R creates a new request.
func (c *Client) R() *resty.Request {
	return c.client.R()
}

// Get performs a GET request.
func (c *Client) Get(ctx context.Context, url string) (*resty.Response, error) {
	return c.client.R().SetContext(ctx).Get(url)
}

// Post performs a POST request.
func (c *Client) Post(ctx context.Context, url string, body interface{}) (*resty.Response, error) {
	return c.client.R().SetContext(ctx).SetBody(body).Post(url)
}

// Put performs a PUT request.
func (c *Client) Put(ctx context.Context, url string, body interface{}) (*resty.Response, error) {
	return c.client.R().SetContext(ctx).SetBody(body).Put(url)
}

// Delete performs a DELETE request.
func (c *Client) Delete(ctx context.Context, url string) (*resty.Response, error) {
	return c.client.R().SetContext(ctx).Delete(url)
}

// Patch performs a PATCH request.
func (c *Client) Patch(ctx context.Context, url string, body interface{}) (*resty.Response, error) {
	return c.client.R().SetContext(ctx).SetBody(body).Patch(url)
}

// Head performs a HEAD request.
func (c *Client) Head(ctx context.Context, url string) (*resty.Response, error) {
	return c.client.R().SetContext(ctx).Head(url)
}

// Options performs an OPTIONS request.
func (c *Client) Options(ctx context.Context, url string) (*resty.Response, error) {
	return c.client.R().SetContext(ctx).Options(url)
}

// SetBaseURL sets the base URL for all requests.
func (c *Client) SetBaseURL(baseURL string) {
	c.client.SetBaseURL(baseURL)
}

// SetHeader sets a header for all requests.
func (c *Client) SetHeader(key, value string) {
	c.client.SetHeader(key, value)
}

// SetHeaders sets multiple headers for all requests.
func (c *Client) SetHeaders(headers map[string]string) {
	c.client.SetHeaders(headers)
}

// SetAuthToken sets the authorization token.
func (c *Client) SetAuthToken(token string) {
	c.client.SetAuthToken(token)
}

// SetAuthScheme sets the authorization scheme.
func (c *Client) SetAuthScheme(scheme string) {
	c.client.SetAuthScheme(scheme)
}

// SetBasicAuth sets basic authentication.
func (c *Client) SetBasicAuth(username, password string) {
	c.client.SetBasicAuth(username, password)
}

// Close closes the HTTP client and releases resources.
func (c *Client) Close() {
	c.client.Close()
}
