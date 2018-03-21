package eventhubs

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/tls"
	"encoding/base64"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"qpid.apache.org/amqp"
	"qpid.apache.org/electron"
)

type Client struct {
	namespace    string
	hub          string
	keyName      string
	key          string
	container    electron.Container
	connection   electron.Connection
	authDuration time.Duration
	authSender   electron.Sender
	sender       electron.Sender
}

type Message struct {
	client *Client
	Body   []byte
}

func NewClient(namespace, hub, keyName, key string) (*Client, error) {
	client := &Client{
		namespace:    namespace,
		hub:          hub,
		keyName:      keyName,
		key:          key,
		container:    electron.NewContainer(fmt.Sprintf("amqp_sender_%v", os.Getpid())),
		authDuration: 5 * time.Minute,
	}
	return client, client.RefreshConnection()
}

func (c *Client) RefreshConnection() error {
	// Establish TCP connection
	tlsConn, err := tls.Dial("tcp", fmt.Sprintf("%s.servicebus.windows.net:5671", c.namespace), &tls.Config{})
	if err != nil {
		return err
	}

	// Upgrade TCP connection to AMQP
	c.connection, err = c.container.Connection(
		tlsConn,
		electron.VirtualHost(fmt.Sprintf("%s.servicebus.windows.net", c.namespace)),
		electron.SASLAllowedMechs("ANONYMOUS"),
		electron.Heartbeat(60*time.Second))
	if err != nil {
		return err
	}

	// Create auth channel
	c.authSender, err = c.connection.Sender(electron.Target("$cbs"))
	if err != nil {
		return err
	}

	err = c.Authenticate()
	if err != nil {
		return err
	}

	// Create hub channel
	c.sender, err = c.connection.Sender(electron.Target(c.hub))
	if err != nil {
		return err
	}
	return nil
}

func (c *Client) Authenticate() error {
	// Generate the SASL token
	hubUri := fmt.Sprintf("amqp://%s.servicebus.windows.net/%s", c.namespace, c.hub)
	saslToken, err := generateSASLToken(hubUri, c.authDuration, c.keyName, c.key)
	if err != nil {
		return err
	}

	// Create the authentication message
	cbsMsg := amqp.NewMessage()
	cbsMsg.SetApplicationProperties(map[string]interface{}{
		"operation": "put-token",
		"type":      "servicebus.windows.net:sastoken",
		"name":      hubUri,
	})
	cbsMsg.Marshal(saslToken)

	// Send the authentication message
	cbsOutcome := c.authSender.SendSync(cbsMsg)
	if cbsOutcome.Error != nil {
		return cbsOutcome.Error
	}
	if cbsOutcome.Status != electron.Accepted {
		return fmt.Errorf("Server did not accept the CBS message. Sattus %s", cbsOutcome.Status.String())
	}
	return nil
}

func generateSASLToken(uri string, duration time.Duration, keyName, key string) (string, error) {
	tokenExpiry := strconv.Itoa(int(time.Now().UTC().Add(duration).Round(time.Second).Unix()))
	sigUri := strings.ToLower(url.QueryEscape(uri))
	h := hmac.New(sha256.New, []byte(key))
	_, err := h.Write([]byte(sigUri + "\n" + tokenExpiry))
	if err != nil {
		return "", err
	}
	sig := url.QueryEscape(base64.StdEncoding.EncodeToString(h.Sum(nil)))
	return fmt.Sprintf("SharedAccessSignature sig=%s&se=%s&skn=%s&sr=%s", sig, tokenExpiry, keyName, sigUri), nil
}

func (c *Client) Close() error {
	c.connection.Close(nil)
	return nil
}

func (c *Client) NewMessage(body []byte) (*Message, error) {
	return &Message{
		client: c,
		Body:   body,
	}, nil
}

func (m *Message) Send() error {
	// Create Message
	msg := amqp.NewMessage()
	msg.SetInferred(true)
	msg.Marshal(m.Body)
	outcome := m.client.sender.SendSync(msg)
	if outcome.Error != nil {
		return outcome.Error
	}
	if outcome.Status != electron.Accepted {
		return fmt.Errorf("Server did not accept the message. Status: %s", outcome.Status.String())
	}
	return nil
}
