package app

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"os"
	"regexp"
	"time"

	"github.com/pquerna/otp/totp"
	kiteconnect "github.com/zerodha/gokiteconnect/v4"

	"github.com/prabhatrastogik/tradesmart-go/internal/config"
)

type LoginResponse struct {
	Data struct {
		RequestID string `json:"request_id"`
	} `json:"data"`
}

// generateRequestId initiates the login process and returns a request ID for 2FA
func generateRequestId(client *http.Client, creds config.Credentials) (string, error) {
	loginPayload := url.Values{}
	loginPayload.Set("user_id", creds.UserName)
	loginPayload.Set("password", creds.Password)
	resp, err := client.PostForm("https://kite.zerodha.com/api/login", loginPayload)
	if err != nil {
		return "", fmt.Errorf("failed to post login request: %w", err)
	}
	defer func() {
		_ = resp.Body.Close()
	}()
	body, _ := io.ReadAll(resp.Body)
	var loginRes LoginResponse
	if err := json.Unmarshal(body, &loginRes); err != nil {
		return "", fmt.Errorf("failed to parse login response: %w", err)
	}
	return loginRes.Data.RequestID, nil
}

// totpVerify verifies the TOTP code for two-factor authentication
func totpVerify(client *http.Client, creds config.Credentials, requestID string) error {
	totpCode, err := totp.GenerateCode(creds.TOTPKey, time.Now())
	if err != nil {
		return fmt.Errorf("failed to generate TOTP code: %w", err)
	}
	totpPayload := url.Values{}
	totpPayload.Set("user_id", creds.UserName)
	totpPayload.Set("request_id", requestID)
	totpPayload.Set("twofa_value", totpCode)
	totpPayload.Set("twofa_type", "totp")
	// totpPayload.Set("skip_session", "true")
	totpResp, err := client.PostForm("https://kite.zerodha.com/api/twofa", totpPayload)
	if err != nil {
		return fmt.Errorf("failed to post TOTP verification: %w", err)
	}
	defer func() {
		_ = totpResp.Body.Close()
	}()

	if totpResp.StatusCode != http.StatusOK {
		return errors.New("failed to verify TOTP")
	}
	return nil
}

// getRequestTokenFromResponse extracts the request token from the final redirect URL
func getRequestTokenFromResponse(client *http.Client, z_url string) (string, error) {
	z_url = z_url + "&skip_session=true"
	finalResp, err := client.Get(z_url)
	if err != nil {
		// Try to extract from error if redirect fails
		re := regexp.MustCompile(`request_token=[A-Za-z0-9]+`)
		matches := re.FindStringSubmatch(err.Error())
		if len(matches) == 0 {
			return "", errors.New("request_token not found in error")
		}
		return matches[0][len("request_token="):], nil
	}
	defer func() {
		_ = finalResp.Body.Close()
	}()

	finalURL := finalResp.Request.URL.String()
	parsed, err := url.Parse(finalURL)
	if err != nil {
		return "", err
	}

	query := parsed.Query()
	tokens := query["request_token"]
	if len(tokens) == 0 {
		return "", errors.New("request_token not found in redirect URL " + finalURL)
	}

	return tokens[0], nil
}

func getRequestToken(creds config.Credentials) (string, error) {
	z_url := kiteconnect.New(creds.APIKey).GetLoginURL()
	jar, _ := cookiejar.New(nil)
	client := &http.Client{
		Jar:     jar,
		Timeout: 30 * time.Second,
	}

	resp, err := client.Get(z_url)
	if err != nil {
		return "", err
	}
	defer func() {
		_ = resp.Body.Close()
	}()
	z_url = resp.Request.URL.String()

	requestID, err := generateRequestId(client, creds)
	if err != nil {
		return "", err
	}

	if totpVerifyErr := totpVerify(client, creds, requestID); totpVerifyErr != nil {
		return "", totpVerifyErr
	}
	return getRequestTokenFromResponse(client, z_url)
}

// saveEncryptedToken encrypts and saves the access token to disk using AES-GCM
func saveEncryptedToken(token, secretKey string) error {
	block, err := aes.NewCipher([]byte(secretKey))
	if err != nil {
		return fmt.Errorf("failed to create cipher: %w", err)
	}

	aesGCM, err := cipher.NewGCM(block)
	if err != nil {
		return fmt.Errorf("failed to create GCM: %w", err)
	}

	nonce := make([]byte, aesGCM.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return fmt.Errorf("failed to generate nonce: %w", err)
	}

	ciphertext := aesGCM.Seal(nonce, nonce, []byte(token), nil)
	return os.WriteFile(config.TokenFile, ciphertext, 0600)
}

// loadEncryptedToken loads and decrypts the access token from disk
func loadEncryptedToken(secretKey string) (string, error) {
	block, err := aes.NewCipher([]byte(secretKey))
	if err != nil {
		return "", fmt.Errorf("failed to create cipher: %w", err)
	}

	data, err := os.ReadFile(config.TokenFile)
	if err != nil {
		return "", fmt.Errorf("failed to read token file: %w", err)
	}

	aesGCM, err := cipher.NewGCM(block)
	if err != nil {
		return "", fmt.Errorf("failed to create GCM: %w", err)
	}

	if len(data) < aesGCM.NonceSize() {
		return "", errors.New("ciphertext too short")
	}

	nonce, ciphertext := data[:aesGCM.NonceSize()], data[aesGCM.NonceSize():]
	plaintext, err := aesGCM.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return "", fmt.Errorf("failed to decrypt token: %w", err)
	}
	return string(plaintext), nil
}

func setNewAccessToken(creds config.Credentials, requestToken string) error {
	kite := kiteconnect.New(creds.APIKey)
	accessToken, err := kite.GenerateSession(requestToken, creds.APISecret)
	if err != nil {
		return fmt.Errorf("failed to generate access token: %w", err)
	}
	err = saveEncryptedToken(accessToken.AccessToken, creds.APISecret)
	if err != nil {
		return fmt.Errorf("failed to set access token in env: %w", err)
	}
	return nil
}

// validateAccessToken verifies if the current token is valid by making a test API call
func validateAccessToken(creds config.Credentials, token string) (*kiteconnect.Client, error) {
	kite := kiteconnect.New(creds.APIKey)
	kite.SetAccessToken(token)

	_, err := kite.GetUserProfile()
	if err == nil {
		return kite, nil
	}
	return nil, fmt.Errorf("failed to validate access token: %w", err)
}

// GetZerodhaClient returns an authenticated Zerodha client.
// It handles token validation, refresh, and encryption.
// The client will automatically refresh the token if it's invalid or expired.
func GetZerodhaClient(creds config.Credentials) (*kiteconnect.Client, error) {
	token, err := loadEncryptedToken(creds.APISecret)
	kite, err2 := validateAccessToken(creds, token)
	if err != nil || err2 != nil {
		requestToken, err := getRequestToken(creds)
		if err != nil {
			return nil, fmt.Errorf("failed to get request token: %w", err)
		}

		err = setNewAccessToken(creds, requestToken)
		if err != nil {
			return nil, fmt.Errorf("failed to get access token: %w", err)
		}

		token, err = loadEncryptedToken(creds.APISecret)
		if err != nil {
			return nil, fmt.Errorf("failed to load access token: %w", err)
		}

		kite, err = validateAccessToken(creds, token)
		if err != nil {
			return nil, err
		}
	}
	return kite, nil
}
