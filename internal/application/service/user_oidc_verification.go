package service

import (
	"context"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"strings"
	"time"

	"github.com/Tencent/WeKnora/internal/config"
	"github.com/golang-jwt/jwt/v5"
)

type oidcJWK struct {
	Kty string `json:"kty"`
	Kid string `json:"kid"`
	Alg string `json:"alg"`
	Use string `json:"use"`
	N   string `json:"n"`
	E   string `json:"e"`
}

type oidcJWKS struct {
	Keys []oidcJWK `json:"keys"`
}

func decodeJWKBase64(value string) ([]byte, error) {
	if decoded, err := base64.RawURLEncoding.DecodeString(value); err == nil && len(decoded) > 0 {
		return decoded, nil
	}
	return base64.URLEncoding.DecodeString(value)
}

func (key oidcJWK) rsaPublicKey() (*rsa.PublicKey, error) {
	if !strings.EqualFold(key.Kty, "RSA") {
		return nil, fmt.Errorf("unsupported JWK key type: %s", key.Kty)
	}
	modulus, err := decodeJWKBase64(key.N)
	if err != nil {
		return nil, fmt.Errorf("invalid JWK modulus: %w", err)
	}
	exponent, err := decodeJWKBase64(key.E)
	if err != nil {
		return nil, fmt.Errorf("invalid JWK exponent: %w", err)
	}
	if len(modulus) == 0 || len(exponent) == 0 {
		return nil, errors.New("empty JWK modulus or exponent")
	}
	e := new(big.Int).SetBytes(exponent)
	if !e.IsInt64() || e.Int64() <= 0 {
		return nil, errors.New("invalid JWK exponent value")
	}
	return &rsa.PublicKey{N: new(big.Int).SetBytes(modulus), E: int(e.Int64())}, nil
}

func (keys *oidcJWKS) rsaKeyForKid(kid string) (*rsa.PublicKey, error) {
	var usable []oidcJWK
	for _, key := range keys.Keys {
		if key.Use != "" && !strings.EqualFold(key.Use, "sig") {
			continue
		}
		if key.Kty != "" && !strings.EqualFold(key.Kty, "RSA") {
			continue
		}
		if kid != "" && key.Kid != kid {
			continue
		}
		if _, err := key.rsaPublicKey(); err != nil {
			continue
		}
		usable = append(usable, key)
	}
	if kid != "" {
		if len(usable) == 0 {
			return nil, fmt.Errorf("no matching JWKS RSA key for kid %q", kid)
		}
		return usable[0].rsaPublicKey()
	}
	if len(usable) == 0 {
		return nil, errors.New("no matching JWKS key for id_token")
	}
	if len(usable) > 1 {
		return nil, errors.New("id_token missing kid and JWKS contains multiple RSA signing keys")
	}
	return usable[0].rsaPublicKey()
}

func (s *userService) fetchOIDCJWKS(ctx context.Context, jwksURI string) (*oidcJWKS, error) {
	if err := validateOIDCEndpoint("jwks", jwksURI, true); err != nil {
		return nil, err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, jwksURI, nil)
	if err != nil {
		return nil, err
	}
	request.Header.Set("Accept", "application/json")
	response, err := newOIDCHTTPClient().Do(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 2048))
		return nil, fmt.Errorf("JWKS request failed: status=%d", response.StatusCode)
	}
	var keys oidcJWKS
	if err := json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(&keys); err != nil {
		return nil, fmt.Errorf("failed to decode JWKS document: %w", err)
	}
	if len(keys.Keys) == 0 {
		return nil, errors.New("JWKS document contains no keys")
	}
	return &keys, nil
}

const oidcIDTokenLeeway = 2 * time.Minute

func (s *userService) verifyOIDCIDToken(
	ctx context.Context, cfg *config.OIDCAuthConfig, idToken string,
) (map[string]interface{}, error) {
	if strings.TrimSpace(cfg.JwksURI) == "" {
		return nil, errors.New("cannot verify OIDC id_token: no jwks_uri configured")
	}
	if strings.TrimSpace(cfg.IssuerURL) == "" {
		return nil, errors.New("cannot verify OIDC id_token: issuer is not configured")
	}
	if strings.TrimSpace(cfg.ClientID) == "" {
		return nil, errors.New("cannot verify OIDC id_token: client_id is not configured")
	}
	keys, err := s.fetchOIDCJWKS(ctx, cfg.JwksURI)
	if err != nil {
		return nil, err
	}
	keyFunc := func(token *jwt.Token) (interface{}, error) {
		if _, ok := token.Method.(*jwt.SigningMethodRSA); !ok {
			return nil, fmt.Errorf("unexpected id_token signing method: %v", token.Header["alg"])
		}
		kid, _ := token.Header["kid"].(string)
		return keys.rsaKeyForKid(kid)
	}
	claims := jwt.MapClaims{}
	if _, err := jwt.NewParser(
		jwt.WithValidMethods([]string{"RS256", "RS384", "RS512"}),
		jwt.WithExpirationRequired(),
		jwt.WithLeeway(oidcIDTokenLeeway),
		jwt.WithIssuer(strings.TrimSpace(cfg.IssuerURL)),
		jwt.WithAudience(strings.TrimSpace(cfg.ClientID)),
	).ParseWithClaims(idToken, claims, keyFunc); err != nil {
		return nil, fmt.Errorf("id_token verification failed: %w", err)
	}
	verified := map[string]interface{}(claims)
	if strings.TrimSpace(extractClaimAsString(verified, "sub")) == "" {
		return nil, errors.New("id_token missing sub claim")
	}
	return verified, nil
}
