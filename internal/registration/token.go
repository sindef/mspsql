/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package registration

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"time"
)

const TokenLifetime = 10 * time.Minute

type Token struct {
	Value     string
	Hash      []byte
	ExpiresAt time.Time
}

func NewToken(now time.Time) (Token, error) {
	value := make([]byte, 32)
	if _, err := rand.Read(value); err != nil {
		return Token{}, err
	}
	encoded := base64.RawURLEncoding.EncodeToString(value)
	sum := sha256.Sum256([]byte(encoded))
	return Token{Value: encoded, Hash: sum[:], ExpiresAt: now.Add(TokenLifetime)}, nil
}

func Verify(value string, expectedHash []byte, expiresAt, now time.Time) error {
	if len(expectedHash) != sha256.Size {
		return errors.New("registration token hash is invalid")
	}
	if !now.Before(expiresAt) {
		return errors.New("registration token has expired")
	}
	actual := sha256.Sum256([]byte(value))
	if subtle.ConstantTimeCompare(actual[:], expectedHash) != 1 {
		return errors.New("registration token is invalid")
	}
	return nil
}
