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

package wireguard

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"net"
	"net/netip"
	"slices"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"

	api "github.com/sindef/mspsql/api/v1alpha1"
	"golang.org/x/crypto/curve25519"
)

const (
	DefaultNetworkCIDR = "10.254.0.0/16"
	IdentitySecretName = "mspsql-wireguard-identity"
	PeersConfigMapName = "mspsql-wireguard-peers"
	allocationsName    = "mspsql-wireguard-addresses"
	peerLabel          = "multisite-postgres.dev/wireguard-peer"
)

type HubIdentity struct {
	PublicKey string
	Address   netip.Addr
	Prefix    netip.Prefix
}

func EnsureHubIdentity(ctx context.Context, kube client.Client, namespace, cidr string,
) (HubIdentity, error) {
	if cidr == "" {
		cidr = DefaultNetworkCIDR
	}
	prefix, err := parseNetwork(cidr)
	if err != nil {
		return HubIdentity{}, err
	}
	address := prefix.Addr().Next()
	key := types.NamespacedName{Namespace: namespace, Name: IdentitySecretName}
	var secret corev1.Secret
	if err := kube.Get(ctx, key, &secret); err == nil {
		if string(secret.Data["networkCIDR"]) != prefix.String() ||
			string(secret.Data["address"]) != address.String() ||
			len(secret.Data["publicKey"]) == 0 || len(secret.Data["interface.conf"]) == 0 {
			return HubIdentity{}, fmt.Errorf("%s contains an incompatible WireGuard identity", key)
		}
		return HubIdentity{
			PublicKey: string(secret.Data["publicKey"]), Address: address, Prefix: prefix,
		}, nil
	} else if !apierrors.IsNotFound(err) {
		return HubIdentity{}, err
	}
	privateKey, publicKey, err := generateKeyPair()
	if err != nil {
		return HubIdentity{}, err
	}
	secret = corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: IdentitySecretName},
		Immutable:  pointer(true),
		Data: map[string][]byte{
			"privateKey":  []byte(privateKey),
			"publicKey":   []byte(publicKey),
			"networkCIDR": []byte(prefix.String()),
			"address":     []byte(address.String()),
			"interface.conf": fmt.Appendf(nil,
				"[Interface]\nPrivateKey = %s\nAddress = %s/%d\nListenPort = 51820\n",
				privateKey, address, prefix.Bits()),
		},
	}
	if err := kube.Create(ctx, &secret); err != nil {
		if apierrors.IsAlreadyExists(err) {
			return EnsureHubIdentity(ctx, kube, namespace, cidr)
		}
		return HubIdentity{}, err
	}
	return HubIdentity{PublicKey: publicKey, Address: address, Prefix: prefix}, nil
}

func AuthorizePeer(ctx context.Context, kube client.Client, namespace, cidr, endpoint string,
	site *api.SiteRegistration, publicKey string,
) (string, error) {
	if err := validateEndpoint(endpoint); err != nil {
		return "", err
	}
	decodedKey, err := base64.StdEncoding.DecodeString(publicKey)
	if err != nil {
		return "", fmt.Errorf("WireGuard public key is not valid base64: %w", err)
	}
	if len(decodedKey) != curve25519.PointSize {
		return "", fmt.Errorf("WireGuard public key must decode to %d bytes", curve25519.PointSize)
	}
	identity, err := EnsureHubIdentity(ctx, kube, namespace, cidr)
	if err != nil {
		return "", err
	}
	address, err := reserveAddress(ctx, kube, namespace, identity.Prefix, string(site.UID))
	if err != nil {
		return "", err
	}
	peer := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace, Name: "wireguard-peer-" + string(site.UID),
			Labels: map[string]string{peerLabel: "true"},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: api.GroupVersion.String(), Kind: "SiteRegistration",
				Name: site.Name, UID: site.UID,
				Controller: pointer(true), BlockOwnerDeletion: pointer(true),
			}},
		},
		Data: map[string][]byte{
			"publicKey": []byte(publicKey), "address": []byte(address.String()),
			"siteName": []byte(site.Name), "state": []byte("authorized"),
		},
	}
	if err := kube.Create(ctx, peer); err != nil {
		if !apierrors.IsAlreadyExists(err) {
			return "", err
		}
		var existing corev1.Secret
		if err := kube.Get(ctx, client.ObjectKeyFromObject(peer), &existing); err != nil {
			return "", err
		}
		if string(existing.Data["publicKey"]) != publicKey ||
			string(existing.Data["address"]) != address.String() {
			return "", fmt.Errorf("site already has a different authorized WireGuard key")
		}
	}
	if err := RenderPeers(ctx, kube, namespace); err != nil {
		return "", err
	}
	return fmt.Sprintf(
		"Address = %s/32\n\n[Peer]\nPublicKey = %s\nEndpoint = %s\nAllowedIPs = %s/32\nPersistentKeepalive = 25\n",
		address, identity.PublicKey, endpoint, identity.Address), nil
}

func RevokePeer(ctx context.Context, kube client.Client, namespace string, uid types.UID) error {
	peer := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{
		Namespace: namespace, Name: "wireguard-peer-" + string(uid),
	}}
	if err := kube.Delete(ctx, peer); err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	if err := releaseAddress(ctx, kube, namespace, string(uid)); err != nil {
		return err
	}
	return RenderPeers(ctx, kube, namespace)
}

func RenderPeers(ctx context.Context, kube client.Client, namespace string) error {
	var secrets corev1.SecretList
	if err := kube.List(ctx, &secrets, client.InNamespace(namespace),
		client.MatchingLabels{peerLabel: "true"}); err != nil {
		return err
	}
	peers := make([]string, 0, len(secrets.Items))
	for i := range secrets.Items {
		secret := &secrets.Items[i]
		if string(secret.Data["state"]) != "authorized" {
			continue
		}
		publicKey := string(secret.Data["publicKey"])
		address, err := netip.ParseAddr(string(secret.Data["address"]))
		if err != nil || !address.Is4() || publicKey == "" {
			return fmt.Errorf("peer Secret %s has invalid key or address", secret.Name)
		}
		peers = append(peers, fmt.Sprintf("# %s\n[Peer]\nPublicKey = %s\nAllowedIPs = %s/32\n",
			string(secret.Data["siteName"]), publicKey, address))
	}
	slices.Sort(peers)
	key := types.NamespacedName{Namespace: namespace, Name: PeersConfigMapName}
	var configMap corev1.ConfigMap
	err := kube.Get(ctx, key, &configMap)
	if apierrors.IsNotFound(err) {
		return kube.Create(ctx, &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: PeersConfigMapName},
			Data:       map[string]string{"peers.conf": strings.Join(peers, "\n")},
		})
	}
	if err != nil {
		return err
	}
	rendered := strings.Join(peers, "\n")
	if configMap.Data["peers.conf"] == rendered {
		return nil
	}
	configMap.Data = map[string]string{"peers.conf": rendered}
	return kube.Update(ctx, &configMap)
}

func reserveAddress(ctx context.Context, kube client.Client, namespace string, prefix netip.Prefix,
	uid string,
) (netip.Addr, error) {
	var reserved netip.Addr
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		key := types.NamespacedName{Namespace: namespace, Name: allocationsName}
		var configMap corev1.ConfigMap
		if err := kube.Get(ctx, key, &configMap); apierrors.IsNotFound(err) {
			configMap = corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: allocationsName},
				Data:       map[string]string{},
			}
			if err := kube.Create(ctx, &configMap); err != nil {
				if apierrors.IsAlreadyExists(err) {
					return apierrors.NewConflict(corev1.Resource("configmaps"), allocationsName, err)
				}
				return err
			}
		} else if err != nil {
			return err
		}
		if value := configMap.Data[uid]; value != "" {
			address, err := netip.ParseAddr(value)
			if err != nil || !prefix.Contains(address) {
				return fmt.Errorf("stored WireGuard allocation for %s is invalid", uid)
			}
			reserved = address
			return nil
		}
		used := make(map[string]struct{}, len(configMap.Data))
		for _, value := range configMap.Data {
			used[value] = struct{}{}
		}
		for candidate := prefix.Addr().Next().Next(); prefix.Contains(candidate.Next()); candidate = candidate.Next() {
			if _, found := used[candidate.String()]; found {
				continue
			}
			configMap.Data[uid] = candidate.String()
			if err := kube.Update(ctx, &configMap); err != nil {
				return err
			}
			reserved = candidate
			return nil
		}
		return fmt.Errorf("WireGuard network %s has no available peer addresses", prefix)
	})
	return reserved, err
}

func releaseAddress(ctx context.Context, kube client.Client, namespace, uid string) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var configMap corev1.ConfigMap
		key := types.NamespacedName{Namespace: namespace, Name: allocationsName}
		if err := kube.Get(ctx, key, &configMap); err != nil {
			return client.IgnoreNotFound(err)
		}
		if _, found := configMap.Data[uid]; !found {
			return nil
		}
		delete(configMap.Data, uid)
		return kube.Update(ctx, &configMap)
	})
}

func parseNetwork(value string) (netip.Prefix, error) {
	prefix, err := netip.ParsePrefix(value)
	if err != nil || !prefix.Addr().Is4() || prefix.Bits() > 29 {
		return netip.Prefix{}, fmt.Errorf("WireGuard network must be an IPv4 CIDR with at least six usable addresses")
	}
	if prefix != prefix.Masked() {
		return netip.Prefix{}, fmt.Errorf("WireGuard network must use its canonical network address")
	}
	return prefix, nil
}

func validateEndpoint(value string) error {
	host, portValue, err := net.SplitHostPort(value)
	if err != nil || strings.TrimSpace(host) == "" {
		return fmt.Errorf("WireGuard endpoint must be a host:port")
	}
	port, err := strconv.Atoi(portValue)
	if err != nil || port < 1 || port > 65535 {
		return fmt.Errorf("WireGuard endpoint port is invalid")
	}
	return nil
}

func generateKeyPair() (string, string, error) {
	privateKey := make([]byte, curve25519.ScalarSize)
	if _, err := rand.Read(privateKey); err != nil {
		return "", "", err
	}
	privateKey[0] &= 248
	privateKey[31] &= 127
	privateKey[31] |= 64
	publicKey, err := curve25519.X25519(privateKey, curve25519.Basepoint)
	if err != nil {
		return "", "", err
	}
	return base64.StdEncoding.EncodeToString(privateKey),
		base64.StdEncoding.EncodeToString(publicKey), nil
}

func pointer[T any](value T) *T {
	return &value
}
