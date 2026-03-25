// Copyright 2019 free5GC.org
//
// SPDX-License-Identifier: Apache-2.0
//

package util

import (
	"fmt"

	"github.com/5GC-DEV/openapi-cdac/models"
)

// SearchNFServiceUri searches for a specific service URI within an NF Profile based on service name and status.
func SearchNFServiceUri(nfProfile models.NfProfile, serviceName models.ServiceName,
	nfServiceStatus models.NfServiceStatus,
) string {
	if nfProfile.NfServices == nil {
		return ""
	}

	for _, service := range *nfProfile.NfServices {
		// Filter by service name and status
		if service.ServiceName == serviceName && service.NfServiceStatus == nfServiceStatus {
			// Extract URI using helper to keep nesting low
			if uri := extractUri(nfProfile, service); uri != "" {
				return uri
			}
		}
	}

	return ""
}

// extractUri handles the priority logic for selecting an NF URI.
func extractUri(nfProfile models.NfProfile, service models.NfService) string {
	// Priority 1: Global NF FQDN
	if nfProfile.Fqdn != "" {
		return nfProfile.Fqdn
	}
	// Priority 2: Service specific FQDN
	if service.Fqdn != "" {
		return service.Fqdn
	}
	// Priority 3: API Prefix
	if service.ApiPrefix != "" {
		return service.ApiPrefix
	}
	// Priority 4: IP Endpoints
	return resolveUriFromEndpoints(nfProfile, service)
}

// resolveUriFromEndpoints extracts URI from IP endpoints or falls back to NF addresses.
func resolveUriFromEndpoints(nfProfile models.NfProfile, service models.NfService) string {
	if service.IpEndPoints == nil || len(*service.IpEndPoints) == 0 {
		return ""
	}

	point := (*service.IpEndPoints)[0]
	// Use endpoint IP if available
	if point.Ipv4Address != "" {
		return getSbiUri(service.Scheme, point.Ipv4Address, point.Port)
	}
	// Fallback to NF level IPv4 address if endpoint address is missing
	if len(nfProfile.Ipv4Addresses) > 0 {
		return getSbiUri(service.Scheme, nfProfile.Ipv4Addresses[0], point.Port)
	}

	return ""
}

// getSbiUri constructs a URI string based on scheme, IP, and port.
func getSbiUri(scheme models.UriScheme, ipv4Address string, port int32) string {
	if port != 0 {
		return fmt.Sprintf("%s://%s:%d", scheme, ipv4Address, port)
	}

	// Default ports for HTTP/HTTPS if not specified
	switch scheme {
	case models.UriScheme_HTTP:
		return fmt.Sprintf("%s://%s:80", scheme, ipv4Address)
	case models.UriScheme_HTTPS:
		return fmt.Sprintf("%s://%s:443", scheme, ipv4Address)
	default:
		return fmt.Sprintf("%s://%s", scheme, ipv4Address)
	}
}
