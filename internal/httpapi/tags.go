package httpapi

import "strings"

// operationTags groups both Huma handlers and manually documented routes.
func operationTags(path string) []string {
	path = strings.TrimPrefix(path, "/api/v1/")
	prefix := ""
	if strings.HasPrefix(path, "admin/") {
		prefix = "Admin / "
		path = strings.TrimPrefix(path, "admin/")
	}
	resource, _, _ := strings.Cut(path, "/")
	groups := map[string]string{
		"auth":       "Authentication",
		"me":         "Profile",
		"users":      "Users",
		"audit":      "Audit",
		"tables":     "Tables",
		"kafka":      "Kafka",
		"jobs":       "Jobs",
		"imports":    "Imports",
		"exports":    "Exports",
		"ws-tickets": "Realtime",
	}
	group, ok := groups[resource]
	if !ok {
		group = resource
	}
	return []string{prefix + group}
}
