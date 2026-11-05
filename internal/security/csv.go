package security

import "strings"

// SafeCSV защищает текстовое значение от интерпретации в качестве формулы таблицы.
func SafeCSV(value string) string {
	if value != "" && strings.ContainsAny(string(value[0]), "=+-@\t\r\n") {
		return "'" + value
	}
	return value
}
