package api

// ValidReviewedHead accepts a complete canonical Git object ID, never a
// revision expression or abbreviated prefix. Existence and current candidate
// authority remain Store checks.
func ValidReviewedHead(value string) bool {
	if len(value) != 40 && len(value) != 64 {
		return false
	}
	for _, c := range value {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}
