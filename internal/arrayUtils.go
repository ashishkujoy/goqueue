package internal

func Contains[T comparable](arr []T, element T) bool {
	for _, e := range arr {
		if e == element {
			return true
		}
	}

	return false
}

func Filter[T any](elements []T, f func(T) bool) []T {
	var filtered []T
	for _, element := range elements {
		if f(element) {
			filtered = append(filtered, element)
		}
	}

	return filtered
}
