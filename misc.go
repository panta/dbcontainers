package dbcontainers

import (
	"fmt"
)

func parseInt(s string) int {
	var i int
	fmt.Sscanf(s, "%d", &i)
	return i
}
