//go:build windows

package daemon

func runtimeUserID() string {
	return "user"
}