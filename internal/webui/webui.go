// Package webui embeds the built Svelte dashboard into the jellysync
// binary. The real content is produced by `npm run build` in web/ and
// copied here as part of the Docker build; static/index.html as checked
// into git is just a placeholder so a plain `go build` works without
// Node installed.
package webui

import (
	"embed"
	"io/fs"
	"net/http"
)

//go:embed static
var embedded embed.FS

// Handler serves the embedded dashboard assets at "/".
func Handler() (http.Handler, error) {
	sub, err := fs.Sub(embedded, "static")
	if err != nil {
		return nil, err
	}
	return http.FileServer(http.FS(sub)), nil
}
