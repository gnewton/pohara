package pohara

import (
	"io"
)

type PoharaWriter struct {
	key    []byte
	length int
	pohara *Pohara
}

func (po *Pohara) Writer(key []byte) (io.WriteCloser, error) {
	w := new(PoharaWriter)
	w.key = key
	w.length = 0
	w.pohara = po
	return w, nil
}

func (w *PoharaWriter) Write(p []byte) (int, error) {
	n, err := w.pohara.writeBytes(p)
	w.length += n
	return 0, err
}

func (w *PoharaWriter) Close() error {

	w.pohara.WriteIndex(w.key, w.length)
	w.pohara.offset += w.length
	return nil
}
