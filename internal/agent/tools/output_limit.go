package tools

import "bytes"

type boundedCapture struct {
	limit     int
	buf       bytes.Buffer
	truncated bool
}

func newBoundedCapture(limit int) *boundedCapture {
	return &boundedCapture{limit: limit}
}

func (b *boundedCapture) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if b.limit <= 0 {
		b.truncated = true
		return len(p), nil
	}

	remaining := b.limit - b.buf.Len()
	if remaining <= 0 {
		b.truncated = true
		return len(p), nil
	}

	if len(p) <= remaining {
		_, _ = b.buf.Write(p)
		return len(p), nil
	}

	_, _ = b.buf.Write(p[:remaining])
	b.truncated = true
	return len(p), nil
}

func (b *boundedCapture) String() string {
	return b.buf.String()
}

func (b *boundedCapture) Truncated() bool {
	return b.truncated
}
