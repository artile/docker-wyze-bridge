package main

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func readTestdata(t *testing.T, name string) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// The camera's own bytes: SPS/PPS and 12 P frames (no IDR) captured from
// a Wyze Cam OG (GW_GC1, FW 1.1.0.123) over the Gwell UDP relay. Their
// slices hold 63 unescaped zero runs. The golden file is ffmpeg's
// rewrite of the same bytes:
//
//	ffmpeg -f h264 -i og-pframes.h264 -c copy -copyinkf \
//	  -bsf:v h264_metadata -f h264 og-pframes.h264metadata.h264
//
// That rewrite is what made Apple VideoToolbox accept every frame (234
// of 234 rejected before, 0 after). The escaper must give the same NAL
// units, whatever the chunking of the input.
func TestNALEscaperMatchesFFmpegOnCameraCapture(t *testing.T) {
	in := readTestdata(t, "og-pframes.h264")
	want := nalUnits(readTestdata(t, "og-pframes.h264metadata.h264"))
	// The capture is not legal before the rewrite.
	if !hasUnescapedZeroRun(nalUnits(in)) {
		t.Fatal("fixture has no unescaped zero run")
	}
	for _, chunk := range []int{0, 1, 3, 1400, 4096} {
		out := escapeAll(t, in, chunk)
		// The last NAL unit waits for a start code that never comes.
		out = append(out, escapeTail(in)...)
		got := nalUnits(out)
		assertLegal(t, got)
		if len(got) != len(want) {
			t.Fatalf("chunk %d: %d NAL units, want %d", chunk, len(got), len(want))
		}
		for i := range want {
			if !bytes.Equal(got[i], want[i]) {
				t.Fatalf("chunk %d: NAL %d (type %d) differs from ffmpeg: len %d, want %d", chunk, i, want[i][0]&0x1f, len(got[i]), len(want[i]))
			}
		}
	}
}

// escapeTail returns the escaped last NAL unit of b, which the escaper
// holds until the next start code.
func escapeTail(b []byte) []byte {
	var out bytes.Buffer
	e := newNALEscaper(&out)
	_, _ = e.Write(b)
	_, _ = e.Write([]byte{0, 0, 0, 1})
	all := out.Bytes()
	i := bytes.LastIndex(all, []byte{0, 0, 0, 1})
	return all[i:]
}

func hasUnescapedZeroRun(nals [][]byte) bool {
	for _, n := range nals {
		if bytes.Contains(n, []byte{0, 0, 0}) {
			return true
		}
	}
	return false
}
