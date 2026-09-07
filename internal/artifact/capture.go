package artifact

import (
	"bytes"
	"context"
	"errors"
	"image"
	_ "image/jpeg"
	_ "image/png"
	"os/exec"
	"strconv"
	"strings"
	"unicode/utf8"

	_ "golang.org/x/image/webp"
)

type GitCapture struct {
	Capture Capture
	Parts   []Part
}

type boundedBuffer struct {
	bytes.Buffer
	limit int
}

func (b *boundedBuffer) Write(p []byte) (int, error) {
	if len(p) > b.limit-b.Len() {
		return 0, ErrQuota
	}
	return b.Buffer.Write(p)
}

func gitRead(ctx context.Context, directory string, limit int, args ...string) ([]byte, error) {
	command := exec.CommandContext(ctx, "git", "--no-pager", "--literal-pathspecs", "-c", "core.quotePath=false")
	command.Args = append(command.Args, args...)
	command.Dir = directory
	out := &boundedBuffer{limit: limit}
	command.Stdout = out
	if err := command.Run(); err != nil {
		return nil, errors.Join(ErrInvalid, err)
	}
	return out.Bytes(), nil
}

func CaptureGit(ctx context.Context, directory, base, head string, contextLines int) (GitCapture, error) {
	var result GitCapture
	if base == "" || head == "" || strings.HasPrefix(base, "-") || strings.HasPrefix(head, "-") || contextLines < 0 || contextLines > 100 {
		return result, ErrInvalid
	}
	resolve := func(ref string) (string, error) {
		data, err := gitRead(ctx, directory, 256, "rev-parse", "--verify", "--end-of-options", ref+"^{commit}")
		if err != nil {
			return "", err
		}
		value := strings.TrimSpace(string(data))
		if !ValidHash(value, 40) {
			return "", ErrUnsupported
		}
		return value, nil
	}
	baseSHA, err := resolve(base)
	if err != nil {
		return result, err
	}
	headSHA, err := resolve(head)
	if err != nil {
		return result, err
	}
	merge, err := gitRead(ctx, directory, 256, "merge-base", baseSHA, headSHA)
	if err != nil {
		return result, err
	}
	mergeSHA := strings.TrimSpace(string(merge))
	if !ValidHash(mergeSHA, 40) {
		return result, ErrInvalid
	}
	result.Capture = Capture{Base: baseSHA, Head: headSHA, MergeBase: mergeSHA, ContextLines: contextLines, FileContext: "changed_files"}
	diff, err := gitRead(ctx, directory, MaxTextBytes, "diff", "--no-ext-diff", "--no-textconv", "--no-renames", "--unified="+strconv.Itoa(contextLines), mergeSHA, headSHA, "--")
	if err != nil {
		return result, err
	}
	if !utf8.Valid(diff) {
		return result, ErrUnsupported
	}
	result.Parts = append(result.Parts, Part{Sequence: 0, MediaType: "text/x-diff; charset=utf-8", Side: "diff", SHA256: Digest(diff), Data: bytes.Clone(diff)})
	names, err := gitRead(ctx, directory, MaxManifestBytes, "diff", "--no-ext-diff", "--no-textconv", "--no-renames", "--name-only", "-z", mergeSHA, headSHA, "--")
	if err != nil {
		return result, err
	}
	total := len(diff)
	for _, name := range strings.Split(string(names), "\x00") {
		if name == "" {
			continue
		}
		if !validPath(name) {
			return result, ErrInvalid
		}
		for _, side := range []struct{ name, sha string }{{"base", mergeSHA}, {"head", headSHA}} {
			entry, err := gitRead(ctx, directory, 8192, "ls-tree", "-z", side.sha, "--", name)
			if err != nil {
				return result, err
			}
			if len(entry) == 0 {
				continue
			}
			metadata, _, ok := strings.Cut(string(entry), "\t")
			fields := strings.Fields(metadata)
			if !ok || len(fields) != 3 || fields[1] != "blob" || fields[0] != "100644" && fields[0] != "100755" {
				return result, ErrUnsupported
			}
			data, err := gitRead(ctx, directory, MaxTextBytes, "cat-file", "blob", fields[2])
			if err != nil {
				return result, err
			}
			if !utf8.Valid(data) || bytes.ContainsRune(data, 0) {
				return result, ErrUnsupported
			}
			total += len(data)
			if total > MaxArtifactBytes-MaxManifestBytes || len(result.Parts) >= MaxObjects {
				return result, ErrQuota
			}
			result.Parts = append(result.Parts, Part{Sequence: len(result.Parts), MediaType: "text/plain; charset=utf-8", Path: name, Side: side.name, SHA256: Digest(data), Data: bytes.Clone(data)})
		}
	}
	return result, nil
}

func validateMedia(media string, data []byte) error {
	if strings.HasPrefix(media, "text/") {
		if !utf8.Valid(data) {
			return ErrInvalid
		}
		return nil
	}
	if strings.HasPrefix(media, "image/") {
		cfg, kind, err := image.DecodeConfig(bytes.NewReader(data))
		if err != nil || "image/"+kind != media || cfg.Width <= 0 || cfg.Height <= 0 || int64(cfg.Width)*int64(cfg.Height) > 16000000 {
			return ErrInvalid
		}
		return nil
	}
	if media == "video/mp4" && len(data) >= 12 && string(data[4:8]) == "ftyp" {
		return nil
	}
	if media == "video/webm" && len(data) >= 4 && bytes.Equal(data[:4], []byte{0x1a, 0x45, 0xdf, 0xa3}) {
		return nil
	}
	return ErrInvalid
}
