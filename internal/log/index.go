package log

import (
	"github.com/tysonmote/gommap"
	"io"
	"os"
)

var (
	offWidth uint64 = 4                   // offset is uint32
	posWidth uint64 = 8                   // position is uint64
	entWidth        = offWidth + posWidth // allows to jump from offset to the position (offset * entWidth = position of entry)
)

type index struct {
	file  *os.File
	mmmap gommap.MMap
	size  uint64
}

func newIndex(f *os.File, c Config) (*index, error) {
	idx := &index{
		file: f,
	}
	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}

	idx.size = uint64(fi.Size())

	// We grow the file to the max index size before memory-mapping the file
	if err = os.Truncate(f.Name(), int64(c.Segment.MaxIndexBytes)); err != nil {
		return nil, err
	}
	if idx.mmmap, err = gommap.Map(
		idx.file.Fd(),
		gommap.PROT_READ|gommap.PROT_WRITE, gommap.MAP_SHARED,
	); err != nil {
		return nil, err
	}

	return idx, nil
}

func (i *index) Close() error {
	if err := i.mmmap.Sync(gommap.MS_SYNC); err != nil {
		return err
	}
	if err := i.file.Sync(); err != nil {
		return err
	}
	// Truncate file to the size it actually is
	if err := i.file.Truncate(int64(i.size)); err != nil {
		return err
	}
	return i.file.Close()
}

/*
Read(int64) takes in an offset and returns the associated record’s position in the store
The given offset is relative to the segment’s base offset;
- 0 is always the offset of the index’s first entry,
- 1 is the second entry, and so on
*/
func (i *index) Read(in int64) (out uint32, pos uint64, err error) {
	if i.size == 0 {
		return 0, 0, io.EOF
	}
	if in == -1 {
		// return the last record
		out = uint32((i.size / entWidth) - 1)
	} else {
		out = uint32(in)
	}
	pos = uint64(out) * entWidth

	if i.size < pos+entWidth {
		return 0, 0, io.EOF
	}

	// [0000 0000.0000]
	// pos: [offset 4 bytes]  pos+offWidth: [position 8 bytes]
	out = enc.Uint32(i.mmmap[pos : pos+offWidth])
	pos = enc.Uint64(i.mmmap[pos+offWidth : pos+entWidth])

	return out, pos, nil
}

func (i *index) Write(off uint32, pos uint64) error {
	if uint64(len(i.mmmap)) < i.size+entWidth {
		return io.EOF
	}

	enc.PutUint32(i.mmmap[i.size:i.size+offWidth], off)
	enc.PutUint64(i.mmmap[i.size+offWidth:i.size+entWidth], pos)
	i.size += entWidth

	return nil
}

func (i *index) Name() string {
	return i.file.Name()
}
