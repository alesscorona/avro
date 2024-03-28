package avro

import (
	"errors"
	"fmt"
	"io"
	"reflect"
	"unsafe"

	"github.com/modern-go/reflect2"
)

func createDecoderOfArray(cfg *frozenConfig, schema Schema, typ reflect2.Type, seen seenDecoderStructCache) ValDecoder {
	if typ.Kind() == reflect.Slice {
		return decoderOfArray(cfg, schema, typ, seen)
	}

	return &errorDecoder{err: fmt.Errorf("avro: %s is unsupported for Avro %s", typ.String(), schema.Type())}
}

func createEncoderOfArray(cfg *frozenConfig, schema Schema, typ reflect2.Type, seen seenEncoderStructCache) ValEncoder {
	if typ.Kind() == reflect.Slice {
		return encoderOfArray(cfg, schema, typ, seen)
	}

	return &errorEncoder{err: fmt.Errorf("avro: %s is unsupported for Avro %s", typ.String(), schema.Type())}
}

func decoderOfArray(cfg *frozenConfig, schema Schema, typ reflect2.Type, seen seenDecoderStructCache) ValDecoder {
	arr := schema.(*ArraySchema)
	sliceType := typ.(*reflect2.UnsafeSliceType)
	decoder := decoderOfType(cfg, arr.Items(), sliceType.Elem(), seen)

	return &arrayDecoder{typ: sliceType, decoder: decoder}
}

type arrayDecoder struct {
	typ     *reflect2.UnsafeSliceType
	decoder ValDecoder
}

func (d *arrayDecoder) Decode(ptr unsafe.Pointer, r *Reader, seen seenDecoderStructCache) {
	var size int
	sliceType := d.typ

	for {
		l, _ := r.ReadBlockHeader()
		if l == 0 {
			break
		}

		start := size
		size += int(l)
		sliceType.UnsafeGrow(ptr, size)

		for i := start; i < size; i++ {
			elemPtr := sliceType.UnsafeGetIndex(ptr, i)
			d.decoder.Decode(elemPtr, r, seen)
			if r.Error != nil && !errors.Is(r.Error, io.EOF) {
				r.Error = fmt.Errorf("%s: %w", d.typ.String(), r.Error)
				return
			}
		}
	}

	if r.Error != nil && !errors.Is(r.Error, io.EOF) {
		r.Error = fmt.Errorf("%v: %w", d.typ, r.Error)
	}
}

func encoderOfArray(cfg *frozenConfig, schema Schema, typ reflect2.Type, seen seenEncoderStructCache) ValEncoder {
	arr := schema.(*ArraySchema)
	sliceType := typ.(*reflect2.UnsafeSliceType)
	encoder := encoderOfType(cfg, arr.Items(), sliceType.Elem(), seen)

	return &arrayEncoder{
		blockLength: cfg.getBlockLength(),
		typ:         sliceType,
		encoder:     encoder,
	}
}

type arrayEncoder struct {
	blockLength int
	typ         *reflect2.UnsafeSliceType
	encoder     ValEncoder
}

func (e *arrayEncoder) Encode(ptr unsafe.Pointer, w *Writer, seen seenEncoderStructCache) {
	blockLength := e.blockLength
	length := e.typ.UnsafeLengthOf(ptr)

	for i := 0; i < length; i += blockLength {
		w.WriteBlockCB(func(w *Writer) int64 {
			count := int64(0)
			for j := i; j < i+blockLength && j < length; j++ {
				elemPtr := e.typ.UnsafeGetIndex(ptr, j)
				e.encoder.Encode(elemPtr, w, seen)
				if w.Error != nil && !errors.Is(w.Error, io.EOF) {
					w.Error = fmt.Errorf("%s: %w", e.typ.String(), w.Error)
					return count
				}
				count++
			}

			return count
		})
	}

	w.WriteBlockHeader(0, 0)

	if w.Error != nil && !errors.Is(w.Error, io.EOF) {
		w.Error = fmt.Errorf("%v: %w", e.typ, w.Error)
	}
}
