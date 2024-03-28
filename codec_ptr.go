package avro

import (
	"errors"
	"unsafe"

	"github.com/modern-go/reflect2"
)

func decoderOfPtr(cfg *frozenConfig, schema Schema, typ reflect2.Type, seen seenDecoderStructCache) ValDecoder {
	ptrType := typ.(*reflect2.UnsafePtrType)
	elemType := ptrType.Elem()

	decoder := decoderOfType(cfg, schema, elemType, seen)

	return &dereferenceDecoder{typ: elemType, decoder: decoder}
}

type dereferenceDecoder struct {
	typ     reflect2.Type
	decoder ValDecoder
}

func (d *dereferenceDecoder) Decode(ptr unsafe.Pointer, r *Reader, seen seenDecoderStructCache) {
	if *((*unsafe.Pointer)(ptr)) == nil {
		// Create new instance
		newPtr := d.typ.UnsafeNew()
		d.decoder.Decode(newPtr, r, seen)
		*((*unsafe.Pointer)(ptr)) = newPtr
		return
	}

	// Reuse existing instance
	d.decoder.Decode(*((*unsafe.Pointer)(ptr)), r, seen)
}

func encoderOfPtr(cfg *frozenConfig, schema Schema, typ reflect2.Type, seen seenEncoderStructCache) ValEncoder {
	ptrType := typ.(*reflect2.UnsafePtrType)
	elemType := ptrType.Elem()

	enc := encoderOfType(cfg, schema, elemType, seen)

	return &dereferenceEncoder{typ: elemType, encoder: enc}
}

type dereferenceEncoder struct {
	typ     reflect2.Type
	encoder ValEncoder
}

func (d *dereferenceEncoder) Encode(ptr unsafe.Pointer, w *Writer, seen seenEncoderStructCache) {
	if *((*unsafe.Pointer)(ptr)) == nil {
		w.Error = errors.New("avro: cannot encode nil pointer")
		return
	}

	d.encoder.Encode(*((*unsafe.Pointer)(ptr)), w, seen)
}

type referenceDecoder struct {
	decoder ValDecoder
}

func (decoder *referenceDecoder) Decode(ptr unsafe.Pointer, r *Reader, seen seenDecoderStructCache) {
	decoder.decoder.Decode(unsafe.Pointer(&ptr), r, seen)
}
