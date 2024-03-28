package avro

import (
	"fmt"
	"math/big"
	"reflect"
	"time"
	"unsafe"

	"github.com/modern-go/reflect2"
)

var (
	timeType = reflect.TypeOf(time.Time{})
	ratType  = reflect.TypeOf(big.Rat{})
	durType  = reflect.TypeOf(LogicalDuration{})
)

type null struct{}

type seenEncoderStructCache map[string]*structEncoder

func (c seenEncoderStructCache) Add(name string, encoder *structEncoder) *structEncoder {
	if encoderFound, ok := c[name]; ok {
		return encoderFound
	}
	c[name] = encoder
	return nil
}

type seenDecoderStructCache map[string]*structDecoder

func (c seenDecoderStructCache) Add(name string, decoder *structDecoder) *structDecoder {
	if decoderFound, ok := c[name]; ok {
		return decoderFound
	}
	c[name] = decoder
	return nil
}

// ValDecoder represents an internal value decoder.
//
// You should never use ValDecoder directly.
type ValDecoder interface {
	Decode(ptr unsafe.Pointer, r *Reader, seen seenDecoderStructCache)
}

// ValEncoder represents an internal value encoder.
//
// You should never use ValEncoder directly.
type ValEncoder interface {
	Encode(ptr unsafe.Pointer, w *Writer, seen seenEncoderStructCache)
}

// ReadVal parses Avro value and stores the result in the value pointed to by obj.
func (r *Reader) ReadVal(schema Schema, obj any) {
	decoder := r.cfg.getDecoderFromCache(schema.CacheFingerprint(), reflect2.RTypeOf(obj))
	seen := seenDecoderStructCache{}
	if decoder == nil {
		typ := reflect2.TypeOf(obj)
		if typ.Kind() != reflect.Ptr {
			r.ReportError("ReadVal", "can only unmarshal into pointer")
			return
		}
		decoder = r.cfg.DecoderOf(schema, typ, seen)
	}

	ptr := reflect2.PtrOf(obj)
	if ptr == nil {
		r.ReportError("ReadVal", "can not read into nil pointer")
		return
	}

	decoder.Decode(ptr, r, seen)
}

// WriteVal writes the Avro encoding of obj.
func (w *Writer) WriteVal(schema Schema, val any) {
	encoder := w.cfg.getEncoderFromCache(schema.Fingerprint(), reflect2.RTypeOf(val))
	seen := seenEncoderStructCache{}
	if encoder == nil {
		typ := reflect2.TypeOf(val)

		encoder = w.cfg.EncoderOf(schema, typ, seen)
	}
	encoder.Encode(reflect2.PtrOf(val), w, seen)
}

func (c *frozenConfig) DecoderOf(schema Schema, typ reflect2.Type, seen seenDecoderStructCache) ValDecoder {
	rtype := typ.RType()
	decoder := c.getDecoderFromCache(schema.CacheFingerprint(), rtype)
	if decoder != nil {
		return decoder
	}

	ptrType := typ.(*reflect2.UnsafePtrType)
	decoder = decoderOfType(c, schema, ptrType.Elem(), seen)
	c.addDecoderToCache(schema.CacheFingerprint(), rtype, decoder)
	return decoder
}

func decoderOfType(cfg *frozenConfig, schema Schema, typ reflect2.Type, seen seenDecoderStructCache) ValDecoder {
	if dec := createDecoderOfMarshaler(cfg, schema, typ); dec != nil {
		return dec
	}

	// Handle eface case when it isnt a union
	if typ.Kind() == reflect.Interface && schema.Type() != Union {
		if _, ok := typ.(*reflect2.UnsafeIFaceType); !ok {
			return newEfaceDecoder(cfg, schema, seen)
		}
	}

	switch schema.Type() {
	case String, Bytes, Int, Long, Float, Double, Boolean:
		return createDecoderOfNative(schema.(*PrimitiveSchema), typ)

	case Record:
		return createDecoderOfRecord(cfg, schema, typ, seen)

	case Ref:
		return decoderOfType(cfg, schema.(*RefSchema).Schema(), typ, seen)

	case Enum:
		return createDecoderOfEnum(schema, typ)

	case Array:
		return createDecoderOfArray(cfg, schema, typ, seen)

	case Map:
		return createDecoderOfMap(cfg, schema, typ, seen)

	case Union:
		return createDecoderOfUnion(cfg, schema, typ, seen)

	case Fixed:
		return createDecoderOfFixed(schema, typ)

	default:
		// It is impossible to get here with a valid schema
		return &errorDecoder{err: fmt.Errorf("avro: schema type %s is unsupported", schema.Type())}
	}
}

func (c *frozenConfig) EncoderOf(schema Schema, typ reflect2.Type, seen seenEncoderStructCache) ValEncoder {
	if typ == nil {
		typ = reflect2.TypeOf((*null)(nil))
	}

	rtype := typ.RType()
	encoder := c.getEncoderFromCache(schema.Fingerprint(), rtype)
	if encoder != nil {
		return encoder
	}
	encoder = encoderOfType(c, schema, typ, seen)
	if typ.LikePtr() {
		encoder = &onePtrEncoder{encoder}
	}
	c.addEncoderToCache(schema.Fingerprint(), rtype, encoder)
	return encoder
}

type onePtrEncoder struct {
	enc ValEncoder
}

func (e *onePtrEncoder) Encode(ptr unsafe.Pointer, w *Writer, seen seenEncoderStructCache) {
	e.enc.Encode(noescape(unsafe.Pointer(&ptr)), w, seen)
}

func encoderOfType(cfg *frozenConfig, schema Schema, typ reflect2.Type, seen seenEncoderStructCache) ValEncoder {
	if enc := createEncoderOfMarshaler(cfg, schema, typ); enc != nil {
		return enc
	}

	if typ.Kind() == reflect.Interface {
		return &interfaceEncoder{schema: schema, typ: typ}
	}
	switch schema.Type() {
	case String, Bytes, Int, Long, Float, Double, Boolean, Null:
		return createEncoderOfNative(schema, typ)

	case Record:
		return createEncoderOfRecord(cfg, schema, typ, seen)

	case Ref:
		return encoderOfType(cfg, schema.(*RefSchema).Schema(), typ, seen)

	case Enum:
		return createEncoderOfEnum(schema, typ)

	case Array:
		return createEncoderOfArray(cfg, schema, typ, seen)

	case Map:
		return createEncoderOfMap(cfg, schema, typ, seen)

	case Union:
		return createEncoderOfUnion(cfg, schema, typ, seen)

	case Fixed:
		return createEncoderOfFixed(schema, typ)

	default:
		// It is impossible to get here with a valid schema
		return &errorEncoder{err: fmt.Errorf("avro: schema type %s is unsupported", schema.Type())}
	}
}

type errorDecoder struct {
	err error
}

func (d *errorDecoder) Decode(_ unsafe.Pointer, r *Reader, seen seenDecoderStructCache) {
	if r.Error == nil {
		r.Error = d.err
	}
}

type errorEncoder struct {
	err error
}

func (e *errorEncoder) Encode(_ unsafe.Pointer, w *Writer, seen seenEncoderStructCache) {
	if w.Error == nil {
		w.Error = e.err
	}
}
