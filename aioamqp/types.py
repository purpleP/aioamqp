from itertools import takewhile, chain
from enum import IntEnum
from operator import concat
import aioamqp
from collections import namedtuple
from functools import reduce, singledispatch, partial
import struct
from decimal import Decimal as Decimal
import io
from more_functools import nwise_iter

from aioamqp.properties import Properties


SimpleType = namedtuple('SimpleType', 'format size')
ProcessedType = namedtuple('ProcessedType', 'types packer unpacker')
Nothing = namedtuple('Nothing', '')
Container = namedtuple('Container', 'py_type item_encoder iter_items item_reader')
StringType = namedtuple('StringType', 'length_type')


def signed_unsigned_pair(format, size):
    return SimpleType('!' + format, size), SimpleType('!' + format.upper(), size)


def bits(byte):
    return ((byte & 1 << i) != 0 for i in range(8)),


def to_byte(bit):
    padding = (False, ) * 8 - len(bits)
    return encode(CHAR, int.from_bytes(padding + bits, 'big')),


def pack_decimal(value):
    normalized = value.normalize()
    if normalized.as_tuple().exponent < 0:
        decimals = - value.as_tuple().exponent
        raw = int(value * (bDecimal(10) ** decimals))
    else:
        decimals = 0
        raw = int(normalized)
    return decimals, raw


def unpack_decimal(decimals, raw):
    return Decimal(raw) * (Decimal(10) ** -decimals)


CHAR = SimpleType('!c', 1)
SIGNED_CHAR, UNSIGNED_CHAR = signed_unsigned_pair('b', 1)
SIGNED_SHORT, SHORT = signed_unsigned_pair('h', 2)
SIGNED_LONG, LONG = signed_unsigned_pair('i', 4)
SIGNED_LONGLONG, LONGLONG = signed_unsigned_pair('q', 8)
FLOAT = SimpleType('!f', 4)
DOUBLE = SimpleType('!d', 8)
TIMESTAMP = LONGLONG
NOTHING = Nothing
PACKED_BOOLS = ProcessedType((CHAR,), bits, to_byte)
DECIMAL = ProcessedType((UNSIGNED_CHAR, SIGNED_LONG), pack_decimal, unpack_decimal)
SHORTSTR, LONGSTR = StringType(UNSIGNED_CHAR), StringType(LONG)
ARRAY = Container(tuple, encode_typed, lambda x: x, read_typed)
TABLE = Container(dict, encode_key_value, lambda x: x.items(), read_key_value)


@singledispatch
def read(as_type, stream):
    raise TypeError('Can not read from stream as type {}'.format(as_type))


@singledispatch
def write(as_type, stream, value):
    stream.write(encode(as_type, value))


@singledispatch
def encode(as_type, value):
    return value


@singledispatch
def decode(as_type, buffer):
    return buffer


@encode.register(Nothing)
def encode_nothing(as_type, value):
    return b''

@decode.register(Nothing)
def decode_nothing(as_type, buffer):
    pass


@read.register(SimpleType)
def read_simple_type(as_type, stream):
    return decode_simple_type(as_type, stream.read(as_type.size))


@write.register(SimpleType)
def write_simple_type(as_type, stream, value):
    stream.write(encode_simple_type(as_type, value))


@encode.register(SimpleType)
def encode_simple_type(as_type, value):
    return struct.pack(as_type.format)


@decode.register(SimpleType)
def decode_simple_type(as_type, buffer):
    return struct.unpack(as_type.format)


@decode.register(ProcessedType)
def decode_processed_type(as_type, buffer):
    stream = io.BytesIO(buffer)
    packed = (read(t, stream) for t in as_type.types)
    return as_type.unpacker(*packed)


@encode.regiser(ProcessedType)
def encode_processed_type(as_type, value):
    return b''.join(
        encode(type, packed)
        for type, packed in zip(as_type.types, as_type.packer(value))
    )


@read.register(StringType)
def read_string(as_type, stream):
    length = read_simple_type(stream, as_type.length_type)
    return encode(as_type, stream.read(length))


@write.register(StringType)
def write_string(as_type, stream, value):
    write_simple_type(stream, len(value), as_type.length_type)
    stream.write(value)


def encode_typed(value):
    amqp_type = py_to_amqp[type(value)]
    return type_to_abbreviation[amqp_type], encode(amqp_type, value)


def read_typed(stream):
    amqp_type = abbreviation_to_type[chr(read(CHAR, stream))]
    return read(amqp_type, stream)


def encode_key_value(key_value):
    key, value = key_value
    return (encode(SHORTSTR, key), ) + encode_typed(value)


def read_key_value(stream):
    return read(SHORTSTR, stream), read_typed(stream)


@read.register(Container)
def read_container(as_type, stream):
    length = read(LONG, stream)
    buffer = stream.read(length)
    return decode(as_type, buffer)

@write.register(Container)
def write_container(as_type, stream, value):
    buffer = encode(as_type, value)
    stream.write(LONG, len(buffer))
    stream.write(buffer)


@decode.register(Container)
def decode_container(as_type, buffer):
    data = io.BytesIO(buffer)
    length = len(buffer)
    def iter_items():
        while data.tell() < length:
            yield as_type.item_reader(data)
        return as_type.py_type(iter_items)
    return data.readall()


@encode.register(Container)
def encode_container(as_type, value):
    return b''.join(
        chain.from_iterable(
            map(as_type.item_encode, as_type.iter_items(value))
        )
    )


type_to_abbreviation = {
    b't': PACKED_BOOLS,
    b'b': CHAR,
    b'B': SIGNED_CHAR,
    b'U': SIGNED_SHORT,
    b'u': SHORT,
    b'I': SIGNED_LONG,
    b'i': LONG,
    b'L': SIGNED_LONGLONG,
    b'l': LONGLONG,
    b'f': FLOAT,
    b'd': DOUBLE,
    b'D': Decimal,
    b's': SHORTSTR,
    b'S': LONGSTR,
    b'A': ARRAY,
    b'T': TIMESTAMP,
    b'F': TABLE,
    b'V': NOTHING
}

abbreviation_to_type = {v: k for k, v in type_to_abbreviation.items()}

py_to_amqp = {
    bytes: LONGSTR,
    str: LONGSTR,
    int: LONG,
    dict: TABLE,
    bool: PACKED_BOOLS,
}


class FrameTypes(IntEnum):
    method = 1
    header = 2
    body = 3
    heartbeat = 8


class Frame(namedtuple('Frame', 'type channel payload frame_end')):
    payload_types = {
        FrameTypes.method: MethodPayload,
        FrameTypes.header: HeaderPayload,
        FrameTypes.body: BodyPayload,
        FrameTypes.heartbeat: HeartbeatPayload,
    }

    @classmethod
    def read(cls, stream):
        type = CHAR.read(stream)
        channel = SHORT.read(stream)
        payload_data = io.BytesIO(stream.read(LONG.read(stream)))
        payload = cls.payload_types[type].read(Frame.payload_data)
        frame_end = stream.readexactly(1)
        if frame_end != aioamqp.constants.FRAME_END:
            raise Exception('Amqp frame have incorrect frame end {}'.format(frame_end))
        return Frame(type, channel, payload)

    @classmethod
    def write(cls, stream):
        data 


class MessageProperties(AmqpType):
    field_types = (
        SHORTSTR,
        SHORTSTR,
        TABLE,
        CHAR,
        CHAR,
        SHORTSTR,
        SHORTSTR,
        SHORTSTR,
        SHORTSTR,
        LONGLONG,
        SHORTSTR,
        SHORTSTR,
        SHORTSTR,
        SHORTSTR,
    )

    @classmethod
    def read(cls, stream):
        flags = SHORT.read(stream).as_bytes(length=2, byteorder='big')[0:-2]
        return Properties(*(
            type.read(stream) if flag else None
            for flag, type in zip(flags, cls.fields_types)
        ))

    @classmethod
    def write(cls, value, stream):
        assert isinstance(value, Properties) #TODO make error msg
        def encode(amqp_type, value):
            _ = io.BytesIO()
            amqp_type.write(value, _)
            return _.readall()
        flags, data = zip(*(
            (0 if property is None else 1, encode(type, property))
            for property, type in zip(value, cls.field_types)
        ))
        SHORT.write(int.decode(flags + (0, 0), byteorder='big'))
        stream.write(data)


Payload = namedtuple('Payload', 'size class_id method_id')
MethodPayload = namedtuple('MethodPayload', Payload._fields)
BodyPayload = namedtuple(
    'ContentPayload',
    Payload._fields + ('body_size', 'property_flags', 'property_list')
)
HeaderPayload = namedtuple(
    'HeaderPayload', Payload._fields + ('weight', 'body_size', 'properties'),
)

class PayloadT(ComplexStaticSizeType):
    types = (LONG, SHORT, SHORT)

class MethodPayloadT(PayloadT):
    py_type = MethodPayload

class HeaderPayloadT(PayloadT):
    py_type = HeaderPayload
    types = PayloadT + (SHORT, LONGLONG, MessageProperties)
