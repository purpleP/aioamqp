from itertools import takewhile
from enum import IntEnum
from operator import concat
import aioamqp
from collections import namedtuple
from functools import reduce, singledispatch, partial
import struct
from decimal import Decimal as bDecimal
import io
from more_functools import nwise_iter

from aioamqp.helpers import typed_namedtuple
from aioamqp.properties import Properties


class AmqpType:
    @classmethod
    def read(cls, reader):
        raise NotImplementedError()

    @classmethod
    def write(cls, value, stream):
        raise NotImplementedError()


class Primitive(AmqpType):
    format = None
    size = None
    @classmethod
    def read(cls, reader):
        return struct.unpack(cls.format, reader.read(cls.size))[0]

    @classmethod
    def write(cls, value, stream):
        stream.write(struct.pack(cls.format, value))


class Char(Primitive):
    format = '!c'
    size = 1


class Bool(AmqpType):
    @classmethod
    def read(cls, reader):
        return bool(Char.read(reader))
    
    @classmethod
    def write(cls, stream, bits):
        assert len(bits) <= 8
        padding = (False,) * 8 - len(bits)
        Char.write(stream, int.from_bytes(padding + bits))


class UnsignedChar(Char):
    format = '!B'


class SignedChar(Char):
    format = '!b'


class Short(Primitive):
    format = '!H'
    size = 2


class SignedShort(Short):
    format = '!h'


class Long(Primitive):
    format = '!I'
    size = 4


class SignedLong(Long):
    format = '!i'


class LongLong(Primitive):
    format = '!Q'
    size = 8


class SignedLongLong(LongLong):
    format = '!q'


class UnsignedLongLong(Primitive):
    pass


class Float(Primitive):
    format = '!f'
    size = 4


class Double(Primitive):
    format = '!d'
    size = 8


class BaseStr(AmqpType):
    len_type = None
    @classmethod
    def read(cls, reader):
        length = cls.len_type.read(reader)
        return reader.read(length).decode()

    @staticmethod
    def write_str(value, stream):
        if isinstance(value, str):
            stream.write(value.encode())
        elif isinstance(value, (bytes, bytearray)):
            stream.write(value)

    @classmethod
    def write(cls, value, stream):
        cls.len_type.write(stream, len(value))
        cls.write_str(value, stream)


class ShortStr(BaseStr):
    len_type = UnsignedChar


class LongStr(BaseStr):
    len_type = Long


class Decimal(AmqpType):
    @classmethod
    def read(cls, reader):
        decimals = UnsignedChar.read(reader)
        value = SignedLong.read(reader)
        return Decimal(value) * (Decimal(10) ** -decimals)

    @classmethod
    def write(cls, value, stream):
        normalized = value.normalize()
        if normalized.as_tuple().exponent < 0:
            decimals = - value.as_tuple().exponent
            raw = int(value * (bDecimal(10) ** decimals))
        else:
            decimals = 0
            raw = int(normalized)
        UnsignedChar.write(stream, decimals)
        SignedLong.write(stream, raw)


class TimeStamp(AmqpType):
    @classmethod
    def read(cls, reader):
        return LongLong.read(reader)

    @classmethod
    def write(cls, value, stream):
        LongLong.write(value, stream)

type_to_abbreviation_ = {
    b't': Bool,
    b'b': Char,
    b'B': SignedChar,
    b'U': SignedShort,
    b'u': Short,
    b'I': SignedLong,
    b'i': Long,
    b'L': UnsignedLongLong,
    b'l': LongLong,
    b'f': Float,
    b'd': Double,
    b'D': Decimal,
    b's': ShortStr,
    b'S': LongStr,
    b'A': List,
    b'T': TimeStamp,
    b'F': Table,
}

type_to_abbreviation = {
    Bool: 't',
    Char: 'b',
    SignedChar: 'B',
    SignedShort: 'U',
    Short: 'u',
    SignedLong: 'I',
    Long: 'i',
    UnsignedLongLong: 'L',
    LongLong: 'l',
    Float: 'f',
    Double: 'd',
    Decimal: 'D',
    ShortStr: 's',
    LongStr: 'S',
    List: 'A',
    TimeStamp: 'T',
    Table: 'F',
}

py_to_amqp = {
    bytes: LongStr,
    str: LongStr,
    int: Long,
    dict: Table,
    bool: Bool,
}


class Container(AmqpType):

    @classmethod
    def read(cls, reader):
        length = Long.read(reader)
        data = io.BytesIO(reader.read(length))
        def iter_items():
            while data.tell() < length:
                yield cls.read_item(data)

        return cls.py_type(iter_items())

    @classmethod
    def read_item(cls, data):
        type_key = chr(Char.read(data))
        if type_key == 'V':
            return None
        else:
            try:
                return type_to_abbreviation_[type_key].read(data)
            except KeyError as e:
                raise ValueError('Uknown value_type {}'.format(type_key))

    @classmethod
    def write_item(cls, payload, item):
        payload.write()

class List(AmqpType):
    py_type = tuple


class Table(AmqpType):
    py_type = dict
    @classmethod
    def read_item(cls, data):
        key = ShortStr.read(data)
        value = super().read_item(data)
        return key, value

    @classmethod
    def write(cls, value, stream):

        def write_with_type(key_value, stream):
            key, value = key_value
            amqp_type = py_to_amqp[type(value)]
            stream.write(type_to_abbreviation[amqp_type])
            ShortStr.write(key, stream)
            amqp_type.write(value, stream)
            return stream

        if value is not None and len(value):
            payload = io.BytesIO()
            reduce(write_with_type, value.items(), payload)
            stream.write(payload.getvalue())


class FrameTypes(IntEnum):
    method = 1
    header = 2
    body = 3
    heartbeat = 8


class ComplexStaticSizeType(AmqpType):
    @classmethod
    def read(cls, stream):
        return cls.py_type(*(t.read(stream) for t in cls.types))

    @classmethod
    def write(cls, value, stream):
        for t in cls.types:
            t.write(value, stream)

class Frame(namedtuple('Frame', 'type channel payload frame_end')):
    payload_types = {
        FrameTypes.method: MethodPayload,
        FrameTypes.header: HeaderPayload,
        FrameTypes.body: BodyPayload,
        FrameTypes.heartbeat: HeartbeatPayload,
    }

    @classmethod
    def read(cls, stream):
        type = Char.read(stream)
        channel = Short.read(stream)
        payload_data = io.BytesIO(stream.read(Long.read(stream)))
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
        ShortStr,
        ShortStr,
        Table,
        Char,
        Char,
        ShortStr,
        ShortStr,
        ShortStr,
        ShortStr,
        LongLong,
        ShortStr,
        ShortStr,
        ShortStr,
        ShortStr,
    )

    @classmethod
    def read(cls, stream):
        flags = Short.read(stream).as_bytes(length=2, byteorder='big')[0:-2]
        return Properties(*(
            type.read(stream) if flag else None
            for flag, type in zip(flags, cls.fields_types)
        ))

    @classmethod
    def write(cls, value, stream):
        assert isinstance(value, Properties) #TODO make error msg
        def to_bytes(amqp_type, value):
            _ = io.BytesIO()
            amqp_type.write(value, _)
            return _.readall()
        flags, data = zip(*(
            (0 if property is None else 1, to_bytes(type, property))
            for property, type in zip(value, cls.field_types)
        ))
        Short.write(int.from_bytes(flags + (0, 0), byteorder='big'))
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
    types = (Long, Short, Short)

class MethodPayloadT(PayloadT):
    py_type = MethodPayload

class HeaderPayloadT(PayloadT):
    py_type = HeaderPayload
    types = PayloadT + (Short, LongLong, MessageProperties)
