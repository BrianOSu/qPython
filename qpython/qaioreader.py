#
#  Copyright (c) 2011-2014 Exxeleron GmbH
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

import sys

from qpython.qtype import *  # @UnusedWildImport
from qpython.qreader import Reader, QMessage, QReaderException

try:
    from qpython.fastutils import uncompress
except:
    from qpython.utils import uncompress


class QReader(Reader):
    async def read(self, source=None, **options):
        message = await self.read_header(source)
        message.data = await self.read_data(message.size, message.compression_mode, **options)
        return message

    async def read_header(self, source=None):
        if self._stream:
            header = await self._read_bytes(8)
            self._buffer.wrap(header)
        else:
            self._buffer.wrap(source)

        self._buffer.endianness = '<' if self._buffer.get_byte() == 1 else '>'
        self._is_native = self._buffer.endianness == ('<' if sys.byteorder == 'little' else '>')
        message_type = self._buffer.get_byte()
        message_compression_mode = self._buffer.get_byte()
        message_size_ext = self._buffer.get_byte()

        message_size = self._buffer.get_uint()
        message_size += message_size_ext << 32
        return QMessage(None, message_type, message_size, message_compression_mode)

    async def read_data(self, message_size, compression_mode=0, **options):
        super().read_data(message_size, compression_mode, **options)

        if compression_mode > 0:
            comprHeaderLen = 4 if compression_mode == 1 else 8
            if self._stream:
                self._buffer.wrap(await self._read_bytes(comprHeaderLen))
            uncompressed_size = -8 + (self._buffer.get_uint() if compression_mode == 1 else self._buffer.get_long())
            compressed_data = await self._read_bytes(message_size - (8+comprHeaderLen)) if self._stream else self._buffer.raw(message_size - (8+comprHeaderLen))

            raw_data = numpy.frombuffer(compressed_data, dtype=numpy.uint8)
            if uncompressed_size <= 0:
                raise QReaderException('Error while data decompression.')

            raw_data = uncompress(raw_data, numpy.int64(uncompressed_size))
            raw_data = numpy.ndarray.tobytes(raw_data)
            self._buffer.wrap(raw_data)
        elif self._stream:
            raw_data = await self._read_bytes(message_size - 8)
            self._buffer.wrap(raw_data)
        if not self._stream and self._options.raw:
            raw_data = self._buffer.raw(message_size - 8)

        return raw_data if self._options.raw else self._read_object()

    async def _read_bytes(self, length):
        if not self._stream:
            raise QReaderException('There is no input data. QReader requires either stream or data chunk')

        if length == 0:
            return b''
        else:
            data = await self._stream.read(length)

        if len(data) == 0:
            raise QReaderException('Error while reading data')
        return data
