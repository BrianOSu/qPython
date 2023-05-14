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

try:
    from cStringIO import BytesIO
except ImportError:
    from io import BytesIO

from qpython.qwriter import Writer, QWriterException


class QWriter(Writer):
    async def write(self, data, msg_type, **options):
        super().write(data, msg_type, **options)
        # write data to stream
        if self._stream:
            self._stream.write(self._buffer.getvalue())
            await self._stream.drain()
        else:
            return self._buffer.getvalue()
