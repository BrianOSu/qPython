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

import asyncio
import struct


from qpython.qtype import QException
from qpython.qaioreader import QReader, QReaderException
from qpython.qaiowriter import QWriter, QWriterException
from qpython.qconnection import Connection, QConnectionException, QAuthenticationException, MessageType


class QConnection(Connection):
    """Connector class for interfacing with the q service.

    Provides methods for synchronous and asynchronous interaction.

    The :class:`.QConnection` class provides a context manager API and can be
    used with a ``with`` statement::

        with qconnection.QConnection(host = 'localhost', port = 5000) as q:
            print(q)
            print(q('{`int$ til x}', 10))

    :Parameters:
     - `host` (`string`) - q service hostname
     - `port` (`integer`) - q service port
     - `username` (`string` or `None`) - username for q authentication/authorization
     - `password` (`string` or `None`) - password for q authentication/authorization
     - `timeout` (`nonnegative float` or `None`) - set a timeout on blocking socket operations
     - `encoding` (`string`) - string encoding for data deserialization
     - `reader_class` (subclass of `QReader`) - data deserializer
     - `writer_class` (subclass of `QWriter`) - data serializer
    :Options:
     - `raw` (`boolean`) - if ``True`` returns raw data chunk instead of parsed
       data, **Default**: ``False``
     - `numpy_temporals` (`boolean`) - if ``False`` temporal vectors are
       backed by raw q representation (:class:`.QTemporalList`,
       :class:`.QTemporal`) instances, otherwise are represented as
       `numpy datetime64`/`timedelta64` arrays and atoms,
       **Default**: ``False``
     - `single_char_strings` (`boolean`) - if ``True`` single char Python
       strings are encoded as q strings instead of chars, **Default**: ``False``
    """

    def __init__(self, host, port, username=None, password=None, timeout=None,
                 encoding='latin-1', reader_class=None, writer_class=None, **options):
        self._s_reader = None
        self._s_writer = None

        # Todo: Add pands support
        self._reader_class = QReader
        self._writer_class = QWriter

        if reader_class:
            self._reader_class = reader_class

        if writer_class:
            self._writer_class = writer_class

        super().__init__(host, port, username, password, timeout, encoding, reader_class, writer_class, **options)

    async def open(self):
        """Initialises connection to q service.

        If the connection hasn't been initialised yet, invoking the
        :func:`.open` creates a new socket and performs a handshake with a q
        service.

        :raises: :class:`.QConnectionException`, :class:`.QAuthenticationException`
        """
        if not self._s_writer:
            if not self.host:
                raise QConnectionException('Host cannot be None')

            await self._init_socket()
            await self._initialize()

            self._writer = self._writer_class(self._s_writer, protocol_version=self._protocol_version,
                                              encoding=self._encoding)
            self._reader = self._reader_class(self._s_reader, encoding=self._encoding)

    async def _init_socket(self):
        """Initialises the socket used for communicating with a q service,"""
        try:
            self._s_reader, self._s_writer = await asyncio.open_connection(self.host, self.port)
        except:
            self._s_reader, self._s_writer = None, None
            raise

    async def close(self):
        """Closes connection with the q service."""
        if self._s_writer:
            self._s_writer.close()
            await self._s_writer.wait_closed()

    def is_connected(self):
        """Checks whether connection with a q service has been established.

        Connection is considered inactive when:
         - it has not been initialised,
         - it has been closed.

        :returns: `boolean` -- ``True`` if connection has been established,
                  ``False`` otherwise
        """
        return True if self._s_writer else False

    async def _initialize(self):
        """Performs a IPC protocol handshake."""
        credentials = (self.username if self.username else '') + ':' + (self.password if self.password else '')
        credentials = credentials.encode(self._encoding)
        self._s_writer.write(credentials + bytes([self.MAX_PROTOCOL_VERSION, 0]))
        await self._s_writer.drain()
        response = await self._s_reader.read(1)

        if len(response) != 1:
            await self._s_writer.close()
            await self._init_socket()

            self._s_writer.write(credentials + b'\0')
            await self._s_writer.drain()
            response = await self._s_reader.read(1)
            if len(response) != 1:
                await self.close()
                raise QAuthenticationException('Connection denied.')

        self._protocol_version = min(struct.unpack('B', response)[0], self.MAX_PROTOCOL_VERSION)

    async def query(self, msg_type, query, *parameters, **options):
        """Performs a query against a q service.

        In typical use case, `query` is the name of the function to call and
        `parameters` are its parameters. When `parameters` list is empty, the
        query can be an arbitrary q expression (e.g. ``0 +/ til 100``).

        Calls a anonymous function with a single parameter:

            >>> q.query(qconnection.MessageType.SYNC,'{til x}', 10)

        Executes a q expression:

            >>> q.query(qconnection.MessageType.SYNC,'til 10')

        :Parameters:
         - `msg_type` (one of the constants defined in :class:`.MessageType`) -
           type of the query to be executed
         - `query` (`string`) - query to be executed
         - `parameters` (`list` or `None`) - parameters for the query
        :Options:
         - `single_char_strings` (`boolean`) - if ``True`` single char Python
           strings are encoded as q strings instead of chars,
           **Default**: ``False``

        :raises: :class:`.QConnectionException`, :class:`.QWriterException`
        """
        if not self._s_writer:
            raise QConnectionException('Connection is not established.')

        if parameters and len(parameters) > 8:
            raise QWriterException('Too many parameters.')

        if not parameters or len(parameters) == 0:
            await self._writer.write(query, msg_type, **self._options.union_dict(**options))
        else:
            await self._writer.write([query] + list(parameters), msg_type, **self._options.union_dict(**options))

    async def sendSync(self, query, *parameters, **options):
        """Performs a synchronous query against a q service and returns parsed
        data.

        In typical use case, `query` is the name of the function to call and
        `parameters` are its parameters. When `parameters` list is empty, the
        query can be an arbitrary q expression (e.g. ``0 +/ til 100``).

        Executes a q expression:

            >>> print(q.sendSync('til 10'))
            [0 1 2 3 4 5 6 7 8 9]

        Executes an anonymous q function with a single parameter:

            >>> print(q.sendSync('{til x}', 10))
            [0 1 2 3 4 5 6 7 8 9]

        Executes an anonymous q function with two parameters:

            >>> print(q.sendSync('{y + til x}', 10, 1))
            [ 1  2  3  4  5  6  7  8  9 10]

            >>> print(q.sendSync('{y + til x}', *[10, 1]))
            [ 1  2  3  4  5  6  7  8  9 10]

        The :func:`.sendSync` is called from the overloaded :func:`.__call__`
        function. This allows :class:`.QConnection` instance to be called as
        a function:

            >>> print(q('{y + til x}', 10, 1))
            [ 1  2  3  4  5  6  7  8  9 10]


        :Parameters:
         - `query` (`string`) - query to be executed
         - `parameters` (`list` or `None`) - parameters for the query
        :Options:
         - `raw` (`boolean`) - if ``True`` returns raw data chunk instead of
           parsed data, **Default**: ``False``
         - `numpy_temporals` (`boolean`) - if ``False`` temporal vectors are
           backed by raw q representation (:class:`.QTemporalList`,
           :class:`.QTemporal`) instances, otherwise are represented as
           `numpy datetime64`/`timedelta64` arrays and atoms,
           **Default**: ``False``
         - `single_char_strings` (`boolean`) - if ``True`` single char Python
           strings are encoded as q strings instead of chars,
           **Default**: ``False``

        :returns: query result parsed to Python data structures

        :raises: :class:`.QConnectionException`, :class:`.QWriterException`,
                 :class:`.QReaderException`
        """
        await self.query(MessageType.SYNC, query, *parameters, **options)
        response = await self.receive(data_only=False, **options)

        if response.type == MessageType.RESPONSE:
            return response.data
        else:
            self._writer.write(QException('nyi: qPython expected response message'), MessageType.ASYNC if response.type == MessageType.ASYNC else MessageType.RESPONSE)
            raise QReaderException('Received message of type: %s where response was expected')

    async def sendAsync(self, query, *parameters, **options):
        """Performs an asynchronous query and returns **without** retrieving of
        the response.

        In typical use case, `query` is the name of the function to call and
        `parameters` are its parameters. When `parameters` list is empty, the
        query can be an arbitrary q expression (e.g. ``0 +/ til 100``).

        Calls a anonymous function with a single parameter:

            >>> q.sendAsync('{til x}', 10)

        Executes a q expression:

            >>> q.sendAsync('til 10')

        :Parameters:
         - `query` (`string`) - query to be executed
         - `parameters` (`list` or `None`) - parameters for the query
        :Options:
         - `single_char_strings` (`boolean`) - if ``True`` single char Python
           strings are encoded as q strings instead of chars,
           **Default**: ``False``

        :raises: :class:`.QConnectionException`, :class:`.QWriterException`
        """
        await self.query(MessageType.ASYNC, query, *parameters, **options)

    async def receive(self, data_only=True, **options):
        """Reads and (optionally) parses the response from a q service.

        Retrieves query result along with meta-information:

            >>> q.query(qconnection.MessageType.SYNC,'{x}', 10)
            >>> print(q.receive(data_only = False, raw = False))
            QMessage: message type: 2, data size: 13, is_compressed: False, data: 10

        Retrieves parsed query result:

            >>> q.query(qconnection.MessageType.SYNC,'{x}', 10)
            >>> print(q.receive(data_only = True, raw = False))
            10

        Retrieves not-parsed (raw) query result:

            >>> from binascii import hexlify
            >>> q.query(qconnection.MessageType.SYNC,'{x}', 10)
            >>> print(hexlify(q.receive(data_only = True, raw = True)))
            fa0a000000

        :Parameters:
         - `data_only` (`boolean`) - if ``True`` returns only data part of the
           message, otherwise returns data and message meta-information
           encapsulated in :class:`.QMessage` instance
        :Options:
         - `raw` (`boolean`) - if ``True`` returns raw data chunk instead of
           parsed data, **Default**: ``False``
         - `numpy_temporals` (`boolean`) - if ``False`` temporal vectors are
           backed by raw q representation (:class:`.QTemporalList`,
           :class:`.QTemporal`) instances, otherwise are represented as
           `numpy datetime64`/`timedelta64` arrays and atoms,
           **Default**: ``False``

        :returns: depending on parameter flags: :class:`.QMessage` instance,
                  parsed message, raw data
        :raises: :class:`.QReaderException`
        """
        result = await self._reader.read(**self._options.union_dict(**options))
        return result.data if data_only else result

    async def __call__(self, *parameters, **options):
        result = await self.sendSync(parameters[0], *parameters[1:], **options)
        return result
