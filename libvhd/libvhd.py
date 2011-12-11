# vim: tabstop=4 shiftwidth=4 softtabstop=4
#
# Copyright 2011, Chris Behrens <cbehrens@codestud.com>
# All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
#    implied. See the License for the specific language governing
#    permissions and limitations under the License.
"""
Module for interfacing with xen's vhd library.
"""

import ctypes
import os
import sys

VHD_SECTOR_SIZE = 512

libvhd_handle = ctypes.CDLL("libvhd.so", use_errno=True)

VHD_DISK_TYPES = {
        'fixed': 2,
        'dynamic': 3,
        'differenciating': 4}

VHD_OPEN_FLAGS = {
        'rdonly': 0x00000001,
        'rdwr': 0x00000002,
        'fast': 0x00000004,
        'strict': 0x00000008,
        'ignore_disabled': 0x00000010}


class VHDException(Exception):
    pass


class VHDInvalidSize(VHDException):
    pass


class VHDInvalidDiskType(VHDException):
    pass


class VHDInvalidOpenFlag(VHDException):
    pass


class VHDWriteError(VHDException):
    pass


class VHDOpenFailure(VHDException):
    pass


class VHDInvalidBuffer(VHDException):
    pass


class BufferFailure(Exception):
    pass


class BufferInvalidOffset(BufferFailure):
    pass


class BufferInvalidSize(BufferFailure):
    pass


class VHDPrtLoc(ctypes.Structure):
    _fields_ = [
            ('code', ctypes.c_uint),
            ('data_space', ctypes.c_uint),
            ('data_len', ctypes.c_uint),
            ('res', ctypes.c_uint),
            ('data_offset', ctypes.c_ulonglong)]


class VHDHeader(ctypes.Structure):
    _fields_ = [
            ('cookie', ctypes.c_char * 8),
            ('data_offset', ctypes.c_ulonglong),
            ('table_offset', ctypes.c_ulonglong),
            ('hdr_ver', ctypes.c_uint),
            ('max_bat_size', ctypes.c_uint),
            ('block_size', ctypes.c_uint),
            ('checksum', ctypes.c_uint),
            ('prt_uuid', ctypes.c_char * 16),
            ('prt_ts', ctypes.c_uint),
            ('res1', ctypes.c_uint),
            ('prt_name', ctypes.c_char * 512),
            ('loc', VHDPrtLoc * 8),
            ('res2', ctypes.c_char * 256)]


class VHDFooter(ctypes.Structure):
    _fields_ = [
            ('cookie', ctypes.c_char * 8),
            ('features', ctypes.c_uint),
            ('ff_version', ctypes.c_uint),
            ('data_offset', ctypes.c_ulonglong),
            ('timestamp', ctypes.c_uint),
            ('crtr_app', ctypes.c_char * 4),
            ('crtr_ver', ctypes.c_uint),
            ('crtr_os', ctypes.c_uint),
            ('orig_size', ctypes.c_ulonglong),
            ('curr_size', ctypes.c_ulonglong),
            ('geometry', ctypes.c_uint),
            ('type', ctypes.c_uint),
            ('checksum', ctypes.c_uint),
            ('uuid', ctypes.c_char * 16),
            ('saved', ctypes.c_char),
            ('hidden', ctypes.c_char),
            ('reserved', ctypes.c_char * 426)]


class VHDBat(ctypes.Structure):
    _fields_ = [
            ('spb', ctypes.c_uint),
            ('entries', ctypes.c_uint),
            ('bat', ctypes.c_void_p)]


class VHDBatMapHeader(ctypes.Structure):
    _fields_ = [
            ('cookie', ctypes.c_char * 8),
            ('batmap_offset', ctypes.c_ulonglong),
            ('batmap_size', ctypes.c_uint),
            ('batmap_version', ctypes.c_uint),
            ('checksum', ctypes.c_uint)]


class VHDBatMap(ctypes.Structure):
    _fields_ = [
            ('header', VHDBatMapHeader),
            ('map', ctypes.c_void_p)]


class VHDContext(ctypes.Structure):
    _fields_ = [
            ('fd', ctypes.c_int),
            ('file', ctypes.c_char_p),
            ('oflags', ctypes.c_int),
            ('isblock', ctypes.c_int),
            ('spb', ctypes.c_uint),
            ('bm_secs', ctypes.c_uint),
            ('header', VHDHeader),
            ('footer', VHDFooter),
            ('bat', VHDBat),
            ('batmap', VHDBatMap)]


class AlignedBuffer(object):
    def __init__(self, size, alignment=None):
        if alignment is None:
            alignment = 512
        self.size = size
        buf_size = alignment + self.size - 1
        self.buf = ctypes.create_string_buffer(buf_size)
        self.buf_addr = ctypes.addressof(self.buf)
        if self.buf_addr % alignment:
            self.buf_addr += alignment - (self.buf_addr % alignment)

    def get_pointer(self, offset=0, size=None):
        """Return a pointer into the aligned buffer at an offset, where
        the offset defaults to 0.  An optional size can be specified to
        limit the pointer to a specific range
        """
        diff = self.size - offset
        if diff <= 0:
            raise BufferInvalidOffset()
        if size is None:
            size = diff
        elif size > diff:
            raise BufferInvalidSize()
        t = ctypes.c_char * size
        return ctypes.pointer(t.from_address(self.buf_addr + offset))

    def write(self, data, offset=0):
        """Write data into the aligned buffer at a specific offset,
        where the offset defaults to 0.
        """
        p = self.get_pointer(offset=offset)
        p.contents.value = data

    def read(self, offset=0, size=None):
        """Return data from the buffer at a specific offset.  An optional
        size can be specified to limit the data returned.  Size will
        default to the rest of the buffer.
        """
        p = self.get_pointer(offset=offset, size=size)
        return p.contents.value


class VHD(object):
    _closed = True

    def __init__(self, filename, flags=None):
        """Open a VHD."""
        if flags is None:
            flags = 'rdonly'
        open_flags = 0
        try:
            for flag in flags.split(','):
                open_flags |= VHD_OPEN_FLAGS[flag]
        except KeyError:
            valid_flags = ','.join(VHD_OPEN_FLAGS.iterkeys())
            raise VHDInvalidOpenFlag("Valid open flags are: %s" %
                    valid_flags)

        self.filename = filename
        self.open_flags = flags
        self.vhd_context = VHDContext()
        ret = _call('vhd_open',
                ctypes.pointer(self.vhd_context),
                ctypes.c_char_p(filename), ctypes.c_int(open_flags))
        if ret:
            raise VHDOpenFailure("Error opening: %s" % ctypes.get_errno())
        self._closed = False

    def close(self):
        """Close a VHD."""
        if self._closed:
            return
        _call('vhd_close', ctypes.pointer(self.vhd_context))
        self._closed = True
        self.vhd_context = None

    def __del__(self):
        """Make sure the VHD gets closed."""
        self.close()

    def get_footer(self):
        """Get the VHD footer data."""
        ftr = {}
        footer = self.vhd_context.footer
        for field, _ in footer._fields_:
            ftr[field] = getattr(footer, field)
        return ftr

    def io_write(self, buf, cur_sec, num_secs):
        """Write sectors from an aligned buffer into a VHD."""
        if not isinstance(buf, AlignedBuffer):
            raise VHDInvalidBuffer("buf argument should be a AlignedBuffer"
                    " instance")
        ret = _call('vhd_io_write',
                ctypes.pointer(self.vhd_context),
                buf.get_pointer(),
                ctypes.c_ulonglong(cur_sec),
                ctypes.c_uint(num_secs))
        if not ret:
            return
        errno = ctypes.get_errno()
        raise VHDWriteError("Error writing: %s" % errno)

    def __repr__(self):
        if self._closed:
            return "<%s: closed>" % self.filename
        footer_values = self.get_footer()
        return "<%s: opened '%s', footer '%s'>" % (
                self.filename, self.open_flags, footer_values)


def _call(fn_name, *args):
    """Call a function in libvhd.so"""
    fn = getattr(libvhd_handle, fn_name)
    return fn(*args)


def vhd_create(filename, size, disk_type=None, create_flags=None):
    """Usage: <filename> <size> [<disk_type>] [<create_flags>]"""

    if disk_type is None:
        disk_type = 'dynamic'
    if create_flags is None:
        create_flags = 0

    try:
        disk_type = VHD_DISK_TYPES[disk_type]
    except KeyError:
        valid_disk_types = ','.join(VHD_DISK_TYPES.iterkeys())
        raise VHDInvalidDiskType("Valid disk types are: %s" %
                valid_disk_types)
    size = int(size)
    create_flags = int(create_flags)

    if size % VHD_SECTOR_SIZE:
        raise VHDInvalidSize("size is not a multiple of %d" %
                VHD_SECTOR_SIZE)
    return _call('vhd_create', ctypes.c_char_p(filename),
            ctypes.c_ulonglong(size),
            ctypes.c_int(disk_type),
            ctypes.c_uint(create_flags))


def vhd_convert_from_raw(src_filename, dest_filename, sparse=None):
    """Usage: <src_filename> <dest_filename> [<0|1>]"""
    """Arguments: 1 == Sparse"""

    if sparse == '1':
        sparse = 1
    else:
        sparse = 0

    size = os.stat(src_filename).st_size
    if size % VHD_SECTOR_SIZE:
        size += VHD_SECTOR_SIZE - (size % VHD_SECTOR_SIZE)

    cur_sec = 0
    num_secs_to_read = 2 * 128
    buf_size = VHD_SECTOR_SIZE * num_secs_to_read
    buf = AlignedBuffer(buf_size, alignment=VHD_SECTOR_SIZE)

    all_zero_sector = '\x00' * VHD_SECTOR_SIZE

    def _write_sectors(data, start, end):
        buf.write(data[start:end])
        num_secs = (end - start) / VHD_SECTOR_SIZE
        vhd.io_write(buf, cur_sec, num_secs)

    with open(src_filename, 'rb') as f:
        vhd_create(dest_filename, size, 'dynamic')
        vhd = VHD(dest_filename, 'rdwr')

        while True:
            data = f.read(buf_size)
            if len(data) == 0:
                break
            data_len = len(data)
            if (data_len < buf_size and
                    data_len % VHD_SECTOR_SIZE):
                pad = VHD_SECTOR_SIZE - (data_len % VHD_SECTOR_SIZE)
                data += '\x00' * pad
                data_len += pad

            max_num = data_len / VHD_SECTOR_SIZE

            if not sparse:
                _write_sectors(data, 0, data_len)
                cur_sec += max_num
                continue

            non_zero_start = None
            non_zero_end = None
            for i in xrange(max_num):
                beginning = i * VHD_SECTOR_SIZE
                ending = beginning + VHD_SECTOR_SIZE
                sector = data[beginning:ending]
                if sector == all_zero_sector:
                    if non_zero_start is not None:
                        # We can write the previous sectors
                        _write_sectors(data, non_zero_start, non_zero_end)
                        non_zero_start = None
                else:
                    if non_zero_start is None:
                        # First non-zero sector
                        non_zero_start = beginning
                    non_zero_end = ending
                cur_sec += 1
            if non_zero_start is not None:
                _write_sectors(data, non_zero_start, non_zero_end)
        vhd.close()
