# SFTP storage backend for Django.
# Author: Brent Tubbs <brent.tubbs@gmail.com>
# License: MIT
#
# Originally modeled on the FTP storage by Rafal Jonca <jonca.rafal@gmail.com>
from __future__ import print_function

import getpass
import io
import os
import posixpath
import stat
from datetime import datetime

import paramiko
from django.core.files.base import File
from django.core.files.storage import Storage
from django.utils.deconstruct import deconstructible

from storages.utils import setting

try:
    from django.utils.six.moves.urllib import parse as urlparse
except ImportError:
    from urllib import parse as urlparse


@deconstructible
class SFTPStorage(Storage):
    """
    SFTP storage backend for Django. 
    This is a wrapper around the paramiko.SFTPClient.
    """
    def __init__(self, host=None, params=None, interactive=None, file_mode=None,
                 dir_mode=None, uid=None, gid=None, known_host_file=None,
                 root_path=None, base_url=None, buffer_size=None, pipelined=None):
        """
        `buffer_size` is used as the paramiko sftp `bufsize` argument. 
        buffer_size=-1 to uses the default Python buffer size, buffer_size=0 for 
        unbuffered. 
        See: http://docs.paramiko.org/en/stable/api/sftp.html#paramiko.sftp_client.SFTPClient.open

        `pipelined` turns on pipelining for write operations, where paramiko 
        won't wait for the server response before sending the next write.
        """
        self._host = host or setting('SFTP_STORAGE_HOST')

        self._params = params or setting('SFTP_STORAGE_PARAMS', {})
        self._interactive = setting('SFTP_STORAGE_INTERACTIVE', False) \
            if interactive is None else interactive
        self._file_mode = setting('SFTP_STORAGE_FILE_MODE') \
            if file_mode is None else file_mode
        self._dir_mode = setting('SFTP_STORAGE_DIR_MODE') if \
            dir_mode is None else dir_mode

        self._uid = setting('SFTP_STORAGE_UID') if uid is None else uid
        self._gid = setting('SFTP_STORAGE_GID') if gid is None else gid
        self._known_host_file = setting('SFTP_KNOWN_HOST_FILE') \
            if known_host_file is None else known_host_file

        self.buffer_size = setting('SFTP_STORAGE_BUFFER_SIZE', default=-1) \
                            if buffer_size is None else buffer_size

        self._pipelined = setting('SFTP_STORAGE_PIPELINED', default=False) \
            if pipelined is None else pipelined

        self._root_path = setting('SFTP_STORAGE_ROOT', '') \
            if root_path is None else root_path
        self._base_url = setting('MEDIA_URL') if base_url is None else base_url

        self._sftp = None

    def _connect(self):
        self._ssh = paramiko.SSHClient()

        known_host_file = self._known_host_file or os.path.expanduser(
            os.path.join("~", ".ssh", "known_hosts")
        )

        if os.path.exists(known_host_file):
            self._ssh.load_host_keys(known_host_file)

        # and automatically add new host keys for hosts we haven't seen before.
        self._ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        try:
            self._ssh.connect(self._host, **self._params)
        except paramiko.AuthenticationException as e:
            if self._interactive and 'password' not in self._params:
                # If authentication has failed, and we haven't already tried
                # username/password, and configuration allows it, then try
                # again with username/password.
                if 'username' not in self._params:
                    self._params['username'] = getpass.getuser()
                self._params['password'] = getpass.getpass()
                self._connect()
            else:
                raise paramiko.AuthenticationException(e)

        if self._ssh.get_transport():
            self._sftp = self._ssh.open_sftp()

    @property
    def sftp(self):
        """Lazy SFTP connection"""
        if not self._sftp or not self._ssh.get_transport().is_active():
            self._connect()
        return self._sftp

    def _remote_path(self, name):
        return posixpath.join(self._root_path, name)

    def _open(self, name, mode='rb'):
        return SFTPStorageFile(name, self, mode, pipelined=self._pipelined)

    def _read(self, name):
        remote_path = self._remote_path(name)
        sftpfile = self.sftp.open(remote_path, 'rb')
        sftpfile.set_pipelined(self._pipelined)
        return sftpfile

    def _chown(self, path, uid=None, gid=None):
        """Set uid and/or gid for file at path."""
        # Paramiko's chown requires both uid and gid, so look them up first if
        # we're only supposed to set one.
        if uid is None or gid is None:
            attr = self.sftp.stat(path)
            uid = uid or attr.st_uid
            gid = gid or attr.st_gid
        self.sftp.chown(path, uid, gid)

    def _mkdir(self, path):
        """Create directory, recursing up to create parent dirs if
        necessary."""
        parent = posixpath.dirname(path)
        if not self.exists(parent):
            self._mkdir(parent)
        self.sftp.mkdir(path)

        if self._dir_mode is not None:
            self.sftp.chmod(path, self._dir_mode)

        if self._uid or self._gid:
            self._chown(path, uid=self._uid, gid=self._gid)

    def _save(self, name, content):
        """Save file via SFTP."""
        content.open()
        path = self._remote_path(name)
        dirname = posixpath.dirname(path)
        if not self.exists(dirname):
            self._mkdir(dirname)

        # The read chunk size usually matches the write buffer size, but can't be 0 or -1 
        # (unlike bufsize for paramiko, which might be), so we set a default in this case.
        read_bufsize = self.buffer_size
        if read_bufsize < 1:
            read_bufsize = io.DEFAULT_BUFFER_SIZE  # probably 8192

        with self.sftp.open(path, mode='wb', bufsize=self.buffer_size) as f:
            f.set_pipelined(self._pipelined)
            for chunk in iter(lambda: content.read(read_bufsize), b''):
                f.write(chunk)

        # Or dogfood using an SFTPStorageFile instance rather than paramiko directly ?
        # with self.open(name, 'wb') as f:
        #     for chunk in iter(lambda: content.read(read_bufsize), b''):
        #         f.write(chunk)

        # This method uses pipelining (and less code), but doesn't seem to allow the 
        # write buffer size to be set (but we could wrap content like 
        # io.BufferedReader(content, buffer_size=read_bufsize)
        # self.sftp.putfo(content, path, confirm=True)

        # set file permissions if configured
        if self._file_mode is not None:
            self.sftp.chmod(path, self._file_mode)
        if self._uid or self._gid:
            self._chown(path, uid=self._uid, gid=self._gid)
        return name

    def delete(self, name):
        _path = self._remote_path(name)
        if self._isdir_attr(self.sftp.stat(_path)):
            self.sftp.rmdir(_path)
        else:
            self.sftp.remove(_path)

    def exists(self, name):
        try:
            self.sftp.stat(self._remote_path(name))
            return True
        except IOError:
            return False

    def _isdir_attr(self, item):
        # Return whether an item in sftp.listdir_attr results is a directory
        if item.st_mode is not None:
            return stat.S_IFMT(item.st_mode) == stat.S_IFDIR
        else:
            return False

    def listdir(self, path):
        remote_path = self._remote_path(path)
        dirs, files = [], []
        for item in self.sftp.listdir_attr(remote_path):
            if self._isdir_attr(item):
                dirs.append(item.filename)
            else:
                files.append(item.filename)
        return dirs, files

    def size(self, name):
        remote_path = self._remote_path(name)
        return self.sftp.stat(remote_path).st_size

    def accessed_time(self, name):
        remote_path = self._remote_path(name)
        utime = self.sftp.stat(remote_path).st_atime
        return datetime.fromtimestamp(utime)

    def modified_time(self, name):
        remote_path = self._remote_path(name)
        utime = self.sftp.stat(remote_path).st_mtime
        return datetime.fromtimestamp(utime)

    def url(self, name):
        if self._base_url is None:
            raise ValueError("This file is not accessible via a URL.")
        return urlparse.urljoin(self._base_url, name).replace('\\', '/')


class SFTPStorageFile(File):
    def __init__(self, name, storage, mode, pipelined=False):
        self.name = name
        self.mode = mode
        self.file = None
        self._storage = storage
        self._pipelined = pipelined

    @property
    def size(self):
        if not hasattr(self, '_size'):
            self._size = self._storage.size(self.name)
        return self._size

    @property
    def pipelined(self):
        return self._pipelined

    @pipelined.setter
    def pipelined(self, value):
        self._pipelined = value
        if self.file:
            self.file.set_pipelined(self._pipelined)

    def read(self, num_bytes=None):
        if not self.file:
            self.open(mode=self.mode or 'rb')
        if not self.file.readable():
            raise AttributeError(f"File was opened for write-only access.")

        return self.file.read(num_bytes)

    def write(self, content):
        if not self.file:
            self.open(mode=self.mode or 'wb')
        if not self.file.writeable():
            raise AttributeError(f"File was opened for read-only access.")

        self.file.write(content)

    def open(self, mode=None):
        # From Django 3.0 docs: 
        # "When reopening a file, mode will override whatever mode 
        # the file was originally opened with; None means to reopen 
        # with the original mode."
        if not self.closed:
            if mode != self.mode:
                self.close()
            elif mode is None:
                self.close()
                mode = self.mode
            else:
                self.seek(0)
                return

        self.file = self._storage.sftp.open(
            self._storage._remote_path(self.name), 
            mode or self.mode,
            bufsize=self._storage.buffer_size)
        
        self.file.set_pipelined(self._pipelined)

    def close(self):
        if self.file:
            self.file.close()
